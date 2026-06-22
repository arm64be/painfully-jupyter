from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import argparse
import asyncio
import base64
import contextlib
import hashlib
import json
import os
import secrets
import signal

from websockets import connect
from websockets.exceptions import ConnectionClosed

from painfully_jupyter.errors import BrokerProtocolError
from painfully_jupyter.protocol import PROTOCOL_VERSION


@dataclass
class CommandSession:
    handle: str
    process: asyncio.subprocess.Process
    events: list[dict[str, Any]] = field(default_factory=list)
    exit_code: int | None = None
    reader_tasks: list[asyncio.Task[None]] = field(default_factory=list)

    @property
    def running(self) -> bool:
        return self.exit_code is None


class RemoteHelper:
    def __init__(self, *, broker_url: str, cwd: Path | str):
        self.broker_url = broker_url
        self.cwd = Path(cwd).resolve()
        self.claim_token: str | None = None
        self._ws: Any | None = None
        self._stop = asyncio.Event()
        self._commands: dict[str, CommandSession] = {}

    async def start(self) -> str:
        try:
            self._ws = await connect(self.broker_url)
        except OSError as exc:
            raise BrokerProtocolError(
                f"remote helper could not reach broker {self.broker_url}: {exc}"
            ) from exc
        await self._send(
            {
                "type": "remote.register",
                "protocol": PROTOCOL_VERSION,
                "remote_cwd": str(self.cwd),
            }
        )
        message = await self._recv()
        if message.get("type") == "error":
            raise BrokerProtocolError(str(message.get("message", "broker rejected remote helper")))
        if message.get("type") != "remote.registered":
            raise BrokerProtocolError(f"unexpected broker registration message: {message!r}")
        token = message.get("claim_token")
        if not isinstance(token, str) or not token:
            raise BrokerProtocolError("broker registration did not return a claim token")
        self.claim_token = token
        return token

    async def run_until_stopped(self) -> None:
        if self._ws is None:
            await self.start()
        assert self._ws is not None
        try:
            async for raw in self._ws:
                message = _loads_json(raw)
                if message.get("type") != "session.message":
                    continue
                payload = message.get("payload")
                if isinstance(payload, dict) and payload.get("kind") == "request":
                    asyncio.create_task(self._handle_request(message.get("session_id"), payload))
        except ConnectionClosed:
            return
        finally:
            self._stop.set()

    async def close(self) -> None:
        for command in list(self._commands.values()):
            if command.running:
                terminate_process(command.process)
        if self._ws is not None:
            await self._ws.close()
        self._stop.set()

    async def _handle_request(self, session_id: object, payload: dict[str, Any]) -> None:
        request_id = payload.get("id")
        op = payload.get("op")
        params = payload.get("params", {})
        if not isinstance(request_id, str) or not isinstance(op, str) or not isinstance(params, dict):
            return
        try:
            result = await self._dispatch(op, params)
            response = {
                "kind": "response",
                "id": request_id,
                "ok": True,
                "result": result,
            }
        except Exception as exc:
            response = {
                "kind": "response",
                "id": request_id,
                "ok": False,
                "error": str(exc),
            }
        await self._send(
            {
                "type": "session.message",
                "session_id": session_id,
                "payload": response,
            }
        )

    async def _dispatch(self, op: str, params: dict[str, Any]) -> dict[str, Any]:
        if op == "sync.apply":
            return await self._sync_apply(params)
        if op == "command.start":
            return await self._command_start(params)
        if op == "command.read":
            return self._command_read(params)
        if op == "command.stdin":
            return await self._command_stdin(params)
        if op == "file.fetch":
            return self._file_fetch(params)
        if op == "disconnect":
            return await self._disconnect(params)
        raise ValueError(f"unsupported remote operation: {op}")

    async def _sync_apply(self, params: dict[str, Any]) -> dict[str, Any]:
        writes = params.get("writes", [])
        deletes = params.get("deletes", [])
        if not isinstance(writes, list) or not isinstance(deletes, list):
            raise ValueError("sync.apply expects writes and deletes arrays")
        deleted = 0
        for remote_path in deletes:
            if not isinstance(remote_path, str):
                raise ValueError("delete paths must be strings")
            path = self._resolve(remote_path)
            if path.exists() and path.is_file():
                path.unlink()
                deleted += 1
        written = 0
        for item in writes:
            if not isinstance(item, dict):
                raise ValueError("write entries must be objects")
            remote_path = item.get("path")
            content_b64 = item.get("content_b64")
            if not isinstance(remote_path, str) or not isinstance(content_b64, str):
                raise ValueError("write entries must contain path and content_b64")
            data = base64.b64decode(content_b64.encode("ascii"), validate=True)
            expected_sha256 = item.get("sha256")
            if isinstance(expected_sha256, str) and hashlib.sha256(data).hexdigest() != expected_sha256:
                raise ValueError(f"sync payload checksum mismatch for {remote_path}")
            path = self._resolve(remote_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            mode = item.get("mode")
            if isinstance(mode, int):
                with contextlib.suppress(OSError):
                    path.chmod(mode)
            written += 1
        return {"written": written, "deleted": deleted}

    async def _command_start(self, params: dict[str, Any]) -> dict[str, Any]:
        command = params.get("command")
        if not isinstance(command, str) or not command:
            raise ValueError("command.start requires a command string")
        wait = bool(params.get("wait", False))
        timeout = params.get("timeout_seconds")
        if timeout is not None:
            if not isinstance(timeout, int | float) or timeout < 0:
                raise ValueError("timeout_seconds must be a non-negative number")
            timeout = float(timeout)
        cwd_param = params.get("cwd")
        cwd = self.cwd if cwd_param in (None, "") else self._resolve(str(cwd_param))
        process = await asyncio.create_subprocess_shell(
            command,
            cwd=cwd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        handle = f"cmd-{secrets.token_urlsafe(10)}"
        session = CommandSession(handle=handle, process=process)
        self._commands[handle] = session
        assert process.stdout is not None
        assert process.stderr is not None
        session.reader_tasks = [
            asyncio.create_task(self._read_stream(session, "stdout", process.stdout)),
            asyncio.create_task(self._read_stream(session, "stderr", process.stderr)),
            asyncio.create_task(self._wait_for_process(session)),
        ]
        if wait:
            try:
                await asyncio.wait_for(self._wait_until_done(session), timeout=timeout)
            except asyncio.TimeoutError:
                return self._command_snapshot(session, cursor=0)
        return self._command_snapshot(session, cursor=0)

    def _command_read(self, params: dict[str, Any]) -> dict[str, Any]:
        session = self._get_command(params)
        cursor = params.get("cursor", 0)
        if not isinstance(cursor, int) or cursor < 0:
            raise ValueError("cursor must be a non-negative integer")
        return self._command_snapshot(session, cursor=cursor)

    async def _command_stdin(self, params: dict[str, Any]) -> dict[str, Any]:
        session = self._get_command(params)
        if not session.running:
            raise ValueError("command session has already exited")
        data = params.get("data", "")
        if not isinstance(data, str):
            raise ValueError("stdin data must be a string")
        close = bool(params.get("close", False))
        assert session.process.stdin is not None
        if data:
            session.process.stdin.write(data.encode("utf-8"))
            await session.process.stdin.drain()
        if close:
            session.process.stdin.close()
        return {"written": len(data), "closed": close}

    def _file_fetch(self, params: dict[str, Any]) -> dict[str, Any]:
        remote_path = params.get("path")
        if not isinstance(remote_path, str) or not remote_path:
            raise ValueError("file.fetch requires a path")
        path = self._resolve(remote_path)
        if not path.is_file():
            raise ValueError(f"remote file does not exist: {remote_path}")
        data = path.read_bytes()
        return {
            "path": remote_path,
            "content_b64": base64.b64encode(data).decode("ascii"),
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }

    async def _disconnect(self, params: dict[str, Any]) -> dict[str, Any]:
        mode = params.get("mode", "detach")
        if mode not in {"detach", "terminate"}:
            raise ValueError("disconnect mode must be detach or terminate")
        if mode == "terminate":
            for session in list(self._commands.values()):
                if session.running:
                    terminate_process(session.process)
            await self.close()
        return {"mode": mode}

    async def _read_stream(
        self,
        session: CommandSession,
        stream: str,
        reader: asyncio.StreamReader,
    ) -> None:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            session.events.append(
                {
                    "stream": stream,
                    "text": data.decode("utf-8", errors="replace"),
                }
            )

    async def _wait_for_process(self, session: CommandSession) -> None:
        session.exit_code = await session.process.wait()

    async def _wait_until_done(self, session: CommandSession) -> None:
        while session.exit_code is None:
            await asyncio.sleep(0.01)
        await asyncio.gather(*session.reader_tasks, return_exceptions=True)

    def _command_snapshot(self, session: CommandSession, *, cursor: int) -> dict[str, Any]:
        events = session.events[cursor:]
        return {
            "handle": session.handle,
            "running": session.running,
            "exit_code": session.exit_code,
            "events": events,
            "next_cursor": cursor + len(events),
        }

    def _get_command(self, params: dict[str, Any]) -> CommandSession:
        handle = params.get("handle")
        if not isinstance(handle, str):
            raise ValueError("command handle is required")
        try:
            return self._commands[handle]
        except KeyError as exc:
            raise ValueError(f"unknown command handle: {handle}") from exc

    def _resolve(self, remote_path: str) -> Path:
        path = Path(remote_path)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"remote path must be relative to remote cwd: {remote_path!r}")
        resolved = (self.cwd / path).resolve()
        try:
            resolved.relative_to(self.cwd)
        except ValueError as exc:
            raise ValueError(f"remote path escapes remote cwd: {remote_path!r}") from exc
        return resolved

    async def _send(self, message: dict[str, Any]) -> None:
        if self._ws is None:
            raise BrokerProtocolError("remote helper is not connected")
        await self._ws.send(json.dumps(message, separators=(",", ":")))

    async def _recv(self) -> dict[str, Any]:
        if self._ws is None:
            raise BrokerProtocolError("remote helper is not connected")
        raw = await self._ws.recv()
        return _loads_json(raw)


def terminate_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        if os.name == "posix":
            process.send_signal(signal.SIGTERM)
        else:
            process.terminate()


def _loads_json(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    message = json.loads(raw)
    if not isinstance(message, dict):
        raise ValueError("remote helper messages must be JSON objects")
    return message


async def _amain() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("broker_url")
    parser.add_argument("--cwd", default=".")
    args = parser.parse_args()
    helper = RemoteHelper(broker_url=args.broker_url, cwd=args.cwd)
    token = await helper.start()
    print(f"Claim Token: {token}", flush=True)
    await helper.run_until_stopped()


def main() -> None:
    asyncio.run(_amain())
