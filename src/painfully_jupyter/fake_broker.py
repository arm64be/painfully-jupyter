from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import argparse
import asyncio
import contextlib
import json
import secrets

from websockets import serve
from websockets.exceptions import ConnectionClosed

from painfully_jupyter.protocol import PROTOCOL_VERSION


@dataclass
class _RemoteRegistration:
    websocket: Any
    remote_cwd: str
    claimed: bool = False


@dataclass
class _Session:
    session_id: str
    session_key: str
    remote: Any
    local: Any


class FakeBroker:
    """Small in-process broker implementing the V1 pairing and relay protocol."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.port = port
        self.url: str | None = None
        self._server: Any | None = None
        self._registrations: dict[str, _RemoteRegistration] = {}
        self._sessions: dict[str, _Session] = {}

    async def __aenter__(self) -> "FakeBroker":
        await self.start()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.stop()

    async def start(self) -> None:
        self._server = await serve(self._handler, self.host, self.port)
        socket = self._server.sockets[0]
        host, port = socket.getsockname()[:2]
        self.url = f"ws://{host}:{port}"

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handler(self, websocket: Any) -> None:
        try:
            first = await _recv_json(websocket)
            message_type = first.get("type")
            if message_type == "remote.register":
                await self._handle_remote(websocket, first)
                return
            if message_type == "local.claim":
                await self._handle_local(websocket, first)
                return
            await _send_json(websocket, {"type": "error", "message": "expected register or claim"})
        except ConnectionClosed:
            return

    async def _handle_remote(
        self,
        websocket: Any,
        first: dict[str, Any],
    ) -> None:
        if first.get("protocol") != PROTOCOL_VERSION:
            await _send_json(websocket, {"type": "error", "message": "unsupported protocol"})
            return
        remote_cwd = first.get("remote_cwd")
        if not isinstance(remote_cwd, str) or not remote_cwd:
            await _send_json(websocket, {"type": "error", "message": "remote_cwd is required"})
            return
        token = secrets.token_urlsafe(18)
        self._registrations[token] = _RemoteRegistration(websocket=websocket, remote_cwd=remote_cwd)
        await _send_json(
            websocket,
            {
                "type": "remote.registered",
                "claim_token": token,
                "remote_cwd": remote_cwd,
            },
        )
        try:
            async for raw in websocket:
                message = _loads_json(raw)
                if message.get("type") != "session.message":
                    continue
                session_id = message.get("session_id")
                session = self._sessions.get(session_id)
                if session is None:
                    continue
                await _send_json(session.local, message)
        finally:
            self._registrations.pop(token, None)
            await self._drop_sessions_for(websocket)

    async def _handle_local(
        self,
        websocket: Any,
        first: dict[str, Any],
    ) -> None:
        if first.get("protocol") != PROTOCOL_VERSION:
            await _send_json(websocket, {"type": "error", "message": "unsupported protocol"})
            return
        claim_token = first.get("claim_token")
        if not isinstance(claim_token, str):
            await _send_json(websocket, {"type": "error", "message": "claim_token is required"})
            return
        registration = self._registrations.get(claim_token)
        if registration is None or registration.claimed:
            await _send_json(websocket, {"type": "error", "message": "claim token is invalid or used"})
            return
        registration.claimed = True
        self._registrations.pop(claim_token, None)
        session_id = secrets.token_urlsafe(18)
        session_key = secrets.token_urlsafe(24)
        self._sessions[session_id] = _Session(
            session_id=session_id,
            session_key=session_key,
            remote=registration.websocket,
            local=websocket,
        )
        await _send_json(
            websocket,
            {
                "type": "local.claimed",
                "remote_cwd": registration.remote_cwd,
                "session_credentials": {
                    "session_id": session_id,
                    "session_key": session_key,
                },
            },
        )
        await _send_json(
            registration.websocket,
            {
                "type": "local.claimed",
                "session_id": session_id,
            },
        )
        try:
            async for raw in websocket:
                message = _loads_json(raw)
                if message.get("type") != "session.message":
                    continue
                if message.get("session_id") != session_id or message.get("session_key") != session_key:
                    await _send_json(websocket, {"type": "error", "message": "invalid session credentials"})
                    continue
                relay = {
                    "type": "session.message",
                    "session_id": session_id,
                    "payload": message.get("payload", {}),
                }
                await _send_json(registration.websocket, relay)
        finally:
            self._sessions.pop(session_id, None)

    async def _drop_sessions_for(self, websocket: Any) -> None:
        drop = [
            session_id
            for session_id, session in self._sessions.items()
            if session.remote is websocket or session.local is websocket
        ]
        for session_id in drop:
            session = self._sessions.pop(session_id)
            with contextlib.suppress(Exception):
                await session.local.close()
            with contextlib.suppress(Exception):
                await session.remote.close()


async def _send_json(websocket: Any, message: dict[str, Any]) -> None:
    await websocket.send(json.dumps(message, separators=(",", ":")))


async def _recv_json(websocket: Any) -> dict[str, Any]:
    raw = await websocket.recv()
    return _loads_json(raw)


def _loads_json(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    message = json.loads(raw)
    if not isinstance(message, dict):
        raise ValueError("broker messages must be JSON objects")
    return message


async def _amain() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    broker = FakeBroker(args.host, args.port)
    await broker.start()
    print(broker.url, flush=True)
    await asyncio.Future()


def main() -> None:
    asyncio.run(_amain())
