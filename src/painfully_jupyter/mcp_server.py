from __future__ import annotations

from pathlib import Path
from typing import Any, Awaitable, Callable
import asyncio
import json
import sys

from painfully_jupyter.app import PainfullyJupyterApp
from painfully_jupyter.errors import PainfullyJupyterError


ToolHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class McpServer:
    def __init__(self, app: PainfullyJupyterApp):
        self.app = app
        self._tools: dict[str, tuple[str, dict[str, Any], ToolHandler]] = {
            "status": (
                "Report project configuration, broker profiles, runtime state, and live session.",
                {"type": "object", "properties": {}, "additionalProperties": False},
                self._status,
            ),
            "claim_remote": (
                "Claim a Remote Helper by one-time Claim Token.",
                {
                    "type": "object",
                    "properties": {
                        "token": {"type": "string"},
                        "profile": {"type": "string"},
                        "replace": {"type": "boolean", "default": False},
                    },
                    "required": ["token"],
                    "additionalProperties": False,
                },
                self._claim_remote,
            ),
            "sync_upload": (
                "Explicitly upload the configured Upload Allowlist into the remote cwd.",
                {"type": "object", "properties": {}, "additionalProperties": False},
                self._sync_upload,
            ),
            "run_command": (
                "Run a command in the remote cwd as foreground or background work.",
                {
                    "type": "object",
                    "properties": {
                        "command": {"type": "string"},
                        "mode": {"type": "string", "enum": ["foreground", "background"], "default": "foreground"},
                        "timeout_seconds": {"type": "number"},
                        "cwd": {"type": "string"},
                    },
                    "required": ["command"],
                    "additionalProperties": False,
                },
                self._run_command,
            ),
            "read_command_session": (
                "Poll output and exit status for a remote command session.",
                {
                    "type": "object",
                    "properties": {
                        "handle": {"type": "string"},
                        "cursor": {"type": "integer", "minimum": 0, "default": 0},
                    },
                    "required": ["handle"],
                    "additionalProperties": False,
                },
                self._read_command_session,
            ),
            "write_command_stdin": (
                "Write stdin to a live remote command session.",
                {
                    "type": "object",
                    "properties": {
                        "handle": {"type": "string"},
                        "data": {"type": "string"},
                        "close": {"type": "boolean", "default": False},
                    },
                    "required": ["handle", "data"],
                    "additionalProperties": False,
                },
                self._write_command_stdin,
            ),
            "fetch_file": (
                "Explicitly fetch one remote file to a local project path.",
                {
                    "type": "object",
                    "properties": {
                        "remote_path": {"type": "string"},
                        "local_path": {"type": "string"},
                        "overwrite": {"type": "boolean", "default": False},
                        "allow_ignored": {"type": "boolean", "default": False},
                    },
                    "required": ["remote_path", "local_path"],
                    "additionalProperties": False,
                },
                self._fetch_file,
            ),
            "disconnect": (
                "Detach from the Remote Session by default, or explicitly terminate it.",
                {
                    "type": "object",
                    "properties": {
                        "mode": {"type": "string", "enum": ["detach", "terminate"], "default": "detach"},
                    },
                    "additionalProperties": False,
                },
                self._disconnect,
            ),
        }

    async def run_stdio(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            try:
                message = json.loads(line)
                response = await self._handle_message(message)
            except Exception as exc:
                response = self._jsonrpc_error(None, -32700, str(exc))
            if response is not None:
                print(json.dumps(response, separators=(",", ":")), flush=True)

    async def _handle_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        request_id = message.get("id")
        method = message.get("method")
        params = message.get("params", {})
        if method == "notifications/initialized":
            return None
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": params.get("protocolVersion", "2024-11-05")
                    if isinstance(params, dict)
                    else "2024-11-05",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "painfully-jupyter", "version": "0.1.0"},
                },
            }
        if method == "ping":
            return {"jsonrpc": "2.0", "id": request_id, "result": {}}
        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {
                            "name": name,
                            "description": description,
                            "inputSchema": schema,
                        }
                        for name, (description, schema, _handler) in self._tools.items()
                    ]
                },
            }
        if method == "tools/call":
            if not isinstance(params, dict):
                return self._jsonrpc_error(request_id, -32602, "tools/call params must be an object")
            name = params.get("name")
            arguments = params.get("arguments", {})
            if not isinstance(name, str) or name not in self._tools:
                return self._jsonrpc_error(request_id, -32602, f"unknown tool: {name!r}")
            if not isinstance(arguments, dict):
                return self._jsonrpc_error(request_id, -32602, "tool arguments must be an object")
            handler = self._tools[name][2]
            try:
                result = await handler(arguments)
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(result, indent=2, sort_keys=True),
                            }
                        ],
                        "isError": False,
                    },
                }
            except PainfullyJupyterError as exc:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": str(exc)}],
                        "isError": True,
                    },
                }
        return self._jsonrpc_error(request_id, -32601, f"method not found: {method!r}")

    async def _status(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return await self.app.status()

    async def _claim_remote(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return await self.app.claim_remote(
            token=str(arguments["token"]),
            profile=arguments.get("profile"),
            replace=bool(arguments.get("replace", False)),
        )

    async def _sync_upload(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return await self.app.sync_upload()

    async def _run_command(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return await self.app.run_command(
            command=str(arguments["command"]),
            mode=str(arguments.get("mode", "foreground")),
            timeout_seconds=arguments.get("timeout_seconds"),
            cwd=arguments.get("cwd"),
        )

    async def _read_command_session(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return await self.app.read_command_session(
            handle=str(arguments["handle"]),
            cursor=int(arguments.get("cursor", 0)),
        )

    async def _write_command_stdin(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return await self.app.write_command_stdin(
            handle=str(arguments["handle"]),
            data=str(arguments["data"]),
            close=bool(arguments.get("close", False)),
        )

    async def _fetch_file(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return await self.app.fetch_file(
            remote_path=str(arguments["remote_path"]),
            local_path=str(arguments["local_path"]),
            overwrite=bool(arguments.get("overwrite", False)),
            allow_ignored=bool(arguments.get("allow_ignored", False)),
        )

    async def _disconnect(self, arguments: dict[str, Any]) -> dict[str, Any]:
        return await self.app.disconnect(mode=str(arguments.get("mode", "detach")))

    def _jsonrpc_error(self, request_id: object, code: int, message: str) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }


async def _amain() -> None:
    app = PainfullyJupyterApp(project_root=Path.cwd())
    await McpServer(app).run_stdio()


def main() -> None:
    asyncio.run(_amain())
