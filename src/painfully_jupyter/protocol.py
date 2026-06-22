from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import asyncio
import json
import secrets

from websockets import connect
from websockets.exceptions import ConnectionClosed

from painfully_jupyter.errors import BrokerProtocolError, RemoteSessionError


PROTOCOL_VERSION = "painfully-jupyter.v1"


@dataclass(frozen=True)
class ClaimedRemote:
    session_id: str
    session_key: str
    remote_cwd: str
    profile: str
    broker_url: str


class BrokerClient:
    def __init__(
        self,
        *,
        websocket: Any,
        claimed: ClaimedRemote,
    ):
        self._ws = websocket
        self.claimed = claimed
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._listener: asyncio.Task[None] | None = None
        self._closed = False

    @classmethod
    async def claim(
        cls,
        *,
        broker_url: str,
        claim_token: str,
        profile: str,
        project_id: str,
    ) -> "BrokerClient":
        try:
            websocket = await connect(broker_url)
        except OSError as exc:
            raise BrokerProtocolError(f"failed to reach broker {broker_url}: {exc}") from exc
        await _send_json(
            websocket,
            {
                "type": "local.claim",
                "protocol": PROTOCOL_VERSION,
                "claim_token": claim_token,
                "project_id": project_id,
            },
        )
        message = await _recv_json(websocket)
        if message.get("type") == "error":
            await websocket.close()
            raise BrokerProtocolError(str(message.get("message", "broker rejected claim")))
        if message.get("type") != "local.claimed":
            await websocket.close()
            raise BrokerProtocolError(f"broker returned unexpected message: {message!r}")
        credentials = message.get("session_credentials")
        if not isinstance(credentials, dict):
            await websocket.close()
            raise BrokerProtocolError("broker claim response omitted session credentials")
        session_id = _require_str(credentials, "session_id")
        session_key = _require_str(credentials, "session_key")
        remote_cwd = _require_str(message, "remote_cwd")
        client = cls(
            websocket=websocket,
            claimed=ClaimedRemote(
                session_id=session_id,
                session_key=session_key,
                remote_cwd=remote_cwd,
                profile=profile,
                broker_url=broker_url,
            ),
        )
        client._listener = asyncio.create_task(client._listen())
        return client

    async def request(
        self,
        op: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        if self._closed:
            raise RemoteSessionError("remote session is closed")
        request_id = secrets.token_urlsafe(12)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[request_id] = future
        await _send_json(
            self._ws,
            {
                "type": "session.message",
                "session_id": self.claimed.session_id,
                "session_key": self.claimed.session_key,
                "payload": {
                    "kind": "request",
                    "id": request_id,
                    "op": op,
                    "params": params or {},
                },
            },
        )
        try:
            if timeout is None:
                return await future
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(request_id, None)

    async def disconnect(self, mode: str = "detach") -> None:
        if self._closed:
            return
        try:
            await self.request("disconnect", {"mode": mode}, timeout=5)
        except Exception:
            pass
        await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._listener is not None:
            self._listener.cancel()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RemoteSessionError("remote session closed"))
        await self._ws.close()

    async def _listen(self) -> None:
        try:
            async for raw in self._ws:
                message = _loads_json(raw)
                if message.get("type") != "session.message":
                    continue
                payload = message.get("payload")
                if not isinstance(payload, dict):
                    continue
                if payload.get("kind") != "response":
                    continue
                request_id = payload.get("id")
                if not isinstance(request_id, str):
                    continue
                future = self._pending.get(request_id)
                if future is None or future.done():
                    continue
                if payload.get("ok") is True:
                    result = payload.get("result", {})
                    future.set_result(result if isinstance(result, dict) else {"result": result})
                else:
                    future.set_exception(RemoteSessionError(str(payload.get("error", "remote error"))))
        except asyncio.CancelledError:
            raise
        except ConnectionClosed as exc:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(RemoteSessionError(f"remote session disconnected: {exc}"))
        finally:
            self._closed = True


async def _send_json(websocket: Any, message: dict[str, Any]) -> None:
    await websocket.send(json.dumps(message, separators=(",", ":")))


async def _recv_json(websocket: Any) -> dict[str, Any]:
    raw = await websocket.recv()
    return _loads_json(raw)


def _loads_json(raw: str | bytes) -> dict[str, Any]:
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        message = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BrokerProtocolError("received invalid JSON over broker connection") from exc
    if not isinstance(message, dict):
        raise BrokerProtocolError("received non-object JSON over broker connection")
    return message


def _require_str(table: dict[str, Any], key: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value:
        raise BrokerProtocolError(f"broker message omitted string {key!r}")
    return value
