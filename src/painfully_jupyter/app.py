from __future__ import annotations

from pathlib import Path
from typing import Any
import hashlib

from painfully_jupyter.config import (
    InstallationConfig,
    ProjectConfig,
    load_installation_config,
    load_project_config,
)
from painfully_jupyter.errors import ConfigError, RemoteSessionError, StateError
from painfully_jupyter.protocol import BrokerClient
from painfully_jupyter.state import StateStore
from painfully_jupyter.sync import (
    build_upload_plan,
    decode_fetch_content,
    validate_fetch_destination,
)


class PainfullyJupyterApp:
    def __init__(
        self,
        *,
        project_root: Path | str,
        installation_config_path: Path | str | None = None,
        installation_config: InstallationConfig | None = None,
    ):
        self.project_root = Path(project_root).resolve()
        self._installation_config_error: ConfigError | None = None
        if installation_config is not None:
            self.installation_config: InstallationConfig | None = installation_config
        else:
            try:
                self.installation_config = load_installation_config(installation_config_path)
            except ConfigError as exc:
                self.installation_config = None
                self._installation_config_error = exc
        self.project_config: ProjectConfig = load_project_config(self.project_root)
        self.state_store = StateStore(self.project_root)
        self._client: BrokerClient | None = None
        self._active_commands: set[str] = set()

    async def status(self) -> dict[str, Any]:
        state = self.state_store.load()
        live = self._client.claimed if self._client is not None else None
        return {
            "project_root": str(self.project_root),
            "project_config": str(self.project_config.source_path) if self.project_config.source_path else None,
            "installation_config": (
                str(self.installation_config.source_path)
                if self.installation_config and self.installation_config.source_path
                else None
            ),
            "installation_config_error": (
                str(self._installation_config_error) if self._installation_config_error else None
            ),
            "broker_profiles": [
                {
                    "name": profile.name,
                    "label": profile.label,
                    "url": profile.url,
                    "default": profile.name == self.installation_config.default_profile,
                }
                for profile in (
                    self.installation_config.broker_profiles.values()
                    if self.installation_config
                    else ()
                )
            ],
            "runtime_state_dir": str(self.state_store.runtime_dir),
            "runtime_state_ignored": self.state_store.runtime_state_ignored(),
            "remembered_session": (
                state.remembered_session.to_json() if state.remembered_session else None
            ),
            "live_session": (
                {
                    "session_id": live.session_id,
                    "profile": live.profile,
                    "broker_url": live.broker_url,
                    "remote_cwd": live.remote_cwd,
                }
                if live
                else None
            ),
            "active_command_sessions": sorted(self._active_commands),
        }

    async def claim_remote(
        self,
        *,
        token: str,
        profile: str | None = None,
        replace: bool = False,
    ) -> dict[str, Any]:
        installation_config = self._require_installation_config()
        state = self.state_store.load()
        if state.remembered_session and state.remembered_session.status == "claimed":
            if not replace:
                raise StateError(
                    "a remote session is already claimed for this project; "
                    "disconnect it or pass replace=true"
                )
            await self.disconnect(mode="detach")

        selected = installation_config.resolve_profile(profile)
        client = await BrokerClient.claim(
            broker_url=selected.url,
            claim_token=token,
            profile=selected.name,
            project_id=self._project_id(),
        )
        self._client = client
        self.state_store.remember_session(
            session_id=client.claimed.session_id,
            profile=selected.name,
            broker_url=selected.url,
            remote_cwd=client.claimed.remote_cwd,
            status="claimed",
        )
        return {
            "session_id": client.claimed.session_id,
            "profile": selected.name,
            "label": selected.label,
            "remote_cwd": client.claimed.remote_cwd,
            "broker_url": selected.url,
        }

    async def sync_upload(self) -> dict[str, Any]:
        client = self._require_live_client()
        state = self.state_store.load()
        plan = build_upload_plan(
            self.project_root,
            self.project_config,
            state.managed_files,
        )
        result = await client.request("sync.apply", plan.to_remote_payload())
        self.state_store.update_managed_files(plan.desired_manifest)
        warnings = []
        if self._active_commands:
            warnings.append(
                "upload completed while command sessions were active; remote reads may race file changes"
            )
        return {
            "written": result.get("written", len(plan.writes)),
            "deleted": result.get("deleted", len(plan.deletes)),
            "managed_files": len(plan.desired_manifest),
            "warnings": warnings,
        }

    async def run_command(
        self,
        *,
        command: str,
        mode: str = "foreground",
        timeout_seconds: float | None = None,
        cwd: str | None = None,
    ) -> dict[str, Any]:
        if mode not in {"foreground", "background"}:
            raise RemoteSessionError("command mode must be foreground or background")
        client = self._require_live_client()
        effective_timeout = timeout_seconds
        if effective_timeout is None:
            effective_timeout = self.project_config.commands.timeout_seconds
        result = await client.request(
            "command.start",
            {
                "command": command,
                "wait": mode == "foreground",
                "timeout_seconds": effective_timeout,
                "cwd": cwd,
            },
            timeout=None if effective_timeout is None else effective_timeout + 5,
        )
        handle = result.get("handle")
        if isinstance(handle, str) and result.get("running", False):
            self._active_commands.add(handle)
        return result

    async def read_command_session(self, *, handle: str, cursor: int = 0) -> dict[str, Any]:
        client = self._require_live_client()
        result = await client.request("command.read", {"handle": handle, "cursor": cursor})
        if result.get("running") is False:
            self._active_commands.discard(handle)
        return result

    async def write_command_stdin(
        self,
        *,
        handle: str,
        data: str,
        close: bool = False,
    ) -> dict[str, Any]:
        client = self._require_live_client()
        return await client.request(
            "command.stdin",
            {
                "handle": handle,
                "data": data,
                "close": close,
            },
        )

    async def fetch_file(
        self,
        *,
        remote_path: str,
        local_path: str,
        overwrite: bool = False,
        allow_ignored: bool = False,
    ) -> dict[str, Any]:
        client = self._require_live_client()
        destination = validate_fetch_destination(
            self.project_root,
            self.project_config,
            local_path,
            overwrite=overwrite,
            allow_ignored=allow_ignored,
        )
        result = await client.request("file.fetch", {"path": remote_path})
        content = result.get("content_b64")
        if not isinstance(content, str):
            raise RemoteSessionError("remote fetch response omitted content")
        data = decode_fetch_content(content)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        return {
            "remote_path": remote_path,
            "local_path": str(destination),
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }

    async def disconnect(self, *, mode: str = "detach") -> dict[str, Any]:
        if mode not in {"detach", "terminate"}:
            raise RemoteSessionError("disconnect mode must be detach or terminate")
        if self._client is not None:
            await self._client.disconnect(mode)
            self._client = None
        self._active_commands.clear()
        self.state_store.mark_session_status("terminated" if mode == "terminate" else "detached")
        return {"mode": mode}

    def _require_live_client(self) -> BrokerClient:
        if self._client is None:
            raise RemoteSessionError("no live remote session; claim a remote helper first")
        return self._client

    def _require_installation_config(self) -> InstallationConfig:
        if self.installation_config is None:
            raise ConfigError(
                "Painfully Jupyter installation config is missing or invalid: "
                f"{self._installation_config_error}"
            )
        return self.installation_config

    def _project_id(self) -> str:
        return hashlib.sha256(str(self.project_root).encode("utf-8")).hexdigest()
