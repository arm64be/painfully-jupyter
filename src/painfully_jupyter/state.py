from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json
import subprocess

from painfully_jupyter.config import DEFAULT_RUNTIME_DIR
from painfully_jupyter.errors import StateError


STATE_VERSION = 1
STATE_FILE_NAME = "state.json"


@dataclass(frozen=True)
class RememberedSession:
    session_id: str
    profile: str
    broker_url: str
    remote_cwd: str
    status: str
    updated_at: str

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "RememberedSession":
        return cls(
            session_id=str(data["session_id"]),
            profile=str(data["profile"]),
            broker_url=str(data["broker_url"]),
            remote_cwd=str(data["remote_cwd"]),
            status=str(data["status"]),
            updated_at=str(data["updated_at"]),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "profile": self.profile,
            "broker_url": self.broker_url,
            "remote_cwd": self.remote_cwd,
            "status": self.status,
            "updated_at": self.updated_at,
        }


@dataclass
class RuntimeState:
    remembered_session: RememberedSession | None
    managed_files: dict[str, str]

    @classmethod
    def empty(cls) -> "RuntimeState":
        return cls(remembered_session=None, managed_files={})


class StateStore:
    def __init__(self, project_root: Path | str, runtime_dir_name: str = DEFAULT_RUNTIME_DIR):
        self.project_root = Path(project_root).resolve()
        self.runtime_dir = self.project_root / runtime_dir_name
        self.state_path = self.runtime_dir / STATE_FILE_NAME

    def load(self) -> RuntimeState:
        if not self.state_path.exists():
            return RuntimeState.empty()
        try:
            with self.state_path.open("r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except json.JSONDecodeError as exc:
            raise StateError(f"invalid runtime state JSON: {self.state_path}") from exc
        if raw.get("version") != STATE_VERSION:
            raise StateError(f"unsupported runtime state version in {self.state_path}")
        raw_session = raw.get("remembered_session")
        remembered = RememberedSession.from_json(raw_session) if raw_session else None
        managed_files = raw.get("managed_files", {})
        if not isinstance(managed_files, dict):
            raise StateError("runtime state managed_files must be an object")
        return RuntimeState(
            remembered_session=remembered,
            managed_files={str(path): str(digest) for path, digest in managed_files.items()},
        )

    def save(self, state: RuntimeState) -> None:
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        raw = {
            "version": STATE_VERSION,
            "remembered_session": (
                state.remembered_session.to_json() if state.remembered_session else None
            ),
            "managed_files": dict(sorted(state.managed_files.items())),
        }
        tmp_path = self.state_path.with_suffix(".json.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(raw, handle, indent=2, sort_keys=True)
            handle.write("\n")
        tmp_path.replace(self.state_path)

    def remember_session(
        self,
        *,
        session_id: str,
        profile: str,
        broker_url: str,
        remote_cwd: str,
        status: str = "claimed",
    ) -> RememberedSession:
        state = self.load()
        remembered = RememberedSession(
            session_id=session_id,
            profile=profile,
            broker_url=broker_url,
            remote_cwd=remote_cwd,
            status=status,
            updated_at=_now_iso(),
        )
        state.remembered_session = remembered
        self.save(state)
        return remembered

    def mark_session_status(self, status: str) -> None:
        state = self.load()
        if state.remembered_session is None:
            return
        remembered = state.remembered_session
        state.remembered_session = RememberedSession(
            session_id=remembered.session_id,
            profile=remembered.profile,
            broker_url=remembered.broker_url,
            remote_cwd=remembered.remote_cwd,
            status=status,
            updated_at=_now_iso(),
        )
        self.save(state)

    def update_managed_files(self, managed_files: dict[str, str]) -> None:
        state = self.load()
        state.managed_files = dict(sorted(managed_files.items()))
        self.save(state)

    def runtime_state_ignored(self) -> bool:
        return runtime_state_ignored(self.project_root, self.runtime_dir.name)


def runtime_state_ignored(project_root: Path | str, runtime_dir_name: str = DEFAULT_RUNTIME_DIR) -> bool:
    root = Path(project_root).resolve()
    git_dir = root / ".git"
    if git_dir.exists():
        try:
            result = subprocess.run(
                ["git", "check-ignore", "-q", f"{runtime_dir_name}/"],
                cwd=root,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError:
            result = None
        if result is not None and result.returncode == 0:
            return True

    gitignore = root / ".gitignore"
    if not gitignore.exists():
        return False
    marker = runtime_dir_name.rstrip("/")
    for raw_line in gitignore.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("!"):
            continue
        normalized = line.strip("/")
        if normalized == marker:
            return True
    return False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
