from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import base64
import fnmatch
import hashlib
import os
import stat

from painfully_jupyter.config import ProjectConfig, SyncPolicy
from painfully_jupyter.errors import SyncError


@dataclass(frozen=True)
class FileUpload:
    path: str
    content_b64: str
    mode: int
    sha256: str
    size: int


@dataclass(frozen=True)
class UploadPlan:
    writes: tuple[FileUpload, ...]
    deletes: tuple[str, ...]
    desired_manifest: dict[str, str]

    def to_remote_payload(self) -> dict[str, object]:
        return {
            "writes": [
                {
                    "path": write.path,
                    "content_b64": write.content_b64,
                    "mode": write.mode,
                    "sha256": write.sha256,
                    "size": write.size,
                }
                for write in self.writes
            ],
            "deletes": list(self.deletes),
        }


class IgnoreMatcher:
    def __init__(self, patterns: list[str]):
        self.patterns = [pattern for pattern in patterns if pattern and not pattern.startswith("#")]

    @classmethod
    def for_project(cls, project_root: Path, policy: SyncPolicy) -> "IgnoreMatcher":
        patterns = list(policy.ignore)
        if policy.respect_gitignore:
            gitignore = project_root / ".gitignore"
            if gitignore.exists():
                patterns.extend(gitignore.read_text(encoding="utf-8").splitlines())
        return cls(patterns)

    def matches(self, relative_path: str, *, is_dir: bool = False) -> bool:
        path = relative_path.strip("/")
        ignored = False
        for raw_pattern in self.patterns:
            pattern = raw_pattern.strip()
            if not pattern or pattern.startswith("#"):
                continue
            negate = pattern.startswith("!")
            if negate:
                pattern = pattern[1:].strip()
            if not pattern:
                continue
            if _pattern_matches(pattern, path, is_dir=is_dir):
                ignored = not negate
        return ignored


def build_upload_plan(
    project_root: Path | str,
    config: ProjectConfig,
    previous_manifest: dict[str, str],
) -> UploadPlan:
    root = Path(project_root).resolve()
    matcher = IgnoreMatcher.for_project(root, config.sync)
    desired: dict[str, FileUpload] = {}

    for entry in config.sync.allowlist:
        local_path = _resolve_project_path(root, entry)
        if not local_path.exists():
            continue
        if local_path.is_file():
            rel = _relative_posix(root, local_path)
            if not matcher.matches(rel, is_dir=False):
                desired[rel] = _make_upload(root, local_path)
            continue
        if local_path.is_dir():
            for current_root, dirs, files in os.walk(local_path):
                current = Path(current_root)
                kept_dirs: list[str] = []
                for dirname in dirs:
                    child = current / dirname
                    rel = _relative_posix(root, child)
                    if not matcher.matches(rel, is_dir=True):
                        kept_dirs.append(dirname)
                dirs[:] = kept_dirs
                for filename in files:
                    child = current / filename
                    rel = _relative_posix(root, child)
                    if matcher.matches(rel, is_dir=False):
                        continue
                    desired[rel] = _make_upload(root, child)
            continue
        raise SyncError(f"allowlist path is neither a regular file nor directory: {entry}")

    desired_manifest = {path: upload.sha256 for path, upload in desired.items()}
    deletes = sorted(path for path in previous_manifest if path not in desired_manifest)
    writes = tuple(desired[path] for path in sorted(desired))
    return UploadPlan(writes=writes, deletes=tuple(deletes), desired_manifest=desired_manifest)


def validate_fetch_destination(
    project_root: Path | str,
    config: ProjectConfig,
    local_path: str,
    *,
    overwrite: bool,
    allow_ignored: bool,
) -> Path:
    root = Path(project_root).resolve()
    destination = _resolve_project_path(root, local_path)
    if destination.exists() and not overwrite:
        raise SyncError(f"fetch destination already exists; pass overwrite=true: {local_path}")
    rel = _relative_posix(root, destination)
    matcher = IgnoreMatcher.for_project(root, config.sync)
    if matcher.matches(rel, is_dir=False) and not allow_ignored:
        raise SyncError(
            f"fetch destination is ignored; pass allow_ignored=true to write it: {local_path}"
        )
    return destination


def decode_fetch_content(content_b64: str) -> bytes:
    try:
        return base64.b64decode(content_b64.encode("ascii"), validate=True)
    except Exception as exc:
        raise SyncError("remote returned invalid base64 file content") from exc


def _make_upload(root: Path, local_path: Path) -> FileUpload:
    data = local_path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    mode = stat.S_IMODE(local_path.stat().st_mode)
    return FileUpload(
        path=_relative_posix(root, local_path),
        content_b64=base64.b64encode(data).decode("ascii"),
        mode=mode,
        sha256=digest,
        size=len(data),
    )


def _resolve_project_path(root: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise SyncError(f"path must be relative and stay inside the project: {value!r}")
    resolved = (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise SyncError(f"path escapes project root: {value!r}") from exc
    return resolved


def _relative_posix(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root).as_posix()


def _pattern_matches(pattern: str, path: str, *, is_dir: bool) -> bool:
    original = pattern
    anchored = pattern.startswith("/")
    pattern = pattern.lstrip("/")
    dir_only = pattern.endswith("/")
    pattern = pattern.rstrip("/")
    if not pattern:
        return False
    if dir_only and not is_dir and not path.startswith(pattern + "/"):
        return False

    if "/" in pattern or anchored:
        if fnmatch.fnmatch(path, pattern) or path.startswith(pattern + "/"):
            return True
    else:
        parts = PurePosixPath(path).parts
        if any(fnmatch.fnmatch(part, pattern) for part in parts):
            return True
    if original.endswith("/") and path.startswith(pattern + "/"):
        return True
    return False
