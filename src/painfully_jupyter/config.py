from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import os
import tomllib

from painfully_jupyter.errors import ConfigError


PROJECT_CONFIG_NAME = "painfully-jupyter.toml"
DEFAULT_RUNTIME_DIR = ".painfully-jupyter"
INSTALLATION_ENV_VAR = "PAINFULLY_JUPYTER_CONFIG"

_PROJECT_TOP_LEVEL_KEYS = {"sync", "commands"}
_PROJECT_FORBIDDEN_KEYS = {
    "auth",
    "broker",
    "brokers",
    "credential",
    "credentials",
    "profile",
    "profiles",
    "provider",
    "providers",
    "secret",
    "session",
    "token",
}


@dataclass(frozen=True)
class BrokerProfile:
    name: str
    label: str
    url: str
    setup_url: str | None = None


@dataclass(frozen=True)
class InstallationConfig:
    broker_profiles: dict[str, BrokerProfile]
    default_profile: str
    source_path: Path | None = None

    def resolve_profile(self, override: str | None = None) -> BrokerProfile:
        name = override or self.default_profile
        try:
            return self.broker_profiles[name]
        except KeyError as exc:
            known = ", ".join(sorted(self.broker_profiles)) or "<none>"
            raise ConfigError(f"unknown broker profile {name!r}; configured profiles: {known}") from exc


@dataclass(frozen=True)
class SyncPolicy:
    allowlist: tuple[str, ...] = ()
    ignore: tuple[str, ...] = ()
    respect_gitignore: bool = True


@dataclass(frozen=True)
class CommandDefaults:
    shell: str | None = None
    timeout_seconds: float | None = None


@dataclass(frozen=True)
class ProjectConfig:
    sync: SyncPolicy = field(default_factory=SyncPolicy)
    commands: CommandDefaults = field(default_factory=CommandDefaults)
    source_path: Path | None = None


def default_installation_config_path() -> Path:
    configured = os.environ.get(INSTALLATION_ENV_VAR)
    if configured:
        return Path(configured).expanduser()
    config_home = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return config_home / "painfully-jupyter" / "config.toml"


def load_installation_config(path: Path | str | None = None) -> InstallationConfig:
    config_path = Path(path).expanduser() if path is not None else default_installation_config_path()
    if not config_path.exists():
        raise ConfigError(f"installation config not found: {config_path}")
    data = _load_toml(config_path)
    default_profile = _require_str(data, "default_profile", context="installation config")
    raw_profiles = data.get("brokers")
    if not isinstance(raw_profiles, dict) or not raw_profiles:
        raise ConfigError("installation config must define at least one [brokers.<name>] profile")

    profiles: dict[str, BrokerProfile] = {}
    for name, raw_profile in raw_profiles.items():
        if not isinstance(raw_profile, dict):
            raise ConfigError(f"broker profile {name!r} must be a table")
        url = _require_str(raw_profile, "url", context=f"broker profile {name!r}")
        label = raw_profile.get("label", name)
        if not isinstance(label, str) or not label.strip():
            raise ConfigError(f"broker profile {name!r} label must be a non-empty string")
        setup_url = raw_profile.get("setup_url")
        if setup_url is not None and not isinstance(setup_url, str):
            raise ConfigError(f"broker profile {name!r} setup_url must be a string")
        profiles[name] = BrokerProfile(name=name, label=label, url=url, setup_url=setup_url)

    config = InstallationConfig(
        broker_profiles=profiles,
        default_profile=default_profile,
        source_path=config_path,
    )
    config.resolve_profile()
    return config


def load_project_config(project_root: Path | str) -> ProjectConfig:
    root = Path(project_root).resolve()
    config_path = root / PROJECT_CONFIG_NAME
    if not config_path.exists():
        return ProjectConfig(source_path=None)
    data = _load_toml(config_path)
    _reject_project_settings(data)

    sync_data = data.get("sync", {})
    commands_data = data.get("commands", {})
    if not isinstance(sync_data, dict):
        raise ConfigError("[sync] must be a table")
    if not isinstance(commands_data, dict):
        raise ConfigError("[commands] must be a table")

    allowlist = _string_tuple(sync_data.get("allowlist", ()), "[sync].allowlist")
    ignore = _string_tuple(sync_data.get("ignore", ()), "[sync].ignore")
    respect_gitignore = sync_data.get("respect_gitignore", True)
    if not isinstance(respect_gitignore, bool):
        raise ConfigError("[sync].respect_gitignore must be a boolean")
    for path in allowlist:
        _validate_relative_project_path(path, "[sync].allowlist")

    shell = commands_data.get("shell")
    if shell is not None and not isinstance(shell, str):
        raise ConfigError("[commands].shell must be a string")
    timeout_seconds = commands_data.get("timeout_seconds")
    if timeout_seconds is not None:
        if not isinstance(timeout_seconds, int | float) or timeout_seconds < 0:
            raise ConfigError("[commands].timeout_seconds must be a non-negative number")
        timeout_seconds = float(timeout_seconds)

    return ProjectConfig(
        sync=SyncPolicy(
            allowlist=allowlist,
            ignore=ignore,
            respect_gitignore=respect_gitignore,
        ),
        commands=CommandDefaults(shell=shell, timeout_seconds=timeout_seconds),
        source_path=config_path,
    )


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a TOML table")
    return data


def _require_str(table: dict[str, Any], key: str, *, context: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{context} must define non-empty string {key!r}")
    return value


def _string_tuple(value: Any, key: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list | tuple):
        raise ConfigError(f"{key} must be an array of strings")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ConfigError(f"{key} must contain only non-empty strings")
        result.append(item)
    return tuple(result)


def _validate_relative_project_path(value: str, key: str) -> None:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise ConfigError(f"{key} entries must be relative paths inside the project: {value!r}")


def _reject_project_settings(data: dict[str, Any]) -> None:
    for key, value in data.items():
        normalized = key.lower().replace("-", "_")
        if normalized not in _PROJECT_TOP_LEVEL_KEYS:
            raise ConfigError(
                f"project config may only contain [sync] and [commands], found [{key}]"
            )
        if isinstance(value, dict):
            _reject_forbidden_nested_settings(value, parent=key)


def _reject_forbidden_nested_settings(table: dict[str, Any], *, parent: str) -> None:
    for key, value in table.items():
        normalized = key.lower().replace("-", "_")
        if normalized in _PROJECT_FORBIDDEN_KEYS:
            raise ConfigError(
                f"project config must not contain broker/provider/auth setting "
                f"{parent}.{key}"
            )
        if isinstance(value, dict):
            _reject_forbidden_nested_settings(value, parent=f"{parent}.{key}")
