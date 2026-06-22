from pathlib import Path
import asyncio

import pytest

from painfully_jupyter.app import PainfullyJupyterApp
from painfully_jupyter.config import (
    ConfigError,
    load_installation_config,
    load_project_config,
)


def test_installation_config_resolves_default_and_override(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
default_profile = "kaggle"

[brokers.kaggle]
label = "Kaggle"
url = "ws://127.0.0.1:8765"

[brokers.lab]
label = "Lab"
url = "ws://127.0.0.1:9999"
setup_url = "https://example.invalid/setup.sh"
""".strip(),
        encoding="utf-8",
    )

    config = load_installation_config(config_path)

    assert config.resolve_profile().name == "kaggle"
    assert config.resolve_profile("lab").setup_url == "https://example.invalid/setup.sh"


def test_project_config_parses_only_sync_policy_and_command_defaults(tmp_path: Path) -> None:
    (tmp_path / "painfully-jupyter.toml").write_text(
        """
[sync]
allowlist = ["src", "notebook.ipynb"]
ignore = ["*.tmp", "__pycache__/"]
respect_gitignore = true

[commands]
shell = "/bin/sh"
timeout_seconds = 3.5
""".strip(),
        encoding="utf-8",
    )

    config = load_project_config(tmp_path)

    assert config.sync.allowlist == ("src", "notebook.ipynb")
    assert config.sync.ignore == ("*.tmp", "__pycache__/")
    assert config.sync.respect_gitignore is True
    assert config.commands.shell == "/bin/sh"
    assert config.commands.timeout_seconds == 3.5


@pytest.mark.parametrize(
    "body",
    [
        """
[broker]
url = "ws://example.invalid"
""",
        """
[sync]
token = "secret"
""",
        """
[commands.provider]
name = "kaggle"
""",
    ],
)
def test_project_config_rejects_broker_provider_and_auth_settings(
    tmp_path: Path,
    body: str,
) -> None:
    (tmp_path / "painfully-jupyter.toml").write_text(body.strip(), encoding="utf-8")

    with pytest.raises(ConfigError):
        load_project_config(tmp_path)


def test_mcp_app_status_reports_missing_installation_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing_config = tmp_path / "missing-config.toml"
    monkeypatch.setenv("PAINFULLY_JUPYTER_CONFIG", str(missing_config))

    app = PainfullyJupyterApp(project_root=tmp_path)
    status = asyncio.run(app.status())

    assert status["broker_profiles"] == []
    assert "installation config not found" in status["installation_config_error"]
