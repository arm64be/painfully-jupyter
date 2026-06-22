from pathlib import Path

import pytest

from painfully_jupyter.config import CommandDefaults, ProjectConfig, SyncPolicy
from painfully_jupyter.errors import SyncError
from painfully_jupyter.sync import build_upload_plan, validate_fetch_destination


def test_upload_plan_respects_ignores_and_deletes_missing_managed_files(tmp_path: Path) -> None:
    (tmp_path / ".gitignore").write_text(
        """
.painfully-jupyter/
*.log
""".strip(),
        encoding="utf-8",
    )
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "keep.py").write_text("print('keep')\n", encoding="utf-8")
    (tmp_path / "src" / "drop.tmp").write_text("ignore\n", encoding="utf-8")
    (tmp_path / "src" / "trace.log").write_text("ignore\n", encoding="utf-8")
    (tmp_path / "src" / "__pycache__").mkdir()
    (tmp_path / "src" / "__pycache__" / "keep.pyc").write_bytes(b"ignore")

    config = ProjectConfig(
        sync=SyncPolicy(
            allowlist=("src",),
            ignore=("*.tmp", "__pycache__/"),
            respect_gitignore=True,
        ),
        commands=CommandDefaults(),
        source_path=None,
    )

    plan = build_upload_plan(
        tmp_path,
        config,
        previous_manifest={"src/deleted.py": "old-digest"},
    )

    assert [write.path for write in plan.writes] == ["src/keep.py"]
    assert plan.deletes == ("src/deleted.py",)
    assert "src/drop.tmp" not in plan.desired_manifest
    assert "src/trace.log" not in plan.desired_manifest
    assert "src/__pycache__/keep.pyc" not in plan.desired_manifest


def test_fetch_destination_requires_explicit_overwrite_and_ignored_write(tmp_path: Path) -> None:
    (tmp_path / ".gitignore").write_text("ignored.out\n", encoding="utf-8")
    (tmp_path / "existing.txt").write_text("already here\n", encoding="utf-8")
    config = ProjectConfig(sync=SyncPolicy(respect_gitignore=True))

    with pytest.raises(SyncError, match="overwrite=true"):
        validate_fetch_destination(
            tmp_path,
            config,
            "existing.txt",
            overwrite=False,
            allow_ignored=False,
        )

    assert validate_fetch_destination(
        tmp_path,
        config,
        "existing.txt",
        overwrite=True,
        allow_ignored=False,
    ) == tmp_path / "existing.txt"

    with pytest.raises(SyncError, match="allow_ignored=true"):
        validate_fetch_destination(
            tmp_path,
            config,
            "ignored.out",
            overwrite=False,
            allow_ignored=False,
        )

    assert validate_fetch_destination(
        tmp_path,
        config,
        "ignored.out",
        overwrite=False,
        allow_ignored=True,
    ) == tmp_path / "ignored.out"
