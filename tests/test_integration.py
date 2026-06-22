from pathlib import Path
import asyncio
import contextlib
import sys

import pytest

from painfully_jupyter.app import PainfullyJupyterApp
from painfully_jupyter.config import BrokerProfile, InstallationConfig
from painfully_jupyter.errors import SyncError
from painfully_jupyter.fake_broker import FakeBroker
from painfully_jupyter.remote_helper import RemoteHelper


def test_fake_broker_helper_end_to_end(tmp_path: Path) -> None:
    asyncio.run(_fake_broker_helper_end_to_end(tmp_path))


async def _fake_broker_helper_end_to_end(tmp_path: Path) -> None:
    local_root = tmp_path / "local"
    remote_root = tmp_path / "remote"
    local_root.mkdir()
    remote_root.mkdir()
    (local_root / ".gitignore").write_text(
        """
.painfully-jupyter/
*.ignored
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (local_root / "painfully-jupyter.toml").write_text(
        """
[sync]
allowlist = ["src"]
ignore = ["*.tmp"]
respect_gitignore = true
""".strip(),
        encoding="utf-8",
    )
    (local_root / "src").mkdir()
    (local_root / "src" / "input.txt").write_text("payload\n", encoding="utf-8")
    (local_root / "src" / "skip.tmp").write_text("skip\n", encoding="utf-8")
    (remote_root / "unmanaged.txt").write_text("do not prune\n", encoding="utf-8")

    async with FakeBroker() as broker:
        assert broker.url is not None
        helper = RemoteHelper(broker_url=broker.url, cwd=remote_root)
        token = await helper.start()
        helper_task = asyncio.create_task(helper.run_until_stopped())
        app = PainfullyJupyterApp(
            project_root=local_root,
            installation_config=InstallationConfig(
                broker_profiles={
                    "kaggle": BrokerProfile(
                        name="kaggle",
                        label="Kaggle",
                        url=broker.url,
                    )
                },
                default_profile="kaggle",
            ),
        )
        try:
            claim = await app.claim_remote(token=token)
            assert claim["remote_cwd"] == str(remote_root)

            sync = await app.sync_upload()
            assert sync["written"] == 1
            assert (remote_root / "src" / "input.txt").read_text(encoding="utf-8") == "payload\n"
            assert not (remote_root / "src" / "skip.tmp").exists()
            assert (remote_root / "unmanaged.txt").exists()

            command = (
                f"{sys.executable} -c "
                "\"import pathlib, sys; "
                "line=sys.stdin.readline().strip(); "
                "print(line.upper()); "
                "pathlib.Path('artifact.txt').write_text('artifact:' + line)\""
            )
            started = await app.run_command(command=command, mode="background")
            handle = started["handle"]
            assert started["running"] is True

            raced_sync = await app.sync_upload()
            assert raced_sync["warnings"]

            await app.write_command_stdin(handle=handle, data="hello\n", close=True)
            output = ""
            cursor = 0
            final = None
            for _ in range(100):
                read = await app.read_command_session(handle=handle, cursor=cursor)
                cursor = read["next_cursor"]
                output += "".join(event["text"] for event in read["events"])
                if not read["running"]:
                    final = read
                    break
                await asyncio.sleep(0.05)

            assert final is not None
            assert final["exit_code"] == 0
            assert "HELLO" in output

            fetched = await app.fetch_file(
                remote_path="artifact.txt",
                local_path="fetched/artifact.txt",
            )
            assert fetched["size"] == len("artifact:hello")
            assert (local_root / "fetched" / "artifact.txt").read_text(encoding="utf-8") == "artifact:hello"

            with pytest.raises(SyncError, match="overwrite=true"):
                await app.fetch_file(
                    remote_path="artifact.txt",
                    local_path="fetched/artifact.txt",
                )

            (remote_root / "ignored.ignored").write_text("ignored fetch\n", encoding="utf-8")
            with pytest.raises(SyncError, match="allow_ignored=true"):
                await app.fetch_file(
                    remote_path="ignored.ignored",
                    local_path="ignored.ignored",
                )
            await app.fetch_file(
                remote_path="ignored.ignored",
                local_path="ignored.ignored",
                allow_ignored=True,
            )

            disconnected = await app.disconnect(mode="detach")
            assert disconnected["mode"] == "detach"
        finally:
            await app.disconnect(mode="detach")
            await helper.close()
            helper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await helper_task
