from pathlib import Path
import asyncio
import contextlib

import pytest

from painfully_jupyter.app import PainfullyJupyterApp
from painfully_jupyter.config import BrokerProfile, InstallationConfig
from painfully_jupyter.errors import BrokerProtocolError, RemoteSessionError, StateError
from painfully_jupyter.fake_broker import FakeBroker
from painfully_jupyter.protocol import BrokerClient
from painfully_jupyter.remote_helper import RemoteHelper


def test_claim_token_can_be_used_once(tmp_path: Path) -> None:
    asyncio.run(_claim_token_can_be_used_once(tmp_path))


async def _claim_token_can_be_used_once(tmp_path: Path) -> None:
    remote_root = tmp_path / "remote"
    remote_root.mkdir()
    async with FakeBroker() as broker:
        assert broker.url is not None
        helper = RemoteHelper(broker_url=broker.url, cwd=remote_root)
        token = await helper.start()
        helper_task = asyncio.create_task(helper.run_until_stopped())
        try:
            client = await BrokerClient.claim(
                broker_url=broker.url,
                claim_token=token,
                profile="kaggle",
                project_id="project",
            )
            with pytest.raises(BrokerProtocolError, match="invalid or used"):
                await BrokerClient.claim(
                    broker_url=broker.url,
                    claim_token=token,
                    profile="kaggle",
                    project_id="project",
                )
            await client.disconnect("detach")
        finally:
            await helper.close()
            helper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await helper_task


def test_project_requires_explicit_replace_for_existing_claimed_session(tmp_path: Path) -> None:
    asyncio.run(_project_requires_explicit_replace_for_existing_claimed_session(tmp_path))


async def _project_requires_explicit_replace_for_existing_claimed_session(tmp_path: Path) -> None:
    local_root = tmp_path / "local"
    remote_one = tmp_path / "remote-one"
    remote_two = tmp_path / "remote-two"
    local_root.mkdir()
    remote_one.mkdir()
    remote_two.mkdir()
    (local_root / ".gitignore").write_text(".painfully-jupyter/\n", encoding="utf-8")
    async with FakeBroker() as broker:
        assert broker.url is not None
        install_config = InstallationConfig(
            broker_profiles={
                "kaggle": BrokerProfile(
                    name="kaggle",
                    label="Kaggle",
                    url=broker.url,
                )
            },
            default_profile="kaggle",
        )
        helper_one = RemoteHelper(broker_url=broker.url, cwd=remote_one)
        helper_two = RemoteHelper(broker_url=broker.url, cwd=remote_two)
        token_one = await helper_one.start()
        token_two = await helper_two.start()
        task_one = asyncio.create_task(helper_one.run_until_stopped())
        task_two = asyncio.create_task(helper_two.run_until_stopped())
        app = PainfullyJupyterApp(project_root=local_root, installation_config=install_config)
        try:
            await app.claim_remote(token=token_one)

            with pytest.raises(StateError, match="already claimed"):
                await app.claim_remote(token=token_two)

            with pytest.raises(RemoteSessionError, match="no live remote session"):
                restarted_app = PainfullyJupyterApp(
                    project_root=local_root,
                    installation_config=install_config,
                )
                await restarted_app.run_command(command="echo should-not-run")

            replaced = await app.claim_remote(token=token_two, replace=True)
            assert replaced["remote_cwd"] == str(remote_two)
        finally:
            await app.disconnect(mode="detach")
            await helper_one.close()
            await helper_two.close()
            for task in (task_one, task_two):
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
