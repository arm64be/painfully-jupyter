# Painfully Jupyter

Painfully Jupyter lets a local Codex session control one active remote notebook through a user-hosted Broker. The Remote Helper runs inside the notebook environment, prints a one-time Claim Token, and the local MCP server claims that token to upload allowlisted files, run commands, stream output, write stdin, and fetch artifacts.

V1 is designed for Kaggle-style cloud notebooks, but the live protocol is provider-neutral after the helper connects.

## Install The Codex Skill

This repo includes a Codex plugin marketplace with a Painfully Jupyter setup and operations skill.

Add the marketplace:

```bash
codex plugin marketplace add arm64be/painfully-jupyter --ref main --sparse .agents/plugins --sparse plugins/painfully-jupyter
```

Install the plugin:

```bash
codex plugin add painfully-jupyter --marketplace painfully-jupyter
```

Restart Codex or start a new thread. Then ask Codex:

```text
Use the Painfully Jupyter remote notebook skill to set up this project for a remote notebook session.
```

The skill will guide the agent through MCP installation, broker profile setup, project sync policy, remote setup, token claim, command execution, artifact fetch, and disconnect behavior.

## Install The MCP Server

For local development from a clone:

```bash
git clone https://github.com/arm64be/painfully-jupyter.git
cd painfully-jupyter
codex mcp add painfully-jupyter -- uv --directory "$PWD" run painfully-jupyter-mcp
```

Restart Codex or start a new thread, then confirm the `painfully-jupyter` MCP tools are visible.

## Configure A Broker Profile

Broker settings are local installation settings, not project settings. Create:

```text
~/.config/painfully-jupyter/config.toml
```

Example:

```toml
default_profile = "kaggle"

[brokers.kaggle]
label = "Kaggle"
url = "wss://BROKER_HOST/PATH/"
```

Use a Broker you host or trust. If the Broker serves setup and WebSocket traffic from the same endpoint, the local config normally uses `wss://...` while the notebook setup command uses the corresponding `https://...` URL.

## Configure Project Sync

Project config is optional and lives in:

```text
painfully-jupyter.toml
```

It contains sync policy and command defaults only. Do not put broker URLs, provider names, auth tokens, or secrets here.

Example:

```toml
[sync]
allowlist = ["src", "notebooks", "requirements.txt"]
ignore = [".venv/", "__pycache__/", "*.pyc"]
respect_gitignore = true

[commands]
timeout_seconds = 60
```

Upload is explicit: the agent must call `sync_upload`. Remote-to-local transfer is explicit fetch only.

## Remote Notebook Flow

In the remote notebook working directory, run the setup command provided by your trusted Broker. It usually looks like:

```bash
curl -fsSL https://BROKER_HOST/PATH/ | bash
```

The helper prints:

```text
Claim Token: ...
```

Give that token to Codex and ask it to claim the session with Painfully Jupyter. Once claimed, Codex can:

- run foreground or background commands in the remote cwd
- poll command output and final exit status
- write stdin to live command sessions
- upload allowlisted local files
- fetch explicit remote artifacts
- detach or terminate the session

## Safety Model

- The Broker is a trust boundary. Use one you host or deliberately trust.
- A Claim Token is single-use.
- V1 supports one active Remote Session per local project.
- Arbitrary remote shell commands are allowed after pairing.
- Runtime state is non-secret but should stay ignored.
- Fetch overwrites and ignored-path writes require explicit flags.

## Development

Run tests:

```bash
uv run --extra test pytest
```

Run a local fake Broker:

```bash
uv run painfully-jupyter-fake-broker --host 127.0.0.1 --port 8765
```

Run a Remote Helper against it:

```bash
uv run painfully-jupyter-helper ws://127.0.0.1:8765/
```
