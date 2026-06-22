---
name: remote-notebook
description: Set up and operate Painfully Jupyter Remote Sessions through a user-hosted Broker and local MCP tools. Use when the user wants to connect Codex to Kaggle or another cloud notebook, configure broker profiles, run the remote setup command, claim tokens, sync files, run commands, write stdin, fetch artifacts, or troubleshoot Painfully Jupyter.
---

# Painfully Jupyter Remote Notebook

Use this workflow to connect a local Codex session to one remote notebook helper through Painfully Jupyter.

## Terms

- Broker: user-hosted WebSocket relay that pairs local Codex with a notebook helper.
- Remote Helper: process running inside the notebook working directory.
- Claim Token: one-time token printed by the Remote Helper and consumed by local Codex.
- Remote Session: one claimed helper connection for one local project.
- Upload Allowlist: local project paths eligible for explicit upload.

## Setup Checklist

1. Confirm the local MCP server is installed and visible.
   - In Codex, look for `mcp__painfully_jupyter` tools.
   - If missing, install the MCP server from the project README, then restart Codex.
2. Confirm the user has a trusted Broker.
   - Do not invent or hardcode a broker URL.
   - Use the broker WebSocket URL for local config, usually `wss://...`.
   - Use the broker-provided shell setup URL or command for the remote notebook, usually `curl -fsSL https://... | bash`.
3. Ensure local installation config exists at `~/.config/painfully-jupyter/config.toml`:

```toml
default_profile = "kaggle"

[brokers.kaggle]
label = "Kaggle"
url = "wss://BROKER_HOST/PATH/"
```

4. Ensure project sync policy exists only when upload is needed:

```toml
[sync]
allowlist = ["src", "notebooks", "requirements.txt"]
ignore = [".venv/", "__pycache__/", "*.pyc"]
respect_gitignore = true
```

Never put broker URLs, provider credentials, auth tokens, or secrets in `painfully-jupyter.toml`.

## Pair And Operate

1. Ask the user to run the trusted broker setup command in the notebook working directory.
2. Wait for the Remote Helper to print `Claim Token: ...`.
3. Claim the token with `claim_remote(token, profile?)`.
4. Run `status` and confirm:
   - `live_session.remote_cwd` is the expected notebook working directory.
   - `runtime_state_ignored` is true or the user understands runtime state is non-secret but should stay untracked.
5. Upload files only when requested or needed with `sync_upload`.
   - Upload is explicit, not watcher-based.
   - If command sessions are active, report the race warning.
6. Run commands with `run_command`.
   - Default cwd is the remote cwd where setup ran.
   - Use `mode="background"` for long-running work.
   - Use `read_command_session` to poll output and exit status.
   - Use `write_command_stdin` only for live sessions expecting input.
7. Fetch artifacts explicitly with `fetch_file`.
   - Do not overwrite local files unless `overwrite=true` is intentional.
   - Do not write ignored paths unless `allow_ignored=true` is intentional.
8. Disconnect when finished.
   - Default `disconnect(mode="detach")` ends local trust without terminating remote processes.
   - Use `mode="terminate"` only when the user explicitly wants the remote helper/session ended.

## Troubleshooting

- Missing MCP tools: install the MCP server and restart Codex.
- Claim token rejected: tokens are one-time use; rerun the remote setup command for a fresh token.
- Helper cannot connect: confirm notebook outbound internet access and broker URL reachability.
- Project config rejected: remove broker/provider/auth settings from `painfully-jupyter.toml`.
- Empty upload: check `[sync].allowlist`; no allowlist means no managed upload files.
- Fetch refused: pass `overwrite=true` or `allow_ignored=true` only after confirming the destination is intended.
