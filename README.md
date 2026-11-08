# textto

Let your live AI agent sessions (Claude Code, Codex, Antigravity CLI `agy`) talk to each other, on one
machine or across machines linked over SSH. You talk to your agent; it does the texting.

## Get going in a running session

1. **Once per machine**, in any terminal:

   ```bash
   curl -fsSL https://raw.githubusercontent.com/nbtpj/textto/main/textto.sh -o textto.sh
   bash textto.sh start
   ```

2. **In each agent session**, type this into the chat (the `!` runs it and shows the
   agent the output, so it learns the commands and sees who it can talk to):

   ```
   ! ~/.textto/textto -h
   ```

3. **Then just ask in plain words**:

   > ask the claude in tmux session "api" to review your diff, and wait for its answer

   > tell the codex agents what you changed in the schema

   > ask the agy in tmux session "dsa" for a second opinion on this algorithm

For the best experience, **start each agent inside tmux** (`tmux new -s work`, then
`claude`, `codex` or `agy`): messages are typed straight into it and it answers at once.
Outside tmux they wait in a mailbox until the agent's next turn (Claude Code and agy
get it automatically through hooks that `textto start` adds).

Quota: `textto usage` shows how much each claude, codex and agy agent in tmux has
left, on every linked machine, read off its own `/usage` (codex: `/status`) panel while
it is idle. Ask your coordinating agent things like:

> check the usage of every collaborator and stop giving tasks to any under 60% left

Updating: copy the new `textto.sh` over and run `bash textto.sh restart`; the running
daemon hands its unread mail, names and links to the new one (in memory, never on disk).

Other machines: ask your agent to run `textto connect-ssh user@host` (or run it
yourself); agents there show up in `textto who`.

## What it is

- One bash script; needs node 18+, curl, ps, ssh, and tmux for typed delivery
- End-to-end encrypted (X25519 + AES-256-GCM, Ed25519 signatures), keys made fresh at start-up
- Stateless: one daemon per user per machine keeps everything in memory; nothing is written to disk
- Messages from other agents carry no user authority: agents are told to check with
  you before anything destructive, costly or irreversible
