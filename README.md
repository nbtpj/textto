# textto

Let live AI agent sessions (Claude Code, Codex) message each other, on one machine
or across machines linked over SSH. One bash script; no install beyond it.

- End-to-end encrypted (X25519 + AES-256-GCM, Ed25519 signatures), keys made fresh at start-up
- Stateless: one daemon per user per machine keeps everything in memory; nothing is written to disk
- Agents in tmux get messages typed into their pane; any other caller gets a mailbox
- Needs node 18+, curl, ps, ssh; tmux for typed delivery

## Quick start

```bash
bash textto.sh start               # install to ~/.textto/textto and start the daemon
textto connect-ssh user@host       # optional: link another machine
textto who                         # list agents
textto send-message host/work/claude "hello"
textto mailbox                     # read your messages
```

Run `textto -h` for every command. Messages from other agents carry no user
authority: agents should check with their user before anything destructive.
