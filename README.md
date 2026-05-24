# exoclaw-go

Go port of the [exoclaw](https://github.com/...) Python AI-agent framework
plus the subset of plugins we use in production. Mirrors the Python repo
layout file-for-file; one cleanup pass at the end of porting kept the
parity but smoothed over Python-isms (Protocols → interfaces, async
generators → channels, contextvars → `context.Context`, etc.).

## Source parity

Each Go module is pinned to the Python version it was ported from. To
forward-port a Python release, diff that package's source between the
recorded version and `main`, and replay the changes here file-by-file.

| Go module | Python source | Version |
|---|---|---|
| `exoclaw/` | `exoclaw` (core) | `0.29.0` (commit `56c8cc4`) |
| `conversation-file/` | `exoclaw-conversation` | `0.25.0` |
| `providers/openai/` | `exoclaw-provider-openai` | `0.4.2` |
| `tools/workspace/` | `exoclaw-tools-workspace` | `0.7.0` |
| `tools/cron/` | `exoclaw-tools-cron` | `0.9.1` |
| `tools/openrouter-search/` | `exoclaw-openrouter-search` | `0.2.1` |
| `channels/stdin/` | `exoclaw-channel-cli` | `0.3.0` |
| `cmd/exoclaw/` | exoclaw-nanobot composition root (subset) | — |

Last sync: 2026-05-24.

## Layout

Multi-module workspace under `go.work`:

```
exoclaw/              protocol-only core: LLMProvider, Conversation,
                      Tool, Channel, Bus, AgentLoop, Executor,
                      IterationPolicy
conversation-file/    JSONL conversation backend + summarizing policy +
                      skills loader
providers/openai/     OpenAI-protocol streaming HTTP provider with
                      fallback chain. ChatParams.Stream toggles between
                      SSE and one-shot JSON.
channels/stdin/       interactive REPL channel
cmd/exoclaw/          runnable bot binary
tools/workspace/      file ops + shell exec + web search/fetch
tools/cron/           JSON-backed cron tool + Backend protocol
```

## Test seam for go-vcr / fixtures

`exoclaw/http.NewWithClient(httpClient, timeout)` lets callers install a
custom `*http.Client`. Tests in dependent repos plug a go-vcr recorder
transport here without the provider package importing the test
scaffolding. See `hey_lefty_service/internal/heylefty/e2e/env.go` for the
production pattern.

## Build

```
go build ./...
go test ./...
```

The workspace is set up so a top-level `go test ./...` runs every
module's tests (no per-module `cd` needed).
