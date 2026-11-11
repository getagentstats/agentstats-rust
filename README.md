# agentstats (Rust)

[AgentStats](https://agentstats.io) analytics for Rust MCP servers built on [`rmcp`](https://crates.io/crates/rmcp), and for command-line tools (with a one-call integration for [`clap`](https://crates.io/crates/clap)).

## MCP servers

```sh
cargo add agentstats
```

```rust
use agentstats::{AgentStats, Options};

// Reads AGENTSTATS_MCP_KEY
let service = AgentStats::new(MyServer::new(), Options::default());
let tracker = service.tracker().clone();
service.serve(stdio()).await?.waiting().await?;
tracker.shutdown(); // sends what's left
```

`AgentStats` is itself a `ServerHandler` that forwards every method to yours, so it works with any rmcp transport. It records every request from Claude, Cursor, ChatGPT and other clients: the tool, outcome, latency, client and tool list. Tool arguments and results are never sent.

Streamable HTTP creates one handler per session, so share one tracker between them:

```rust
let tracker = agentstats::Tracker::new(Options::default());
StreamableHttpService::new(
    move || Ok(AgentStats::with_tracker(MyServer::new(), tracker.clone())),
    sessions,
    config,
);
```

Recording a call is a timestamp and a push under a short mutex. A plain background thread with blocking `ureq` sends batches every 2 seconds, so it needs no async runtime of its own and never keeps the process alive. Failed sends are dropped, never retried inline, and nothing is returned to your server. Without a key, nothing is recorded.

Options (`Options { .., ..Options::default() }`): `api_key`, `server_name`, `sample_rate`, `capture_errors`, `capture_catalog`, `feedback_tool` (adds a `send_feedback` tool the SDK answers itself, so agents and users can report problems), `flush_interval`, `max_batch`, `max_buffer`, `debug`. `Options::capture_errors_with` decides per request what is kept of an error message; `Options::auth_info_with` reads who signed in. Environment: `AGENTSTATS_MCP_KEY`, `AGENTSTATS_DISABLED=1`, `AGENTSTATS_DEBUG=1`.

## Command-line tools

```sh
cargo add agentstats --no-default-features --features clap
```

```rust
use agentstats::cli::{Options, Tracker};

fn main() {
    // Create the tracker first: the binary starts itself again in the background to send.
    let stats = Tracker::new(Options::new("acme").version(env!("CARGO_PKG_VERSION")).api_key("al_cli_…"));
    let cli: Cli = stats.parse(); // instead of Cli::parse()
    let code = match run(cli) {
        Ok(()) => 0,
        Err(e) => { eprintln!("{e}"); stats.error(&e); 1 }
    };
    stats.exit(code)
}
```

`default-features = false` leaves out rmcp, which only MCP servers need. It records which commands coding agents, people and CI run, and where they fail. Argument values are never sent, only the command and flag names. Nothing waits on the network: at exit one line is appended to a spool file in `~/.cache/agentstats`, and a detached copy of the binary sends it. Without clap, call `stats.command`, `stats.error` and `stats.exit(code)` yourself.

On the first run in a terminal a one-time telemetry notice is printed to stderr (`.no_notice()` turns it off if your CLI already tells users). Users opt out with `ACME_TELEMETRY_DISABLED=1` (your CLI's name, uppercased) or `DO_NOT_TRACK=1`.

Docs: [MCP servers](https://agentstats.io/docs/mcp-servers/sdks) · [CLIs](https://agentstats.io/docs/cli/sdk) · [API reference](https://docs.rs/agentstats)

```sh
cargo test --all-features
```

MIT licensed.
