# agentstats (Rust)

Analytics for [`rmcp`](https://crates.io/crates/rmcp) servers. Wraps any `ServerHandler`:

```rust
let service = agentstats::AgentStats::new(MyServer::new(), agentstats::Options::default());
service.serve(stdio()).await?.waiting().await?;

// Streamable HTTP creates one service per session; share a tracker:
let tracker = agentstats::Tracker::new(agentstats::Options::default());
let service = agentstats::AgentStats::with_tracker(MyServer::new(), tracker.clone());
```

A plain background thread with blocking `ureq` ships batches, so it needs no async runtime of its own and never keeps the process alive.

```sh
cargo test
```
