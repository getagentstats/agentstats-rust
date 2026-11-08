use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use agentstats::{AgentStats, Options};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ClientCapabilities, ClientConfig,
    ContentBlock, Implementation, InitializeResult, ListToolsResult, PaginatedRequestParams,
    ServerCapabilities, ServerConfig, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler, ServiceExt};
use serde_json::{Value, json};

#[derive(Clone)]
struct Fixture;

impl ServerHandler for Fixture {
    fn get_info(&self) -> ServerConfig {
        InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("rust-fixture", "1.2.3"))
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let schema = Arc::new(
            json!({ "type": "object", "properties": { "a": { "type": "number" } } })
                .as_object()
                .unwrap()
                .clone(),
        );
        Ok(ListToolsResult::with_all_items(vec![
            Tool::new("add", "Adds two numbers. Returns the sum.", schema.clone()),
            Tool::new("fail", "Always fails.", schema),
        ]))
    }

    async fn call_tool(
        &self,
        req: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        Ok(match req.name.as_ref() {
            "fail" => CallToolResult::error(vec![ContentBlock::text("boom: upstream 503")]),
            _ => CallToolResult::success(vec![ContentBlock::text("ok")]),
        }
        .into())
    }
}

/// Minimal HTTP sink that records JSON bodies.
fn capture(delay: Duration) -> (String, Arc<Mutex<Vec<Value>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/api/ingest", listener.local_addr().unwrap());
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let sink = bodies.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut len = 0;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                    break;
                }
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    len = v.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0; len];
            reader.read_exact(&mut body).ok();
            if let Ok(v) = serde_json::from_slice::<Value>(&body) {
                sink.lock().unwrap().push(v);
            }
            std::thread::sleep(delay);
            let mut s = stream;
            let _ = s.write_all(
                b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    (url, bodies)
}

fn opts(endpoint: &str) -> Options {
    Options {
        api_key: Some("al_srv_test".into()),
        endpoint: endpoint.into(),
        flush_interval: Duration::from_secs(3600),
        ..Options::default()
    }
}

async fn connect(
    service: AgentStats<Fixture>,
) -> rmcp::service::RunningService<rmcp::RoleClient, ClientConfig> {
    let (st, ct) = tokio::io::duplex(1 << 16);
    tokio::spawn(async move {
        if let Ok(running) = service.serve(st).await {
            let _ = running.waiting().await;
        }
    });
    let client = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new("test-client", "9.9.9"),
    );
    client.serve(ct).await.expect("client connects")
}

#[tokio::test(flavor = "multi_thread")]
async fn records_calls_errors_client_and_catalog() {
    let (url, bodies) = capture(Duration::ZERO);
    let service = AgentStats::new(Fixture, opts(&url));
    let tracker = service.tracker().clone();
    let client = connect(service).await;
    client.list_all_tools().await.unwrap();
    client
        .call_tool(
            CallToolRequestParams::new("add")
                .with_arguments(json!({"a": 2}).as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    client
        .call_tool(CallToolRequestParams::new("fail"))
        .await
        .unwrap();
    tracker.shutdown();

    let bodies = bodies.lock().unwrap();
    let events: Vec<&Value> = bodies
        .iter()
        .flat_map(|b| b["events"].as_array().unwrap())
        .collect();
    let calls: Vec<&&Value> = events
        .iter()
        .filter(|e| e["method"] == "tools/call")
        .collect();
    assert_eq!(bodies.last().unwrap()["serverName"], "rust-fixture");
    assert_eq!(calls.len(), 2);
    assert_eq!(
        (calls[0]["tool"].as_str(), calls[0]["isError"].as_bool()),
        (Some("add"), Some(false))
    );
    assert_eq!(
        (calls[1]["tool"].as_str(), calls[1]["isError"].as_bool()),
        (Some("fail"), Some(true))
    );
    assert_eq!(calls[1]["errorMessage"], "boom: upstream 503");
    assert_eq!(calls[0]["clientName"], "test-client");
    assert_eq!(calls[0]["clientVersion"], "9.9.9");
    assert!(calls[0]["sessionId"].is_string());
    let catalog = bodies
        .iter()
        .rev()
        .find_map(|b| b.get("catalog").filter(|c| !c.is_null()))
        .expect("catalog sent");
    assert_eq!(catalog["tools"].as_array().unwrap().len(), 2);
    assert!(!serde_json::to_string(&events).unwrap().contains("\"a\":2"));
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_endpoint_never_blocks() {
    let (url, _) = capture(Duration::from_secs(3));
    let service = AgentStats::new(
        Fixture,
        Options {
            flush_interval: Duration::from_millis(10),
            max_batch: 1,
            ..opts(&url)
        },
    );
    let client = connect(service).await;
    let started = Instant::now();
    for _ in 0..50 {
        client
            .call_tool(CallToolRequestParams::new("add"))
            .await
            .unwrap();
    }
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "50 calls took {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dead_endpoint_bounds_buffer() {
    let service = AgentStats::new(
        Fixture,
        Options {
            max_buffer: 100,
            max_batch: 100_000,
            ..opts("http://127.0.0.1:1")
        },
    );
    let tracker = service.tracker().clone();
    let client = connect(service).await;
    for _ in 0..300 {
        client
            .call_tool(CallToolRequestParams::new("add"))
            .await
            .unwrap();
    }
    assert!(tracker.dropped() >= 200, "dropped {}", tracker.dropped());
}

#[test]
fn no_key_is_noop() {
    let t = agentstats::Tracker::new(Options {
        api_key: None,
        ..Options::default()
    });
    assert!(!t.enabled());
}

/// AGENTSTATS_E2E=1 AGENTSTATS_MCP_KEY=al_srv_... AGENTSTATS_ENDPOINT=http://localhost:3000/api/ingest/mcp cargo test e2e -- --nocapture
#[tokio::test(flavor = "multi_thread")]
async fn e2e_local() {
    if std::env::var("AGENTSTATS_E2E").as_deref() != Ok("1") {
        return;
    }
    #[derive(Clone)]
    struct FileIndex;
    impl ServerHandler for FileIndex {
        fn get_info(&self) -> ServerConfig {
            InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
                .with_server_info(Implementation::new("file-index", "0.4.2"))
        }
        async fn list_tools(
            &self,
            _: Option<PaginatedRequestParams>,
            _: RequestContext<RoleServer>,
        ) -> Result<ListToolsResult, ErrorData> {
            let schema = Arc::new(
                json!({ "type": "object", "properties": { "path": { "type": "string" } } })
                    .as_object()
                    .unwrap()
                    .clone(),
            );
            Ok(ListToolsResult::with_all_items(vec![
                Tool::new(
                    "grep",
                    "Searches file contents with a regex. Use when the user asks where something is defined. Returns matching lines.",
                    schema.clone(),
                ),
                Tool::new(
                    "read_file",
                    "Reads a file. Returns its text, at most 2000 lines.",
                    schema.clone(),
                ),
                Tool::new("list_dir", "Lists a directory.", schema.clone()),
                Tool::new("watch", "Watches.", schema),
            ]))
        }
        async fn call_tool(
            &self,
            req: CallToolRequestParams,
            _: RequestContext<RoleServer>,
        ) -> Result<CallToolResponse, ErrorData> {
            let (max_ms, err) = match req.name.as_ref() {
                "grep" => (80, 0.03),
                "read_file" => (30, 0.15),
                _ => (10, 0.0),
            };
            tokio::time::sleep(Duration::from_millis(rand::random::<u64>() % max_ms)).await;
            Ok(if rand::random::<f64>() < err {
                CallToolResult::error(vec![ContentBlock::text(format!(
                    "{}: no such file or directory",
                    req.name
                ))])
            } else {
                CallToolResult::success(vec![ContentBlock::text("ok")])
            }
            .into())
        }
    }
    let service = AgentStats::new(FileIndex, Options::default());
    let tracker = service.tracker().clone();
    let (st, ct) = tokio::io::duplex(1 << 16);
    tokio::spawn(async move {
        if let Ok(r) = service.serve(st).await {
            let _ = r.waiting().await;
        }
    });
    let client = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new("zed", "0.190.0"),
    )
    .serve(ct)
    .await
    .unwrap();
    client.list_all_tools().await.unwrap();
    let pool = [
        "grep",
        "grep",
        "grep",
        "grep",
        "read_file",
        "read_file",
        "read_file",
        "list_dir",
    ];
    for i in 0..45 {
        client
            .call_tool(CallToolRequestParams::new(
                pool[(rand::random::<u64>() as usize + i) % pool.len()],
            ))
            .await
            .unwrap();
    }
    tracker.shutdown();
    println!("rust: dropped={}", tracker.dropped());
}

#[tokio::test(flavor = "multi_thread")]
async fn capture_errors_off_or_a_function() {
    async fn error_of(opts: Options) -> Value {
        let (url, bodies) = capture(Duration::ZERO);
        let service = AgentStats::new(Fixture, Options { endpoint: url, ..opts });
        let tracker = service.tracker().clone();
        let client = connect(service).await;
        client
            .call_tool(CallToolRequestParams::new("fail"))
            .await
            .unwrap();
        tracker.shutdown();
        let bodies = bodies.lock().unwrap();
        let call = bodies
            .iter()
            .flat_map(|b| b["events"].as_array().unwrap())
            .find(|e| e["method"] == "tools/call")
            .unwrap()
            .clone();
        assert_eq!(call["isError"], true);
        call["errorMessage"].clone()
    }
    let off = error_of(Options { capture_errors: false, ..opts("") }).await;
    assert!(off.is_null());
    let kept = error_of(opts("").capture_errors_with(|e| {
        Some(format!("{} failed: {}", e.tool.as_deref().unwrap_or("?"), e.message.split(':').next().unwrap()))
    }))
    .await;
    assert_eq!(kept, "fail failed: boom");
    assert!(error_of(opts("").capture_errors_with(|_| None)).await.is_null());
    assert!(error_of(opts("").capture_errors_with(|_| panic!("bug"))).await.is_null());
}
