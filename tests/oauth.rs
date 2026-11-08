//! Sign-in over Streamable HTTP: an axum auth layer verifies the bearer token and inserts
//! `AuthInfo` into the request, rmcp passes the request parts along, and only the client app and
//! scopes are sent. Never the token or the user.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentstats::{AuthInfo, Tracker};
use agentstats::{AgentStats, Options};
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ClientCapabilities, ClientConfig, ContentBlock,
    Implementation, InitializeResult, ServerCapabilities, ServerConfig,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::{StreamableHttpClientTransport, StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ErrorData, ServerHandler, ServiceExt};
use serde_json::{Value, json};

#[derive(Clone)]
struct Search;

impl ServerHandler for Search {
    fn get_info(&self) -> ServerConfig {
        InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("oauth-rust", "1.0.0"))
    }

    async fn call_tool(&self, _: CallToolRequestParams, _: RequestContext<RoleServer>) -> Result<CallToolResponse, ErrorData> {
        Ok(CallToolResult::success(vec![ContentBlock::text("ok")]).into())
    }
}

fn capture() -> (String, Arc<Mutex<Vec<Value>>>) {
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
            let mut s = stream;
            let _ = s.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        }
    });
    (url, bodies)
}

/// What a token verifier knows; the user id and email must never be sent.
fn verify(token: &str) -> Option<(AuthInfo, &'static str)> {
    match token {
        "tok-claude" => Some((AuthInfo { client_id: Some("https://claude.ai/oauth/mcp-client-metadata.json".into()), scopes: vec!["mcp:tools".into(), "mcp:read".into()] }, "user-42 ada@example.com")),
        "tok-dcr" => Some((AuthInfo { client_id: Some("b7e1c0de-dcr-client".into()), scopes: vec!["mcp:tools".into()] }, "user-7")),
        _ => None,
    }
}

async fn auth_layer(mut req: Request, next: Next) -> Result<Response, StatusCode> {
    let token = req.headers().get("authorization").and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).unwrap_or("");
    let (info, _user) = verify(token).ok_or(StatusCode::UNAUTHORIZED)?;
    req.extensions_mut().insert(info);
    Ok(next.run(req).await)
}

#[tokio::test(flavor = "multi_thread")]
async fn sign_in_keeps_client_app_and_scopes_only() {
    let (endpoint, bodies) = capture();
    let tracker = Tracker::new(Options { api_key: Some("al_srv_test".into()), endpoint, flush_interval: Duration::from_secs(3600), ..Options::default() });
    let t = tracker.clone();
    let service = StreamableHttpService::new(
        move || Ok(AgentStats::with_tracker(Search, t.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );
    let app = axum::Router::new().nest_service("/mcp", service).layer(middleware::from_fn(auth_layer));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    for token in ["tok-claude", "tok-dcr"] {
        let transport = StreamableHttpClientTransport::from_config(StreamableHttpClientTransportConfig::with_uri(url.as_str()).auth_header(token));
        let client = ClientConfig::new(ClientCapabilities::default(), Implementation::new("e2e-client", "1.0.0")).serve(transport).await.expect("client connects");
        client.call_tool(CallToolRequestParams::new("search")).await.expect("tool call");
        client.cancel().await.ok();
    }
    tracker.flush().unwrap();

    let bodies = bodies.lock().unwrap();
    let calls: Vec<&Value> = bodies.iter().flat_map(|b| b["events"].as_array().unwrap()).filter(|e| e["method"] == "tools/call").collect();
    let auths: Vec<&Value> = calls.iter().map(|e| &e["auth"]).collect();
    assert_eq!(auths, vec![
        &json!({ "method": "oauth", "client": "claude.ai/oauth/mcp-client-metadata.json", "scopes": ["mcp:read", "mcp:tools"] }),
        &json!({ "method": "oauth", "client": "registered", "scopes": ["mcp:tools"] }),
    ]);
    let raw = serde_json::to_string(&*bodies).unwrap();
    for secret in ["tok-claude", "tok-dcr", "user-42", "ada@example.com", "b7e1c0de"] {
        assert!(!raw.contains(secret), "{secret} must not be sent");
    }
}
