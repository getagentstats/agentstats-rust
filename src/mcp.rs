//! AgentStats MCP analytics for [`rmcp`].
//!
//! ```ignore
//! use agentstats::AgentStats;
//! let service = AgentStats::new(MyServer::new(), agentstats::Options::default());
//! service.serve(stdio()).await?.waiting().await?;
//!
//! // Streamable HTTP: one tracker shared by every session's handler.
//! let tracker = agentstats::Tracker::new(agentstats::Options::default());
//! StreamableHttpService::new(move || Ok(AgentStats::with_tracker(MyServer::new(), tracker.clone())), sessions, config);
//! ```
//!
//! `AgentStats` is itself a `ServerHandler` that forwards every method to yours, so it works with
//! any rmcp transport.
//!
//! Recording a call is a timestamp and a push under a short mutex. A background thread
//! ships batches; on failure events are dropped, never retried inline, and nothing is
//! returned to your server. Tool arguments and results are never sent. See <https://github.com/getagentstats/spec/blob/main/PROTOCOL.md>.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rmcp::ErrorData as McpError;
use rmcp::ServerHandler;
#[allow(deprecated)]
use rmcp::model::*;
use rmcp::service::{
    MaybeSendFuture, NotificationContext, RequestContext, RoleServer, SubscriptionContext,
};
use serde::Serialize;
use serde_json::{Value, json};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// A failed request, as passed to [`Options::capture_errors_with`].
#[derive(Clone, Debug)]
pub struct ErrorInfo {
    /// The tool for `tools/call`, otherwise `None`.
    pub tool: Option<String>,
    pub method: String,
    /// The JSON-RPC error message, or the first text content of an `isError` result.
    pub message: String,
    /// The JSON-RPC error code; `None` for `isError` results.
    pub code: Option<i32>,
}

/// Decides what is sent of a failed request's message. See [`Options::capture_errors_with`].
#[derive(Clone)]
pub struct ErrorText(Arc<dyn Fn(&ErrorInfo) -> Option<String> + Send + Sync>);

impl std::fmt::Debug for ErrorText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ErrorText(..)")
    }
}

/// The validated OAuth access token of a request, as your auth middleware knows it. Insert it into
/// the HTTP request's extensions (`req.extensions_mut().insert(AuthInfo { .. })` in an axum or
/// tower layer, after verifying the token) and the tracker reads it from the request parts rmcp
/// passes along. Only the client app and the scopes are sent (see [`auth_summary`]).
#[derive(Clone, Debug, Default)]
pub struct AuthInfo {
    pub client_id: Option<String>,
    pub scopes: Vec<String>,
}

/// Reads a request's [`AuthInfo`] some other way. Set with [`Options::auth_info_with`].
#[derive(Clone)]
pub struct AuthInfoFn(Arc<dyn Fn(&rmcp::model::Extensions) -> Option<AuthInfo> + Send + Sync>);

impl std::fmt::Debug for AuthInfoFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthInfoFn(..)")
    }
}

/// What's kept of a validated token (<https://github.com/getagentstats/spec/blob/main/PROTOCOL.md>, "Sign-in"): the client app by its Client ID
/// Metadata Document URL (https, with a path, no credentials or query), else "registered"
/// (Dynamic Client Registration ids are random per installation), and the granted scopes,
/// de-duplicated, sorted, at most 20 of at most 80 chars. Never the token or the user.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct Auth {
    pub method: &'static str,
    pub client: Option<String>,
    pub scopes: Vec<String>,
}

pub fn auth_summary(info: &AuthInfo) -> Auth {
    let client = info
        .client_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|id| {
            id.strip_prefix("https://")
                .and_then(|rest| {
                    let (host, path) = rest.split_at(rest.find('/')?);
                    let ok = !host.is_empty()
                        && !host.contains('@')
                        && !id.contains('?')
                        && !id.contains('#')
                        && path.len() > 1;
                    ok.then(|| {
                        format!("{host}{path}")
                            .chars()
                            .take(200)
                            .collect::<String>()
                    })
                })
                .unwrap_or_else(|| "registered".into())
        });
    let mut scopes: Vec<String> = info
        .scopes
        .iter()
        .filter(|s| !s.is_empty() && s.len() <= 80)
        .cloned()
        .collect();
    scopes.sort();
    scopes.dedup();
    scopes.truncate(20);
    Auth {
        method: "oauth",
        client,
        scopes,
    }
}

/// Configuration. `Default` reads `AGENTSTATS_MCP_KEY`.
#[derive(Clone, Debug)]
pub struct Options {
    pub api_key: Option<String>,
    pub server_name: Option<String>,
    /// Fraction of `tools/call` requests recorded.
    pub sample_rate: f64,
    /// Send error messages. `false` keeps only that the request failed.
    pub capture_errors: bool,
    /// Set with [`Options::capture_errors_with`].
    pub error_text: Option<ErrorText>,
    pub capture_catalog: bool,
    /// Add a `send_feedback` tool to the server's tools, answered by the SDK (it never reaches your
    /// handler), so agents can report a tool that failed them and users can pass something on.
    /// What they write is sent to AgentStats (Feedback, under the server). Off by default.
    pub feedback_tool: bool,
    pub flush_interval: Duration,
    pub max_batch: usize,
    pub max_buffer: usize,
    pub debug: bool,
    /// Set with [`Options::auth_info_with`]; by default the [`AuthInfo`] in the request parts.
    pub auth_info: Option<AuthInfoFn>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            api_key: std::env::var("AGENTSTATS_MCP_KEY")
                .ok()
                .filter(|s| !s.is_empty()),
            server_name: None,
            sample_rate: 1.0,
            capture_errors: true,
            error_text: None,
            capture_catalog: true,
            feedback_tool: false,
            flush_interval: Duration::from_secs(2),
            max_batch: 250,
            max_buffer: 10_000,
            debug: std::env::var("AGENTSTATS_DEBUG").as_deref() == Ok("1"),
            auth_info: None,
        }
    }
}

impl Options {
    /// Decide per request what is sent of an error's message: return the text to keep, or
    /// `None` for none. It runs as the response is sent, so keep it cheap; a panic keeps nothing.
    ///
    /// ```ignore
    /// Options::default().capture_errors_with(|e| match e.tool.as_deref() {
    ///     Some("run_sql") => e.message.lines().next().map(String::from),
    ///     _ => Some(e.message.clone()),
    /// })
    /// ```
    pub fn capture_errors_with(
        mut self,
        f: impl Fn(&ErrorInfo) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.error_text = Some(ErrorText(Arc::new(f)));
        self
    }

    /// Read a request's validated token your own way, e.g. from your claims type in the request
    /// parts. Return `None` when the request isn't signed in.
    pub fn auth_info_with(
        mut self,
        f: impl Fn(&rmcp::model::Extensions) -> Option<AuthInfo> + Send + Sync + 'static,
    ) -> Self {
        self.auth_info = Some(AuthInfoFn(Arc::new(f)));
        self
    }
}

/// The request's [`AuthInfo`]: the custom reader, else the one in the HTTP request parts (or put
/// straight into rmcp's extensions).
fn request_auth(opts: &Options, ext: &rmcp::model::Extensions) -> Option<Auth> {
    let info = match &opts.auth_info {
        Some(f) => std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (f.0)(ext)))
            .ok()
            .flatten(),
        None => ext
            .get::<http::request::Parts>()
            .and_then(|p| p.extensions.get::<AuthInfo>())
            .or_else(|| ext.get::<AuthInfo>())
            .cloned(),
    };
    info.map(|i| auth_summary(&i))
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Event {
    ts: u128,
    method: String,
    tool: Option<String>,
    duration_ms: f64,
    is_error: bool,
    /// What failed: "tool" (an isError result) or "protocol" (a JSON-RPC error, with its code).
    #[serde(skip_serializing_if = "Option::is_none")]
    error_kind: Option<&'static str>,
    error_message: Option<String>,
    /// The JSON-RPC code, for capture_errors_with; sent as `errorCode` for protocol errors.
    #[serde(skip)]
    error_code: Option<i32>,
    #[serde(rename = "errorCode", skip_serializing_if = "Option::is_none")]
    wire_error_code: Option<i32>,
    /// Characters of text a successful tools/call, resources/read or prompts/get result handed the model.
    #[serde(skip_serializing_if = "Option::is_none")]
    result_chars: Option<usize>,
    client_name: Option<String>,
    client_version: Option<String>,
    session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<Auth>,
}

#[derive(Default)]
struct State {
    buf: VecDeque<Event>,
    server_name: Option<String>,
    catalog: serde_json::Map<String, Value>,
    catalog_sig: String,
    catalog_dirty: bool,
    feedback: VecDeque<Value>,
    /// The server lists a send_feedback tool of its own: leave its calls alone.
    server_has_feedback: bool,
    dropped: u64,
    inflight: u8,
    stopped: bool,
}

/// Buffers events and ships them from a background thread. Cheap to clone.
#[derive(Clone)]
pub struct Tracker {
    inner: Arc<TrackerInner>,
}

struct TrackerInner {
    opts: Options,
    endpoint: String,
    enabled: bool,
    state: Mutex<State>,
    wake: Condvar,
    thread: OnceLock<()>,
    agent: ureq::Agent,
}

impl Tracker {
    pub fn new(opts: Options) -> Self {
        let enabled =
            opts.api_key.is_some() && std::env::var("AGENTSTATS_DISABLED").as_deref() != Ok("1");
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(5)))
            .http_status_as_error(false)
            .build()
            .into();
        let state = State {
            server_name: opts.server_name.clone(),
            ..Default::default()
        };
        let t = Self {
            inner: Arc::new(TrackerInner {
                opts,
                endpoint: crate::ingest_url("mcp"),
                enabled,
                state: Mutex::new(state),
                wake: Condvar::new(),
                thread: OnceLock::new(),
                agent,
            }),
        };
        if !enabled {
            t.debug("no API key, analytics disabled");
        }
        t
    }

    pub fn enabled(&self) -> bool {
        self.inner.enabled
    }

    fn debug(&self, msg: &str) {
        if self.inner.opts.debug {
            eprintln!("[agentstats] {msg}");
        }
    }

    fn sample(&self) -> bool {
        self.inner.opts.sample_rate >= 1.0 || rand::random::<f64>() < self.inner.opts.sample_rate
    }

    fn set_server_info(&self, name: &str, version: &str, instructions: Option<&str>) {
        let mut s = self.inner.state.lock().unwrap();
        if s.server_name.is_none() {
            s.server_name = Some(name.to_string());
        }
        s.catalog.insert(
            "serverInfo".into(),
            json!({ "name": name, "version": version }),
        );
        if let Some(i) = instructions {
            s.catalog.insert("instructions".into(), json!(i));
        }
    }

    fn set_tools(&self, tools: &[rmcp::model::Tool]) {
        if !self.inner.opts.capture_catalog {
            return;
        }
        let mut sig = tools.len().to_string();
        for t in tools {
            sig.push_str(&format!(
                "|{}:{}",
                t.name,
                t.description.as_ref().map_or(0, |d| d.len())
            ));
        }
        let mut s = self.inner.state.lock().unwrap();
        if s.catalog_sig == sig {
            return;
        }
        let list: Vec<Value> = tools
            .iter()
            .map(|t| json!({ "name": t.name, "description": t.description, "inputSchema": *t.input_schema }))
            .collect();
        s.catalog.insert("tools".into(), Value::Array(list));
        s.catalog_sig = sig;
        s.catalog_dirty = true;
    }

    fn record(&self, mut e: Event, started: Instant) {
        e.duration_ms = started.elapsed().as_nanos() as f64 / 1e6;
        e.ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        if !self.inner.opts.capture_errors {
            e.error_message = None;
        } else if let Some(f) = &self.inner.opts.error_text
            && let Some(message) = e.error_message.take()
        {
            let info = ErrorInfo {
                tool: e.tool.clone(),
                method: e.method.clone(),
                message,
                code: e.error_code,
            };
            e.error_message =
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (f.0)(&info))) {
                    Ok(text) => text.filter(|t| !t.is_empty()),
                    Err(_) => {
                        self.debug("capture_errors_with panicked");
                        None
                    }
                };
        }
        if let Some(m) = e.error_message.as_mut() {
            if m.len() > 300 {
                let mut cut = 300;
                while !m.is_char_boundary(cut) {
                    cut -= 1;
                }
                m.truncate(cut);
            }
        }
        let full = {
            let mut s = self.inner.state.lock().unwrap();
            if s.buf.len() >= self.inner.opts.max_buffer {
                s.buf.pop_front();
                s.dropped += 1;
            }
            s.buf.push_back(e);
            s.buf.len() >= self.inner.opts.max_batch
        };
        self.ensure_thread();
        if full {
            self.inner.wake.notify_one();
        }
    }

    fn ensure_thread(&self) {
        self.inner.thread.get_or_init(|| {
            let me = self.clone();
            // A plain thread never keeps the process alive past main.
            std::thread::Builder::new()
                .name("agentstats".into())
                .spawn(move || me.run())
                .expect("spawn agentstats thread");
        });
    }

    fn run(&self) {
        loop {
            {
                let s = self.inner.state.lock().unwrap();
                let (s, _) = self
                    .inner
                    .wake
                    .wait_timeout(s, self.inner.opts.flush_interval)
                    .unwrap();
                if s.stopped {
                    return;
                }
            }
            let _ = self.flush();
        }
    }

    /// Send buffered events now. Returns an error message on failure (events are dropped).
    pub fn flush(&self) -> Result<(), String> {
        if !self.enabled() {
            return Ok(());
        }
        let (events, feedback, catalog, name, dropped) = {
            let mut s = self.inner.state.lock().unwrap();
            if s.inflight >= 2 || (s.buf.is_empty() && !s.catalog_dirty && s.feedback.is_empty()) {
                return Ok(());
            }
            let n = s.buf.len().min(self.inner.opts.max_batch);
            let events: Vec<Event> = s.buf.drain(..n).collect();
            let m = s.feedback.len().min(50);
            let feedback: Vec<Value> = s.feedback.drain(..m).collect();
            let catalog = if s.catalog_dirty {
                Some(Value::Object(s.catalog.clone()))
            } else {
                None
            };
            s.catalog_dirty = false;
            s.inflight += 1;
            let dropped = std::mem::take(&mut s.dropped);
            (
                events,
                feedback,
                catalog,
                s.server_name
                    .clone()
                    .unwrap_or_else(|| "unnamed-mcp-server".into()),
                dropped,
            )
        };
        let mut body = json!({ "serverName": name, "events": events, "dropped": dropped });
        if let Some(c) = &catalog {
            body["catalog"] = c.clone();
        }
        if !feedback.is_empty() {
            body["feedback"] = Value::Array(feedback);
        }
        let result = self
            .inner
            .agent
            .post(&self.inner.endpoint)
            .header(
                "Authorization",
                &format!(
                    "Bearer {}",
                    self.inner.opts.api_key.as_deref().unwrap_or("")
                ),
            )
            .header("X-AgentStats-SDK", &format!("rust/{VERSION}"))
            .header("Content-Type", "application/json")
            .send(body.to_string())
            .map_err(|e| e.to_string())
            .and_then(|r| {
                if r.status().as_u16() < 300 {
                    Ok(())
                } else {
                    Err(format!("HTTP {}", r.status()))
                }
            });
        let mut s = self.inner.state.lock().unwrap();
        s.inflight -= 1;
        if let Err(e) = &result {
            s.dropped += dropped + events.len() as u64;
            if catalog.is_some() {
                s.catalog_dirty = true;
            }
            drop(s);
            self.debug(&format!(
                "ingest failed, dropped {} events: {e}",
                events.len()
            ));
        }
        result
    }

    /// Stop the background thread and send what is left.
    pub fn shutdown(&self) {
        self.inner.state.lock().unwrap().stopped = true;
        self.inner.wake.notify_all();
        for _ in 0..100 {
            let left = {
                let s = self.inner.state.lock().unwrap();
                !s.buf.is_empty() || s.catalog_dirty || !s.feedback.is_empty()
            };
            if !left || self.flush().is_err() {
                return;
            }
        }
    }

    pub fn dropped(&self) -> u64 {
        self.inner.state.lock().unwrap().dropped
    }

    fn feedback_enabled(&self) -> bool {
        self.enabled() && self.inner.opts.feedback_tool
    }

    /// Queue what a send_feedback call said. `false` when there's nothing to keep (empty content).
    fn record_feedback(
        &self,
        args: Option<&rmcp::model::JsonObject>,
        session_id: &str,
        client: Option<(String, String)>,
    ) -> bool {
        let get = |k: &str| {
            args.and_then(|a| a.get(k))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .unwrap_or("")
        };
        let mut content = get("content").to_string();
        if content.is_empty() {
            return false;
        }
        truncate(&mut content, 4000);
        let mut tool = get("tool").to_string();
        truncate(&mut tool, 200);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let item = json!({
            "ts": ts,
            "sessionId": session_id,
            "clientName": client.as_ref().map(|c| c.0.clone()),
            "clientVersion": client.as_ref().map(|c| c.1.clone()),
            "source": if get("source") == "user" { "user" } else { "agent" },
            "tool": if tool.is_empty() { Value::Null } else { Value::String(tool) },
            "content": content,
        });
        {
            let mut s = self.inner.state.lock().unwrap();
            if s.feedback.len() >= 100 {
                s.feedback.pop_front();
            }
            s.feedback.push_back(item);
        }
        self.ensure_thread();
        true
    }
}

fn truncate(s: &mut String, max: usize) {
    if s.len() > max {
        let mut cut = max;
        while !s.is_char_boundary(cut) {
            cut -= 1;
        }
        s.truncate(cut);
    }
}

/// The send_feedback tool (PROTOCOL.md, "Feedback").
pub const FEEDBACK_TOOL_NAME: &str = "send_feedback";
const FEEDBACK_TITLE: &str = "Send feedback";
const FEEDBACK_DESCRIPTION: &str = "Send feedback about this server to the people who build it. Use it when a tool failed, returned something wrong or unhelpful, or when you could not do what the user needed with these tools; or when the user asks you to pass something on. Describe what you tried and what you expected in a few sentences. Leave out personal information, secrets and the user's data.";
const FEEDBACK_REPLY: &str = "Thanks: your feedback was sent to the developers of this server.";
const FEEDBACK_EMPTY: &str = "Nothing was sent: `content` is empty.";

fn feedback_tool() -> Tool {
    let schema = json!({
        "type": "object",
        "properties": {
            "content": { "type": "string", "description": "What happened and what you expected, in a few sentences." },
            "source": { "type": "string", "enum": ["user", "agent"], "description": "\"user\" when the user asked you to send it, \"agent\" when you are reporting on your own." },
            "tool": { "type": "string", "description": "The tool it is about, if it is about one." }
        },
        "required": ["content", "source"]
    });
    let schema = match schema {
        Value::Object(m) => m,
        _ => unreachable!(),
    };
    Tool::new(FEEDBACK_TOOL_NAME, FEEDBACK_DESCRIPTION, Arc::new(schema))
        .with_title(FEEDBACK_TITLE)
        .with_annotations(
            ToolAnnotations::with_title(FEEDBACK_TITLE)
                .read_only(false)
                .destructive(false)
                .open_world(false),
        )
}

/// Characters of text a content block hands the model.
fn block_chars(c: &rmcp::model::ContentBlock) -> usize {
    if let Some(t) = c.as_text() {
        return t.text.len();
    }
    if let Some(r) = c.as_resource()
        && let rmcp::model::ResourceContents::TextResourceContents { text, .. } = &r.resource
    {
        return text.len();
    }
    0
}

/// Wraps an rmcp server handler (anything implementing `Service<RoleServer>`, which every
/// `ServerHandler` does) and records each request.
pub struct AgentStats<S> {
    inner: S,
    tracker: Tracker,
    session_id: String,
    /// The session's sign-in, from its last request that carried one.
    auth: Mutex<Option<Auth>>,
}

/// A request being observed: what's known before the handler runs.
struct Begin {
    method: &'static str,
    tool: Option<String>,
    client: Option<(String, String)>,
    auth: Option<Auth>,
    started: Instant,
}

impl<S: ServerHandler> AgentStats<S> {
    pub fn new(inner: S, opts: Options) -> Self {
        Self::with_tracker(inner, Tracker::new(opts))
    }

    /// Share one tracker across many connections (Streamable HTTP creates a service per session).
    pub fn with_tracker(inner: S, tracker: Tracker) -> Self {
        if tracker.enabled() {
            let info: ServerConfig = inner.get_info();
            tracker.set_server_info(
                &info.server_info.name,
                &info.server_info.version,
                info.instructions.as_deref(),
            );
        }
        let session_id = format!("{:012x}", rand::random::<u64>() >> 16);
        Self {
            inner,
            tracker,
            session_id,
            auth: Mutex::new(None),
        }
    }

    pub fn tracker(&self) -> &Tracker {
        &self.tracker
    }

    /// Start observing a request, or `None` when it isn't recorded (disabled, or not sampled).
    fn begin(
        &self,
        method: &'static str,
        tool: Option<String>,
        context: &RequestContext<RoleServer>,
    ) -> Option<Begin> {
        let t = &self.tracker;
        if !t.enabled() || (method == "tools/call" && !t.sample()) {
            return None;
        }
        let client = context
            .client_info()
            .map(|c| (c.name.to_string(), c.version.to_string()));
        let auth = request_auth(&t.inner.opts, &context.extensions);
        if auth.is_some() {
            *self.auth.lock().unwrap() = auth.clone();
        }
        let auth = auth.or_else(|| self.auth.lock().unwrap().clone());
        Some(Begin {
            method,
            tool,
            client,
            auth,
            started: Instant::now(),
        })
    }

    fn end(
        &self,
        b: Begin,
        is_error: bool,
        error_message: Option<String>,
        error_code: Option<i32>,
    ) {
        self.end_with(b, is_error, error_message, error_code, None)
    }

    /// `error_code` is set for JSON-RPC errors only, so an error without one is a tool's isError result.
    fn end_with(
        &self,
        b: Begin,
        is_error: bool,
        error_message: Option<String>,
        error_code: Option<i32>,
        result_chars: Option<usize>,
    ) {
        let kind = match (is_error, error_code) {
            (false, _) => None,
            (true, Some(_)) => Some("protocol"),
            (true, None) => Some("tool"),
        };
        let event = Event {
            ts: 0,
            method: b.method.to_string(),
            tool: b.tool,
            duration_ms: 0.0,
            is_error,
            error_kind: kind,
            error_message: if is_error { error_message } else { None },
            error_code,
            wire_error_code: if kind == Some("protocol") {
                error_code
            } else {
                None
            },
            result_chars: if is_error { None } else { result_chars },
            client_name: b.client.as_ref().map(|c| c.0.clone()),
            client_version: b.client.as_ref().map(|c| c.1.clone()),
            session_id: Some(self.session_id.clone()),
            auth: b.auth,
        };
        self.tracker.record(event, b.started);
    }
}

#[allow(deprecated)]
impl<S: ServerHandler> ServerHandler for AgentStats<S> {
    fn ping(
        &self,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_ {
        self.inner.ping(context)
    }

    fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<InitializeResult, McpError>> + MaybeSendFuture + '_ {
        let begin = self.begin("initialize", None, &context);
        async move {
            let result = self.inner.initialize(request, context).await;
            if let Some(b) = begin {
                let (is_error, message, code) = match &result {
                    Err(e) => (true, Some(e.message.to_string()), Some(e.code.0)),
                    Ok(r) => {
                        self.tracker.set_server_info(
                            &r.server_info.name,
                            &r.server_info.version,
                            r.instructions.as_deref(),
                        );
                        (false, None, None)
                    }
                };
                self.end(b, is_error, message, code);
            }
            result
        }
    }

    fn negotiate_initialize(
        &self,
        request: &InitializeRequestParams,
    ) -> Result<InitializeResult, McpError> {
        self.inner.negotiate_initialize(request)
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        self.inner.supported_protocol_versions()
    }

    fn discover(
        &self,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<DiscoverResult, McpError>> + MaybeSendFuture + '_ {
        let begin = self.begin("server/discover", None, &context);
        async move {
            let result = self.inner.discover(context).await;
            if let Some(b) = begin {
                let (is_error, message, code) = match &result {
                    Err(e) => (true, Some(e.message.to_string()), Some(e.code.0)),
                    Ok(_) => (false, None, None),
                };
                self.end(b, is_error, message, code);
            }
            result
        }
    }

    fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CompleteResult, McpError>> + MaybeSendFuture + '_ {
        self.inner.complete(request, context)
    }

    fn set_level(
        &self,
        request: SetLevelRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_ {
        self.inner.set_level(request, context)
    }

    fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<GetPromptResponse, McpError>> + MaybeSendFuture + '_ {
        let begin = self.begin("prompts/get", None, &context);
        async move {
            let result = self.inner.get_prompt(request, context).await;
            if let Some(b) = begin {
                let (is_error, message, code) = match &result {
                    Err(e) => (true, Some(e.message.to_string()), Some(e.code.0)),
                    Ok(_) => (false, None, None),
                };
                let chars = match &result {
                    Ok(GetPromptResponse::Complete(r)) => {
                        Some(r.messages.iter().map(|m| block_chars(&m.content)).sum())
                    }
                    _ => None,
                };
                self.end_with(b, is_error, message, code, chars);
            }
            result
        }
    }

    fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListPromptsResult, McpError>> + MaybeSendFuture + '_ {
        let begin = self.begin("prompts/list", None, &context);
        async move {
            let result = self.inner.list_prompts(request, context).await;
            if let Some(b) = begin {
                let (is_error, message, code) = match &result {
                    Err(e) => (true, Some(e.message.to_string()), Some(e.code.0)),
                    Ok(_) => (false, None, None),
                };
                self.end(b, is_error, message, code);
            }
            result
        }
    }

    fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListResourcesResult, McpError>> + MaybeSendFuture + '_ {
        let begin = self.begin("resources/list", None, &context);
        async move {
            let result = self.inner.list_resources(request, context).await;
            if let Some(b) = begin {
                let (is_error, message, code) = match &result {
                    Err(e) => (true, Some(e.message.to_string()), Some(e.code.0)),
                    Ok(_) => (false, None, None),
                };
                self.end(b, is_error, message, code);
            }
            result
        }
    }

    fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListResourceTemplatesResult, McpError>> + MaybeSendFuture + '_
    {
        self.inner.list_resource_templates(request, context)
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ReadResourceResponse, McpError>> + MaybeSendFuture + '_ {
        let begin = self.begin("resources/read", None, &context);
        async move {
            let result = self.inner.read_resource(request, context).await;
            if let Some(b) = begin {
                let (is_error, message, code) = match &result {
                    Err(e) => (true, Some(e.message.to_string()), Some(e.code.0)),
                    Ok(_) => (false, None, None),
                };
                let chars = match &result {
                    Ok(ReadResourceResponse::Complete(r)) => Some(
                        r.contents
                            .iter()
                            .map(|c| match c {
                                rmcp::model::ResourceContents::TextResourceContents {
                                    text,
                                    ..
                                } => text.len(),
                                _ => 0,
                            })
                            .sum(),
                    ),
                    _ => None,
                };
                self.end_with(b, is_error, message, code, chars);
            }
            result
        }
    }

    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        self.inner.accepted_subscription_filter(requested)
    }

    fn listen(
        &self,
        context: SubscriptionContext,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_ {
        self.inner.listen(context)
    }

    fn subscribe(
        &self,
        request: SubscribeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_ {
        self.inner.subscribe(request, context)
    }

    fn unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_ {
        self.inner.unsubscribe(request, context)
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CallToolResponse, McpError>> + MaybeSendFuture + '_ {
        // send_feedback (Options::feedback_tool): answered here, never by the server, and not a tool call.
        let feedback = (self.tracker.feedback_enabled()
            && request.name == FEEDBACK_TOOL_NAME
            && !self.tracker.inner.state.lock().unwrap().server_has_feedback)
            .then(|| {
                let client = context
                    .client_info()
                    .map(|c| (c.name.to_string(), c.version.to_string()));
                self.tracker
                    .record_feedback(request.arguments.as_ref(), &self.session_id, client)
            });
        let begin = if feedback.is_some() {
            None
        } else {
            self.begin("tools/call", Some(request.name.to_string()), &context)
        };
        async move {
            if let Some(kept) = feedback {
                let content = vec![ContentBlock::text(if kept {
                    FEEDBACK_REPLY
                } else {
                    FEEDBACK_EMPTY
                })];
                return Ok(CallToolResponse::Complete(if kept {
                    CallToolResult::success(content)
                } else {
                    CallToolResult::error(content)
                }));
            }
            let result = self.inner.call_tool(request, context).await;
            if let Some(b) = begin {
                let (is_error, message, code) = match &result {
                    Err(e) => (true, Some(e.message.to_string()), Some(e.code.0)),
                    Ok(CallToolResponse::Complete(r)) if r.is_error == Some(true) => (
                        true,
                        r.content
                            .iter()
                            .find_map(|c| c.as_text().map(|t| t.text.clone())),
                        None,
                    ),
                    Ok(_) => (false, None, None),
                };
                let chars = match &result {
                    Ok(CallToolResponse::Complete(r)) => {
                        let n: usize = r.content.iter().map(block_chars).sum();
                        Some(if n == 0 {
                            r.structured_content
                                .as_ref()
                                .map_or(0, |v| v.to_string().len())
                        } else {
                            n
                        })
                    }
                    _ => None,
                };
                self.end_with(b, is_error, message, code, chars);
            }
            result
        }
    }

    fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, McpError>> + MaybeSendFuture + '_ {
        let begin = self.begin("tools/list", None, &context);
        async move {
            let mut result = self.inner.list_tools(request, context).await;
            if let Some(b) = begin {
                let (is_error, message, code) = match &result {
                    Err(e) => (true, Some(e.message.to_string()), Some(e.code.0)),
                    Ok(r) => {
                        if r.next_cursor.is_none() {
                            self.tracker.set_tools(&r.tools);
                        }
                        (false, None, None)
                    }
                };
                self.end(b, is_error, message, code);
            }
            // The client's list gets send_feedback; the catalog above keeps the server's own.
            if self.tracker.feedback_enabled()
                && let Ok(r) = result.as_mut()
                && r.next_cursor.is_none()
            {
                if r.tools.iter().any(|t| t.name == FEEDBACK_TOOL_NAME) {
                    self.tracker.inner.state.lock().unwrap().server_has_feedback = true;
                } else {
                    r.tools.push(feedback_tool());
                }
            }
            result
        }
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.inner.get_tool(name)
    }

    fn on_custom_request(
        &self,
        request: CustomRequest,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<CustomResult, McpError>> + MaybeSendFuture + '_ {
        self.inner.on_custom_request(request, context)
    }

    fn on_cancelled(
        &self,
        notification: CancelledNotificationParam,
        context: NotificationContext<RoleServer>,
    ) -> impl Future<Output = ()> + MaybeSendFuture + '_ {
        self.inner.on_cancelled(notification, context)
    }

    fn on_progress(
        &self,
        notification: ProgressNotificationParam,
        context: NotificationContext<RoleServer>,
    ) -> impl Future<Output = ()> + MaybeSendFuture + '_ {
        self.inner.on_progress(notification, context)
    }

    fn on_initialized(
        &self,
        context: NotificationContext<RoleServer>,
    ) -> impl Future<Output = ()> + MaybeSendFuture + '_ {
        self.inner.on_initialized(context)
    }

    fn on_roots_list_changed(
        &self,
        context: NotificationContext<RoleServer>,
    ) -> impl Future<Output = ()> + MaybeSendFuture + '_ {
        self.inner.on_roots_list_changed(context)
    }

    fn on_custom_notification(
        &self,
        notification: CustomNotification,
        context: NotificationContext<RoleServer>,
    ) -> impl Future<Output = ()> + MaybeSendFuture + '_ {
        self.inner.on_custom_notification(notification, context)
    }

    fn get_info(&self) -> ServerConfig {
        self.inner.get_info()
    }

    fn get_task(
        &self,
        request: GetTaskParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<GetTaskResult, McpError>> + MaybeSendFuture + '_ {
        self.inner.get_task(request, context)
    }

    fn update_task(
        &self,
        request: UpdateTaskParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_ {
        self.inner.update_task(request, context)
    }

    fn cancel_task(
        &self,
        request: CancelTaskParams,
        context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_ {
        self.inner.cancel_task(request, context)
    }
}
