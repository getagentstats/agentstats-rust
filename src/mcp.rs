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
//! returned to your server. Tool arguments and results are never sent. See https://github.com/getagentstats/spec/blob/main/PROTOCOL.md.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rmcp::ErrorData as McpError;
use rmcp::ServerHandler;
#[allow(deprecated)]
use rmcp::model::*;
use rmcp::service::{MaybeSendFuture, NotificationContext, RequestContext, RoleServer, SubscriptionContext};
use serde::Serialize;
use serde_json::{Value, json};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_ENDPOINT: &str = "https://agentstats.io/api/ingest/mcp";
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

/// What's kept of a validated token (https://github.com/getagentstats/spec/blob/main/PROTOCOL.md "Sign-in"): the client app by its Client ID
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
    let client = info.client_id.as_deref().filter(|s| !s.is_empty()).map(|id| {
        id.strip_prefix("https://")
            .and_then(|rest| {
                let (host, path) = rest.split_at(rest.find('/')?);
                let ok = !host.is_empty() && !host.contains('@') && !id.contains('?') && !id.contains('#') && path.len() > 1;
                ok.then(|| format!("{host}{path}").chars().take(200).collect::<String>())
            })
            .unwrap_or_else(|| "registered".into())
    });
    let mut scopes: Vec<String> = info.scopes.iter().filter(|s| !s.is_empty() && s.len() <= 80).cloned().collect();
    scopes.sort();
    scopes.dedup();
    scopes.truncate(20);
    Auth { method: "oauth", client, scopes }
}

/// Configuration. `Default` reads `AGENTSTATS_MCP_KEY` and `AGENTSTATS_ENDPOINT`.
#[derive(Clone, Debug)]
pub struct Options {
    pub api_key: Option<String>,
    pub server_name: Option<String>,
    pub endpoint: String,
    /// Fraction of `tools/call` requests recorded.
    pub sample_rate: f64,
    /// Send error messages. `false` keeps only that the request failed.
    pub capture_errors: bool,
    /// Set with [`Options::capture_errors_with`].
    pub error_text: Option<ErrorText>,
    pub capture_catalog: bool,
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
            endpoint: std::env::var("AGENTSTATS_ENDPOINT")
                .unwrap_or_else(|_| DEFAULT_ENDPOINT.into()),
            sample_rate: 1.0,
            capture_errors: true,
            error_text: None,
            capture_catalog: true,
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
        Some(f) => std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (f.0)(ext))).ok().flatten(),
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
    error_message: Option<String>,
    #[serde(skip)]
    error_code: Option<i32>,
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
        let (events, catalog, name, dropped) = {
            let mut s = self.inner.state.lock().unwrap();
            if s.inflight >= 2 || (s.buf.is_empty() && !s.catalog_dirty) {
                return Ok(());
            }
            let n = s.buf.len().min(self.inner.opts.max_batch);
            let events: Vec<Event> = s.buf.drain(..n).collect();
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
        let result = self
            .inner
            .agent
            .post(&self.inner.opts.endpoint)
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
                !s.buf.is_empty() || s.catalog_dirty
            };
            if !left || self.flush().is_err() {
                return;
            }
        }
    }

    pub fn dropped(&self) -> u64 {
        self.inner.state.lock().unwrap().dropped
    }
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
    fn begin(&self, method: &'static str, tool: Option<String>, context: &RequestContext<RoleServer>) -> Option<Begin> {
        let t = &self.tracker;
        if !t.enabled() || (method == "tools/call" && !t.sample()) {
            return None;
        }
        let client = context.client_info().map(|c| (c.name.to_string(), c.version.to_string()));
        let auth = request_auth(&t.inner.opts, &context.extensions);
        if auth.is_some() {
            *self.auth.lock().unwrap() = auth.clone();
        }
        let auth = auth.or_else(|| self.auth.lock().unwrap().clone());
        Some(Begin { method, tool, client, auth, started: Instant::now() })
    }

    fn end(&self, b: Begin, is_error: bool, error_message: Option<String>, error_code: Option<i32>) {
        let event = Event {
            ts: 0,
            method: b.method.to_string(),
            tool: b.tool,
            duration_ms: 0.0,
            is_error,
            error_message: if is_error { error_message } else { None },
            error_code,
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
                        self.tracker.set_server_info(&r.server_info.name, &r.server_info.version, r.instructions.as_deref());
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
                self.end(b, is_error, message, code);
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
                self.end(b, is_error, message, code);
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
        let begin = self.begin("tools/call", Some(request.name.to_string()), &context);
        async move {
            let result = self.inner.call_tool(request, context).await;
            if let Some(b) = begin {
                let (is_error, message, code) = match &result {
                    Err(e) => (true, Some(e.message.to_string()), Some(e.code.0)),
                    Ok(CallToolResponse::Complete(r)) if r.is_error == Some(true) => (true, r.content.iter().find_map(|c| c.as_text().map(|t| t.text.clone())), None),
                    Ok(_) => (false, None, None),
                };
                self.end(b, is_error, message, code);
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
            let result = self.inner.list_tools(request, context).await;
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
