//! AgentStats analytics for command-line tools: which commands coding agents, people and CI
//! run, and where they fail. A port of the Node SDK (`@agentstats/cli`); events, spool and
//! session files are the same.
//!
//! The CLI never waits on AgentStats. [`Tracker::finish`] appends one line to a spool file in
//! the user's cache directory and starts the CLI's own binary again in the background to send
//! it ([`Tracker::new`] recognises that run and sends instead of starting the CLI). Anything
//! that fails is dropped silently. Argument values are never sent: only the command name and
//! flag names.
//!
//! With clap (feature `clap`), one call replaces `Cli::parse()`:
//!
//! ```ignore
//! let stats = agentstats::cli::Tracker::new(Options::new("acme").version(env!("CARGO_PKG_VERSION")));
//! let cli: Cli = stats.parse();          // records the command, help, unknown commands, parse errors
//! let result = run(cli);
//! if let Err(e) = &result { stats.error(e) }
//! stats.exit(if result.is_ok() { 0 } else { 1 }) // records, then exits
//! ```

use std::collections::HashMap;
use std::fmt::Display;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[cfg(feature = "clap")]
mod clap_support;
#[cfg(feature = "clap")]
pub use clap_support::clap_commands;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
const MAX_SPOOL_BYTES: u64 = 512 * 1024;
/// Marks the background run that sends a spool instead of running the CLI.
const SEND_ENV: &str = "AGENTSTATS_CLI_SEND_SPOOL";

/// A failed command, as passed to [`Options::capture_errors_with`].
#[derive(Clone, Debug)]
pub struct ErrorInfo {
    /// The command path, e.g. "db migrate".
    pub command: String,
    /// What was passed to [`Tracker::error`], as text.
    pub message: String,
    pub exit_code: i32,
}

type ErrorFn = dyn Fn(&ErrorInfo) -> Option<String> + Send + Sync;
type FlagsFn = dyn Fn(&str, &[String]) -> Vec<String> + Send + Sync;

/// A user-supplied capture function, set with [`Options::capture_errors_with`] or
/// [`Options::capture_flags_with`].
pub struct Capture<F: ?Sized>(std::sync::Arc<F>);

impl<F: ?Sized> Clone for Capture<F> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<F: ?Sized> std::fmt::Debug for Capture<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Capture(..)")
    }
}

/// Configuration. Unset fields fall back to environment variables and defaults.
#[derive(Clone, Debug)]
pub struct Options {
    /// Your CLI's name, e.g. "acme".
    pub cli: String,
    pub version: Option<String>,
    /// Defaults to `AGENTSTATS_CLI_KEY`. Without a key the tracker is a no-op.
    pub api_key: Option<String>,
    /// `al_sec_…`, release CI only: replaces the command list on the dashboard before exit.
    /// Defaults to `AGENTSTATS_CLI_PUBLISH_KEY`. Never ship it.
    pub publish_key: Option<String>,
    /// Printed once to stderr on the first run in a terminal. `None` uses the default text.
    pub notice: Option<String>,
    /// Turn the notice off when your CLI already tells users about telemetry.
    pub show_notice: bool,
    /// Send flag names (`--preview`, never values).
    pub capture_flags: bool,
    /// Send the message passed to [`Tracker::error`] (truncated to 300 chars).
    pub capture_errors: bool,
    /// Set with [`Options::capture_errors_with`].
    pub error_text: Option<Capture<ErrorFn>>,
    /// Set with [`Options::capture_flags_with`].
    pub flag_filter: Option<Capture<FlagsFn>>,
    /// Commands by the same actor in the same directory within this window share a session.
    pub session_idle: Duration,
    /// Defaults to `AGENTSTATS_CLI_DIR`, else ~/.cache/agentstats.
    pub dir: Option<PathBuf>,
    pub enabled: bool,
    pub debug: bool,
}

impl Options {
    pub fn new(cli: impl Into<String>) -> Self {
        Self {
            cli: cli.into(),
            version: None,
            api_key: None,
            publish_key: None,
            notice: None,
            show_notice: true,
            capture_flags: true,
            capture_errors: true,
            error_text: None,
            flag_filter: None,
            session_idle: Duration::from_secs(30 * 60),
            dir: None,
            enabled: true,
            debug: false,
        }
    }
    pub fn version(mut self, v: impl Into<String>) -> Self {
        self.version = Some(v.into());
        self
    }
    pub fn api_key(mut self, k: impl Into<String>) -> Self {
        self.api_key = Some(k.into());
        self
    }
    /// Decide per command what is sent of its error: return the text to keep, or `None`.
    /// A panic keeps nothing.
    pub fn capture_errors_with(
        mut self,
        f: impl Fn(&ErrorInfo) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.error_text = Some(Capture(std::sync::Arc::new(f)));
        self
    }
    /// Decide which flag names are sent, given the command and the names found.
    pub fn capture_flags_with(
        mut self,
        f: impl Fn(&str, &[String]) -> Vec<String> + Send + Sync + 'static,
    ) -> Self {
        self.flag_filter = Some(Capture(std::sync::Arc::new(f)));
        self
    }
    /// No first-run notice: your CLI already tells users about telemetry.
    pub fn no_notice(mut self) -> Self {
        self.show_notice = false;
        self
    }
}

/// One command your CLI offers, for the command list. Only `name` is required.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CommandSpec {
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub description: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub aliases: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub arguments: Vec<ArgSpec>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub options: Vec<OptSpec>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ArgSpec {
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub description: String,
    #[serde(skip_serializing_if = "is_false", default)]
    pub required: bool,
    #[serde(skip_serializing_if = "is_false", default)]
    pub variadic: bool,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub choices: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct OptSpec {
    /// Long form when there is one: "--env".
    pub name: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub short: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub description: String,
    #[serde(rename = "takesValue", skip_serializing_if = "is_false", default)]
    pub takes_value: bool,
    #[serde(skip_serializing_if = "is_false", default)]
    pub required: bool,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub choices: Vec<String>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// Who runs the CLI: a coding agent, a person at a terminal, CI, or unknown.
#[derive(Clone, Debug, PartialEq)]
pub struct Actor {
    pub agent: String,
    pub version: Option<String>,
    pub via: Option<String>,
    pub ci: Option<String>,
}

#[derive(Default)]
struct Run {
    name: Option<String>,
    flags: Option<Vec<String>>,
    unknown: bool,
    help: bool,
    error: Option<String>,
    done: bool,
    catalog: Option<Vec<CommandSpec>>,
}

/// Records the one command a CLI run executes. Every method is a no-op when disabled.
pub struct Tracker {
    opts: Options,
    api_key: String,
    endpoint: String,
    enabled: bool,
    actor: Actor,
    started: Instant,
    dirs: Vec<PathBuf>,
    run: Mutex<Run>,
}

impl Tracker {
    /// Creates the tracker. Call it first thing in `main`: when the CLI was started in the
    /// background to send events, this sends them and exits.
    pub fn new(opts: Options) -> Self {
        if let Ok(spool) = std::env::var(SEND_ENV)
            && !spool.is_empty()
        {
            std::process::exit(run_sender(Path::new(&spool)));
        }
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let api_key = opts
            .api_key
            .clone()
            .or_else(|| env("AGENTSTATS_CLI_KEY"))
            .unwrap_or_default();
        let endpoint = crate::ingest_url("cli");
        let mut opts = opts;
        if opts.publish_key.is_none() {
            opts.publish_key = env("AGENTSTATS_CLI_PUBLISH_KEY");
        }
        if env("AGENTSTATS_DEBUG").as_deref() == Some("1") {
            opts.debug = true;
        }
        let dir = opts
            .dir
            .clone()
            .or_else(|| env("AGENTSTATS_CLI_DIR").map(PathBuf::from))
            .unwrap_or_else(default_dir);
        let tmp = env("AGENTSTATS_CLI_TMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("agentstats"));
        let mut dirs = vec![dir];
        if !dirs.contains(&tmp) {
            dirs.push(tmp);
        }
        let actor = detect_actor(&|k| std::env::var(k).ok(), is_terminal());
        let mut t = Self {
            opts,
            api_key,
            endpoint,
            enabled: false,
            actor,
            started: Instant::now(),
            dirs,
            run: Mutex::new(Run::default()),
        };
        t.enabled = !t.api_key.is_empty() && t.opts.enabled && !t.opted_out();
        if t.enabled {
            t.show_notice();
        } else {
            t.log("disabled");
        }
        t
    }

    /// Whether events are recorded: a key is set and the user hasn't opted out.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn actor(&self) -> &Actor {
        &self.actor
    }

    /// The command that is about to run, as a path: "deploy" or "db migrate".
    pub fn command(&self, name: &str) {
        let name: String = name.trim().chars().take(200).collect();
        let name = if name.is_empty() {
            self.opts.cli.clone()
        } else {
            name
        };
        self.with(|r| r.name = Some(name));
    }

    /// Flag names to send instead of the ones read from the arguments.
    pub fn flags(&self, flags: Vec<String>) {
        self.with(|r| r.flags = Some(flags));
    }

    /// A command that doesn't exist, recorded as a failed call. What was typed is only kept
    /// when it looks like a command word: anything else could be a value.
    pub fn unknown_command(&self, name: &str) {
        self.command(&safe_command_name(name));
        self.with(|r| r.unknown = true);
    }

    /// Help was shown, for a command or (with "") for the CLI itself.
    pub fn help(&self, name: &str) {
        self.command(name);
        self.with(|r| r.help = true);
    }

    /// The error that made the command fail. Only its message is kept, as `capture_errors`
    /// and [`Options::capture_errors_with`] decide.
    pub fn error(&self, err: impl Display) {
        if self.opts.capture_errors {
            let msg: String = err.to_string().chars().take(4000).collect();
            self.with(|r| r.error = Some(msg));
        }
    }

    fn error_text(&self, command: &str, message: Option<String>, exit_code: i32) -> Option<String> {
        let message = message?;
        let text = match &self.opts.error_text {
            None => Some(message),
            Some(f) => {
                let info = ErrorInfo {
                    command: command.to_string(),
                    message,
                    exit_code,
                };
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (f.0)(&info)))
                    .unwrap_or_else(|_| {
                        self.log("capture_errors_with panicked");
                        None
                    })
            }
        };
        text.filter(|t| !t.is_empty())
            .map(|t| t.chars().take(300).collect())
    }

    fn flag_list(&self, command: &str, flags: Option<Vec<String>>) -> Vec<String> {
        if !self.opts.capture_flags {
            return vec![];
        }
        let flags =
            flags.unwrap_or_else(|| flag_names(&std::env::args().skip(1).collect::<Vec<_>>()));
        match &self.opts.flag_filter {
            None => flags,
            Some(f) => {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (f.0)(command, &flags)))
                    .unwrap_or_default()
                    .into_iter()
                    .map(|f| f.chars().take(60).collect())
                    .take(30)
                    .collect()
            }
        }
    }

    /// Your CLI's full command list, sent the first time and whenever it changes.
    pub fn commands(&self, list: Vec<CommandSpec>) {
        let list = normalize_catalog(list);
        self.with(|r| r.catalog = Some(list));
    }

    /// Records the run with this exit code, then exits the process.
    pub fn exit(&self, code: i32) -> ! {
        self.finish(code);
        std::process::exit(code)
    }

    /// Persists the user's choice, e.g. from `acme telemetry disable`, for the next run.
    pub fn set_enabled(&self, on: bool) {
        let name = format!("disabled-{}", slug(&self.opts.cli));
        for dir in &self.dirs {
            if on {
                let _ = fs::remove_file(dir.join(&name));
            } else if !self.dirs[0].join(&name).exists() {
                let _ = fs::create_dir_all(dir);
                let _ = fs::write(dir.join(&name), "");
            }
        }
    }

    /// Records the run: one append to the spool, then a background sender. Never blocks on the
    /// network (except with a publish key, in release CI) and never panics. Once per run.
    pub fn finish(&self, exit_code: i32) {
        if !self.enabled {
            return;
        }
        let run = {
            let mut r = self.run.lock().unwrap_or_else(|e| e.into_inner());
            if r.done {
                return;
            }
            r.done = true;
            std::mem::take(&mut *r)
        };
        let now = now_ms();
        let command = run.name.clone().unwrap_or_else(|| self.opts.cli.clone());
        let flags = self.flag_list(&command, run.flags.clone());
        let error = if exit_code != 0 || run.unknown {
            self.error_text(&command, run.error.clone(), exit_code)
        } else {
            None
        };
        let mut event = json!({
            "ts": now,
            "cli": self.opts.cli,
            "cliVersion": self.opts.version,
            "command": command,
            "flags": flags,
            "exitCode": exit_code,
            "durationMs": self.started.elapsed().as_millis() as u64,
            "unknown": run.unknown,
            "help": run.help,
            "errorMessage": error,
            "agent": self.actor.agent,
            "agentVersion": self.actor.version,
            "via": self.actor.via,
            "ci": self.actor.ci,
            "sessionId": "",
            "os": os_name(),
            "arch": arch_name(),
            "runtime": "rust",
        });
        let spool_name = format!("spool-{}.jsonl", hash(&self.api_key));
        for dir in &self.dirs {
            match self.write(dir, &mut event, now, &spool_name, &run.catalog) {
                Ok(()) => break,
                Err(e) => self.log(&format!("cannot write to {}: {e}", dir.display())),
            }
        }
        self.publish_catalog(&run.catalog);
        if truthy(
            std::env::var("CODEX_SANDBOX_NETWORK_DISABLED")
                .ok()
                .as_deref(),
        ) {
            return;
        }
        for dir in &self.dirs {
            let spool = dir.join(&spool_name);
            if !spool.exists() && !with_suffix(&spool, ".catalog.json").exists() {
                continue;
            }
            if let Err(e) = self.spawn_sender(&spool) {
                self.log(&format!("could not start sender: {e}"));
            }
        }
    }

    fn with(&self, f: impl FnOnce(&mut Run)) {
        if self.enabled {
            f(&mut self.run.lock().unwrap_or_else(|e| e.into_inner()));
        }
    }

    fn write(
        &self,
        dir: &Path,
        event: &mut Value,
        now: u64,
        spool_name: &str,
        catalog: &Option<Vec<CommandSpec>>,
    ) -> std::io::Result<()> {
        fs::create_dir_all(dir)?;
        event["sessionId"] = json!(self.session(dir, now)?);
        let spool = dir.join(spool_name);
        if fs::metadata(&spool)
            .map(|m| m.len() > MAX_SPOOL_BYTES)
            .unwrap_or(false)
        {
            self.log("spool full, dropped");
        } else {
            let mut f = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&spool)?;
            f.write_all(format!("{event}\n").as_bytes())?;
        }
        if let Some(list) = catalog {
            let body =
                json!({ "cli": self.opts.cli, "version": self.opts.version, "commands": list });
            let sig = hash(&body.to_string());
            if fs::read_to_string(with_suffix(&spool, ".catalog.sent"))
                .ok()
                .as_deref()
                != Some(sig.as_str())
            {
                let mut queued = body;
                queued["sig"] = json!(sig);
                fs::write(with_suffix(&spool, ".catalog.json"), queued.to_string())?;
            }
        }
        Ok(())
    }

    /// The agent's thread when it exposes one, else the same actor in the same directory with no long gap.
    fn session(&self, dir: &Path, now: u64) -> std::io::Result<String> {
        if let Some(thread) = agent_thread(&|k| std::env::var(k).ok()) {
            return Ok(thread_session(&self.actor.agent, &thread));
        }
        let cwd = std::env::current_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let k = hash(&format!("{}|{}|{cwd}", self.opts.cli, self.actor.agent));
        let file = dir.join("sessions.json");
        let mut state: HashMap<String, (String, u64)> = fs::read_to_string(&file)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        let idle = self.opts.session_idle.as_millis() as u64;
        let id = match state.get(&k) {
            Some((id, last)) if now.saturating_sub(*last) < idle => id.clone(),
            _ => hash(&format!("{k}|{now}|{}", rand::random::<u64>())),
        };
        state.insert(k, (id.clone(), now));
        if state.len() > 200 {
            let mut all: Vec<_> = state.iter().map(|(k, v)| (k.clone(), v.1)).collect();
            all.sort_by_key(|x| std::cmp::Reverse(x.1));
            for (k, _) in all.into_iter().skip(200) {
                state.remove(&k);
            }
        }
        fs::write(&file, serde_json::to_string(&state).unwrap_or_default())?;
        Ok(id)
    }

    fn publish_catalog(&self, catalog: &Option<Vec<CommandSpec>>) {
        let (Some(key), Some(list)) = (&self.opts.publish_key, catalog) else {
            return;
        };
        let body = json!({ "cli": self.opts.cli, "events": [], "catalog": { "version": self.opts.version, "commands": list } });
        match post(
            &self.endpoint,
            &self.api_key,
            Some(key),
            &body.to_string(),
            Duration::from_secs(15),
        ) {
            Ok(s) if s < 300 => eprintln!("[agentstats] published {} commands", list.len()),
            Ok(s) => eprintln!("[agentstats] command list not published: HTTP {s}"),
            Err(e) => eprintln!("[agentstats] command list not published: {e}"),
        }
    }

    /// Starts this binary again, detached, to send the spool; unless one started in the last minute.
    fn spawn_sender(&self, spool: &Path) -> std::io::Result<()> {
        let lock = with_suffix(spool, ".lock");
        if let Ok(m) = fs::metadata(&lock).and_then(|m| m.modified())
            && m.elapsed()
                .map(|e| e < Duration::from_secs(60))
                .unwrap_or(false)
        {
            return Ok(());
        }
        let exe = std::env::current_exe()?;
        fs::write(&lock, now_ms().to_string())?;
        let mut cmd = Process::new(exe);
        cmd.env(SEND_ENV, spool)
            .env("AGENTSTATS_SEND_ENDPOINT", &self.endpoint)
            .env("AGENTSTATS_SEND_KEY", &self.api_key)
            .env(
                "AGENTSTATS_SEND_DEBUG",
                if self.opts.debug { "1" } else { "" },
            )
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(if self.opts.debug {
                std::process::Stdio::inherit()
            } else {
                std::process::Stdio::null()
            });
        detach(&mut cmd);
        if let Err(e) = cmd.spawn() {
            let _ = fs::remove_file(&lock);
            return Err(e);
        }
        Ok(())
    }

    fn opted_out(&self) -> bool {
        let env = |k: &str| std::env::var(k).ok();
        if truthy(env("DO_NOT_TRACK").as_deref())
            || truthy(env("AGENTSTATS_DISABLED").as_deref())
            || truthy(env(&opt_out_variable(&self.opts.cli)).as_deref())
        {
            return true;
        }
        let name = format!("disabled-{}", slug(&self.opts.cli));
        self.dirs.iter().any(|d| d.join(&name).exists())
    }

    fn show_notice(&self) {
        if !self.opts.show_notice || !stderr_is_terminal() {
            return;
        }
        for dir in &self.dirs {
            let marker = dir.join(format!("notice-{}", slug(&self.opts.cli)));
            if marker.exists() {
                return;
            }
            if fs::create_dir_all(dir)
                .and_then(|_| fs::write(&marker, now_ms().to_string()))
                .is_err()
            {
                continue;
            }
            let text = self.opts.notice.clone().unwrap_or_else(|| {
                format!(
                    "{} collects anonymous usage data: which commands run, how long they take and whether they fail. Never arguments or file contents.\nTurn it off with {}=1 or DO_NOT_TRACK=1.",
                    self.opts.cli,
                    opt_out_variable(&self.opts.cli)
                )
            });
            eprintln!("\n{text}\n");
            return;
        }
    }

    fn log(&self, msg: &str) {
        if self.opts.debug {
            eprintln!("[agentstats] {msg}");
        }
    }
}

#[derive(Deserialize)]
struct QueuedCatalog {
    sig: String,
    cli: String,
    version: Option<String>,
    commands: Value,
}

/// The background run: takes the spool by renaming it, posts it in batches, and puts lines back
/// for the next run only when the network fails.
fn run_sender(spool: &Path) -> i32 {
    let endpoint = std::env::var("AGENTSTATS_SEND_ENDPOINT").unwrap_or_default();
    let key = std::env::var("AGENTSTATS_SEND_KEY").unwrap_or_default();
    let debug = std::env::var("AGENTSTATS_SEND_DEBUG").as_deref() == Ok("1");
    let log = |m: String| {
        if debug {
            eprintln!("[agentstats sender] {m}")
        }
    };
    let pending = with_suffix(spool, ".catalog.json");
    let read_catalog = || {
        fs::read_to_string(&pending)
            .ok()
            .and_then(|s| serde_json::from_str::<QueuedCatalog>(&s).ok())
    };
    for _ in 0..5 {
        let taken = with_suffix(spool, &format!(".{}.sending", std::process::id()));
        let took = fs::rename(spool, &taken).is_ok();
        let events: Vec<Value> = if took {
            fs::read_to_string(&taken)
                .unwrap_or_default()
                .lines()
                .filter_map(|l| serde_json::from_str(l).ok())
                .collect()
        } else {
            vec![]
        };
        let mut catalog = read_catalog();
        if events.is_empty() && catalog.is_none() {
            if took {
                let _ = fs::remove_file(&taken);
            }
            break;
        }
        let mut failed: Option<usize> = None;
        let mut i = 0;
        while (i < events.len() || catalog.is_some()) && failed.is_none() {
            let batch = &events[i.min(events.len())..(i + 500).min(events.len())];
            let cli = batch
                .first()
                .and_then(|e| e["cli"].as_str().map(String::from))
                .or_else(|| catalog.as_ref().map(|c| c.cli.clone()))
                .unwrap_or_default();
            let mut body = json!({ "cli": cli, "events": batch });
            if let Some(c) = &catalog {
                body["catalog"] = json!({ "version": c.version, "commands": c.commands });
            }
            match post(
                &endpoint,
                &key,
                None,
                &body.to_string(),
                Duration::from_secs(10),
            ) {
                Err(e) => {
                    log(format!("failed: {e}"));
                    failed = Some(i);
                }
                // 429 is "later", like a 5xx: keep the batch for the next run. A 4xx is final.
                Ok(s) if s >= 500 || s == 429 => failed = Some(i),
                Ok(s) => {
                    log(format!("sent {} -> {s}", batch.len()));
                    if let Some(c) = catalog.take() {
                        let _ = fs::write(with_suffix(spool, ".catalog.sent"), &c.sig);
                        if read_catalog().map(|x| x.sig == c.sig).unwrap_or(false) {
                            let _ = fs::remove_file(&pending);
                        }
                    }
                }
            }
            i += 500;
        }
        if let Some(from) = failed
            && events.len() > from
            && let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(spool)
        {
            for e in &events[from..] {
                let _ = writeln!(f, "{e}");
            }
        }
        if took {
            let _ = fs::remove_file(&taken);
        }
        if failed.is_some() {
            break;
        }
    }
    let _ = fs::remove_file(with_suffix(spool, ".lock"));
    0
}

fn post(
    endpoint: &str,
    key: &str,
    publish_key: Option<&str>,
    body: &str,
    timeout: Duration,
) -> Result<u16, String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .build()
        .into();
    let mut req = agent
        .post(endpoint)
        .header("Content-Type", "application/json")
        .header("Authorization", &format!("Bearer {key}"))
        .header("X-AgentStats-SDK", &format!("cli-rust/{VERSION}"));
    if let Some(p) = publish_key {
        req = req.header("X-AgentStats-Publish-Key", p);
    }
    req.send(body)
        .map(|r| r.status().as_u16())
        .map_err(|e| e.to_string())
}

#[cfg(unix)]
fn detach(cmd: &mut Process) {
    use std::os::unix::process::CommandExt;
    // Its own process group, so it outlives the CLI and isn't killed with the terminal's job.
    cmd.process_group(0);
}

#[cfg(windows)]
fn detach(cmd: &mut Process) {
    use std::os::windows::process::CommandExt;
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
}

#[cfg(not(any(unix, windows)))]
fn detach(_: &mut Process) {}

fn is_terminal() -> bool {
    use std::io::IsTerminal;
    std::io::stdin().is_terminal() || std::io::stderr().is_terminal()
}

fn stderr_is_terminal() -> bool {
    use std::io::IsTerminal;
    std::io::stderr().is_terminal()
}

const AGENTS: &[(&str, &[&str])] = &[
    ("amp", &["AMP_CURRENT_THREAD_ID"]),
    ("claude-code", &["CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT"]),
    ("cursor", &["CURSOR_AGENT"]),
    ("gemini-cli", &["GEMINI_CLI"]),
    ("codex", &["CODEX_SANDBOX", "CODEX_THREAD_ID"]),
    ("copilot-cli", &["COPILOT_CLI"]),
    ("opencode", &["OPENCODE_CLIENT", "OPENCODE"]),
    ("augment", &["AUGMENT_AGENT"]),
    ("antigravity", &["ANTIGRAVITY_AGENT"]),
];

const CI: &[(&str, &[&str])] = &[
    ("github-actions", &["GITHUB_ACTIONS"]),
    ("gitlab", &["GITLAB_CI"]),
    ("circleci", &["CIRCLECI"]),
    ("buildkite", &["BUILDKITE"]),
    ("jenkins", &["JENKINS_URL"]),
    ("azure-pipelines", &["TF_BUILD"]),
    ("bitbucket", &["BITBUCKET_BUILD_NUMBER"]),
    ("travis", &["TRAVIS"]),
    ("vercel", &["VERCEL"]),
    ("netlify", &["NETLIFY"]),
];

fn truthy(v: Option<&str>) -> bool {
    matches!(v, Some(v) if !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false"))
}

/// Thread or conversation id an agent exposes, to group its commands into one session.
pub fn agent_thread(env: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    [
        "CLAUDE_CODE_SESSION_ID",
        "CODEX_THREAD_ID",
        "AMP_CURRENT_THREAD_ID",
    ]
    .iter()
    .find_map(|k| env(k).filter(|v| !v.is_empty()))
}

/// `AI_AGENT` as agents set it: `claude-code_2-1-316_agent` or `devin@1`.
pub fn parse_ai_agent(value: &str) -> Option<(String, Option<String>)> {
    let v = value.trim().to_lowercase();
    if v.is_empty() {
        return None;
    }
    let at = v.find(['@', '_']);
    let agent: String = v[..at.unwrap_or(v.len())]
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '-')
        .take(40)
        .collect();
    if agent.is_empty() {
        return None;
    }
    let version = at.and_then(|at| {
        let raw = v[at + 1..].split('_').next().unwrap_or("");
        if !raw.starts_with(|c: char| c.is_ascii_digit()) {
            return None;
        }
        let raw = if v.as_bytes()[at] == b'_' {
            raw.replace('-', ".")
        } else {
            raw.to_string()
        };
        let clean: String = raw
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '+' | '-'))
            .take(40)
            .collect();
        (!clean.is_empty()).then_some(clean)
    });
    Some((agent, version))
}

pub fn ci_provider(env: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    for (provider, vars) in CI {
        if vars.iter().any(|v| truthy(env(v).as_deref())) {
            return Some((*provider).into());
        }
    }
    (truthy(env("CI").as_deref()) || truthy(env("CONTINUOUS_INTEGRATION").as_deref()))
        .then(|| "ci".into())
}

/// Who runs the CLI, from the environment. Hints, not proof: never use them to authorize anything.
pub fn detect_actor(env: &dyn Fn(&str) -> Option<String>, interactive: bool) -> Actor {
    let ci = ci_provider(env);
    // AI_AGENT is the proposed standard (Claude Code sets it); it wins over inferred signals.
    if let Some((agent, version)) = env("AI_AGENT").and_then(|v| parse_ai_agent(&v)) {
        return Actor {
            agent,
            version,
            via: Some("AI_AGENT".into()),
            ci,
        };
    }
    for (agent, vars) in AGENTS {
        if let Some(v) = vars.iter().find(|v| truthy(env(v).as_deref())) {
            return Actor {
                agent: (*agent).into(),
                version: None,
                via: Some((*v).into()),
                ci,
            };
        }
    }
    if ci.is_some() {
        return Actor {
            agent: "ci".into(),
            version: None,
            via: None,
            ci,
        };
    }
    if interactive {
        return Actor {
            agent: "human".into(),
            version: None,
            via: Some("tty".into()),
            ci: None,
        };
    }
    Actor {
        agent: "unknown".into(),
        version: None,
        via: None,
        ci: None,
    }
}

/// `ACME_TELEMETRY_DISABLED` for a CLI named "acme".
pub fn opt_out_variable(cli: &str) -> String {
    format!(
        "{}_TELEMETRY_DISABLED",
        slug(cli).to_uppercase().replace('-', "_")
    )
}

/// Flag names from argv: `--env=prod` becomes `--env`, values are skipped, `-pSECRET` keeps only `-p`. Stops at `--`.
pub fn flag_names(argv: &[String]) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    for a in argv {
        if a == "--" {
            break;
        }
        let name = if a.starts_with("--") && a.len() > 2 {
            Some(
                a.split('=')
                    .next()
                    .unwrap_or(a)
                    .chars()
                    .take(60)
                    .collect::<String>(),
            )
        } else if a.len() >= 2 && a.starts_with('-') && a.as_bytes()[1].is_ascii_alphabetic() {
            Some(a[..2].to_string())
        } else {
            None
        };
        if let Some(n) = name
            && !out.contains(&n)
        {
            out.push(n);
        }
    }
    out.truncate(30);
    out
}

/// Keeps "db push"; "postgres://user:secret@host" or a pasted sentence becomes "(invalid)".
pub fn safe_command_name(name: &str) -> String {
    let words: Vec<&str> = name.split_whitespace().collect();
    let word_ok = |w: &&str| {
        let mut chars = w.chars();
        w.len() <= 40
            && chars.next().is_some_and(|c| c.is_ascii_alphabetic())
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '.' | '-'))
    };
    if words.is_empty() || words.len() > 4 || !words.iter().all(word_ok) {
        return "(invalid)".into();
    }
    words.join(" ")
}

/// Words typed before the first flag, stopping at `--`: candidates for a command path.
pub fn leading_words(argv: &[String]) -> Vec<String> {
    argv.iter()
        .take_while(|a| !a.starts_with('-'))
        .cloned()
        .collect()
}

/// Trims a command list to what the server stores, dropping empty fields and duplicates.
pub fn normalize_catalog(list: Vec<CommandSpec>) -> Vec<CommandSpec> {
    let cut = |s: &str, n: usize| s.trim().chars().take(n).collect::<String>();
    let mut seen = std::collections::HashSet::new();
    let mut out = vec![];
    for mut c in list {
        c.name = cut(&c.name, 200);
        if c.name.is_empty() || !seen.insert(c.name.clone()) {
            continue;
        }
        c.description = cut(&c.description, 2000);
        c.aliases = c
            .aliases
            .iter()
            .map(|a| cut(a, 60))
            .filter(|a| !a.is_empty())
            .take(10)
            .collect();
        c.arguments.truncate(30);
        c.options.truncate(60);
        for a in &mut c.arguments {
            a.description = cut(&a.description, 1000);
        }
        for o in &mut c.options {
            o.description = cut(&o.description, 1000);
        }
        out.push(c);
        if out.len() >= 500 {
            break;
        }
    }
    out
}

fn default_dir() -> PathBuf {
    let env = |k: &str| {
        std::env::var(k)
            .ok()
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    };
    let base = env("XDG_CACHE_HOME")
        .or_else(|| {
            if cfg!(windows) {
                env("LOCALAPPDATA")
            } else {
                None
            }
        })
        .or_else(|| {
            env("HOME")
                .or_else(|| env("USERPROFILE"))
                .map(|h| h.join(".cache"))
        })
        .unwrap_or_else(std::env::temp_dir);
    base.join("agentstats")
}

/// Node's process.platform / process.arch names, so the dashboard groups runs the same way.
fn os_name() -> &'static str {
    match std::env::consts::OS {
        "windows" => "win32",
        "macos" => "darwin",
        other => other,
    }
}

fn arch_name() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "x64",
        "aarch64" => "arm64",
        "x86" => "ia32",
        other => other,
    }
}

/// Session id of an agent's thread: the first 16 hex chars of SHA-256("agent|thread"), the same in
/// every AgentStats SDK, so a thread's CLI commands and API calls (agent_headers) line up.
pub fn thread_session(agent: &str, thread: &str) -> String {
    sha256_hex(format!("{agent}|{thread}").as_bytes())[..16].to_string()
}

/// Headers that tell your API a coding agent is making the call, for your API client or SDK:
/// `X-AI-Agent` (`claude-code/2.1.316`) and `X-AI-Agent-Run`. Empty unless a coding agent
/// (Claude Code, Cursor, Codex…) runs this process, so a deployed app sends nothing. Hints, not
/// proof: never authorize with them. The AgentStats API middleware turns them into sessions.
///
/// ```ignore
/// for (k, v) in agentstats::cli::agent_headers() { req = req.header(k, v); }
/// ```
pub fn agent_headers() -> Vec<(&'static str, String)> {
    agent_headers_from(
        &|k| std::env::var(k).ok().filter(|v| !v.is_empty()),
        &mut |k, v| unsafe { std::env::set_var(k, v) },
    )
}

/// [`agent_headers`] with the environment passed in; `set` keeps a new run id (`AGENTSTATS_RUN_ID`)
/// so child processes inherit it.
pub fn agent_headers_from(
    env: &dyn Fn(&str) -> Option<String>,
    set: &mut dyn FnMut(&str, &str),
) -> Vec<(&'static str, String)> {
    let actor = detect_actor(env, false);
    if matches!(actor.agent.as_str(), "human" | "ci" | "unknown" | "") {
        return Vec::new();
    }
    let name = match &actor.version {
        Some(v) => format!("{}/{v}", actor.agent),
        None => actor.agent.clone(),
    };
    let run = match agent_thread(env) {
        Some(thread) => thread_session(&actor.agent, &thread),
        None => env("AGENTSTATS_RUN_ID").unwrap_or_else(|| {
            let id = format!("{:016x}", rand::random::<u64>());
            set("AGENTSTATS_RUN_ID", &id);
            id
        }),
    };
    vec![("X-AI-Agent", name), ("X-AI-Agent-Run", run)]
}

/// SHA-256 (FIPS 180-4), hex. Small inputs only; avoids a crypto dependency.
fn sha256_hex(data: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&((data.len() as u64) * 8).to_be_bytes());
    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[4 * i],
                chunk[4 * i + 1],
                chunk[4 * i + 2],
                chunk[4 * i + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (x, y) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *x = x.wrapping_add(y);
        }
    }
    h.iter().map(|x| format!("{x:08x}")).collect()
}

fn hash(s: &str) -> String {
    // FNV-1a, 64-bit, twice with different offsets: a stable 16-hex-char id without a crypto dependency.
    let fnv = |seed: u64| {
        s.bytes()
            .fold(seed, |h, b| (h ^ b as u64).wrapping_mul(0x100_0000_01b3))
    };
    format!(
        "{:08x}{:08x}",
        fnv(0xcbf2_9ce4_8422_2325) as u32,
        fnv(0x8422_2325_cbf2_9ce4) as u32
    )
}

fn slug(s: &str) -> String {
    let mut out = String::new();
    for c in s.to_lowercase().chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c);
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    let out = out.trim_matches('-').to_string();
    if out.is_empty() { "cli".into() } else { out }
}

fn with_suffix(p: &Path, suffix: &str) -> PathBuf {
    let mut s = p.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    #[test]
    fn sha256_matches_the_standard() {
        assert_eq!(
            super::sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            super::sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        // Same as threadSession() in @agentstats/cli and the Python and Go SDKs.
        assert_eq!(
            super::thread_session("claude-code", "thread-1"),
            "5598cc3f53e4c2b5"
        );
    }

    #[test]
    fn agent_headers_are_empty_unless_an_agent_runs() {
        use std::collections::HashMap;
        let run = |vars: &[(&str, &str)]| {
            let mut m: HashMap<String, String> = vars
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let snapshot = m.clone();
            let mut set_vars = Vec::new();
            let h = super::agent_headers_from(&|k| snapshot.get(k).cloned(), &mut |k, v| {
                set_vars.push((k.to_string(), v.to_string()))
            });
            for (k, v) in set_vars {
                m.insert(k, v);
            }
            (h, m)
        };
        assert!(run(&[]).0.is_empty());
        assert!(
            run(&[("CI", "true"), ("GITHUB_ACTIONS", "true")])
                .0
                .is_empty()
        );
        let (h, _) = run(&[
            ("AI_AGENT", "claude-code_2-1-316_agent"),
            ("CLAUDE_CODE_SESSION_ID", "thread-1"),
        ]);
        assert_eq!(
            h,
            vec![
                ("X-AI-Agent", "claude-code/2.1.316".to_string()),
                (
                    "X-AI-Agent-Run",
                    super::thread_session("claude-code", "thread-1")
                )
            ]
        );
        let (h, env) = run(&[("CODEX_SANDBOX", "seatbelt")]);
        assert_eq!(h[1].1.len(), 16);
        assert_eq!(env.get("AGENTSTATS_RUN_ID"), Some(&h[1].1));
    }

    use super::*;

    #[test]
    fn names_and_flags() {
        assert_eq!(safe_command_name("db push"), "db push");
        assert_eq!(safe_command_name("postgres://admin:secret@db"), "(invalid)");
        let argv: Vec<String> = [
            "deploy",
            "--env=prod",
            "-pSECRET",
            "--prod",
            "x",
            "--",
            "--after",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(flag_names(&argv), vec!["--env", "-p", "--prod"]);
        assert_eq!(opt_out_variable("my cli"), "MY_CLI_TELEMETRY_DISABLED");
    }

    #[test]
    fn actors() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        assert_eq!(
            detect_actor(&env(&[("AI_AGENT", "claude-code_2-1-316_agent")]), false)
                .version
                .as_deref(),
            Some("2.1.316")
        );
        assert_eq!(
            detect_actor(
                &env(&[("CURSOR_AGENT", "1"), ("GITHUB_ACTIONS", "true")]),
                false
            )
            .ci
            .as_deref(),
            Some("github-actions")
        );
        assert_eq!(detect_actor(&env(&[]), true).agent, "human");
        assert_eq!(detect_actor(&env(&[]), false).agent, "unknown");
    }
}

#[cfg(test)]
mod capture_tests {
    use super::*;

    fn tracker(opts: Options) -> Tracker {
        Tracker::new(opts)
    }

    #[test]
    fn capture_functions_decide_what_is_kept() {
        let t = tracker(
            Options::new("acme")
                .capture_errors_with(|e| {
                    e.message
                        .split(':')
                        .next()
                        .map(|s| format!("{}: {s} (exit {})", e.command, e.exit_code))
                })
                .capture_flags_with(|_, flags| {
                    flags.iter().filter(|f| *f != "--token").cloned().collect()
                }),
        );
        assert_eq!(
            t.error_text(
                "db seed",
                Some("duplicate key: Key (email)=(a@b.co)".into()),
                1
            )
            .as_deref(),
            Some("db seed: duplicate key (exit 1)")
        );
        assert_eq!(
            t.flag_list("db seed", Some(vec!["--token".into(), "--env".into()])),
            vec!["--env".to_string()]
        );

        let panics = tracker(
            Options::new("acme")
                .capture_errors_with(|_| panic!("bug"))
                .capture_flags_with(|_, _| panic!("bug")),
        );
        assert_eq!(panics.error_text("x", Some("secret".into()), 1), None);
        assert!(panics.flag_list("x", Some(vec!["--env".into()])).is_empty());
    }
}
