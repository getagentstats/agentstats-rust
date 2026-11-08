//! Runs the clap example CLI like a user or agent would, against a fake endpoint, and checks
//! what the background sender posts.
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::Value;

fn binary() -> &'static PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let status = Command::new(env!("CARGO"))
            .args([
                "build",
                "--quiet",
                "--example",
                "shipit",
                "--no-default-features",
                "--features",
                "clap",
            ])
            .status()
            .unwrap();
        assert!(status.success());
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/examples/shipit")
    })
}

/// A one-file HTTP server: collects each POST body, answers 202.
fn server() -> (String, Arc<Mutex<Vec<Value>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/api/ingest/cli", listener.local_addr().unwrap());
    let got = Arc::new(Mutex::new(vec![]));
    let sink = got.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut len = 0;
            let mut auth = String::new();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                    break;
                }
                let lower = line.to_lowercase();
                if let Some(v) = lower.strip_prefix("content-length:") {
                    len = v.trim().parse().unwrap_or(0);
                }
                if lower.starts_with("authorization:") {
                    auth = line.trim().to_string();
                }
            }
            let mut body = vec![0; len];
            reader.read_exact(&mut body).unwrap();
            assert!(auth.ends_with("Bearer al_cli_test"), "{auth}");
            sink.lock()
                .unwrap()
                .push(serde_json::from_slice(&body).unwrap());
            let mut s = stream;
            let _ = s.write_all(
                b"HTTP/1.1 202 Accepted\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
            );
        }
    });
    (url, got)
}

fn run(args: &[&str]) -> (Vec<Value>, Option<Value>, i32, String) {
    let (url, got) = server();
    let dir = std::env::temp_dir().join(format!(
        "agentstats-rs-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let out = Command::new(binary())
        .args(args)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("HOME", &dir)
        .env("AGENTSTATS_CLI_KEY", "al_cli_test")
        .env("AGENTSTATS_CLI_ENDPOINT", &url)
        .env("AGENTSTATS_CLI_DIR", &dir)
        .env("AGENTSTATS_CLI_TMP_DIR", dir.with_extension("tmp"))
        .output()
        .unwrap();
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        let locked = std::fs::read_dir(&dir)
            .map(|d| {
                d.flatten()
                    .any(|e| e.file_name().to_string_lossy().ends_with(".lock"))
            })
            .unwrap_or(false);
        if !got.lock().unwrap().is_empty() && !locked {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let bodies = got.lock().unwrap().clone();
    let events = bodies
        .iter()
        .flat_map(|b| b["events"].as_array().cloned().unwrap_or_default())
        .collect();
    let catalog = bodies.iter().find_map(|b| b.get("catalog").cloned());
    let _ = std::fs::remove_dir_all(&dir);
    (
        events,
        catalog,
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn rand_suffix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

#[test]
fn records_commands_help_unknown_and_errors() {
    let cases: &[(&[&str], &[(&str, Value)])] = &[
        (
            &["deploy", "site-a", "--prod"],
            &[
                ("command", "deploy".into()),
                ("exitCode", 0.into()),
                ("flags", serde_json::json!(["--prod"])),
                ("errorMessage", Value::Null),
            ],
        ),
        (
            &["deploy", "--env", "staging"],
            &[
                ("command", "deploy".into()),
                ("exitCode", 3.into()),
                ("errorMessage", "Staging is locked".into()),
            ],
        ),
        (
            &["st"],
            &[("command", "status".into()), ("exitCode", 0.into())],
        ),
        (
            &["db", "migrate", "--url", "postgres://secret@db"],
            &[
                ("command", "db migrate".into()),
                ("exitCode", 0.into()),
                ("flags", serde_json::json!(["--url"])),
            ],
        ),
        (
            &["db", "migrate"],
            &[
                ("command", "db migrate".into()),
                ("exitCode", 2.into()),
                (
                    "errorMessage",
                    "one or more required arguments were not provided: --url".into(),
                ),
            ],
        ),
        (
            &["db", "seed"],
            &[
                ("command", "db seed".into()),
                ("exitCode", 1.into()),
                ("errorMessage", "seed file missing".into()),
            ],
        ),
        (
            &["deploy", "--preview"],
            &[
                ("command", "deploy".into()),
                ("exitCode", 2.into()),
                (
                    "errorMessage",
                    "unexpected argument found: --preview".into(),
                ),
            ],
        ),
        (
            &["deploy", "--env", "secret-env"],
            &[
                ("command", "deploy".into()),
                ("exitCode", 2.into()),
                (
                    "errorMessage",
                    "one of the values isn't valid for an argument: --env".into(),
                ),
            ],
        ),
        (
            &["deplyo"],
            &[
                ("command", "deplyo".into()),
                ("unknown", true.into()),
                ("exitCode", 2.into()),
                ("errorMessage", Value::Null),
            ],
        ),
        (
            &["db", "push"],
            &[
                ("command", "db push".into()),
                ("unknown", true.into()),
                ("exitCode", 2.into()),
            ],
        ),
        (
            &["postgres://admin:secret@db/prod"],
            &[
                ("command", "(invalid)".into()),
                ("unknown", true.into()),
                ("exitCode", 2.into()),
            ],
        ),
        (
            &["deploy", "--help"],
            &[
                ("command", "deploy".into()),
                ("help", true.into()),
                ("exitCode", 0.into()),
            ],
        ),
        (
            &["help", "db", "migrate"],
            &[
                ("command", "db migrate".into()),
                ("help", true.into()),
                ("exitCode", 0.into()),
            ],
        ),
        (
            &["db"],
            &[
                ("command", "db".into()),
                ("help", true.into()),
                ("exitCode", 2.into()),
            ],
        ),
        (
            &["--help"],
            &[
                ("command", "shipit".into()),
                ("help", true.into()),
                ("exitCode", 0.into()),
            ],
        ),
    ];
    for (args, want) in cases {
        let (events, _, code, stderr) = run(args);
        let name = args.join(" ");
        assert_eq!(events.len(), 1, "{name}: {} events\n{stderr}", events.len());
        for (k, v) in *want {
            assert_eq!(&events[0][*k], v, "{name}: {k}\n{stderr}");
        }
        assert_eq!(
            Some(code),
            want.iter()
                .find(|(k, _)| *k == "exitCode")
                .and_then(|(_, v)| v.as_i64())
                .map(|c| c as i32),
            "{name}: process exit"
        );
        assert!(
            !events[0].to_string().contains("secret"),
            "{name}: argument values left the machine: {}",
            events[0]
        );
        assert_eq!(events[0]["runtime"], "rust");
        assert_eq!(
            events[0]["agent"], "unknown",
            "{name}: no terminal and no agent"
        );
    }
}

#[test]
fn sends_the_command_list() {
    let (_, catalog, _, stderr) = run(&["status"]);
    let catalog = catalog.unwrap_or_else(|| panic!("no command list\n{stderr}"));
    let names: Vec<&str> = catalog["commands"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["deploy", "status", "db migrate", "db seed"]);
    assert_eq!(catalog["version"], "1.0.0");
    assert_eq!(
        catalog["commands"][0],
        serde_json::json!({
            "name": "deploy",
            "description": "Deploy the current directory",
            "arguments": [{ "name": "dir", "description": "Directory to deploy" }],
            "options": [
                { "name": "--env", "short": "-e", "description": "Environment to deploy to", "takesValue": true, "choices": ["production", "staging"] },
                { "name": "--prod", "description": "Deploy to production" }
            ]
        })
    );
    assert_eq!(catalog["commands"][1]["aliases"], serde_json::json!(["st"]));
    assert_eq!(catalog["commands"][2]["options"][0]["required"], true);
}

#[test]
fn publishes_the_command_list_before_exit() {
    let (url, got) = server();
    let dir = std::env::temp_dir().join(format!("agentstats-rs-pub-{}", rand_suffix()));
    let out = Command::new(binary())
        .arg("status")
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("HOME", &dir)
        .env("AGENTSTATS_CLI_KEY", "al_cli_test")
        .env("AGENTSTATS_CLI_PUBLISH_KEY", "al_sec_test")
        .env("AGENTSTATS_CLI_ENDPOINT", &url)
        .env("AGENTSTATS_CLI_DIR", &dir)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("[agentstats] published 4 commands"),
        "{stderr}"
    );
    // Sent before the process exited, with no events of its own.
    let bodies = got.lock().unwrap().clone();
    assert!(
        bodies.iter().any(
            |b| b["catalog"]["commands"].as_array().map(|c| c.len()) == Some(4)
                && b["events"].as_array().map(|e| e.is_empty()) == Some(true)
        ),
        "{bodies:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
