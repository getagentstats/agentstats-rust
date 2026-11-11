use std::sync::Mutex;

static ENV: Mutex<()> = Mutex::new(());

/// Runs `make` with `AGENTSTATS_URL` (the SDK's undocumented override) set to `url`. A tracker reads
/// it once, when it's created, and tests run in parallel, so the variable is held for one tracker
/// at a time and put back afterwards.
pub fn sending_to<T>(url: &str, make: impl FnOnce() -> T) -> T {
    let _held = ENV.lock().unwrap_or_else(|e| e.into_inner());
    let before = std::env::var("AGENTSTATS_URL").ok();
    // SAFETY: only these tests change the environment, one at a time under ENV.
    unsafe { std::env::set_var("AGENTSTATS_URL", url) };
    let made = make();
    match before {
        Some(v) => unsafe { std::env::set_var("AGENTSTATS_URL", v) },
        None => unsafe { std::env::remove_var("AGENTSTATS_URL") },
    }
    made
}
