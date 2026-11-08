//! clap (4.x) integration: [`Tracker::parse`] in place of `Cli::parse()`.
//!
//! - the command that runs: the subcommand chain of the parsed matches ("db migrate"),
//!   recorded under its real name when an alias was typed
//! - help (`--help`, `help <command>`, a group without its subcommand), unknown subcommands and
//!   parse errors: clap's error kinds. Only the kind and a flag name are kept from a parse
//!   error, never the value that was typed
//! - the command list: every leaf command, its arguments and options
//!
//! clap's own output and exit codes don't change: errors are printed and exit as before.

use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{ArgMatches, Command};

use super::{ArgSpec, CommandSpec, OptSpec, Tracker, leading_words};

impl Tracker {
    /// Parses the arguments like `C::parse()`, recording what ran. On a parse error or help,
    /// clap prints and exits as usual, after the run is recorded.
    pub fn parse<C: clap::Parser>(&self) -> C {
        self.parse_from(std::env::args_os())
    }

    /// [`Tracker::parse`] with the arguments given (the first one is the binary).
    pub fn parse_from<C, I, T>(&self, args: I) -> C
    where
        C: clap::Parser,
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let args: Vec<std::ffi::OsString> = args.into_iter().map(Into::into).collect();
        let mut cmd = C::command();
        if !self.enabled() {
            return C::parse_from(args);
        }
        self.commands(clap_commands(&cmd));
        match cmd.try_get_matches_from_mut(args.clone()) {
            Ok(matches) => {
                self.command(&matched_path(&matches));
                match C::from_arg_matches(&matches) {
                    Ok(c) => c,
                    Err(e) => self.clap_exit(e.format(&mut cmd), &cmd, &args),
                }
            }
            Err(e) => self.clap_exit(e, &cmd, &args),
        }
    }

    fn clap_exit(&self, e: clap::Error, cmd: &Command, args: &[std::ffi::OsString]) -> ! {
        let words: Vec<String> = args
            .iter()
            .skip(1)
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        let (path, _) = typed_path(cmd, &leading_words(&words));
        match e.kind() {
            ErrorKind::DisplayHelp | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand => {
                // `help db migrate` is clap's generated help subcommand: the path is what follows it.
                let path = if words.first().is_some_and(|w| w == "help") {
                    typed_path(cmd, &leading_words(&words[1..])).0
                } else {
                    path
                };
                self.help(&path);
            }
            ErrorKind::DisplayVersion => self.command(""),
            ErrorKind::InvalidSubcommand => {
                let typed = match e.get(ContextKind::InvalidSubcommand) {
                    Some(ContextValue::String(s)) => s.clone(),
                    _ => String::new(),
                };
                self.unknown_command(format!("{path} {typed}").trim());
            }
            kind => {
                self.command(&path);
                // The kind and the flag involved: the rendered message can repeat a typed value.
                let flag = match e.get(ContextKind::InvalidArg) {
                    Some(ContextValue::String(s)) => Some(s.as_str()),
                    Some(ContextValue::Strings(v)) => v.first().map(String::as_str),
                    _ => None,
                };
                let arg = match flag {
                    Some(s) if s.starts_with('-') => {
                        format!(": {}", s.split([' ', '=']).next().unwrap_or(s))
                    }
                    _ => String::new(),
                };
                self.error(format!(
                    "{}{arg}",
                    kind.as_str().unwrap_or("invalid arguments")
                ));
            }
        }
        self.finish(e.exit_code());
        e.exit()
    }
}

/// "db migrate" from the matched subcommands; "" for the program itself.
fn matched_path(m: &ArgMatches) -> String {
    let mut parts = vec![];
    let mut cur = m;
    while let Some((name, sub)) = cur.subcommand() {
        parts.push(name.to_string());
        cur = sub;
    }
    parts.join(" ")
}

/// Walks the typed words down the command tree (aliases included): the real path, and how many words it used.
fn typed_path(root: &Command, words: &[String]) -> (String, usize) {
    let mut parts = vec![];
    let mut cur = root;
    for w in words {
        match cur
            .get_subcommands()
            .find(|c| c.get_name() == w || c.get_all_aliases().any(|a| a == w))
        {
            Some(sub) => {
                parts.push(sub.get_name().to_string());
                cur = sub;
            }
            None => break,
        }
    }
    let n = parts.len();
    (parts.join(" "), n)
}

/// The command list: every command that runs something (groups that need a subcommand, help
/// and hidden commands left out), with its arguments and options.
pub fn clap_commands(root: &Command) -> Vec<CommandSpec> {
    let mut out = vec![];
    fn walk(c: &Command, path: &str, out: &mut Vec<CommandSpec>, root_name: &str) {
        let has_subs = c
            .get_subcommands()
            .any(|s| s.get_name() != "help" && !s.is_hide_set());
        if !has_subs {
            out.push(describe(c, if path.is_empty() { root_name } else { path }));
        }
        for sub in c.get_subcommands() {
            if sub.is_hide_set() || sub.get_name() == "help" {
                continue;
            }
            let p = if path.is_empty() {
                sub.get_name().to_string()
            } else {
                format!("{path} {}", sub.get_name())
            };
            walk(sub, &p, out, root_name);
        }
    }
    walk(root, "", &mut out, root.get_name());
    out
}

fn describe(c: &Command, name: &str) -> CommandSpec {
    let text = |s: Option<&clap::builder::StyledStr>| s.map(|s| s.to_string()).unwrap_or_default();
    let mut spec = CommandSpec {
        name: name.to_string(),
        description: text(c.get_about().or(c.get_long_about())),
        aliases: c.get_visible_aliases().map(String::from).collect(),
        ..Default::default()
    };
    for a in c.get_arguments() {
        let id = a.get_id().as_str();
        if a.is_hide_set() || id == "help" || id == "version" {
            continue;
        }
        // A switch's possible values are true/false: not choices anyone types.
        let choices: Vec<String> = if a.get_action().takes_values() {
            a.get_possible_values()
                .iter()
                .filter(|p| !p.is_hide_set())
                .map(|p| p.get_name().to_string())
                .collect()
        } else {
            vec![]
        };
        if a.is_positional() {
            spec.arguments.push(ArgSpec {
                name: a
                    .get_value_names()
                    .and_then(|v| v.first())
                    .map(|v| v.to_lowercase())
                    .unwrap_or_else(|| id.to_string()),
                description: text(a.get_help()),
                required: a.is_required_set(),
                variadic: a
                    .get_num_args()
                    .map(|n| n.max_values() > 1)
                    .unwrap_or(false),
                choices,
            });
        } else {
            let long = a.get_long().map(|l| format!("--{l}"));
            let short = a.get_short().map(|s| format!("-{s}"));
            spec.options.push(OptSpec {
                name: long
                    .clone()
                    .or(short.clone())
                    .unwrap_or_else(|| id.to_string()),
                short: if long.is_some() {
                    short.unwrap_or_default()
                } else {
                    String::new()
                },
                description: text(a.get_help()),
                takes_value: a.get_action().takes_values(),
                required: a.is_required_set(),
                choices,
            });
        }
    }
    spec
}
