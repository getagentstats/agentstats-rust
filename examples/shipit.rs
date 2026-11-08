//! A clap CLI with AgentStats wired in, used by tests/cli_clap.rs.
use agentstats::cli::{Options, Tracker};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "shipit", about = "Deploy and run apps")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, ValueEnum)]
enum Env {
    Production,
    Staging,
}

#[derive(Subcommand)]
enum Commands {
    /// Deploy the current directory
    Deploy {
        /// Directory to deploy
        dir: Option<String>,
        /// Environment to deploy to
        #[arg(short, long, value_enum)]
        env: Option<Env>,
        /// Deploy to production
        #[arg(long)]
        prod: bool,
    },
    /// Show the latest deployment
    #[command(visible_alias = "st")]
    Status,
    /// Database commands
    Db {
        #[command(subcommand)]
        command: Db,
    },
    #[command(hide = true)]
    InternalDebug,
}

#[derive(Subcommand)]
enum Db {
    /// Run pending migrations
    Migrate {
        /// Database URL
        #[arg(long)]
        url: String,
    },
    /// Fill the database
    Seed,
}

fn main() {
    let stats = Tracker::new(Options::new("shipit").version("1.0.0").no_notice());
    let cli: Cli = stats.parse();
    let result: Result<(), (i32, String)> = match cli.command {
        Commands::Deploy {
            env: Some(Env::Staging),
            ..
        } => Err((3, "Staging is locked".into())),
        Commands::Db { command: Db::Seed } => Err((1, "seed file missing".into())),
        _ => Ok(()),
    };
    match result {
        Ok(()) => stats.exit(0),
        Err((code, msg)) => {
            eprintln!("error: {msg}");
            stats.error(&msg);
            stats.exit(code)
        }
    }
}
