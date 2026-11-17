//! Command line of the `baid` binary.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// BAiD: a memory accelerator for AI agents.
#[derive(Debug, Parser)]
#[command(name = "baid", version, about)]
pub struct Cli {
    /// Path to the config file.
    #[arg(
        long,
        short,
        global = true,
        env = "BAID_CONFIG",
        default_value = "baid.toml"
    )]
    pub config: PathBuf,

    /// What to do; `serve` when omitted.
    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the server (the default).
    Serve(ServeArgs),
    /// Load and validate the config and its secrets, then exit.
    CheckConfig,
}

/// Flags for `serve`; they override the config file and environment.
#[derive(Debug, Default, Args)]
pub struct ServeArgs {
    /// Public API address, e.g. 0.0.0.0:7070.
    #[arg(long)]
    pub listen: Option<SocketAddr>,
    /// Admin API address, e.g. 127.0.0.1:7071.
    #[arg(long)]
    pub admin_listen: Option<SocketAddr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_serve_with_default_config() {
        let cli = Cli::try_parse_from(["baid"]).unwrap();
        assert!(cli.command.is_none());
        assert_eq!(cli.config, PathBuf::from("baid.toml"));
    }

    #[test]
    fn serve_flags_and_global_config() {
        let cli = Cli::try_parse_from(["baid", "serve", "--listen", "127.0.0.1:1", "-c", "x.toml"])
            .unwrap();
        assert_eq!(cli.config, PathBuf::from("x.toml"));
        let Some(Command::Serve(a)) = cli.command else {
            panic!("expected serve")
        };
        assert_eq!(a.listen, Some("127.0.0.1:1".parse().unwrap()));
    }

    #[test]
    fn bad_address_is_rejected() {
        assert!(Cli::try_parse_from(["baid", "serve", "--listen", "nope"]).is_err());
    }

    #[test]
    fn check_config_subcommand() {
        let cli = Cli::try_parse_from(["baid", "check-config"]).unwrap();
        assert!(matches!(cli.command, Some(Command::CheckConfig)));
    }
}
