#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use anyhow::{Context, Result};
use tracing::{debug, warn};
use std::sync::Arc;

mod args;
mod cmd_datastore;
mod cmd_generate;
mod cmd_github;
mod cmd_report;
mod cmd_rules;
mod cmd_scan;
mod cmd_summarize;
mod reportable;
mod rule_loader;
mod util;

use args::{CommandLineArgs, GlobalArgs};
use noseyparker::datastore::Datastore;

/// Set up the logging / tracing system for the application.
fn configure_tracing(global_args: &GlobalArgs) -> Result<()> {
    use tracing_log::{AsLog, LogTracer};
    use tracing_subscriber::{filter::LevelFilter, EnvFilter};

    // Set the tracing level according to the `-q`/`--quiet` and `-v`/`--verbose` options
    let level_filter = if global_args.quiet {
        LevelFilter::ERROR
    } else {
        match global_args.verbose {
            0 => LevelFilter::WARN,
            1 => LevelFilter::INFO,
            2 => LevelFilter::DEBUG,
            _ => LevelFilter::TRACE,
        }
    };

    // Configure the bridge from the `log` crate to the `tracing` crate
    LogTracer::builder()
        .with_max_level(level_filter.as_log())
        .init()?;

    // Configure logging filters according to the `NP_LOG` environment variable
    let env_filter = EnvFilter::builder()
        .with_default_directive(level_filter.into())
        .with_env_var("NP_LOG")
        .from_env()
        .context("Failed to parse filters from NP_LOG environment variable")?;

    // Install the global tracing subscriber
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_ansi(global_args.use_color(std::io::stderr()))
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

/// Set the process rlimits according to the global arguments.
fn configure_rlimits(global_args: &GlobalArgs) -> Result<()> {
    use rlimit::Resource;
    use std::cmp::max;

    let nofile_limit = global_args.advanced.rlimit_nofile;
    let (soft, hard) = Resource::NOFILE.get()?;
    let soft = max(soft, nofile_limit);
    let hard = max(hard, nofile_limit);
    Resource::NOFILE.set(soft, hard)?;
    debug!("Set {} limit to ({}, {})", Resource::NOFILE.as_name(), soft, hard);
    Ok(())
}

/// Enable or disable colored output according to the global arguments.
fn configure_color(global_args: &GlobalArgs) {
    console::set_colors_enabled(global_args.use_color(std::io::stdout()));
    console::set_colors_enabled_stderr(global_args.use_color(std::io::stderr()));
}

/// Enable or disable backtraces for the process according to the global arguments.
fn configure_backtraces(global_args: &GlobalArgs) {
    if global_args.advanced.enable_backtraces {
        // Print a stack trace in case of panic.
        // This should have no overhead in normal execution.
        let val = if cfg!(feature = "color_backtrace") {
            "full"
        } else {
            "1"
        };
        std::env::set_var("RUST_BACKTRACE", val);
    }

    #[cfg(feature = "color_backtrace")]
    color_backtrace::install();
}

fn try_main(args: &CommandLineArgs) -> Result<()> {
    let global_args = &args.global_args;

    configure_backtraces(global_args);
    configure_color(global_args);
    configure_tracing(global_args).context("Failed to initialize logging")?;

    if let Err(e) = configure_rlimits(global_args) {
        warn!("Failed to initialize resource limits: {e}");
    }

    // Create a single Datastore instance
    // let datastore = Datastore::new_in_memory().context("Failed to create in-memory datastore")?;

    let datastore = Arc::new(Datastore::new_in_memory()?);
// run(&global_args, &args, datastore.clone())?;
    match &args.command {
        args::Command::Scan(scan_args) => {
            let result = cmd_scan::run(&global_args, scan_args, datastore.clone());
            if result.is_ok() {
                // Automatically run report after successful scan
                let report_args = args::ReportArgs {
                    output_args: args::OutputArgs {
                        format: args::ReportOutputFormat::Human,
                        output: None,
                    },
                    filter_args: args::ReportFilterArgs {
                        max_matches: -1,
                        min_score: 0.0,
                        finding_status: None,
                    },
                };
                cmd_report::run(&global_args, &report_args, &datastore)?; //todo: mick look here
            }
            result
        },
        args::Command::Summarize(summarize_args) => cmd_summarize::run(&global_args, summarize_args, &datastore),
        args::Command::Report(report_args) => cmd_report::run(&global_args, report_args, &datastore),
        args::Command::Generate(generate_args) => cmd_generate::run(&global_args, generate_args),
        args::Command::Datastore(datastore_args) => cmd_datastore::run(&global_args, datastore_args),
        _ => todo!(), // Handle other commands or provide a default case
    }
}
fn main() {
    let args = &CommandLineArgs::parse_args();
    if let Err(e) = try_main(args) {
        // Use the more verbose format that includes a backtrace when running with -vv or higher,
        // otherwise use a more compact one-line error format.
        if args.global_args.verbose > 1 {
            eprintln!("Error: {e:?}");
        } else {
            eprintln!("Error: {e:#}");
        }
        std::process::exit(2);
    }
}
