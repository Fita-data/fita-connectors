//! Airbyte source for Excel workbooks with several sheets. Command parsing only.

mod client;
mod spec;
mod streams;

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use airbyte_protocol::{
    AirbyteMessage, ConfiguredAirbyteCatalog, LogLevel, MessageWriter, StateMessage,
};
use clap::{Parser, Subcommand};

use client::{ConnectorError, Workbook};
use spec::Config;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the config schema.
    Spec,
    /// Check that the workbook and sheets can be read.
    Check {
        #[arg(long)]
        config: PathBuf,
    },
    /// List one stream per sheet.
    Discover {
        #[arg(long)]
        config: PathBuf,
    },
    /// Emit the records of the configured streams.
    Read {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        catalog: PathBuf,
        #[arg(long)]
        state: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mut out = MessageWriter::stdout();
    let result = run(cli.command, &mut out);
    let failed = match result {
        Ok(()) => false,
        Err(e) => {
            let _ = out.write(&AirbyteMessage::error(e.failure_type, e.message));
            true
        }
    };
    if out.flush().is_err() || failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn run<W: Write>(command: Command, out: &mut MessageWriter<W>) -> Result<(), ConnectorError> {
    match command {
        Command::Spec => out.write(&AirbyteMessage::spec(spec::connection_specification()))?,
        Command::Check { config } => {
            let message = match check(&config) {
                Ok(()) => AirbyteMessage::connection_succeeded(),
                Err(e) => AirbyteMessage::connection_failed(e.message),
            };
            out.write(&message)?;
        }
        Command::Discover { config } => {
            let config = Config::load(&config)?;
            let mut workbook = Workbook::open(&config.path)?;
            let mut streams = Vec::new();
            for sheet in workbook.select_sheets(&config.sheets)? {
                streams.push(streams::discover_stream(
                    &mut workbook,
                    &sheet,
                    config.header_row,
                )?);
            }
            out.write(&AirbyteMessage::catalog(streams))?;
        }
        Command::Read {
            config,
            catalog,
            state,
        } => read(&config, &catalog, state.as_deref(), out)?,
    }
    Ok(())
}

fn check(config: &Path) -> Result<(), ConnectorError> {
    let config = Config::load(config)?;
    let mut workbook = Workbook::open(&config.path)?;
    for sheet in workbook.select_sheets(&config.sheets)? {
        let mut rows = workbook.rows(&sheet)?;
        streams::read_header(&mut rows, &sheet, config.header_row)?;
    }
    Ok(())
}

fn read<W: Write>(
    config: &Path,
    catalog: &Path,
    state: Option<&Path>,
    out: &mut MessageWriter<W>,
) -> Result<(), ConnectorError> {
    let config = Config::load(config)?;
    let catalog: ConfiguredAirbyteCatalog = load_json(catalog, "catalog")?;
    let start_rows = match state {
        Some(path) => last_rows(load_json(path, "state")?),
        None => HashMap::new(),
    };
    let mut workbook = Workbook::open(&config.path)?;
    let sheets = workbook.select_sheets(&config.sheets)?;
    for configured in &catalog.streams {
        let sheet = &configured.stream.name;
        if !sheets.contains(sheet) {
            return Err(ConnectorError::config(format!(
                "Stream {sheet} is in the catalog but not in the configured sheets. Run discover again"
            )));
        }
        let start_after = start_rows.get(sheet).copied().unwrap_or(0);
        out.write(&AirbyteMessage::log(
            LogLevel::Info,
            format!("Reading sheet {sheet} after row {start_after}"),
        ))?;
        let count =
            streams::read_stream(&mut workbook, sheet, config.header_row, start_after, out)?;
        out.write(&AirbyteMessage::log(
            LogLevel::Info,
            format!("Read {count} records from sheet {sheet}"),
        ))?;
    }
    Ok(())
}

/// Last emitted row per stream, from the state Airbyte passes back.
fn last_rows(states: Vec<StateMessage>) -> HashMap<String, u32> {
    states
        .into_iter()
        .filter_map(|s| s.stream)
        .filter_map(|s| {
            let row = s.stream_state?.get("last_row")?.as_u64()?;
            Some((s.stream_descriptor.name, u32::try_from(row).ok()?))
        })
        .collect()
}

fn load_json<T: serde::de::DeserializeOwned>(path: &Path, what: &str) -> Result<T, ConnectorError> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        ConnectorError::config(format!("Cannot read {what} file {}: {e}", path.display()))
    })?;
    serde_json::from_str(&text).map_err(|e| ConnectorError::config(format!("Invalid {what}: {e}")))
}
