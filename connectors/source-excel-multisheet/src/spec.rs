//! Connector config and its JSON Schema.

use std::path::{Path, PathBuf};

use airbyte_protocol::FailureType;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::client::ConnectorError;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Path to the .xlsx workbook.
    pub path: PathBuf,
    /// Sheets to read. Empty means every sheet.
    #[serde(default)]
    pub sheets: Vec<String>,
    /// 1-based row number that holds the column names, the same for every sheet.
    #[serde(default = "default_header_row")]
    pub header_row: u32,
}

fn default_header_row() -> u32 {
    1
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConnectorError> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            ConnectorError::config(format!("Cannot read config file {}: {e}", path.display()))
        })?;
        let config: Config = serde_json::from_str(&text)
            .map_err(|e| ConnectorError::config(format!("Invalid config: {e}")))?;
        if config.header_row == 0 {
            return Err(ConnectorError::config(
                "header_row starts at 1, the first row of the sheet",
            ));
        }
        Ok(config)
    }
}

pub fn connection_specification() -> Value {
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Excel multisheet source",
        "type": "object",
        "required": ["path"],
        "additionalProperties": true,
        "properties": {
            "path": {
                "type": "string",
                "title": "Path",
                "description": "Path to the .xlsx file.",
                "order": 0
            },
            "sheets": {
                "type": "array",
                "title": "Sheets",
                "description": "Sheets to read, each one becomes a stream. Leave empty to read every sheet.",
                "items": {"type": "string"},
                "default": [],
                "order": 1
            },
            "header_row": {
                "type": "integer",
                "title": "Header row",
                "description": "Row number with the column names, starting at 1. Rows above it are skipped.",
                "minimum": 1,
                "default": 1,
                "order": 2
            }
        }
    })
}

impl ConnectorError {
    pub fn config(message: impl Into<String>) -> Self {
        Self {
            failure_type: FailureType::ConfigError,
            message: message.into(),
        }
    }
}
