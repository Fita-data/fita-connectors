//! Airbyte protocol message types and a writer that emits one message per line.
//!
//! Only the parts of the protocol that the connectors in this repository use are
//! modelled. Optional fields are skipped when empty so the output stays compact.

use std::io::{self, BufWriter, Stdout, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single line of connector output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AirbyteMessage {
    Spec { spec: ConnectorSpecification },
    ConnectionStatus { connection_status: ConnectionStatus },
    Catalog { catalog: AirbyteCatalog },
    Record { record: RecordMessage },
    State { state: StateMessage },
    Log { log: LogMessage },
    Trace { trace: TraceMessage },
}

impl AirbyteMessage {
    pub fn spec(connection_specification: Value) -> Self {
        Self::Spec {
            spec: ConnectorSpecification {
                connection_specification,
                documentation_url: None,
            },
        }
    }

    pub fn connection_succeeded() -> Self {
        Self::ConnectionStatus {
            connection_status: ConnectionStatus {
                status: Status::Succeeded,
                message: None,
            },
        }
    }

    pub fn connection_failed(message: impl Into<String>) -> Self {
        Self::ConnectionStatus {
            connection_status: ConnectionStatus {
                status: Status::Failed,
                message: Some(message.into()),
            },
        }
    }

    pub fn catalog(streams: Vec<AirbyteStream>) -> Self {
        Self::Catalog {
            catalog: AirbyteCatalog { streams },
        }
    }

    pub fn record(stream: impl Into<String>, data: Value) -> Self {
        Self::Record {
            record: RecordMessage {
                stream: stream.into(),
                data,
                emitted_at: now_millis(),
                namespace: None,
            },
        }
    }

    pub fn stream_state(stream: impl Into<String>, stream_state: Value) -> Self {
        Self::State {
            state: StateMessage::stream(stream, stream_state),
        }
    }

    pub fn log(level: LogLevel, message: impl Into<String>) -> Self {
        Self::Log {
            log: LogMessage {
                level,
                message: message.into(),
                stack_trace: None,
            },
        }
    }

    pub fn error(failure_type: FailureType, message: impl Into<String>) -> Self {
        Self::Trace {
            trace: TraceMessage {
                trace_type: TraceType::Error,
                emitted_at: now_millis() as f64,
                error: Some(ErrorTrace {
                    message: message.into(),
                    internal_message: None,
                    stack_trace: None,
                    failure_type: Some(failure_type),
                }),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorSpecification {
    /// JSON Schema of the connector config.
    pub connection_specification: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documentation_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionStatus {
    pub status: Status,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Status {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AirbyteCatalog {
    pub streams: Vec<AirbyteStream>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AirbyteStream {
    pub name: String,
    pub json_schema: Value,
    pub supported_sync_modes: Vec<SyncMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_defined_primary_key: Option<Vec<Vec<String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    FullRefresh,
    Incremental,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationSyncMode {
    Append,
    Overwrite,
    AppendDedup,
}

/// The catalog passed to `read`, with the streams the user selected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfiguredAirbyteCatalog {
    pub streams: Vec<ConfiguredAirbyteStream>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfiguredAirbyteStream {
    pub stream: AirbyteStream,
    pub sync_mode: SyncMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_sync_mode: Option<DestinationSyncMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Vec<Vec<String>>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordMessage {
    pub stream: String,
    pub data: Value,
    /// Milliseconds since the Unix epoch.
    pub emitted_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateMessage {
    #[serde(rename = "type")]
    pub state_type: StateType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<StreamState>,
}

impl StateMessage {
    pub fn stream(name: impl Into<String>, stream_state: Value) -> Self {
        Self {
            state_type: StateType::Stream,
            stream: Some(StreamState {
                stream_descriptor: StreamDescriptor {
                    name: name.into(),
                    namespace: None,
                },
                stream_state: Some(stream_state),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StateType {
    Stream,
    Global,
    Legacy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamState {
    pub stream_descriptor: StreamDescriptor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_state: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDescriptor {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogMessage {
    pub level: LogLevel,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogLevel {
    Fatal,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceMessage {
    #[serde(rename = "type")]
    pub trace_type: TraceType,
    /// Milliseconds since the Unix epoch.
    pub emitted_at: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorTrace>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TraceType {
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorTrace {
    /// Message for a person, saying what to fix.
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_type: Option<FailureType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureType {
    SystemError,
    ConfigError,
    TransientError,
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Writes messages as JSON, one per line.
pub struct MessageWriter<W: Write> {
    out: W,
}

impl MessageWriter<BufWriter<Stdout>> {
    /// A buffered writer on stdout. Call [`MessageWriter::flush`] before exiting.
    pub fn stdout() -> Self {
        Self::new(BufWriter::new(io::stdout()))
    }
}

impl<W: Write> MessageWriter<W> {
    pub fn new(out: W) -> Self {
        Self { out }
    }

    pub fn write(&mut self, message: &AirbyteMessage) -> io::Result<()> {
        serde_json::to_writer(&mut self.out, message)?;
        self.out.write_all(b"\n")
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    pub fn into_inner(self) -> W {
        self.out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn to_line(message: &AirbyteMessage) -> String {
        let mut writer = MessageWriter::new(Vec::new());
        writer.write(message).unwrap();
        String::from_utf8(writer.into_inner()).unwrap()
    }

    #[test]
    fn writes_one_line_per_message() {
        let mut writer = MessageWriter::new(Vec::new());
        writer
            .write(&AirbyteMessage::log(LogLevel::Info, "a\nb"))
            .unwrap();
        writer
            .write(&AirbyteMessage::connection_succeeded())
            .unwrap();
        let out = String::from_utf8(writer.into_inner()).unwrap();
        assert_eq!(out.lines().count(), 2);
        assert!(out.ends_with('\n'));
    }

    #[test]
    fn spec_uses_camel_case_fields() {
        let line = to_line(&AirbyteMessage::spec(json!({"type": "object"})));
        assert_eq!(
            line.trim_end(),
            r#"{"type":"SPEC","spec":{"connectionSpecification":{"type":"object"}}}"#
        );
    }

    #[test]
    fn connection_status_shape() {
        let value: Value =
            serde_json::from_str(&to_line(&AirbyteMessage::connection_failed("no file"))).unwrap();
        assert_eq!(
            value,
            json!({"type": "CONNECTION_STATUS", "connection_status": {"status": "FAILED", "message": "no file"}})
        );
    }

    #[test]
    fn state_shape() {
        let value: Value = serde_json::from_str(&to_line(&AirbyteMessage::stream_state(
            "Sheet1",
            json!({"last_row": 10}),
        )))
        .unwrap();
        assert_eq!(
            value,
            json!({
                "type": "STATE",
                "state": {
                    "type": "STREAM",
                    "stream": {
                        "stream_descriptor": {"name": "Sheet1"},
                        "stream_state": {"last_row": 10}
                    }
                }
            })
        );
    }

    #[test]
    fn trace_error_shape() {
        let value: Value = serde_json::from_str(&to_line(&AirbyteMessage::error(
            FailureType::ConfigError,
            "bad path",
        )))
        .unwrap();
        assert_eq!(value["type"], "TRACE");
        assert_eq!(value["trace"]["type"], "ERROR");
        assert_eq!(value["trace"]["error"]["message"], "bad path");
        assert_eq!(value["trace"]["error"]["failure_type"], "config_error");
    }

    #[test]
    fn every_message_round_trips() {
        let messages = vec![
            AirbyteMessage::spec(json!({})),
            AirbyteMessage::connection_succeeded(),
            AirbyteMessage::catalog(vec![AirbyteStream {
                name: "s".into(),
                json_schema: json!({"type": "object"}),
                supported_sync_modes: vec![SyncMode::FullRefresh],
                source_defined_primary_key: Some(vec![vec!["id".into()]]),
                namespace: None,
            }]),
            AirbyteMessage::record("s", json!({"id": 1})),
            AirbyteMessage::stream_state("s", json!({"last_row": 1})),
            AirbyteMessage::log(LogLevel::Warn, "careful"),
            AirbyteMessage::error(FailureType::SystemError, "boom"),
        ];
        for message in messages {
            let back: AirbyteMessage = serde_json::from_str(&to_line(&message)).unwrap();
            assert_eq!(back, message);
        }
    }

    #[test]
    fn reads_configured_catalog() {
        let catalog: ConfiguredAirbyteCatalog = serde_json::from_value(json!({
            "streams": [{
                "stream": {
                    "name": "Customers",
                    "json_schema": {"type": "object"},
                    "supported_sync_modes": ["full_refresh"]
                },
                "sync_mode": "full_refresh",
                "destination_sync_mode": "overwrite"
            }]
        }))
        .unwrap();
        assert_eq!(catalog.streams[0].stream.name, "Customers");
    }
}
