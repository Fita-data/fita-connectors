//! Runs the binary against a small workbook built here with synthetic data and checks that every
//! stdout line is a valid Airbyte message.

use std::path::{Path, PathBuf};
use std::process::Command;

use airbyte_protocol::{AirbyteMessage, FailureType, Status};
use rust_xlsxwriter::{ExcelDateTime, Format, Workbook};
use serde_json::{json, Value};
use tempfile::TempDir;

const PRODUCT_ROWS: u32 = 2_500;

struct Fixture {
    dir: TempDir,
}

impl Fixture {
    /// Workbook with three sheets. Column names are in row 3 of each one, with a title above.
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let mut book = Workbook::new();
        let date = Format::new().set_num_format("yyyy-mm-dd");
        let datetime = Format::new().set_num_format("yyyy-mm-dd hh:mm:ss");

        let customers = book.add_worksheet().set_name("Customers").unwrap();
        customers
            .write(0, 0, "Customer list, synthetic test data")
            .unwrap();
        let header = [
            "Id", "Name", "Email", "Active", "Since", "Balance", "", "Name",
        ];
        for (col, name) in header.iter().enumerate() {
            if !name.is_empty() {
                customers.write(2, col as u16, *name).unwrap();
            }
        }
        let rows = [
            (1, "Example Trading One", "one@example.test", true, 150.25),
            (2, "Sample Goods Two", "two@example.test", false, -20.0),
            (3, "Demo Supplies Three", "three@example.test", true, 0.5),
        ];
        for (i, (id, name, email, active, balance)) in rows.iter().enumerate() {
            let r = 3 + i as u32;
            customers.write(r, 0, *id).unwrap();
            customers.write(r, 1, *name).unwrap();
            customers.write(r, 2, *email).unwrap();
            customers.write_boolean(r, 3, *active).unwrap();
            customers
                .write_datetime_with_format(
                    r,
                    4,
                    ExcelDateTime::from_ymd(2024, 1, 10 + i as u8).unwrap(),
                    &date,
                )
                .unwrap();
            customers.write(r, 5, *balance).unwrap();
            customers.write(r, 6, "extra").unwrap();
        }
        customers
            .write_datetime_with_format(
                3,
                7,
                ExcelDateTime::parse_from_str("2024-03-01 09:30:00").unwrap(),
                &datetime,
            )
            .unwrap();

        let products = book.add_worksheet().set_name("Products").unwrap();
        products.write(0, 0, "Product list, synthetic").unwrap();
        products.write(2, 0, "Code").unwrap();
        products.write(2, 1, "Description").unwrap();
        for i in 0..PRODUCT_ROWS {
            products.write(3 + i, 0, format!("P{:05}", i + 1)).unwrap();
            products
                .write(3 + i, 1, format!("Test product {}", i + 1))
                .unwrap();
        }

        let notes = book.add_worksheet().set_name("Notes").unwrap();
        notes.write(2, 0, "Note").unwrap();
        notes.write(3, 0, "not selected").unwrap();

        book.save(dir.path().join("book.xlsx")).unwrap();
        Self { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn write_json(&self, name: &str, value: &Value) -> PathBuf {
        let path = self.path(name);
        std::fs::write(&path, value.to_string()).unwrap();
        path
    }

    fn config(&self, sheets: &[&str]) -> PathBuf {
        self.write_json(
            "config.json",
            &json!({
                "path": self.path("book.xlsx"),
                "sheets": sheets,
                "header_row": 3
            }),
        )
    }

    fn catalog(&self, streams: &[&str]) -> PathBuf {
        let streams: Vec<Value> = streams
            .iter()
            .map(|name| {
                json!({
                    "stream": {
                        "name": name,
                        "json_schema": {"type": "object"},
                        "supported_sync_modes": ["full_refresh"]
                    },
                    "sync_mode": "full_refresh",
                    "destination_sync_mode": "overwrite"
                })
            })
            .collect();
        self.write_json("catalog.json", &json!({ "streams": streams }))
    }
}

/// Runs the connector and parses every stdout line as a message. Fails on any invalid line.
fn run(args: &[&Path]) -> (Vec<AirbyteMessage>, bool) {
    let output = Command::new(env!("CARGO_BIN_EXE_source-excel-multisheet"))
        .args(args)
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(stdout.is_empty() || stdout.ends_with('\n'));
    let messages = stdout
        .lines()
        .map(|line| {
            let value: Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("not JSON: {line}: {e}"));
            assert!(value.is_object(), "not an object: {line}");
            serde_json::from_value(value)
                .unwrap_or_else(|e| panic!("not an Airbyte message: {line}: {e}"))
        })
        .collect();
    (messages, output.status.success())
}

fn records(messages: &[AirbyteMessage], stream: &str) -> Vec<Value> {
    messages
        .iter()
        .filter_map(|m| match m {
            AirbyteMessage::Record { record } if record.stream == stream => {
                Some(record.data.clone())
            }
            _ => None,
        })
        .collect()
}

fn states(messages: &[AirbyteMessage], stream: &str) -> Vec<u64> {
    messages
        .iter()
        .filter_map(|m| match m {
            AirbyteMessage::State { state } => state.stream.as_ref(),
            _ => None,
        })
        .filter(|s| s.stream_descriptor.name == stream)
        .map(|s| {
            s.stream_state.as_ref().unwrap()["last_row"]
                .as_u64()
                .unwrap()
        })
        .collect()
}

fn arg(s: &str) -> &Path {
    Path::new(s)
}

#[test]
fn spec() {
    let (messages, ok) = run(&[arg("spec")]);
    assert!(ok, "{:?}", messages.last());
    let [AirbyteMessage::Spec { spec }] = messages.as_slice() else {
        panic!("expected one SPEC, got {messages:?}");
    };
    let schema = &spec.connection_specification;
    assert_eq!(schema["required"], json!(["path"]));
    for field in ["path", "sheets", "header_row"] {
        assert!(schema["properties"][field].is_object(), "missing {field}");
    }
}

#[test]
fn check_succeeds() {
    let fx = Fixture::new();
    let config = fx.config(&["Customers", "Products"]);
    let (messages, ok) = run(&[arg("check"), arg("--config"), &config]);
    assert!(ok, "{:?}", messages.last());
    let [AirbyteMessage::ConnectionStatus { connection_status }] = messages.as_slice() else {
        panic!("expected one CONNECTION_STATUS, got {messages:?}");
    };
    assert_eq!(connection_status.status, Status::Succeeded);
}

#[test]
fn check_fails_on_missing_file_and_sheet() {
    let fx = Fixture::new();
    let missing_file = fx.write_json("missing.json", &json!({"path": fx.path("nope.xlsx")}));
    let missing_sheet = fx.config(&["Customers", "Invoices"]);
    for (config, expected) in [(missing_file, "nope.xlsx"), (missing_sheet, "Invoices")] {
        let (messages, ok) = run(&[arg("check"), arg("--config"), &config]);
        assert!(ok, "{:?}", messages.last());
        let [AirbyteMessage::ConnectionStatus { connection_status }] = messages.as_slice() else {
            panic!("expected one CONNECTION_STATUS, got {messages:?}");
        };
        assert_eq!(connection_status.status, Status::Failed);
        assert!(connection_status
            .message
            .as_deref()
            .unwrap()
            .contains(expected));
    }
}

#[test]
fn discover_one_stream_per_sheet() {
    let fx = Fixture::new();
    let config = fx.config(&["Customers", "Products"]);
    let (messages, ok) = run(&[arg("discover"), arg("--config"), &config]);
    assert!(ok, "{:?}", messages.last());
    let [AirbyteMessage::Catalog { catalog }] = messages.as_slice() else {
        panic!("expected one CATALOG, got {messages:?}");
    };
    let names: Vec<&str> = catalog.streams.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["Customers", "Products"]);

    let customers = &catalog.streams[0];
    let properties = customers.json_schema["properties"].as_object().unwrap();
    let columns: Vec<&str> = properties.keys().map(String::as_str).collect();
    for column in [
        "_row", "Id", "Name", "Email", "Active", "Since", "Balance", "column_G", "Name_2",
    ] {
        assert!(columns.contains(&column), "missing {column} in {columns:?}");
    }
    assert_eq!(
        customers.source_defined_primary_key,
        Some(vec![vec!["_row".to_string()]])
    );
}

#[test]
fn discover_all_sheets_when_none_given() {
    let fx = Fixture::new();
    let config = fx.config(&[]);
    let (messages, ok) = run(&[arg("discover"), arg("--config"), &config]);
    assert!(ok, "{:?}", messages.last());
    let [AirbyteMessage::Catalog { catalog }] = messages.as_slice() else {
        panic!("expected one CATALOG, got {messages:?}");
    };
    assert_eq!(catalog.streams.len(), 3);
}

#[test]
fn read_emits_records_and_state() {
    let fx = Fixture::new();
    let config = fx.config(&["Customers", "Products"]);
    let catalog = fx.catalog(&["Customers", "Products"]);
    let (messages, ok) = run(&[
        arg("read"),
        arg("--config"),
        &config,
        arg("--catalog"),
        &catalog,
    ]);
    assert!(ok, "{:?}", messages.last());

    let customers = records(&messages, "Customers");
    assert_eq!(customers.len(), 3);
    assert_eq!(
        customers[0],
        json!({
            "_row": 4,
            "Id": 1,
            "Name": "Example Trading One",
            "Email": "one@example.test",
            "Active": true,
            "Since": "2024-01-10",
            "Balance": 150.25,
            "column_G": "extra",
            "Name_2": "2024-03-01T09:30:00"
        })
    );
    assert_eq!(customers[2]["Name_2"], Value::Null);
    assert_eq!(states(&messages, "Customers"), [6]);

    let products = records(&messages, "Products");
    assert_eq!(products.len(), PRODUCT_ROWS as usize);
    assert_eq!(products[0]["Code"], "P00001");
    // Header in row 3, so record n is in row n + 3.
    assert_eq!(states(&messages, "Products"), [1_003, 2_003, 2_503]);

    // Each STATE comes right after the record it points to.
    for pair in messages.windows(2) {
        if let [AirbyteMessage::Record { record }, AirbyteMessage::State { state }] = pair {
            let stream_state = state.stream.as_ref().unwrap();
            assert_eq!(stream_state.stream_descriptor.name, record.stream);
            assert_eq!(
                stream_state.stream_state.as_ref().unwrap()["last_row"],
                record.data["_row"]
            );
        }
    }
    assert!(records(&messages, "Notes").is_empty());
}

#[test]
fn read_resumes_from_state() {
    let fx = Fixture::new();
    let config = fx.config(&["Customers", "Products"]);
    let catalog = fx.catalog(&["Products"]);
    let state = fx.write_json(
        "state.json",
        &json!([{
            "type": "STREAM",
            "stream": {
                "stream_descriptor": {"name": "Products"},
                "stream_state": {"last_row": 2_003}
            }
        }]),
    );
    let (messages, ok) = run(&[
        arg("read"),
        arg("--config"),
        &config,
        arg("--catalog"),
        &catalog,
        arg("--state"),
        &state,
    ]);
    assert!(ok, "{:?}", messages.last());
    let products = records(&messages, "Products");
    assert_eq!(products.len(), 500);
    assert_eq!(products[0]["_row"], 2_004);
    assert_eq!(states(&messages, "Products"), [2_503]);
}

#[test]
fn read_fails_with_trace_on_unknown_stream() {
    let fx = Fixture::new();
    let config = fx.config(&["Customers"]);
    let catalog = fx.catalog(&["Products"]);
    let (messages, ok) = run(&[
        arg("read"),
        arg("--config"),
        &config,
        arg("--catalog"),
        &catalog,
    ]);
    assert!(!ok);
    let Some(AirbyteMessage::Trace { trace }) = messages.last() else {
        panic!("expected a TRACE, got {messages:?}");
    };
    let error = trace.error.as_ref().unwrap();
    assert_eq!(error.failure_type, Some(FailureType::ConfigError));
    assert!(error.message.contains("Products"));
}

#[test]
fn wrong_header_row_is_a_config_error() {
    let fx = Fixture::new();
    let config = fx.write_json(
        "config.json",
        &json!({"path": fx.path("book.xlsx"), "sheets": ["Customers"], "header_row": 2}),
    );
    let (messages, ok) = run(&[arg("discover"), arg("--config"), &config]);
    assert!(!ok);
    let Some(AirbyteMessage::Trace { trace }) = messages.last() else {
        panic!("expected a TRACE, got {messages:?}");
    };
    assert!(trace.error.as_ref().unwrap().message.contains("header_row"));
}
