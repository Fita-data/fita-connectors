//! Opens the workbook and streams rows cell by cell, without loading a sheet in memory.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use airbyte_protocol::FailureType;
use calamine::{open_workbook, DataRef, ExcelDateTime, Reader, Xlsx, XlsxCellReader};
use chrono::NaiveTime;
use serde_json::{Number, Value};

#[derive(Debug)]
pub struct ConnectorError {
    pub failure_type: FailureType,
    pub message: String,
}

impl ConnectorError {
    pub fn system(message: impl Into<String>) -> Self {
        Self {
            failure_type: FailureType::SystemError,
            message: message.into(),
        }
    }
}

impl From<std::io::Error> for ConnectorError {
    fn from(e: std::io::Error) -> Self {
        Self::system(format!("Cannot write output: {e}"))
    }
}

pub struct Workbook {
    xlsx: Xlsx<BufReader<File>>,
}

impl Workbook {
    pub fn open(path: &Path) -> Result<Self, ConnectorError> {
        if !path.is_file() {
            return Err(ConnectorError::config(format!(
                "File {} does not exist or is not a file",
                path.display()
            )));
        }
        let xlsx = open_workbook(path).map_err(|e| {
            ConnectorError::config(format!(
                "Cannot open {} as an .xlsx workbook: {e}",
                path.display()
            ))
        })?;
        Ok(Self { xlsx })
    }

    /// The sheets to read, in config order, or every sheet in workbook order when none are given.
    pub fn select_sheets(&self, wanted: &[String]) -> Result<Vec<String>, ConnectorError> {
        let names = self.xlsx.sheet_names();
        if wanted.is_empty() {
            return Ok(names);
        }
        let missing: Vec<&str> = wanted
            .iter()
            .filter(|w| !names.contains(w))
            .map(String::as_str)
            .collect();
        if !missing.is_empty() {
            return Err(ConnectorError::config(format!(
                "Sheets not found in the workbook: {}. Available sheets: {}",
                missing.join(", "),
                names.join(", ")
            )));
        }
        Ok(wanted.to_vec())
    }

    pub fn rows(&mut self, sheet: &str) -> Result<RowReader<'_>, ConnectorError> {
        let cells = self
            .xlsx
            .worksheet_cells_reader(sheet)
            .map_err(|e| ConnectorError::system(format!("Cannot read sheet {sheet}: {e}")))?;
        Ok(RowReader {
            sheet: sheet.to_string(),
            cells,
            pending: None,
            done: false,
        })
    }
}

/// A non-empty row. `number` is the 1-based Excel row number.
#[derive(Debug)]
pub struct Row {
    pub number: u32,
    /// (0-based column index, value), only cells that hold a value.
    pub cells: Vec<(u32, Value)>,
}

pub struct RowReader<'a> {
    sheet: String,
    cells: XlsxCellReader<'a, BufReader<File>>,
    pending: Option<(u32, u32, Value)>,
    /// The cell reader fails if called again after it reached the end of the sheet.
    done: bool,
}

impl RowReader<'_> {
    fn next_cell(&mut self) -> Result<Option<(u32, u32, Value)>, ConnectorError> {
        if let Some(cell) = self.pending.take() {
            return Ok(Some(cell));
        }
        if self.done {
            return Ok(None);
        }
        let cell = self.cells.next_cell().map_err(|e| {
            ConnectorError::system(format!("Cannot read sheet {}: {e}", self.sheet))
        })?;
        self.done = cell.is_none();
        Ok(cell.map(|c| {
            let (row, col) = c.get_position();
            (row, col, to_json(c.get_value()))
        }))
    }

    /// Next row with at least one value. Rows with only empty cells are skipped.
    pub fn next_row(&mut self) -> Result<Option<Row>, ConnectorError> {
        let mut current: Option<Row> = None;
        while let Some((row, col, value)) = self.next_cell()? {
            match &mut current {
                Some(r) if r.number != row + 1 => {
                    if r.cells.is_empty() {
                        current = None;
                    } else {
                        self.pending = Some((row, col, value));
                        return Ok(current);
                    }
                }
                _ => {}
            }
            let r = current.get_or_insert_with(|| Row {
                number: row + 1,
                cells: Vec::new(),
            });
            if !value.is_null() {
                r.cells.push((col, value));
            }
        }
        Ok(current.filter(|r| !r.cells.is_empty()))
    }
}

/// Maps a cell to JSON. Dates become ISO 8601 strings, errors like `#DIV/0!` stay as text.
pub fn to_json(data: &DataRef<'_>) -> Value {
    match data {
        DataRef::Empty => Value::Null,
        DataRef::String(s) => Value::String(s.clone()),
        DataRef::SharedString(s) => Value::String((*s).to_string()),
        DataRef::Int(i) => Value::from(*i),
        DataRef::Float(f) => float(*f),
        DataRef::Bool(b) => Value::Bool(*b),
        DataRef::DateTime(dt) => datetime(dt),
        DataRef::DateTimeIso(s) | DataRef::DurationIso(s) => Value::String(s.clone()),
        DataRef::Error(e) => Value::String(e.to_string()),
    }
}

/// Excel stores every number as a float. Whole numbers that fit exactly are emitted as integers.
fn float(f: f64) -> Value {
    const EXACT: f64 = 9_007_199_254_740_992.0; // 2^53
    if f.fract() == 0.0 && f.abs() < EXACT {
        return Value::from(f as i64);
    }
    Number::from_f64(f)
        .map(Value::Number)
        .unwrap_or_else(|| Value::String(f.to_string()))
}

fn datetime(dt: &ExcelDateTime) -> Value {
    if dt.is_duration() {
        if let Some(d) = dt.as_duration() {
            return Value::String(d.to_string());
        }
    } else if let Some(d) = dt.as_datetime() {
        let text = if d.time() == NaiveTime::MIN {
            d.format("%Y-%m-%d").to_string()
        } else {
            d.format("%Y-%m-%dT%H:%M:%S").to_string()
        };
        return Value::String(text);
    }
    float(dt.as_f64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers() {
        assert_eq!(to_json(&DataRef::Float(3.0)), json!(3));
        assert_eq!(to_json(&DataRef::Float(-2.5)), json!(-2.5));
        assert_eq!(to_json(&DataRef::Float(1e300)), json!(1e300));
        assert_eq!(to_json(&DataRef::Float(f64::NAN)), json!("NaN"));
        assert_eq!(to_json(&DataRef::Int(7)), json!(7));
    }

    #[test]
    fn text_bool_and_empty() {
        assert_eq!(to_json(&DataRef::SharedString("a")), json!("a"));
        assert_eq!(to_json(&DataRef::Bool(false)), json!(false));
        assert_eq!(to_json(&DataRef::Empty), Value::Null);
    }
}
