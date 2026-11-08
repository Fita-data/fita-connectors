//! One stream per sheet: column names from the header row, one record per data row.

use std::collections::{HashMap, HashSet};
use std::io::Write;

use airbyte_protocol::{AirbyteMessage, AirbyteStream, MessageWriter, SyncMode};
use serde_json::{json, Map, Value};

use crate::client::{ConnectorError, RowReader, Workbook};

/// Field that holds the Excel row number. It is the primary key and the resume cursor.
pub const ROW_FIELD: &str = "_row";
pub const STATE_EVERY: u64 = 1_000;

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub index: u32,
    pub name: String,
}

/// Reads rows until the header row and returns its columns.
pub fn read_header(
    rows: &mut RowReader<'_>,
    sheet: &str,
    header_row: u32,
) -> Result<Vec<Column>, ConnectorError> {
    while let Some(row) = rows.next_row()? {
        if row.number < header_row {
            continue;
        }
        if row.number == header_row {
            return Ok(columns(&row.cells));
        }
        break;
    }
    Err(ConnectorError::config(format!(
        "Sheet {sheet} has no column names in row {header_row}. Check header_row in the config"
    )))
}

/// Column names from header cells. Blank headers become `column_<letter>`, repeated ones get `_2`, `_3`.
pub fn columns(cells: &[(u32, Value)]) -> Vec<Column> {
    let mut used: HashSet<String> = HashSet::from([ROW_FIELD.to_string()]);
    let mut out = Vec::new();
    let by_index: HashMap<u32, &Value> = cells.iter().map(|(i, v)| (*i, v)).collect();
    let last = cells.iter().map(|(i, _)| *i).max();
    for index in 0..=last.unwrap_or(0) {
        if last.is_none() {
            break;
        }
        let base = match by_index.get(&index) {
            Some(Value::String(s)) if !s.trim().is_empty() => s.trim().to_string(),
            Some(Value::String(_)) | None => format!("column_{}", column_letter(index)),
            Some(other) => other.to_string(),
        };
        let mut name = base.clone();
        let mut n = 2;
        while !used.insert(name.clone()) {
            name = format!("{base}_{n}");
            n += 1;
        }
        out.push(Column { index, name });
    }
    out
}

pub fn column_letter(mut index: u32) -> String {
    let mut letters = Vec::new();
    loop {
        letters.push(b'A' + (index % 26) as u8);
        if index < 26 {
            break;
        }
        index = index / 26 - 1;
    }
    letters.reverse();
    String::from_utf8(letters).unwrap_or_default()
}

pub fn json_schema(columns: &[Column]) -> Value {
    let mut properties = Map::new();
    properties.insert(ROW_FIELD.into(), json!({"type": "integer"}));
    for column in columns {
        properties.insert(
            column.name.clone(),
            json!({"type": ["null", "string", "number", "boolean"]}),
        );
    }
    json!({
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": properties
    })
}

pub fn discover_stream(
    workbook: &mut Workbook,
    sheet: &str,
    header_row: u32,
) -> Result<AirbyteStream, ConnectorError> {
    let mut rows = workbook.rows(sheet)?;
    let columns = read_header(&mut rows, sheet, header_row)?;
    Ok(AirbyteStream {
        name: sheet.to_string(),
        json_schema: json_schema(&columns),
        supported_sync_modes: vec![SyncMode::FullRefresh],
        source_defined_primary_key: Some(vec![vec![ROW_FIELD.to_string()]]),
        namespace: None,
    })
}

/// Emits one record per data row after `start_after` (an Excel row number), with a STATE every
/// [`STATE_EVERY`] records and one at the end. Returns the number of records.
pub fn read_stream<W: Write>(
    workbook: &mut Workbook,
    sheet: &str,
    header_row: u32,
    start_after: u32,
    out: &mut MessageWriter<W>,
) -> Result<u64, ConnectorError> {
    let mut rows = workbook.rows(sheet)?;
    let columns = read_header(&mut rows, sheet, header_row)?;
    let mut count = 0;
    let mut last_row = start_after.max(header_row);
    while let Some(row) = rows.next_row()? {
        if row.number <= last_row {
            continue;
        }
        let mut values: HashMap<u32, Value> = row.cells.into_iter().collect();
        let mut data = Map::new();
        data.insert(ROW_FIELD.into(), Value::from(row.number));
        for column in &columns {
            let value = values.remove(&column.index).unwrap_or(Value::Null);
            data.insert(column.name.clone(), value);
        }
        out.write(&AirbyteMessage::record(sheet, Value::Object(data)))?;
        last_row = row.number;
        count += 1;
        if count % STATE_EVERY == 0 {
            out.write(&state(sheet, last_row))?;
        }
    }
    if count % STATE_EVERY != 0 || count == 0 {
        out.write(&state(sheet, last_row))?;
    }
    Ok(count)
}

pub fn state(sheet: &str, last_row: u32) -> AirbyteMessage {
    AirbyteMessage::stream_state(sheet, json!({ "last_row": last_row }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn letters() {
        assert_eq!(column_letter(0), "A");
        assert_eq!(column_letter(25), "Z");
        assert_eq!(column_letter(26), "AA");
        assert_eq!(column_letter(701), "ZZ");
        assert_eq!(column_letter(702), "AAA");
    }

    #[test]
    fn header_names() {
        let cells = vec![
            (0, json!("Id")),
            (1, json!(" Name ")),
            (3, json!("Name")),
            (4, json!(2024)),
            (5, json!("_row")),
        ];
        let names: Vec<String> = columns(&cells).into_iter().map(|c| c.name).collect();
        assert_eq!(
            names,
            vec!["Id", "Name", "column_C", "Name_2", "2024", "_row_2"]
        );
    }

    #[test]
    fn empty_header() {
        assert!(columns(&[]).is_empty());
    }
}
