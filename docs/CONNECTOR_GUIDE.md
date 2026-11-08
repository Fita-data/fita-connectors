# Connector guide

## The protocol

A connector is a program with four commands. It writes one JSON message per line to stdout and nothing else. Logs go to stdout as `LOG` messages, never as plain text.

| Command | Output |
| --- | --- |
| `spec` | one `SPEC` message with the JSON Schema of the config |
| `check --config` | one `CONNECTION_STATUS` message |
| `discover --config` | one `CATALOG` message with streams and their JSON Schemas |
| `read --config --catalog [--state]` | `RECORD` messages, `STATE` messages every N records, `LOG` and `TRACE` |

Use the types in `crates/airbyte-protocol`. Do not build JSON by hand.

## Rules every connector follows

- **Read only.** A source connector never writes to the system it reads.
- **Streaming.** Never load a whole source in memory. Read in pages or batches and emit as you go.
- **Resumable.** Emit `STATE` after each batch with enough information to continue from there. A state is only valid once every record before it was emitted.
- **Stable ids.** Each record has a primary key declared in the catalog.
- **Secrets.** Mark secret fields with `airbyte_secret: true` in the spec. Never log them.
- **Errors.** Fail with a `TRACE` error message that a person can act on, for example which credential or permission is missing.
- **Types.** Map source types to JSON Schema types. Dates as ISO 8601 strings. Decimals as strings when precision matters.

## Layout of a connector

```
connectors/source-<name>/
  Cargo.toml
  src/main.rs          command parsing only
  src/spec.rs          config schema
  src/client.rs        talks to the source
  src/streams.rs       one function per stream
  tests/               protocol tests with recorded responses
  Dockerfile
  README.md            config fields, permissions needed, known limits
```

## Testing

- Unit tests for parsing and type mapping.
- Protocol tests that run the binary and check every line is a valid message.
- Tests against a real instance in a container when one exists. Otherwise against recorded responses in `tests/fixtures/`.
- Recorded responses must be anonymized or synthetic before they are committed. Never commit a real response from a customer system.

## SAP S/4HANA notes

- OData v2 and v4 services for Business Partner (`API_BUSINESS_PARTNER`), suppliers and products.
- Paginate with `$skiptoken`. Use delta links when the service offers them.
- Request only the fields in the catalog with `$select`.
- Authentication with basic auth or OAuth client credentials. Both configurable.
