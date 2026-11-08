# fita-connectors

Connectors for [Fita](https://github.com/Fita-data/fita) that the Airbyte catalog does not cover well.

Every connector here speaks the Airbyte protocol, so Fita runs it like any community connector and other tools can use it too. Database and plain file connectors are not here, they are compiled into the Fita binary.

## Layout

```
crates/
  airbyte-protocol          message types and helpers shared by every connector
connectors/
  source-excel-multisheet   Excel workbooks with several sheets and header offsets
  source-fixed-width        fixed width text files from legacy systems
  source-sap-s4hana         SAP S/4HANA Business Partner, suppliers and materials over OData
extensions/
  salesforce/               duplicate warning when a user creates an account (later)
  dynamics/                 same for Dynamics 365 (later)
docs/
  CONNECTOR_GUIDE.md        how to build and test a connector
  ROADMAP.md                planned work
```

## Running a connector

Each connector builds to a binary and a container image.

```sh
cargo run -p source-excel-multisheet -- spec
cargo run -p source-excel-multisheet -- check --config config.json
cargo run -p source-excel-multisheet -- discover --config config.json
cargo run -p source-excel-multisheet -- read --config config.json --catalog catalog.json
```

## License

Apache License 2.0.
