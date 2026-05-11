# SyncLite Client — Command-Line Interface for SyncLite Devices

> Part of the [SyncLite Platform](https://github.com/syncliteio/SyncLite) — Build Anything, Sync Anywhere.

## What is SyncLite Client?

**SyncLite Client** is an interactive command-line SQL client for SyncLite devices. It lets developers and operators connect to any SyncLite-managed embedded database, execute SQL statements interactively or from a script, and observe data consolidation in action — all from the terminal.

It supports two connection modes:

| Mode | How | When to use |
|---|---|---|
| **Embedded** (direct) | Uses SyncLite Logger JDBC in-process | Local development and testing |
| **Remote** (HTTP) | Connects to a running SyncLite DB server | Multi-language environments, remote devices |

## Usage

### Embedded mode (default)

```bash
# Windows — uses default DB at %USERPROFILE%\synclite\job1\db\test.db (SQLITE)
synclite-cli.bat

# Linux / macOS
synclite-cli.sh

# With explicit options
synclite-cli.sh <path/to/db/file> \
    --device-type SQLITE \
    --synclite-logger-config <path/to/synclite_logger.conf>
```

### Remote mode (via SyncLite DB)

```bash
synclite-cli.sh <path/to/db/file> \
    --device-type SQLITE \
    --synclite-logger-config <path/to/synclite_logger.conf> \
    --server http://localhost:5555
```

When `--server` is specified the client sends all SQL over HTTP to the SyncLite DB server instead of using the embedded library.

## Device Types

`SQLITE` · `DUCKDB` · `DERBY` · `H2` · `HYPERSQL` · `STREAMING` · `SQLITE_APPENDER` · `DUCKDB_APPENDER` · `DERBY_APPENDER` · `H2_APPENDER` · `HYPERSQL_APPENDER`

## Interactive Session Example

```
$ synclite-cli.sh ~/synclite/db/myapp.db --device-type SQLITE
Connected to SyncLite SQLITE device: /home/alice/synclite/db/myapp.db
Type SQL statements, or 'exit' to quit.

SyncLite> CREATE TABLE IF NOT EXISTS sensor_data(ts BIGINT, value REAL);
OK

SyncLite> INSERT INTO sensor_data VALUES(1714200000, 23.5);
OK  (1 row affected)

SyncLite> SELECT * FROM sensor_data;
ts            | value
--------------+-------
1714200000    | 23.5

SyncLite> exit
Bye.
```

All statements are not only executed on the local embedded database but also captured in sync log files and eventually consolidated into the destination database by [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator).

## Build

```bash
cd synclite-client/client
mvn -Drevision=oss clean install
```

Built artifacts under `client/target/`: `synclite-cli.bat`, `synclite-cli.sh`, `synclite-logger.conf` (sample config).

## Related Components

| Component | Role |
|---|---|
| [SyncLite DB](https://github.com/syncliteio/synclite-db) | HTTP server the client can connect to in remote mode |
| [SyncLite Logger](https://github.com/syncliteio/synclite-logger-java) | Embedded JDBC driver used in embedded mode |
| [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator) | Picks up the sync logs and replicates to destination |

## Documentation & Community

- Full documentation: https://github.com/syncliteio/SyncLite/blob/main/DOCUMENTATION.md
- Website: https://www.synclite.io
- Community: https://github.com/syncliteio/SyncLite/issues

---

← Back to the [SyncLite Platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)
