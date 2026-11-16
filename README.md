<div align="center">

# stmtarmor

**PostgreSQL wire-protocol statement virtualizer and zero-collision transaction pooling accelerator.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white)](go.mod)
[![Zero Dependencies](https://img.shields.io/badge/Dependencies-Zero%20(Pure%20Go)-green)](#)

<p align="center">
  <a href="#the-problem">The problem</a> •
  <a href="#how-it-works">How it works</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#comparison">Comparison</a> •
  <a href="#installation">Installation</a> •
  <a href="#usage">Usage</a> •
  <a href="#license">License</a>
</p>

</div>

## The problem

The PostgreSQL Extended Query Protocol (v3.0) executes parameterized queries in distinct phases: `Parse` (`'P'`), `Bind` (`'B'`), `Describe` (`'D'`), `Execute` (`'E'`), and `Sync` (`'S'`). By compiling and caching execution plans in backend process memory (`TopMemoryContext`), PostgreSQL avoids re-parsing queries on subsequent calls, reducing planning CPU load by 20% to 50% and enabling binary data transfer.

To handle high-concurrency architectures (serverless functions, Kubernetes pods, microservices), teams deploy connection poolers such as PgBouncer, Supabase Supavisor, or AWS RDS Proxy in **transaction pooling mode** (`pool_mode = transaction`). In this mode, backend physical connections are multiplexed per transaction:

```
  Client Session A                 PgBouncer (Transaction Mode)               Backend Connections
         |                                     |                                       |
         |  [Tx 1] BEGIN; Parse("s0", sql);    |                                       |
         |------------------------------------>|  Assigned to Backend 1                |
         |                                     |-------------------------------------->| [Prepared "s0"]
         |  [Tx 1] COMMIT;                     |                                       |
         |------------------------------------>|  Backend 1 returned to pool           |
         |                                     |                                       |
  Client Session B                             |                                       |
         |  [Tx 2] BEGIN; Parse("s0", sql);    |  Assigned to Backend 1                |
         |------------------------------------>|-------------------------------------->|
         |                                     |                                       | Backend 1 has "s0"!
         |  ERROR: 42P05                       |  <------------------------------------| [SQLSTATE 42P05]
         |<------------------------------------|  "prepared statement already exists"  |
```

This creates two fatal protocol collisions:

1. **`SQLSTATE 42P05` (`prepared statement already exists`)**: Client A prepares statement `s0` on Backend Connection 1. Client B is assigned Backend Connection 1 for its next transaction. Client B's ORM (Prisma, asyncpg, pgx) automatically generates the name `s0` for its first query. Backend Connection 1 already holds `s0`, aborting the transaction immediately.
2. **`SQLSTATE 26000` (`prepared statement does not exist`)**: Client A prepares statement `s0` on Backend Connection 1. In its next transaction, PgBouncer assigns Client A to Backend Connection 2. Client A issues `Bind("s0")`. Backend Connection 2 has never seen `s0`, crashing the transaction.

### Why existing workarounds fail

- **Disabling prepared statements** (`?pgbouncer=true` in Prisma, `statement_cache_size=0` in asyncpg, `QueryExecModeSimpleProtocol` in pgx): Disables binary wire encoding (forcing text formatting for timestamps, UUIDs, floats, and JSONB) and forces PostgreSQL to re-parse and re-plan every query from scratch on every transaction, inflating database CPU usage by 20% to 50%.
- **Session-mode pooling**: Bypasses connection multiplexing entirely. 2,000 serverless functions hold 2,000 backend connections, exhausting database memory and crashing PostgreSQL.
- **PgBouncer `max_prepared_statements`**: Adds proxy memory bloat, increases CPU overhead on PgBouncer's single-threaded event loop, and is unavailable or unsupported on managed cloud proxies like AWS RDS Proxy.

## How it works

`stmtarmor` operates as a transparent wire-protocol proxy between client applications and PgBouncer/PostgreSQL:

```
  Client Application                stmtarmor proxy                   PgBouncer / PostgreSQL
         |                                 |                                    |
         |  Parse("s0", "SELECT ...")      |                                    |
         |-------------------------------->|  1. Computes canonical hash (sa_1) |
         |                                 |  2. Backend unprepared:            |
         |                                 |     Parse("sa_1", "SELECT ...")    |
         |                                 |----------------------------------->|
         |                                 |  <---------------------------------| [ParseComplete]
         |  [ParseComplete]                |                                    |
         |<--------------------------------|                                    |
         |                                 |                                    |
         |  [Next Tx - Assigned Backend 2] |                                    |
         |  Bind("s0", params)             |                                    |
         |-------------------------------->|  3. Detects unprimed Backend 2    |
         |                                 |  4. JIT Parse injected:            |
         |                                 |     Parse("sa_1", sql)             |
         |                                 |     Bind("sa_1", params)           |
         |                                 |----------------------------------->|
         |                                 |  <---------------------------------| [BindComplete]
         |  [BindComplete]                 |                                    |
         |<--------------------------------|                                    |
```

1. **Canonical query fingerprinting**: Computes deterministic 64-bit canonical hashes ($H_{\text{stmt}} = \text{sha256}(\text{normalized\_query} + \text{param\_OIDs})$) formatted as `sa_<hex>`.
2. **Parse suppression (round-trip elimination)**: When a client issues a `Parse` for a statement already prepared on the active backend connection, `stmtarmor` suppresses the packet from the wire and synthesizes `ParseComplete` (`'1'`) immediately, eliminating a database network round-trip.
3. **Just-In-Time (JIT) Bind injection**: When a client issues `Bind` and connection rotation has assigned a backend that has not yet prepared the statement, `stmtarmor` injects an inline `Parse` message immediately before the `Bind` on the wire.
4. **Collision auto-recovery**: Rewrites all client-generated statement names (`s0`, `stmt_1`) to global canonical identifiers, guaranteeing zero `42P05` and zero `26000` errors.

## Architecture

`stmtarmor` is written in pure Go with zero external dependencies.

```
stmtarmor/
├── cmd/
│   └── stmtarmor/
│       ├── main.go             # CLI interface: proxy and doctor commands
│       └── main_test.go        # Command line integration tests
├── pkg/
│   ├── pgwire/
│   │   ├── message.go          # PostgreSQL 3.0 message framing ('P','B','D','E','S','1','2','Z','E')
│   │   ├── reader.go           # Wire stream message deserializer
│   │   ├── writer.go           # Wire stream message serializer
│   │   └── pgwire_test.go      # Protocol framing tests
│   ├── virtualizer/
│   │   ├── fingerprint.go      # Canonical query hash calculator
│   │   ├── virtualizer.go      # Statement virtualizer and JIT injection engine
│   │   └── virtualizer_test.go # Statement deduplication tests
│   ├── proxy/
│   │   ├── proxy.go            # Transparent TCP proxy with connection tracking
│   │   └── proxy_test.go       # End-to-end integration tests with mock pooler
│   └── doctor/
│       ├── doctor.go           # Collision diagnostic prober
│       └── doctor_test.go      # Prober unit tests
├── go.mod
└── README.md
```

## Installation

Building from source:

```bash
git clone https://github.com/OGAbhinavMaurya/stmtarmor.git
cd stmtarmor
go build -o stmtarmor ./cmd/stmtarmor
sudo install -m 0755 stmtarmor /usr/local/bin/
```

## Usage

### 1. Proxy (statement virtualizer)

Run `stmtarmor proxy` between your application and PgBouncer:

```bash
# Listen on 6433 and proxy to PgBouncer on 6432
stmtarmor proxy --listen :6433 --target 127.0.0.1:6432

# Output operational metrics as JSON on shutdown
stmtarmor proxy --listen :6433 --target 127.0.0.1:6432 --json
```

Configure your application database URL to point to port 6433 with statement caching enabled:
```text
DATABASE_URL="postgresql://user:password@127.0.0.1:6433/mydb"
```

Example shutdown summary:
```text
--------------------------------------------------------------------------------
Proxy Summary:
  Total Connections:        142
  Total Parses Intercepted: 8420
  Parses Suppressed (Saved): 7810
  JIT Parses Injected:      610
  Collisions Prevented:     8420
--------------------------------------------------------------------------------
```

### 2. Doctor (collision diagnostic)

Diagnose whether your downstream connection pooler is vulnerable to prepared statement collisions:

```bash
# Probe PgBouncer or RDS Proxy
stmtarmor doctor --target 127.0.0.1:6432

# Output diagnostic report in JSON
stmtarmor doctor --target 127.0.0.1:6432 --json
```

Example doctor report:
```text
================================================================================
                            STMTARMOR DOCTOR REPORT                             
================================================================================
 Target Address:      127.0.0.1:6432
 Status:              [VULNERABLE]
 42P05 Hazard:        true (prepared statement already exists)
 26000 Hazard:        true (prepared statement does not exist)
 Summary:             Target connection pooler is vulnerable to prepared statement collisions under transaction pooling.
 Recommendation:      Deploy StmtArmor proxy in front of PgBouncer to eliminate 42P05 and 26000 errors.
--------------------------------------------------------------------------------
 Diagnostic Trace:
   * Sent PostgreSQL 3.0 startup packet
   * Received ReadyForQuery (server session established)
   * Collision detected: SQLSTATE 42P05 (prepared statement already exists)
================================================================================
```

## Comparison

| Feature | `stmtarmor` | PgBouncer Native (1.21+) | Driver Workaround (`?pgbouncer=true`) | Session Mode |
| :--- | :--- | :--- | :--- | :--- |
| **Transaction Pooling Compatible** | Yes | Partial | Yes | No |
| **Binary Protocol Wire Encoding** | Yes | Yes | No (text only) | Yes |
| **Plan Caching CPU Savings (20-50%)** | Yes | Yes | No (re-plans every query) | Yes |
| **Eliminates `SQLSTATE 42P05`** | Yes | Yes | Yes (by disabling cache) | Yes |
| **Eliminates `SQLSTATE 26000`** | Yes | Yes | Yes (by disabling cache) | Yes |
| **Cross-Client Statement Deduplication**| Yes (canonical hash) | No | No | No |
| **AWS RDS Proxy Support** | Yes (external proxy) | No | Yes (by disabling cache) | No |
| **Zero Code Changes Required** | Yes (port repointing) | Yes | No (driver flags needed) | No (exhausts pool) |

## License

MIT
