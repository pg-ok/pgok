# PG OK (pgok)

<p align="center">
<a href="https://github.com/pg-ok/pgok/releases"><img src="https://img.shields.io/github/release/pg-ok/pgok.svg?style=flat-square" alt="Releases"></a>
<a href="https://go.dev"><img src="https://img.shields.io/badge/go-1.24+-blue.svg?style=flat-square" alt="Go Version"></a>
<a href="https://github.com/pg-ok/pgok/blob/master/LICENSE"><img src="https://img.shields.io/github/license/pg-ok/pgok.svg?style=flat-square" alt="License"></a>
</p>

## Introduction

**pgok** is a CLI utility written in Golang for analyzing PostgreSQL database health, state, and performance.
It automates the detection of problems that usually require crafting complex SQL queries by hand.

It is designed for both **CI/CD pipelines** and **local analysis**.

> **IMPORTANT!** This tool is intended for analysis **only**.
> It will never apply any fixes or modifications to the database;
> such functionality is strictly out of scope for this project.

## Features

- **Index Analysis:** Detect missing, unused, duplicate, and invalid indexes.
- **Locking Prevention:** Identify missing indexes on Foreign Keys.
- **Performance:** Analyze index cache hit ratios and sizes.
- **Health Checks:** Monitor sequence exhaustion and tables missing Primary Keys.
- **Platform Friendly:** Supports JSON/table output and raw SQL inspection.

## Compatibility

- **PostgreSQL:** v12+.
- **OS:** Linux, macOS, Windows.

## Output Example

With `--output=table` (default) `pgok` prints human-readable tables:

```shell
$ pgok index:unused db_demo

+---------------------+-------------------+-------+
| Table               | Index             | Scans |
+---------------------+-------------------+-------+
| orders              | idx_orders_old    | 0     |
| user_logs           | idx_logs_temp     | 0     |
+---------------------+-------------------+-------+
Found 2 unused indexes.
```

With `--output=json`, it produces parsable JSON for your pipelines or external tools:

```shell
$ pgok index:unused db_demo --output=json
[
  {
    "table": "orders",
    "index": "idx_orders_old",
    "scans": 0
  },
  {
    "table": "user_logs",
    "index": "idx_logs_temp",
    "scans": 0
  }
]
```

## Installation

### From Source

You can install the binary directly from the source code using the Go toolchain:

```shell
go install github.com/pg-ok/pgok@latest
```

After installation, make sure `$(go env GOPATH)/bin` is in your `$PATH`.

## Usage

**pgok** supports two ways to connect to a database:
- via a configuration file (for convenience);
- via a direct Connection URI (for CI/CD).

### 1. Using Configuration File (recommended for local dev or standalone analysis service)

Create a `./config/pgok.toml` file in the project root:

```toml
[db]
# db_name = { uri = '...' }
db_payment = { uri = 'postgres://user:password@localhost:5432/payment' }
db_billing = { uri = 'postgres://user:password@localhost:5432/billing' }
```

Now you can run commands using the short name:

```shell
./pgok index:unused db_payment
```

### 2. Using Connection URI (recommended for CI/CD)

You can pass the connection string directly instead of a name.
This is useful for pipelines where credentials come from secrets.

```shell
./pgok index:unused postgres://user:password@localhost:5432/db_name
```

## Commands

All commands (except `app:*` and `help`) support the following flags:

- `--schema=public` to filter by "public" schema name (default `"*"` scans all user schemas).
- `--output=json` (JSON response) or `--output=table` (default).
- `--explain` to view the explanation of the check logic, result interpretation guide, and raw SQL query without executing it.

## Commands list

To view the full list of available commands:

```shell
./pgok help
```

### List predefined databases from the config

```shell
./pgok app:db:list
```

### `index:cache-hit` (Cache Efficiency)

**Problem:** Indexes are most effective when they reside in RAM (shared buffers).
If `idx_blks_read` (disk reads) is high compared to `idx_blks_hit` (RAM hits),
it indicates that indexes are "cold" or there is insufficient memory.

**What it does:** Displays the Cache Hit Ratio for indexes.

```shell
./pgok index:cache-hit db_demo
```

### `index:duplicate` (Find Duplicate and Overlapping Indexes)

**Problem:** A common scenario is creating an index on `(user_id, status)`,
and later another developer adds an index on `(user_id)`.
The second index is redundant because the first (composite) index already covers lookups by `user_id`.
PostgreSQL still wastes resources maintaining both.

**What it does:** Identifies indexes that are fully covered by other indexes (sharing the same column prefix).

*(Note: This command currently detects full duplicates. Detecting partial overlaps is more complex but also possible).*

```shell
./pgok index:duplicate db_demo
```

### `index:missing` (Detect Missing Indexes)

**Problem:** When a table lacks necessary indexes,
the database must perform a Sequential Scan (reading the entire table) to find data.
On large tables, this causes high I/O load and slow query performance.

**What it does:** Identifies tables that have a high ratio of sequential scans compared to index scans,
suggesting where adding an index could improve performance.

```shell
./pgok index:missing db_demo --rows-min=100
```

### `index:missing-fk` (Unindexed Foreign Keys)

**Problem:** If a Foreign Key exists without an index on the child table,
deleting a record from the parent table can lock the **entire** child table (instead of just specific rows) to verify integrity.
This is a major performance killer in high-load systems.

**What it does:** Identifies Foreign Keys that do not have an index starting with the key columns.

```shell
./pgok index:missing-fk db_demo
```

### `index:size` (Index Sizes)

**Problem:** Indexes consume disk space and memory. Over time, they can grow significantly,
sometimes becoming larger than the table itself or suffering from bloat, which impacts storage costs and backup times.

**What it does:** Lists indexes sorted by size (descending) to help identify the largest objects in your database.

```shell
./pgok index:size db_demo --size-min=1000
```

### `index:unused` (Unused Indexes)

**Problem:** Every index slows down `INSERT`, `UPDATE`, and `DELETE` operations.
If an index is never used for reading data,
it becomes pure overhead—wasting disk space and CPU cycles during every data modification.

**What it does:** Identifies indexes that have rarely or never been scanned,
suggesting they may be safe to drop to improve write performance.

```shell
./pgok index:unused db_demo
```

### `index:invalid` (Invalid Indexes)

**Problem:** Indexes typically become "invalid" when a `CREATE INDEX CONCURRENTLY` operation fails or is interrupted.
An invalid index is useless for querying (the planner ignores it) but still consumes disk space
and creates overhead for every `INSERT` or `UPDATE` operation.

**What it does:** Detects indexes marked as invalid that should be dropped or recreated.

```shell
./pgok index:invalid db_demo
```

### `schema:owner` (Ownership Validation)

**Problem:** In PostgreSQL, operations like `VACUUM`, `ALTER TABLE`, or `DROP` often require
you to be the owner of the object. If database migrations were applied by different users
(e.g., developers vs CI/CD robot), you end up with mixed ownership.
This leads to `ERROR: must be owner of table` during maintenance or future migrations.

**What it does:** Detects database objects (tables, sequences, views, types) that are NOT owned by
the specified user and generates SQL commands to fix the ownership.

```shell
./pgok schema:owner db_demo --expected=postgres
```

### `sequence:overflow` (Sequence Exhaustion)

**Problem:** Sequences in PostgreSQL have a finite limit (e.g., ~2.1 billion for a standard `INTEGER`).
If a sequence reaches its maximum value, new data insertions will fail, causing immediate application downtime.
This issue often creeps up silently on long-running systems.

**What it does:** Monitors sequences to detect when they are approaching their maximum limit,
allowing you to migrate to `BIGINT` before an overflow occurs.

```shell
./pgok sequence:overflow db_demo
```

### `table:missing-pk` (Missing Primary Keys)

**Problem:** Tables without a Primary Key allow duplicate rows, compromising data integrity.
They also complicate specific row updates or deletions and are often incompatible with logical replication tools and ORMs.

**What it does:** Identifies tables that lack a Primary Key constraint.

```shell
./pgok table:missing-pk db_demo
```

## CI/CD Integration

`pgok` is ideal for automated validation in pipelines (GitHub Actions, GitLab CI, Jenkins, etc.).

By analyzing the database during testing or staging, you can catch **"cold" indexes**,
**table bloat**, or **missing constraints** early—**before** they become a production issue.

> **Tip:** Use the `--output=json` flag to easily parse results in your CI scripts and fail the build
> if critical issues (like `table:missing-pk`) are found.

### Connection via Connection URI

In a CI environment, it is most convenient to pass connection parameters as a single string (Connection URI).

**Security Note:** This method is secure when used with CI/CD Secrets.
You can inject the connection string as an environment variable (e.g., `$DATABASE_URI`)
so credentials are never hardcoded in your scripts.

Pass the environment variable to ensure safety in pipelines.
```shell
pgok index:cache-hit "$DATABASE_URI"
```

### Exit Codes

The utility returns a **non-zero exit code** if critical issues or connection errors are detected.
This ensures that your CI pipeline automatically stops (fails the build) when a problem is found.

### CI Configuration Examples

#### GitHub Actions

An example workflow step using a Secret for the connection string:

```yml
jobs:
  health-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      # ... steps to build or download pgok ...

      - name: Check Index Cache Hit Ratio
        run: pgok index:cache-hit "${{ secrets.DATABASE_URI }}"

      - name: Check Table Missing Primary Key
        run: pgok table:missing-pk "${{ secrets.DATABASE_URI }}"
```

**Note:** I wrapped `${{ secrets.DATABASE_URI }}` in double quotes in the example.
This is a best practice in GitHub Actions to prevent the shell from breaking
if the password contains spaces or special characters.

#### GitLab CI

Example usage in `gitlab-ci.yml`:

```yml
postgres_health_check:
  stage: test
  script:
    - pgok index:cache-hit "$DATABASE_URI"
    - pgok table:missing-pk "$DATABASE_URI"
  variables:
    # Usually defined in Project Settings > CI/CD > Variables
    DATABASE_URI: "postgres://user:password@postgres-service:5432/db_name"
```

### Best Practices

**Pipeline Gating:**
Configure your CI to fail the build and prevent deployment when critical issues (e.g., `index:duplicate`) are found.

**Environment Strategy:**
Run statistics-heavy checks (cache, bloat) against **staging** or a **production replica**.
Synthetic or empty test databases do not generate representative usage statistics.

## Development

### Local Build

You will need Go 1.24+ installed.

```shell
go build -o pgok main.go
```

### Running Tests

The project includes comprehensive unit and integration tests. Integration tests use [testcontainers-go](https://golang.testcontainers.org/) to automatically spin up PostgreSQL instances in Docker.

#### Quick Start with Makefile

```shell
# Run all tests
make test

# Run only unit tests (fast, no Docker required)
make test-short

# Run tests with coverage report
make test-coverage

# View all available commands
make help
```

#### Test Output

Integration tests will automatically:
1. Pull the PostgreSQL Docker image (first run only)
2. Start a temporary PostgreSQL container
3. Run tests against it
4. Clean up the container after tests complete

### Docker Build

For a consistent development environment without installing Rust locally, you can use Docker Compose.
The container handles the build process automatically.

#### General syntax

```shell
docker-compose run --rm app pgok <COMMAND> [FLAGS]
```

#### Example

List the configured databases using the Docker container:

```shell
docker-compose run --rm app pgok app:db:list
```

## License

- `pgok` project is open-sourced software licensed under the [MIT license](LICENSE) by [Anton Komarev].

## About CyberCog

[CyberCog] is a Social Unity of enthusiasts. Research the best solutions in product & software development is our passion.

- [Follow us on Twitter](https://twitter.com/cybercog)

<a href="https://cybercog.su"><img src="https://cloud.githubusercontent.com/assets/1849174/18418932/e9edb390-7860-11e6-8a43-aa3fad524664.png" alt="CyberCog"></a>

[Anton Komarev]: https://komarev.com
[CyberCog]: https://cybercog.su
