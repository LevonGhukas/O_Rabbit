# Query Mode Specification & SQL Safety Contract

## Overview

Query Mode allows extracting arbitrary SQL queries directly into Arrow/Parquet files and registering them into Apache Iceberg catalogs.

## Supported Database Engines

- PostgreSQL (`postgres`)
- MySQL (`mysql`)
- MariaDB (`mariadb`)
- Microsoft SQL Server (`mssql`)
- Oracle Database (`oracle`)
- ClickHouse (`clickhouse`)
- Trino (`trino`)
- Apache Cassandra (`cassandra`) — CQL SELECT

## Security & Validation Contract

All user-supplied queries and SQL fragments are validated prior to execution:

### 1. Read-Only Query Validation (`NormalizeReadOnlySQLQuery`)
- Must begin with `SELECT` or `WITH ... SELECT`.
- Rejects destructive keywords: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, `GRANT`, `REVOKE`, `MERGE`, `CALL`, `EXEC`, `EXECUTE`, `COPY`, `LOAD`, `UNLOAD`, `REPLACE`, `INTO`.
- Rejects multiple statements (semicolons separating queries) and unterminated comments/literals.

### 2. WHERE Clause Validation (`ValidateWhereClause`)
- Rejects SQL comments (`--`, `/* ... */`).
- Rejects semicolons and multi-statement injection.
- Rejects all destructive keywords.

### 3. Select Columns Validation (`ValidateSelectColumns`)
- Must be a valid column list.
- Rejects semicolon separators, SQL comments, and dangerous function calls / multi-statement subqueries.

## Query Partitioning Strategies

- **Ordered Cursor**: Queries with orderable keys partitioned by high-water-mark range.
- **Single Task**: Bounded queries extracted in a single high-throughput streaming task.
