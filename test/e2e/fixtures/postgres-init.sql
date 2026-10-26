-- Deterministic source data for the end-to-end test. Every column is derived
-- from id, so the test computes the expected rows instead of storing them
-- (see expectedRow in test/e2e/e2e_test.go). The test inserts and changes
-- rows itself between runs.

-- Exported with full overwrite runs.
CREATE TABLE people (
  id         bigint PRIMARY KEY,
  name       text NOT NULL,
  created_at timestamptz NOT NULL
);

INSERT INTO people (id, name, created_at)
SELECT g, 'person-' || g, timestamptz '2026-01-01 00:00:00+00' + g * interval '1 minute'
FROM generate_series(1, 1000) AS g;

-- Exported with incremental append runs; id is the cursor column.
CREATE TABLE events (
  id         bigint PRIMARY KEY,
  name       text NOT NULL,
  created_at timestamptz NOT NULL
);

INSERT INTO events (id, name, created_at)
SELECT g, 'event-' || g, timestamptz '2026-01-01 00:00:00+00' + g * interval '1 minute'
FROM generate_series(1, 1000) AS g;
