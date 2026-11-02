# Contributing to O_Rabbit

Thanks for helping improve O_Rabbit! This guide explains how to propose a
change and what a pull request needs before it can be merged.

## Before you start

- **Bugs:** open an issue with the [bug report form](https://github.com/LevonGhukas/O_Rabbit/issues/new?template=bug_report.yml).
  Include the run's `failure_phase`, `failure_class` and `error_summary`
  (`GET /api/v1/runs/{id}`), with any credentials removed.
- **Features and larger changes:** open a [feature request](https://github.com/LevonGhukas/O_Rabbit/issues/new?template=feature_request.yml)
  first, so the design can be agreed before you write code.
- **Security issues:** do not open a public issue; see [SECURITY.md](SECURITY.md).

## Development setup

You need Go 1.26 (see `go.mod`), Docker for integration tests, and
`golangci-lint` for linting.

```sh
git clone https://github.com/LevonGhukas/O_Rabbit.git
cd O_Rabbit
make build
make test
```

See [Development](README.md#development) in the README for integration tests
and the repository layout.

## Making a change

1. Create a branch from `main`.
2. Keep the change focused on one problem, and match the style of the package
   you are editing.
3. Add or update tests for every behavior change.
4. Before pushing, run:

   ```sh
   gofmt -w <changed .go files>
   go vet ./...
   go test -race ./...
   golangci-lint run
   ```

5. Update `README.md`, `docs/openapi.yaml` and examples in the same change when
   an API, flag, environment variable or file format changes.

## Fuzzing untrusted input

Parsers of user-supplied input have Go fuzz targets: the S3 CSV, JSON and XML
readers and JSON `record_path` (`internal/connectors/s3_fuzz_test.go`), query
mode SQL and `where_clause` validation
(`internal/connectors/query_mode_fuzz_test.go`), and Iceberg table identifiers
(`internal/icebergreg/identifier_fuzz_test.go`).

Their seeds run with every `go test ./...`. To fuzz one target, run:

```sh
go test ./internal/connectors -run='^$' -fuzz='^FuzzValidateWhereClause$' -fuzztime=5m
```

`-fuzz` must match exactly one target in one package. A failing input is saved
under `<package>/testdata/fuzz/<target>/`; commit it with the fix so it stays a
regression test. The [Fuzz workflow](.github/workflows/fuzz.yml) runs every
target daily and can be started by hand with a longer `fuzztime`.

The SQL checks do not know the source engine, so `internal/connectors/sql_lexer.go`
reads the text the way each supported dialect would. A new engine with
different quoting or comment rules needs a dialect entry there.

## Contracts that need extra care

- **Worker protocol:** bump `WorkerProtocolVersion` in `internal/grpc/server.go`
  when the worker RPC contract changes; master and workers must then be
  deployed together.
- **Protobuf:** `internal/grpcpb` is generated from `proto/controlplane.proto`.
  Never edit the generated files by hand.
- **Database migrations:** add a new migration in `internal/db/migrate.go`;
  never change one that may already have run.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```text
fix(arrowio): keep leading zeros in CSV code columns
feat(planner): verify column type overrides for file sources
```

## Pull requests

Fill in the pull request template. CI must pass: unit and integration tests,
lint, Compose validation, the vulnerability check and the image scan.

Never commit real DSNs, access keys, tokens, TLS keys or `.env` files. Only the
`*.example` files belong in the repository.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
