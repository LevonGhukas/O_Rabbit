## What and why

<!-- What does this change, and which problem does it solve? Link the issue: Fixes #123 -->

## How it was tested

<!-- Tests added or updated, and any manual checks (source engine, file format, Iceberg engine). -->

## Checklist

- [ ] `gofmt`, `go vet ./...`, `go test -race ./...` and `golangci-lint run` pass
- [ ] Tests cover the behavior change
- [ ] README, `docs/openapi.yaml` and examples are updated if an API, flag, environment variable or format changed
- [ ] `WorkerProtocolVersion` is bumped if the worker RPC contract changed
- [ ] No real credentials, DSNs, tokens or `.env` files are included
