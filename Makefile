.PHONY: build build-client build-master build-worker test vet lint docker-build-master docker-build-worker docker-up docker-down run-master run-worker

build:
	go build ./...

# Canonical user-facing client binary.
build-client:
	go build -o ./orabbit-client ./cmd/orabbit-client

# Packaged daemon binaries used by local start/run flows and daemon deployments.
build-master:
	go build -o ./orabbit-master ./cmd/master

build-worker:
	go build -o ./orabbit-worker ./cmd/worker

test:
	go test ./...

vet:
	go vet ./...

lint:
	golangci-lint run

# Local daemon image builds for packaging/container validation; these do not
# package the user-facing orabbit-client binary.
docker-build-master:
	docker build -f Dockerfile.orabbit --target master -t orabbit-master:local .

docker-build-worker:
	docker build -f Dockerfile.orabbit --target worker -t orabbit-worker:local .

# Compose brings up daemon/dependency services only; orabbit-client stays a
# separate local or installed CLI binary.
docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

run-master:
	go run ./cmd/master

run-worker:
	go run ./cmd/worker
