# syntax=docker/dockerfile:1
#
# MinIO server and client built from source. MinIO no longer publishes
# community images, and github.com/minio/minio is archived: its final release
# (RELEASE.2025-10-15T17-29-55Z) has unfixed vulnerabilities, including two
# unauthenticated object writes. So both are built from the maintained
# community fork by PGSTY (github.com/pgsty/silo, formerly pgsty/minio, and
# github.com/pgsty/mc), which fixes them. The Go module paths are unchanged
# (github.com/minio/minio, github.com/minio/mc), so the build is the same.
#
# The fork keeps most dependencies current; the build upgrades the few Go
# modules that still have known vulnerabilities (MINIO_DEP_UPGRADES /
# MC_DEP_UPGRADES). Only list versions newer than the fork's go.mod: `go get`
# with an older version would downgrade. When the image scan in CI flags a
# module, add or bump it here.
#
# MinIO and mc are AGPL-3.0. The exact source is recorded in the
# org.opencontainers.image.source/revision labels; the dependency upgrades are
# the list below.
#
# Build and push (multi-arch):
#   docker buildx build --platform linux/amd64,linux/arm64 --target minio \
#     -t levonghukas/minio:RELEASE.2026-10-15T00-00-00Z-r2 --push docker/minio
#   docker buildx build --platform linux/amd64,linux/arm64 --target mc \
#     -t levonghukas/mc:RELEASE.2026-09-16T00-00-00Z-r2 --push docker/minio
# CI (.github/workflows/minio-images.yml) does this on main.

# The fork's go.mod requires Go 1.27.2 or newer; 1.27.3 fixes CVE-2026-111943
# (crypto/x509) in the minio and mc binaries.
ARG GO_VERSION=1.27.3
ARG MINIO_REPO=https://github.com/pgsty/silo
ARG MINIO_RELEASE=RELEASE.2026-10-15T00-00-00Z
ARG MINIO_COMMIT=7a3ca0f82100cce06289a465f54139da2639f642
ARG MC_REPO=https://github.com/pgsty/mc
ARG MC_RELEASE=RELEASE.2026-09-16T00-00-00Z
ARG MC_COMMIT=e952aa78f10a2b77dd525a2b7e3143bcda0cd377

# Bump when the image changes without an upstream release (for example new
# dependency upgrades). Published tags are <release>-r<revision>.
ARG IMAGE_REVISION=2

ARG MINIO_DEP_UPGRADES="\
  github.com/go-jose/go-jose/v4@v4.1.5 \
  go.opentelemetry.io/otel/sdk@v1.46.0"
ARG MC_DEP_UPGRADES="\
  golang.org/x/net@v0.60.0"


FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build-base
# -mod=mod lets the upgraded requirements resolve without a full `go mod
# tidy`, which fails on an unrelated test-only package upstream.
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-mod=mod
WORKDIR /src


FROM build-base AS build-minio
ARG MINIO_REPO MINIO_RELEASE MINIO_COMMIT MINIO_DEP_UPGRADES TARGETOS TARGETARCH
RUN git clone --depth 1 --branch "${MINIO_RELEASE}" "${MINIO_REPO}" . \
    && test "$(git rev-parse HEAD)" = "${MINIO_COMMIT}"
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go get ${MINIO_DEP_UPGRADES} && \
    ts="$(echo "${MINIO_RELEASE#RELEASE.}" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z/T\1:\2:\3Z/')" \
    && ldflags="$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${ts}")" \
    && GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -tags kqueue -trimpath -ldflags "${ldflags}" -o /out/minio .


FROM build-base AS build-mc
ARG MC_REPO MC_RELEASE MC_COMMIT MC_DEP_UPGRADES TARGETOS TARGETARCH
RUN git clone --depth 1 --branch "${MC_RELEASE}" "${MC_REPO}" . \
    && test "$(git rev-parse HEAD)" = "${MC_COMMIT}"
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go get ${MC_DEP_UPGRADES} && \
    ts="$(echo "${MC_RELEASE#RELEASE.}" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z/T\1:\2:\3Z/')" \
    && ldflags="$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${ts}")" \
    && GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -tags kqueue -trimpath -ldflags "${ldflags}" -o /out/mc .


FROM alpine:3.22 AS runtime-base
# Apply OS security updates the base image does not include yet.
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates tzdata curl


# Client: keeps a shell because Compose init jobs run `mc` from sh scripts.
FROM runtime-base AS mc
ARG MC_REPO MC_RELEASE MC_COMMIT
LABEL org.opencontainers.image.title="mc" \
      org.opencontainers.image.version="${MC_RELEASE}" \
      org.opencontainers.image.source="${MC_REPO}" \
      org.opencontainers.image.revision="${MC_COMMIT}" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=build-mc /out/mc /usr/bin/mc
RUN adduser -D -u 10001 mc
USER 10001:10001
ENTRYPOINT ["mc"]


FROM runtime-base AS minio
ARG MINIO_REPO MINIO_RELEASE MINIO_COMMIT
LABEL org.opencontainers.image.title="minio" \
      org.opencontainers.image.version="${MINIO_RELEASE}" \
      org.opencontainers.image.source="${MINIO_REPO}" \
      org.opencontainers.image.revision="${MINIO_COMMIT}" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=build-minio /out/minio /usr/bin/minio
RUN adduser -D -u 10001 minio && install -d -o minio -g minio /data
USER 10001:10001
EXPOSE 9000 9001
VOLUME ["/data"]
HEALTHCHECK --interval=10s --timeout=3s --retries=5 CMD curl -fsS http://127.0.0.1:9000/minio/health/live || exit 1
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
