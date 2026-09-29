# syntax=docker/dockerfile:1
#
# MinIO server and client built from upstream source. MinIO no longer
# publishes community images, and github.com/minio/minio is archived, so
# RELEASE.2025-10-15T17-29-55Z is its final release. Both are built with a
# current Go toolchain so the standard library carries current fixes.
#
# MinIO and mc are AGPL-3.0. These images are unmodified builds; the exact
# source is recorded in the org.opencontainers.image.source/revision labels.
#
# Build and push (multi-arch):
#   docker buildx build --platform linux/amd64,linux/arm64 --target minio \
#     -t levonghukas/minio:RELEASE.2025-10-15T17-29-55Z --push docker/minio
#   docker buildx build --platform linux/amd64,linux/arm64 --target mc \
#     -t levonghukas/mc:RELEASE.2025-08-13T08-35-41Z --push docker/minio

ARG GO_VERSION=1.26.6
ARG MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z
ARG MC_COMMIT=7394ce0dd2a80935aded936b09fa12cbb3cb8096


FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build-base
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src


FROM build-base AS build-minio
ARG MINIO_RELEASE MINIO_COMMIT TARGETOS TARGETARCH
RUN git clone --depth 1 --branch "${MINIO_RELEASE}" https://github.com/minio/minio . \
    && test "$(git rev-parse HEAD)" = "${MINIO_COMMIT}"
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    ts="$(echo "${MINIO_RELEASE#RELEASE.}" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z/T\1:\2:\3Z/')" \
    && ldflags="$(MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${ts}")" \
    && GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -tags kqueue -trimpath -ldflags "${ldflags}" -o /out/minio .


FROM build-base AS build-mc
ARG MC_RELEASE MC_COMMIT TARGETOS TARGETARCH
RUN git clone --depth 1 --branch "${MC_RELEASE}" https://github.com/minio/mc . \
    && test "$(git rev-parse HEAD)" = "${MC_COMMIT}"
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    ts="$(echo "${MC_RELEASE#RELEASE.}" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z/T\1:\2:\3Z/')" \
    && ldflags="$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go "${ts}")" \
    && GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -tags kqueue -trimpath -ldflags "${ldflags}" -o /out/mc .


FROM alpine:3.22 AS runtime-base
RUN apk add --no-cache ca-certificates tzdata curl


# Client: keeps a shell because Compose init jobs run `mc` from sh scripts.
FROM runtime-base AS mc
ARG MC_RELEASE MC_COMMIT
LABEL org.opencontainers.image.title="mc" \
      org.opencontainers.image.version="${MC_RELEASE}" \
      org.opencontainers.image.source="https://github.com/minio/mc" \
      org.opencontainers.image.revision="${MC_COMMIT}" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=build-mc /out/mc /usr/bin/mc
RUN adduser -D -u 10001 mc
USER 10001:10001
ENTRYPOINT ["mc"]


FROM runtime-base AS minio
ARG MINIO_RELEASE MINIO_COMMIT
LABEL org.opencontainers.image.title="minio" \
      org.opencontainers.image.version="${MINIO_RELEASE}" \
      org.opencontainers.image.source="https://github.com/minio/minio" \
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
