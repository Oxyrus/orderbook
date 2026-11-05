# syntax=docker/dockerfile:1

# Base images are pinned by digest so builds are reproducible and a
# compromised tag can't change what ships. Dependabot (.github/dependabot.yml)
# opens PRs when new digests are published.

# ---- Build stage -------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1.27-alpine@sha256:d200d2dd76b03a99b52651fd1325aff7dcd35422c35c01bac56571b0507f3deb AS build

WORKDIR /src

# Download dependencies first so they are cached independently of source code.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG TARGETOS TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
      -ldflags "-s -w \
        -X github.com/Oxyrus/orderbook/internal/buildinfo.Version=${VERSION} \
        -X github.com/Oxyrus/orderbook/internal/buildinfo.Commit=${COMMIT} \
        -X github.com/Oxyrus/orderbook/internal/buildinfo.BuildTime=${BUILD_TIME}" \
      -o /out/api ./cmd/api

# ---- Runtime stage -----------------------------------------------------------
# distroless/static: no shell, no package manager, CA certs + tzdata included,
# runs as uid 65532 (nonroot).
FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329

COPY --from=build /out/api /usr/local/bin/api

USER nonroot:nonroot
EXPOSE 8080 9090

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/api", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/api"]
CMD ["serve"]
