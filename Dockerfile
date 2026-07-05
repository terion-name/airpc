# syntax=docker/dockerfile:1

ARG GO_VERSION=1.22
ARG ALPINE_VERSION=3.20

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

ARG TARGETOS=linux
ARG TARGETARCH

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN targetos="${TARGETOS:-linux}" \
 && targetarch="${TARGETARCH:-$(go env GOARCH)}" \
 && CGO_ENABLED=0 GOOS="${targetos}" GOARCH="${targetarch}" \
      go build -trimpath -ldflags="-s -w" -o /out/airpc ./cmd/airpc \
 && CGO_ENABLED=0 GOOS="${targetos}" GOARCH="${targetarch}" \
      go build -trimpath -ldflags="-s -w" -o /out/airpc-e2e ./cmd/airpc-e2e

FROM alpine:${ALPINE_VERSION}
RUN adduser -D -H -u 10001 appuser
COPY --from=build /out/airpc /usr/local/bin/airpc
COPY --from=build /out/airpc-e2e /usr/local/bin/airpc-e2e
USER appuser
CMD ["/usr/local/bin/airpc"]
