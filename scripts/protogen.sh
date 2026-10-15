#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
PROTOC_VERSION=36.2
GEN_GO_VERSION=v1.36.12
GEN_GRPC_VERSION=v1.6.2
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
if ! command -v unzip >/dev/null; then
  apt-get update -qq && apt-get install -y -qq unzip >/dev/null
fi
curl -sSfL -o "$work/protoc.zip" "https://github.com/protocolbuffers/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip"
unzip -q "$work/protoc.zip" -d "$work/protoc"
GOBIN="$work/bin" go install "google.golang.org/protobuf/cmd/protoc-gen-go@$GEN_GO_VERSION"
GOBIN="$work/bin" go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@$GEN_GRPC_VERSION"
rm -rf api/fivefamiliesv1
PATH="$work/bin:$PATH" "$work/protoc/bin/protoc" -I proto \
  --go_out=. --go_opt=module=github.com/mirzanajafov/fivefamilies \
  --go-grpc_out=. --go-grpc_opt=module=github.com/mirzanajafov/fivefamilies \
  proto/fivefamilies/v1/*.proto
go run ./internal/tools/nocomments api
