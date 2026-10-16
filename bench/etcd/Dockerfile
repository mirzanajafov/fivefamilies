FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
WORKDIR /src/bench/etcd
RUN go mod download && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/etcdgen .

FROM alpine:3.22
COPY --from=build /out/etcdgen /usr/local/bin/etcdgen
USER 10001
ENTRYPOINT ["etcdgen"]
