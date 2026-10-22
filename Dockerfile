FROM golang:1.26-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags="-s -w" -o /out/ ./cmd/node ./cmd/ffctl ./cmd/supervisor ./cmd/controller \
 && mv /out/node /out/ffnode

FROM build AS lab-build
RUN go build -ldflags="-s -w -X main.plantable=yes" -o /lab/ ./cmd/node ./cmd/failgen ./cmd/ffbench \
 && mv /lab/node /lab/ffnode

FROM alpine:3.22 AS base
RUN adduser -D -H -u 10001 ff && mkdir /data && chown ff /data
USER ff
VOLUME /data
ENTRYPOINT ["ffnode"]

FROM base AS lab
COPY --from=build /out/ /usr/local/bin/
COPY --from=lab-build /lab/ /usr/local/bin/

FROM base AS runtime
COPY --from=build /out/ /usr/local/bin/
