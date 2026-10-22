FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG LDFLAGS="-s -w"
RUN CGO_ENABLED=0 go build -trimpath -ldflags="${LDFLAGS}" -o /out/ ./cmd/node ./cmd/ffctl ./cmd/supervisor ./cmd/controller ./cmd/failgen ./cmd/ffbench \
 && mv /out/node /out/ffnode

FROM alpine:3.22 AS node
RUN adduser -D -H -u 10001 ff && mkdir /data && chown ff /data
COPY --from=build /out/ffnode /out/ffctl /out/supervisor /out/controller /usr/local/bin/
USER ff
VOLUME /data
ENTRYPOINT ["ffnode"]

FROM node AS lab
COPY --from=build /out/failgen /out/ffbench /usr/local/bin/

FROM node
