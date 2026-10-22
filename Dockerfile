ARG LDFLAGS="-s -w"

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG LDFLAGS
RUN CGO_ENABLED=0 go build -trimpath -ldflags="${LDFLAGS}" -o /out/ ./cmd/node ./cmd/ffctl ./cmd/supervisor ./cmd/controller \
 && mv /out/node /out/ffnode

FROM build AS lab-tools
ARG LDFLAGS
RUN CGO_ENABLED=0 go build -trimpath -ldflags="${LDFLAGS}" -o /lab/ ./cmd/failgen ./cmd/ffbench

FROM alpine:3.22 AS runtime
RUN adduser -D -H -u 10001 ff && mkdir /data && chown ff /data
COPY --from=build /out/ /usr/local/bin/
USER ff
VOLUME /data
ENTRYPOINT ["ffnode"]

FROM runtime AS lab
COPY --from=lab-tools /lab/ /usr/local/bin/

FROM runtime
