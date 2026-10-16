FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG LDFLAGS="-s -w"
RUN CGO_ENABLED=0 go build -trimpath -ldflags="${LDFLAGS}" -o /out/ffnode ./cmd/node \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ffctl ./cmd/ffctl \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/failgen ./cmd/failgen \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ffbench ./cmd/ffbench

FROM alpine:3.22
RUN adduser -D -H -u 10001 ff && mkdir /data && chown ff /data
COPY --from=build /out/ /usr/local/bin/
USER ff
VOLUME /data
ENTRYPOINT ["ffnode"]
