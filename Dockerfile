FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /altqueued ./cmd/altqueued
RUN CGO_ENABLED=0 go build -trimpath -o /altqueue ./cmd/altqueue

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S altqueue && adduser -S -G altqueue altqueue \
    && mkdir -p /var/lib/altqueue && chown altqueue:altqueue /var/lib/altqueue
COPY --from=build /altqueued /usr/local/bin/altqueued
COPY --from=build /altqueue /usr/local/bin/altqueue
USER altqueue
WORKDIR /var/lib/altqueue
EXPOSE 7331 7332
ENTRYPOINT ["altqueued"]
CMD ["-listen", "0.0.0.0:7331", "-http", "0.0.0.0:7332"]
