# Contributing

Run `make ci` and `gofmt -l .` before opening a PR. Engine tests sit next to the code in `internal/queue/`; end-to-end tests are in `tests/integration/`.

The protobuf bindings in `api/gen/` are checked in. If you change `api/proto/`, regenerate them with `protoc` and commit both.
