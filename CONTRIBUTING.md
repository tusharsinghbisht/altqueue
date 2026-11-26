# Contributing

Thanks for your interest in AltQueue.

1. Fork and branch from `main`.
2. Make your change with tests. Engine tests live beside the source in `internal/queue/`; end-to-end scenarios live in `tests/integration/`.
3. Run `make ci` (vet, race-enabled tests, build) and `gofmt -l .` (should print nothing).
4. Open a pull request describing the behavior change and any delivery-semantics impact.

Protobuf bindings in `api/gen/` are checked in. If you change `api/proto/`, regenerate them with `protoc` and commit both.

Please read [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) first: it lists known limitations and what is intentionally out of scope.
