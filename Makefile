.PHONY: build test race vet fmt bench run docker ci

build:
	go build -o bin/ ./cmd/altqueued ./cmd/altqueue

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

bench:
	go test ./benchmarks -run '^$$' -bench BenchmarkRoundTrip -benchmem

run:
	go run ./cmd/altqueued

docker:
	docker build -t altqueue .

ci: vet race build
