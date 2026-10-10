.PHONY: test integration examples lint all

# Unit tests, no databases required.
test:
	go test -race -count=1 ./...

# Real databases. Defaults match the CI services; override with the env vars.
#   WORK_TEST_POSTGRES=postgres://work:work@127.0.0.1:5432/work_test?sslmode=disable
#   WORK_TEST_MYSQL=work:work@tcp(127.0.0.1:3306)/work_test
# SQLite runs on a temporary file. WORK_TEST_REQUIRE_DB=1 turns an unreachable
# database into a failure instead of a skip.
#   WORK_TEST_REDIS=redis://127.0.0.1:6379/0
integration:
	cd integration && CGO_ENABLED=1 WORK_TEST_REQUIRE_DB=1 go test -race -count=1 ./...
	cd redis && WORK_TEST_REQUIRE_DB=1 go test -race -count=1 ./...

examples:
	cd examples && CGO_ENABLED=1 go run ./basic && CGO_ENABLED=1 go run ./transactional

lint:
	gofmt -l . | (! grep .)
	go vet ./...
	cd integration && go vet ./...
	cd examples && go vet ./...
	cd redis && go vet ./...

all: lint test integration examples
