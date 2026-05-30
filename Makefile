.PHONY: test test-go test-ts build lint lint-go lint-ts clean

# ── All ──────────────────────────────────────────────────────────

test: test-go test-ts

build: build-go

lint: lint-go lint-ts

clean:
	rm -f *.db *.db-wal *.db-shm /tmp/sdk-test.db /tmp/sdk-test.db-wal /tmp/sdk-test.db-shm /tmp/e2e*.db /tmp/e2e*.db-*
	cd sdks/typescript && rm -rf node_modules dist

# ── Go ───────────────────────────────────────────────────────────

test-go:
	go test -count=1 -timeout 60s ./...

build-go:
	go build ./...

lint-go:
	go vet ./...

# ── TypeScript ───────────────────────────────────────────────────

TS_DIR = sdks/typescript

test-ts:
	cd $(TS_DIR) && npm test

build-ts:
	cd $(TS_DIR) && npx tsc

lint-ts:
	cd $(TS_DIR) && npx tsc --noEmit
