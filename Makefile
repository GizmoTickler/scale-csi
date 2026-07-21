.PHONY: check

check:
	go test -race -short ./...
