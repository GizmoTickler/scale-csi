.PHONY: check

# The pkg/driver suite gets a SECOND run under -shuffle=on because much of the
# backend-health and metric state is process-global: a test that leaks a
# published generation into the next test is invisible in declaration order and
# deterministic under some seeds. GF5/H1 shipped exactly that way — the suite
# passed in order and failed on -shuffle=1785654853027566000 — so order
# independence is checked, not assumed.
check:
	go test -race -short ./...
	go test -count=1 -shuffle=on ./pkg/driver/
