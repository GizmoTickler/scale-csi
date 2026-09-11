// Command repolint is a small, repo-specific static analyzer for scale-csi.
// It encodes bug classes that a deep code review found and that generic
// linters (govet, staticcheck, gocritic's built-in checks, ...) would not
// catch, because each one depends on this codebase's own helpers, comments,
// and invariants rather than a general Go footgun.
//
// It is wired into CI as its own step (see .github/workflows/ci.yml, the "Go
// Lint" job) rather than as a golangci-lint module plugin: golangci-lint v2's
// plugin system requires building a dedicated golangci-lint binary via
// `golangci-lint custom`, which is heavier to keep reproducible in CI than a
// `go run` of an ordinary analysis.Analyzer-based tool living in the module
// itself. The rule that IS naturally expressible as a pure AST pattern (raw
// UserProperties indexing) instead lives in hack/ruleguard/rules.go and runs
// through gocritic's ruleguard checker, which golangci-lint already runs.
package main

import (
	"go/ast"
	"go/token"
	"strings"
)

// isGeneratedOrTestFile reports whether file should be skipped: test files
// have their own conventions (mocks, table-driven fixtures) that legitimately
// differ from production code, and are out of scope for every rule below.
func isGeneratedOrTestFile(fset *token.FileSet, file *ast.File) bool {
	name := fset.Position(file.Package).Filename
	return strings.HasSuffix(name, "_test.go")
}
