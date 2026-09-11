package main

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// WedgedGuard requires that a function pattern-matching a hardened command's
// output text also checks isWedgedCommandErr(err) first.
//
// Why: hardenCmd's own doc comment in pkg/util/iscsi.go states the invariant
// this analyzer enforces: "Callers that pattern-match 'not found'/'no
// session' style output on a clean failure MUST check that first [i.e.
// isWedgedCommandErr], so a wedged transport is never misreported as an
// idempotent no-op." WaitDelay forces Wait to give up on a wedged nsenter
// grandchild rather than hang forever, but the command's output buffer at
// that point can be empty or truncated mid-message. Text-matching that
// truncated output for an "already done" idiom without first ruling out
// isWedgedCommandErr(err) risks the exact misclassification the comment
// warns about: treating a wedged, possibly-still-running host command as a
// clean idempotent success. Every existing strings.Contains(output, ...)
// fallback in pkg/util currently skips this check — this analyzer exists so
// that gap cannot spread further while it is fixed.
var WedgedGuard = &analysis.Analyzer{
	Name: "wedgedguard",
	Doc:  "flags strings.Contains(output, ...) idempotency fallbacks that never check isWedgedCommandErr",
	Run:  runWedgedGuard,
}

func runWedgedGuard(pass *analysis.Pass) (any, error) {
	if !strings.HasSuffix(pass.Pkg.Path(), "/pkg/util") && pass.Pkg.Path() != "pkg/util" {
		return nil, nil
	}

	for _, file := range pass.Files {
		if isGeneratedOrTestFile(pass.Fset, file) {
			continue
		}
		lc := newLineComments(pass.Fset, file)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			checkWedgedGuardInFunc(pass, lc, fn.Body)
		}
	}
	return nil, nil
}

func checkWedgedGuardInFunc(pass *analysis.Pass, lc *lineComments, body *ast.BlockStmt) {
	hasGuard := false
	var candidates []*ast.CallExpr

	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "isWedgedCommandErr" {
			hasGuard = true
			return true
		}
		if isStringsContainsOnOutput(call) {
			candidates = append(candidates, call)
		}
		return true
	})

	if hasGuard {
		return
	}
	for _, call := range candidates {
		if lc.suppressed(pass.Fset, int(call.Pos()), "RG-WEDGED-GUARD") {
			continue
		}
		pass.Reportf(call.Pos(),
			"RG-WEDGED-GUARD: strings.Contains on command output is used as an idempotency fallback without first checking isWedgedCommandErr(err) anywhere in this function — see hardenCmd's doc comment in pkg/util/iscsi.go: a WaitDelay-truncated wedged command's output must not be text-matched as a clean no-op")
	}
}

// isStringsContainsOnOutput reports whether call is strings.Contains(...)
// whose first argument mentions an identifier containing "output" — the
// shape of every existing idempotency fallback in this package
// (strings.Contains(string(output), "...")).
func isStringsContainsOnOutput(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Contains" {
		return false
	}
	pkgID, ok := sel.X.(*ast.Ident)
	if !ok || pkgID.Name != "strings" {
		return false
	}
	if len(call.Args) == 0 {
		return false
	}
	return mentionsOutputIdent(call.Args[0])
}

func mentionsOutputIdent(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && strings.Contains(strings.ToLower(id.Name), "output") {
			found = true
		}
		return true
	})
	return found
}
