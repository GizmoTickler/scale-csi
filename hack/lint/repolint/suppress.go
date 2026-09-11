package main

import (
	"go/ast"
	"go/token"
	"strings"
)

// lineComments indexes every comment in a file by its 1-based source line, so
// a repolint finding on that line can be suppressed the same way golangci-lint
// suppresses a `//nolint` finding: a same-line (or immediately-preceding)
// comment containing the marker.
//
// repolint has no golangci-lint integration to hook into for suppression, so
// it defines its own minimal convention:
//
//	//repolint:ignore RULE-ID reason why this specific site is a known,
//	//tracked exception rather than a false positive.
//
// A bare disable with no reason is intentionally NOT supported: every
// suppression in this codebase should read like the //nolint comments
// alongside it (nolintlint enforces the same discipline for golangci-lint).
type lineComments struct {
	fset  *token.FileSet
	byPos map[int]string // file offset (line-start-ish) -> comment text, keyed by line number via lineOf
}

func newLineComments(fset *token.FileSet, file *ast.File) *lineComments {
	lc := &lineComments{fset: fset, byPos: map[int]string{}}
	for _, cg := range file.Comments {
		for _, c := range cg.List {
			line := fset.Position(c.Pos()).Line
			lc.byPos[line] = lc.byPos[line] + " " + c.Text
		}
	}
	return lc
}

// suppressed reports whether ruleID is suppressed at line (same line, or the
// line directly above it, matching where a developer would naturally put an
// explanatory comment above a flagged statement).
func (lc *lineComments) suppressed(fset *token.FileSet, pos int, ruleID string) bool {
	line := fset.Position(token.Pos(pos)).Line
	marker := "repolint:ignore " + ruleID
	for _, l := range []int{line, line - 1} {
		if strings.Contains(lc.byPos[l], marker) {
			return true
		}
	}
	return false
}
