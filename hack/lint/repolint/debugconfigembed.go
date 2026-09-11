package main

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// DebugConfigEmbed bans embedding Config or TrueNASConfig (by value or
// pointer) as an anonymous field in any other struct in pkg/driver.
//
// Why: DebugState (pkg/driver/debug_endpoint.go) documents a hard invariant —
// "every field below is an EXPLICIT non-secret allowlist... because Config
// carries the TrueNAS API key (and future config may carry more credential
// material). When extending this struct, copy individual fields — never
// embed Config, TrueNASConfig, or any other yaml-decoded struct." That
// invariant is currently upheld by discipline plus a regression test
// (TestDebugStateNeverLeaksSecrets); this analyzer makes the same rule
// mechanically enforced repo-wide, so a future JSON-serializable struct
// anywhere in pkg/driver cannot silently embed the credential-bearing config
// types and leak them through a debug/metrics/log surface's default
// marshaling.
var DebugConfigEmbed = &analysis.Analyzer{
	Name: "debugconfigembed",
	Doc:  "flags a struct embedding Config or TrueNASConfig as an anonymous field",
	Run:  runDebugConfigEmbed,
}

var sensitiveConfigTypes = map[string]bool{
	"Config":        true,
	"TrueNASConfig": true,
}

func runDebugConfigEmbed(pass *analysis.Pass) (any, error) {
	if !strings.HasSuffix(pass.Pkg.Path(), "/pkg/driver") && pass.Pkg.Path() != "pkg/driver" {
		return nil, nil
	}

	for _, file := range pass.Files {
		if isGeneratedOrTestFile(pass.Fset, file) {
			continue
		}
		lc := newLineComments(pass.Fset, file)
		ast.Inspect(file, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			st, ok := ts.Type.(*ast.StructType)
			if !ok || st.Fields == nil {
				return true
			}
			for _, field := range st.Fields.List {
				if len(field.Names) != 0 {
					continue // not an embedded/anonymous field
				}
				name := embeddedTypeName(field.Type)
				if sensitiveConfigTypes[name] && !lc.suppressed(pass.Fset, int(field.Pos()), "RG-DEBUG-CONFIG-EMBED") {
					pass.Reportf(field.Pos(),
						"RG-DEBUG-CONFIG-EMBED: struct %q embeds %q as an anonymous field — Config/TrueNASConfig carry credential material (TrueNAS API key; see DebugState's doc comment in pkg/driver/debug_endpoint.go), so embedding rather than copying named non-secret fields risks leaking it through this struct's default JSON/log marshaling",
						ts.Name.Name, name)
				}
			}
			return true
		})
	}
	return nil, nil
}

// embeddedTypeName extracts the bare type name from an embedded field's type
// expression, unwrapping a leading pointer if present (e.g. *Config -> "Config").
func embeddedTypeName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}
