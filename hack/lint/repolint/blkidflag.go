package main

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
)

// BlkidFlag bans constructing a "blkid" argv that omits "-p".
//
// Why: without -p, blkid can answer purely from the /run/blkid/blkid.tab
// cache, which keys on device NAME. A recycled block-device name (e.g. after
// a detach/reattach cycle picks the same /dev/nvmeXnY) can then report a
// completely different, stale physical device's filesystem type. -p forces
// low-level superblock probing of the CURRENT device instead. Separately,
// omitting -s PTTYPE made a partitioned-but-unformatted device (TYPE empty,
// PTTYPE set) indistinguishable from a genuinely blank device (both empty),
// so FormatDeviceWithContext's mkfs -F ran over a live partition table. See
// GetFilesystemTypeWithContext in pkg/util/mount.go for the fixed call and
// its rationale.
var BlkidFlag = &analysis.Analyzer{
	Name: "blkidflag",
	Doc:  "flags a blkid invocation whose argv omits -p",
	Run:  runBlkidFlag,
}

func runBlkidFlag(pass *analysis.Pass) (any, error) {
	if !isTargetPackage(pass.Pkg.Path()) {
		return nil, nil
	}

	for _, file := range pass.Files {
		if isGeneratedOrTestFile(pass.Fset, file) {
			continue
		}
		lc := newLineComments(pass.Fset, file)
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isExecCommandCall(call) {
				return true
			}
			checkBlkidArgs(pass, lc, call)
			return true
		})
	}
	return nil, nil
}

func checkBlkidArgs(pass *analysis.Pass, lc *lineComments, call *ast.CallExpr) {
	// exec.Command(name, args...) vs exec.CommandContext(ctx, name, args...):
	// find the selector name to know where the command-name argument sits.
	sel := call.Fun.(*ast.SelectorExpr) //nolint:forcetypeassert // guarded by isExecCommandCall
	nameIdx := 0
	if sel.Sel.Name == "CommandContext" {
		nameIdx = 1
	}
	if len(call.Args) <= nameIdx {
		return
	}
	nameLit, ok := call.Args[nameIdx].(*ast.BasicLit)
	if !ok || unquote(nameLit) != "blkid" {
		return
	}

	sawDashP := false
	sawVariadicPassthrough := false
	for _, arg := range call.Args[nameIdx+1:] {
		if lit, ok := arg.(*ast.BasicLit); ok {
			if unquote(lit) == "-p" {
				sawDashP = true
			}
			continue
		}
		// A non-literal argument (e.g. a variadic `args...` slice variable)
		// means the full argv is not statically visible; do not guess.
		sawVariadicPassthrough = true
	}
	if sawVariadicPassthrough || sawDashP {
		return
	}
	if lc.suppressed(pass.Fset, int(call.Pos()), "RG-BLKID-NO-P") {
		return
	}
	pass.Reportf(call.Pos(),
		"RG-BLKID-NO-P: blkid invoked without -p — this can answer from the stale /run/blkid/blkid.tab cache for a recycled device name instead of probing the current device, and (combined with missing -s PTTYPE) can make a partitioned device look blank; see GetFilesystemTypeWithContext in pkg/util/mount.go")
}

func unquote(lit *ast.BasicLit) string {
	if len(lit.Value) < 2 {
		return lit.Value
	}
	return lit.Value[1 : len(lit.Value)-1]
}
