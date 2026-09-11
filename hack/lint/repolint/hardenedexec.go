package main

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// HardenedExec bans constructing an *exec.Cmd via exec.Command/exec.CommandContext
// in pkg/util and pkg/driver unless the result is handed to hardenCmd.
//
// Why: the node plugin runs with hostPID and reaches host tools through bash
// wrappers (docker/iscsiadm, docker/nvme, docker/mount, docker/umount) that
// `exec nsenter -t 1 ...`. A plain exec.CommandContext only ever signals its
// direct child (the wrapper script); cancellation does not reach the
// nsenter'd grandchild, which can be left holding the command's output pipe
// open. Cmd.Wait then blocks forever, and since these commands run under a
// per-volume operation lock, the lock is never released — a single wedged
// host command can starve an entire volume's operations. hardenCmd (see
// pkg/util/iscsi.go) puts the child in its own process group, kills the whole
// group on cancel, and backstops with WaitDelay so Wait always returns.
//
// A call site that builds an *exec.Cmd and never reaches hardenCmd is exactly
// the class of bug this analyzer exists to catch: it is real (see
// pkg/driver/node_identity.go's nodeIdentityCommand seam, which execs the
// "nvme" wrapper without hardening).
var HardenedExec = &analysis.Analyzer{
	Name: "hardenedexec",
	Doc:  "flags exec.Command/exec.CommandContext construction in pkg/util or pkg/driver that never reaches hardenCmd",
	Run:  runHardenedExec,
}

func runHardenedExec(pass *analysis.Pass) (any, error) {
	if !isTargetPackage(pass.Pkg.Path()) {
		return nil, nil
	}

	for _, file := range pass.Files {
		if isGeneratedOrTestFile(pass.Fset, file) {
			continue
		}
		lc := newLineComments(pass.Fset, file)

		// Each top-level func (and each package-level var initialized with a
		// func literal, e.g. the nodeIdentityCommand seam) is treated as one
		// flat scope: collect every exec.Command(Context) assignment target
		// and every identifier passed to hardenCmd, then diff them.
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Body != nil {
					checkExecScope(pass, lc, d.Body)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					vs, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, val := range vs.Values {
						if fn, ok := val.(*ast.FuncLit); ok {
							checkExecScope(pass, lc, fn.Body)
						}
					}
				}
			}
		}
	}
	return nil, nil
}

// checkExecScope inspects one function-shaped scope (a FuncDecl body or a
// FuncLit body) for exec.Command/exec.CommandContext calls and reports each
// one that is not provably passed to hardenCmd within the same scope.
func checkExecScope(pass *analysis.Pass, lc *lineComments, body *ast.BlockStmt) {
	hardened := map[string]bool{}
	type finding struct {
		pos ast.Node // an inline/chained call that can never be hardened
	}
	var findings []finding

	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if isHardenCmdCall(call) {
			if len(call.Args) == 1 {
				if id, ok := call.Args[0].(*ast.Ident); ok {
					hardened[id.Name] = true
				}
			}
			return true
		}
		if !isExecCommandCall(call) {
			return true
		}
		// A chained call (e.g. exec.CommandContext(...).Output()) can never
		// be hardened: there is no variable to pass to hardenCmd.
		findings = append(findings, finding{pos: call})
		return true
	})

	// Re-walk to find which exec.Command(Context) calls were actually
	// assigned to a variable (those are excluded from the "inline" findings
	// above and checked against the hardened set instead). A variable may be
	// assigned from more than one call site (e.g. a switch that picks the
	// mkfs binary, or a reassignment on a fallback path), and hardenCmd is
	// typically called once after all of them join — so every call assigned
	// to a given name is judged by that one name's hardened status.
	assigned := map[string][]ast.Node{}
	ast.Inspect(body, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range assign.Rhs {
			call, ok := rhs.(*ast.CallExpr)
			if !ok || !isExecCommandCall(call) {
				continue
			}
			if i >= len(assign.Lhs) {
				continue
			}
			id, ok := assign.Lhs[i].(*ast.Ident)
			if !ok || id.Name == "_" {
				continue
			}
			assigned[id.Name] = append(assigned[id.Name], call)
		}
		return true
	})

	// Findings from the first pass include both inline calls AND calls that
	// were assigned to a variable (an *ast.CallExpr matches isExecCommandCall
	// regardless of context). Drop the ones we now know were assigned; those
	// are judged by variable reach instead.
	reported := map[ast.Node]bool{}
	for name, calls := range assigned {
		for _, call := range calls {
			reported[call] = true
			if !hardened[name] && !lc.suppressed(pass.Fset, int(call.Pos()), "RG-EXEC-UNHARDENED") {
				pass.Reportf(call.Pos(),
					"RG-EXEC-UNHARDENED: exec.Command/exec.CommandContext assigned to %q is never passed to hardenCmd() in this scope — a hostPID node-plugin command left unhardened can wedge Cmd.Wait forever on a killed nsenter grandchild, holding a per-volume lock open (see pkg/util/iscsi.go's hardenCmd doc comment); call hardenCmd(%s) before invoking Output/CombinedOutput/Run/Start", name, name)
			}
		}
	}
	for _, f := range findings {
		if reported[f.pos] {
			continue
		}
		if lc.suppressed(pass.Fset, int(f.pos.Pos()), "RG-EXEC-UNHARDENED") {
			continue
		}
		pass.Reportf(f.pos.Pos(),
			"RG-EXEC-UNHARDENED: exec.Command/exec.CommandContext result is used inline/chained and can never be passed to hardenCmd() — assign it to a variable, call hardenCmd(cmd), then invoke Output/CombinedOutput/Run/Start (see pkg/util/iscsi.go's hardenCmd doc comment for why an un-hardened hostPID exec can wedge forever)")
	}
}

func isHardenCmdCall(call *ast.CallExpr) bool {
	id, ok := call.Fun.(*ast.Ident)
	return ok && id.Name == "hardenCmd"
}

func isExecCommandCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkgID, ok := sel.X.(*ast.Ident)
	if !ok || pkgID.Name != "exec" {
		return false
	}
	return sel.Sel.Name == "Command" || sel.Sel.Name == "CommandContext"
}

func isTargetPackage(path string) bool {
	return strings.HasSuffix(path, "/pkg/util") || strings.HasSuffix(path, "/pkg/driver") ||
		path == "pkg/util" || path == "pkg/driver"
}
