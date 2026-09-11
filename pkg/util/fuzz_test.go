package util

import (
	"fmt"
	"strings"
	"testing"
)

func FuzzParseProcMounts(f *testing.F) {
	f.Add("nas:/export /target nfs4 rw 0 0\n", "/target")
	f.Add("nas:/team\\040data /team\\040data nfs4 rw 0 0\n", "/team data")
	f.Add("", "")
	f.Fuzz(func(t *testing.T, contents, target string) {
		_, _ = parseProcMounts(strings.NewReader(contents), target)
	})
}

func FuzzUnescapeProcMountField(f *testing.F) {
	f.Add("plain", uint8(0o40))
	f.Add("\\040", uint8(0o377))
	f.Fuzz(func(t *testing.T, input string, octet uint8) {
		_ = unescapeProcMountField(input)
		escaped := fmt.Sprintf("\\%03o", octet)
		if got, want := unescapeProcMountField(escaped), string([]byte{octet}); got != want {
			t.Fatalf("unescapeProcMountField(%q) = %q, want byte %03o", escaped, got, octet)
		}
	})
}

func FuzzListNVMeSubsystemsParse(f *testing.F) {
	f.Add([]byte(`{"Subsystems":[{"NQN":"nqn.object"}]}`))
	f.Add([]byte(`[{"HostNQN":"host","Subsystems":[{"NQN":"nqn.host"}]}]`))
	f.Add([]byte(`[{"NQN":"nqn.array"}]`))
	f.Add([]byte(`not-json`))
	f.Fuzz(func(t *testing.T, data []byte) {
		subsystems, err := parseSubsysJSON(data)
		if err != nil {
			return
		}
		for _, subsystem := range subsystems {
			if strings.TrimSpace(subsystem.NQN) == "" {
				t.Fatal("parser returned an empty-NQN phantom subsystem")
			}
		}
	})
}

// FuzzLiveNVMeoFAddressesNeverFabricatesOrDrops targets the multipath surface
// where a real bug lived: code that took Paths[0] as "the" address for a
// subsystem, when kernel enumeration order under multipath is nondeterministic
// (see NVMeoFSessionInfo.Addresses / ListNVMeoFSessions). LiveNVMeoFAddresses
// is the production helper session-GC scoping decisions actually call, and it
// must walk EVERY path of the matching subsystem, not just the first: an
// address list that fabricates or drops a live path is exactly the class of
// bug that made a scoping decision look at a "representative" address that
// happened not to be the one actually configured.
func FuzzLiveNVMeoFAddressesNeverFabricatesOrDrops(f *testing.F) {
	f.Add([]byte(`[{"NQN":"nqn.zero-paths","Paths":[]}]`), "nqn.zero-paths")
	f.Add([]byte(`[{"NQN":"nqn.one","Paths":[{"Address":"traddr=192.0.2.10,trsvcid=4420","State":"live"}]}]`), "nqn.one")
	f.Add([]byte(`[{"NQN":"nqn.multi","Paths":[
		{"Address":"traddr=192.0.2.10,trsvcid=4420","State":"live"},
		{"Address":"traddr=192.0.2.11,trsvcid=4420","State":"connecting"},
		{"Address":"traddr=[2001:db8::1],trsvcid=4420","State":"live"},
		{"Address":"traddr=192.0.2.10,trsvcid=4420","State":"live"}
	]}]`), "nqn.multi")
	f.Add([]byte(`[{"NQN":"nqn.dup-subsys","Paths":[{"Address":"traddr=10.0.0.1,trsvcid=4420","State":"LIVE"}]},
		{"NQN":"nqn.dup-subsys","Paths":[{"Address":"traddr=10.0.0.2,trsvcid=4420","State":" live "}]}]`), "nqn.dup-subsys")
	f.Fuzz(func(t *testing.T, data []byte, nqn string) {
		subsystems, err := parseSubsysJSON(data)
		if err != nil {
			return
		}
		addresses := LiveNVMeoFAddresses(nqn, subsystems)

		seen := make(map[string]bool, len(addresses))
		for _, address := range addresses {
			if seen[address] {
				t.Fatalf("LiveNVMeoFAddresses(%q) returned duplicate address %q", nqn, address)
			}
			seen[address] = true
		}

		liveTraddrs := make(map[string]bool)
		for _, subsystem := range subsystems {
			if subsystem.NQN != nqn {
				continue
			}
			for _, path := range subsystem.Paths {
				if !strings.EqualFold(strings.TrimSpace(path.State), "live") {
					continue
				}
				if traddr := nvmePathField(path.Address, "traddr"); traddr != "" {
					liveTraddrs[traddr] = true
				}
			}
		}

		for _, address := range addresses {
			if !liveTraddrs[address] {
				t.Fatalf("LiveNVMeoFAddresses(%q) fabricated address %q that no live path reported", nqn, address)
			}
		}
		for traddr := range liveTraddrs {
			if !seen[traddr] {
				t.Fatalf("LiveNVMeoFAddresses(%q) dropped live path address %q (Paths[0]-only bug class)", nqn, traddr)
			}
		}
	})
}

// FuzzParseISCSISessionLines targets the `iscsiadm -m session` text-parsing
// surface named in the fuzz-expansion brief. Output shape varies by tool
// version (truncated lines, duplicate sessions, IPv6 portals, empty output);
// the invariant is that the parser never panics and never fabricates a session
// whose IQN does not start with the "iqn." literal that anchors the regex, and
// never returns more sessions than input lines.
func FuzzParseISCSISessionLines(f *testing.F) {
	f.Add([]byte("tcp: [1] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-abc (non-flash)\n"))
	f.Add([]byte("tcp: [1] [2001:db8::1]:3260,1 iqn.2005-10.org.freenas.ctl:pvc-ipv6 (non-flash)\n"))
	f.Add([]byte("No active sessions.\n"))
	f.Add([]byte(""))
	f.Add([]byte("tcp: [1] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-a (non-flash)\ntcp: [2] 192.0.2.100:3260,1 iqn.2005-10.org.freenas.ctl:pvc-a (non-flash)\n"))
	f.Add([]byte("tcp: [999999999999999999999] 192.0.2.100:3260,1 iqn.x\n"))
	f.Fuzz(func(t *testing.T, output []byte) {
		sessions := parseISCSISessionLines(output)
		lineCount := strings.Count(string(output), "\n") + 1
		if len(sessions) > lineCount {
			t.Fatalf("parseISCSISessionLines produced %d sessions from %d lines", len(sessions), lineCount)
		}
		for _, session := range sessions {
			if !strings.HasPrefix(session.IQN, "iqn.") {
				t.Fatalf("session IQN %q does not start with the anchoring literal", session.IQN)
			}
		}
	})
}

// FuzzParseBlkidExportOutput targets `blkid -o export` output parsing, named
// explicitly in the fuzz-expansion brief. The invariant is round-trip
// stability: re-serialising the extracted TYPE/PTTYPE the way blkid itself
// would (quoted, matching the parser's own `Trim(TrimSpace(value), `"`)`
// unquoting) and re-parsing must yield the same pair, and the parser must
// never panic on truncated, duplicated, or out-of-order KEY=value lines.
//
// The reserialization MUST quote the value: parseBlkidExportOutput trims
// surrounding whitespace BEFORE stripping quotes, so a value like " 0" can
// only be produced from a quoted wire line (`TYPE=" 0"`, where the space is
// protected from TrimSpace by the surrounding quotes). An earlier, unquoted
// version of this reserialization step re-fed `TYPE= 0` instead, which
// TrimSpace legitimately collapses to "0" -- a false-positive in the fuzz
// oracle, not a parser bug (found immediately on the very first fuzz run;
// fixed here rather than weakening the parser). Values containing a literal
// `"` are skipped: blkid's own export format has no escaping for embedded
// quotes, so no reserialization of this test's choosing can represent them
// faithfully, and that is a limitation of the test's round-trip encoding, not
// a property of the parser.
func FuzzParseBlkidExportOutput(f *testing.F) {
	f.Add("TYPE=ext4\nPTTYPE=gpt\n")
	f.Add("TYPE=\"xfs\"\n")
	f.Add("PTTYPE=dos\nTYPE=ext4\nPTTYPE=gpt\n")
	f.Add("")
	f.Add("garbage\nTYPE\n=novalue\n")
	f.Add("TYPE=\" 0")
	f.Fuzz(func(t *testing.T, output string) {
		fsType, ptType := parseBlkidExportOutput(output)
		if strings.ContainsRune(fsType, '"') || strings.ContainsRune(ptType, '"') {
			return
		}
		// No escaping needed: fsType/ptType can never contain a newline (they
		// are derived from a single already-split line) and, past the guard
		// above, never contain a quote either, so wrapping in literal quotes
		// reproduces exactly the wire shape parseBlkidExportOutput unquotes.
		reserialized := "TYPE=\"" + fsType + "\"\nPTTYPE=\"" + ptType + "\"\n"
		fsType2, ptType2 := parseBlkidExportOutput(reserialized)
		if fsType2 != fsType || ptType2 != ptType {
			t.Fatalf("parseBlkidExportOutput is not a stable fixed point: (%q,%q) -> reserialize %q -> (%q,%q)",
				fsType, ptType, reserialized, fsType2, ptType2)
		}
	})
}

// FuzzNormalizeSCSIWWID pins idempotence of the WWID normalization multipath
// device-mapper UUID matching depends on: normalizing an already-normalized
// WWID must be a no-op, otherwise a device could silently stop matching its
// own multipath map on a second pass.
func FuzzNormalizeSCSIWWID(f *testing.F) {
	f.Add("naa.5000c500a1b2c3d4")
	f.Add("eui.0011223344556677")
	f.Add("t10.ATA SanDisk SSD Serial 1234")
	f.Add("  NAA.ABCDEF  ")
	f.Add("")
	f.Fuzz(func(t *testing.T, wwid string) {
		once := normalizeSCSIWWID(wwid)
		twice := normalizeSCSIWWID(once)
		if once != twice {
			t.Fatalf("normalizeSCSIWWID is not idempotent: %q -> %q -> %q", wwid, once, twice)
		}
	})
}

// FuzzCanonicalISCSIPortalForComparison pins properties multipath session
// matching depends on: sameISCSIPortal is reflexive for any portal string
// against itself, and canonicalization always produces a host:port-shaped
// string (every return path in canonicalISCSIPortalForComparison goes through
// net.JoinHostPort, or returns the original string only once it has confirmed
// a ":" is already present).
//
// FINDING (NOT fixed -- see docs/guides/fuzzing.md and the fuzz-expansion
// report): canonicalISCSIPortalForComparison is not idempotent, and repeated
// application does not converge within any small fixed number of passes, for
// degenerate bracket-garbage portal strings:
//
//   - "[]": pass1 net.SplitHostPort("[]") errors (no ":port"), so the
//     "no colon" branch fires and returns JoinHostPort("[]","3260") =
//     "[]:3260". pass2: THAT string parses as a valid (if empty) bracketed
//     host, host="", port="3260", which JoinHostPort re-renders WITHOUT
//     brackets (it only adds them when the host contains a colon) as
//     ":3260" -- different from pass1's output. pass3 is stable at ":3260".
//   - "[ ]" (a literal space inside the brackets) needs a THIRD distinct
//     value before stabilizing: "[ ]" -> "[ ]:3260" -> " :3260" -> ":3260".
//
// Both corpus entries are committed as permanent regression pins. Because a
// second malformed input needed one MORE pass than the first, there is no
// evidence of a fixed convergence bound, so this target intentionally does
// NOT assert eventual idempotence -- only properties verified to hold for
// every input regardless of how many passes canonicalization would need.
// This instability means two textually different portal strings surfaced by
// two different code paths (one already run through canonicalization once,
// one not) COULD compare unequal via sameISCSIPortal even though a human
// would call them the same broken portal; the practical exposure is low
// because canonicalISCSIPortalForComparison is only ever called on raw
// `TargetPortal`/configured-address strings, never on its own prior output,
// but the asymmetry is real and unfixed.
func FuzzCanonicalISCSIPortalForComparison(f *testing.F) {
	f.Add("192.0.2.10:3260")
	f.Add("[2001:db8::1]:3260")
	f.Add("2001:db8::1")
	f.Add("Portal.Example.Com")
	f.Add("192.0.2.10")
	f.Add("")
	f.Add("[]")
	f.Add("[ ]")
	f.Fuzz(func(t *testing.T, portal string) {
		if !sameISCSIPortal(portal, portal) {
			t.Fatalf("sameISCSIPortal(%q, %q) must be reflexive", portal, portal)
		}
		canonical := canonicalISCSIPortalForComparison(portal)
		if !strings.Contains(canonical, ":") {
			t.Fatalf("canonicalISCSIPortalForComparison(%q) = %q is not host:port-shaped", portal, canonical)
		}
	})
}
