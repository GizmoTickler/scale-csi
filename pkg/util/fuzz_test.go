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
