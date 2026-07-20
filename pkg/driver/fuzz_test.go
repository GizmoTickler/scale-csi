package driver

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func FuzzProtocolShareName(f *testing.F) {
	f.Add("pvc-123")
	f.Add("🔥")
	f.Add("...")
	f.Add("-leading")
	f.Fuzz(func(t *testing.T, input string) {
		got := protocolShareName(input)
		if input != "" && got == "" {
			t.Fatal("non-empty input produced an empty share name")
		}
		if len(got) > 64 {
			t.Fatalf("share name is %d bytes", len(got))
		}
		if got == "" || !isLowerAlphanumeric(got[0]) {
			t.Fatalf("share name has an invalid leading byte: %q", got)
		}
		for _, r := range got {
			valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == ':' || r == '-'
			if !valid {
				t.Fatalf("share name contains invalid rune %q: %q", r, got)
			}
		}
		if again := protocolShareName(input); again != got {
			t.Fatalf("share name is not deterministic: %q != %q", got, again)
		}
	})
}

func FuzzSanitizeVolumeID(f *testing.F) {
	f.Add("pvc-123")
	f.Add("🔥/volume")
	f.Add(strings.Repeat("a", 127) + "é")
	f.Fuzz(func(t *testing.T, input string) {
		got := sanitizeVolumeID(input)
		if !utf8.ValidString(got) {
			t.Fatalf("sanitized ID is invalid UTF-8: %q", got)
		}
		if strings.Contains(got, "/") {
			t.Fatalf("sanitized ID contains '/': %q", got)
		}
		if len(got) > 128 {
			t.Fatalf("sanitized ID is %d bytes", len(got))
		}
		if got != "" && !isLowerAlphanumeric(got[0]) {
			t.Fatalf("sanitized ID has invalid leading byte: %q", got)
		}
	})
}
