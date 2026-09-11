package driver

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/GizmoTickler/scale-csi/pkg/truenas"
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
	f.Add("snap@shot")
	f.Add("a@b@c/d e")
	f.Add(strings.Repeat("a", 127) + "é")
	f.Fuzz(func(t *testing.T, input string) {
		got := sanitizeVolumeID(input)
		if !utf8.ValidString(got) {
			t.Fatalf("sanitized ID is invalid UTF-8: %q", got)
		}
		if strings.Contains(got, "/") {
			t.Fatalf("sanitized ID contains '/': %q", got)
		}
		// '@' must never survive sanitization: CSI snapshot handle format
		// detection relies on a legacy short-name handle never containing it
		// (see parseQualifiedSnapshotHandle).
		if strings.Contains(got, "@") {
			t.Fatalf("sanitized ID contains '@': %q", got)
		}
		if len(got) > 128 {
			t.Fatalf("sanitized ID is %d bytes", len(got))
		}
		if got != "" && !isLowerAlphanumeric(got[0]) {
			t.Fatalf("sanitized ID has invalid leading byte: %q", got)
		}
	})
}

// FuzzIsLocalUserPropertySource pins the exact-match discipline
// datasetHasLocalOwnershipStamp and every fencing/CHAP-adoption caller depend
// on: a ZFS clone-inherited user property reports its SOURCE as the origin
// snapshot name (e.g. "tank/src@snap"), never the literal string "inherited",
// so a substring/prefix match here would let a clone parse its source
// volume's ownership or auth stamps as its own. The safety property is
// fail-closed: anything that is not an exact (trimmed, case-insensitive)
// "local" must classify as not-local, including origin-snapshot-shaped
// strings and near-miss spellings.
func FuzzIsLocalUserPropertySource(f *testing.F) {
	f.Add("local")
	f.Add("LOCAL")
	f.Add(" Local\t")
	f.Add("inherited")
	f.Add("INHERITED")
	f.Add("tank/src@snap")
	f.Add("localhost")
	f.Add("not-local")
	f.Add("")
	f.Fuzz(func(t *testing.T, source string) {
		got := isLocalUserPropertySource(source)
		want := strings.EqualFold(strings.TrimSpace(source), "local")
		if got != want {
			t.Fatalf("isLocalUserPropertySource(%q) = %v, want %v", source, got, want)
		}
		if strings.Contains(source, "@") && got {
			t.Fatalf("isLocalUserPropertySource(%q) = true for an origin-snapshot-shaped source (clone-inherited value)", source)
		}
	})
}

// FuzzDatasetHasLocalUserProperty pins the fail-closed contract of the
// primitive every local-ownership check in this package is built from: it may
// report true only when the property is BOTH present with the exact expected
// value AND locally sourced. Any other combination (absent, wrong value,
// inherited/unknown source) must report false — an ambiguous or malformed
// wire shape must never look like a confident local stamp.
func FuzzDatasetHasLocalUserProperty(f *testing.F) {
	f.Add("scale-csi:managed_resource", "true", "true", "local", true)
	f.Add("scale-csi:managed_resource", "true", "true", "tank/src@snap", true)
	f.Add("scale-csi:managed_resource", "true", "false", "local", true)
	f.Add("scale-csi:driver_instance_id", "instance-a", "instance-a", "", true)
	f.Add("scale-csi:managed_resource", "true", "true", "LOCAL", false)
	f.Fuzz(func(t *testing.T, key, expected, storedValue, source string, present bool) {
		ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{}}
		if present {
			ds.UserProperties[key] = truenas.UserProperty{Value: storedValue, Source: source}
		}
		got := datasetHasLocalUserProperty(ds, key, expected)
		want := present && storedValue == expected && isLocalUserPropertySource(source)
		if got != want {
			t.Fatalf("datasetHasLocalUserProperty(key=%q, expected=%q; stored=%q, source=%q, present=%v) = %v, want %v",
				key, expected, storedValue, source, present, got, want)
		}
	})
}

// FuzzDatasetHasLocalOwnershipStampFailsClosed is the fuzz-expansion brief's
// central ask made concrete: datasetHasLocalOwnershipStamp gates whether a
// dataset is treated as owned by this driver instance, and an ownership
// verdict here gates deletion elsewhere in the reconciler. The function must
// report owned=true only when the wire ACTUALLY marked one of the two
// ownership stamps as local — never on an inherited, unknown, or absent
// source, regardless of how the value field is spelled. This directly
// reproduces the two branches of datasetHasLocalOwnershipStamp
// (reconcile.go): a local scale-csi:driver_instance_id of ANY value, or a
// local scale-csi:managed_resource whose value is exactly "true".
func FuzzDatasetHasLocalOwnershipStampFailsClosed(f *testing.F) {
	f.Add(true, "instance-a", "local", false, "true", "local")
	f.Add(false, "", "", true, "true", "tank/src@snap")
	f.Add(true, "instance-a", "inherited", true, "true", "inherited")
	f.Add(false, "", "", true, "false", "local")
	f.Add(true, "instance-a", "LOCAL", false, "", "")
	f.Fuzz(func(t *testing.T,
		haveInstanceID bool, instanceIDValue, instanceIDSource string,
		haveManaged bool, managedValue, managedSource string,
	) {
		ds := &truenas.Dataset{UserProperties: map[string]truenas.UserProperty{}}
		if haveInstanceID {
			ds.UserProperties[PropDriverInstanceID] = truenas.UserProperty{Value: instanceIDValue, Source: instanceIDSource}
		}
		if haveManaged {
			ds.UserProperties[PropManagedResource] = truenas.UserProperty{Value: managedValue, Source: managedSource}
		}

		got := datasetHasLocalOwnershipStamp(ds)

		localInstanceID := haveInstanceID && isLocalUserPropertySource(instanceIDSource)
		localManaged := haveManaged && managedValue == "true" && isLocalUserPropertySource(managedSource)
		want := localInstanceID || localManaged

		if got != want {
			t.Fatalf("datasetHasLocalOwnershipStamp diverged from the local-stamp model: got=%v want=%v "+
				"(instanceID present=%v value=%q source=%q; managed present=%v value=%q source=%q)",
				got, want, haveInstanceID, instanceIDValue, instanceIDSource, haveManaged, managedValue, managedSource)
		}
		if got && !localInstanceID && !localManaged {
			t.Fatal("datasetHasLocalOwnershipStamp returned owned=true without any locally-sourced stamp: fail-closed violation")
		}
	})
}
