package truenas

import "strings"

// CSI user-property namespace.
//
// The driver's on-disk bookkeeping rides ZFS user properties. Since v1.10.0
// they are written under the vendor-neutral "scale-csi:" namespace; releases
// before that wrote "truenas-csi:". Every wire decoder in this package folds
// the legacy namespace onto the canonical one at parse time (see
// normalizeCSIUserProperties), so driver code only ever reads canonical keys
// while both spellings keep working on disk:
//
//   - Datasets: a reconciler sweep migrates local legacy stamps to the
//     canonical namespace (write canonical, then remove legacy), so dataset
//     stamps converge on scale-csi:*.
//   - Snapshots: TrueNAS 26.0 has NO working property-mutation path for
//     existing snapshots (zfs.resource.snapshot.update does not exist and
//     pool.snapshot.update silently drops the request — see
//     SnapshotSetUserProperty), so legacy snapshot stamps can never be
//     rewritten there. The decode-time fold is therefore PERMANENT for
//     snapshots, not a transition aid.
//
// Only the namespace prefix differs between the two spellings; the key suffix
// (including hashed ledger/marker suffixes, which hash the identity and not
// the prefix) is identical, so a prefix swap is a complete translation.
const (
	CSIPropertyNamespace       = "scale-csi:"
	LegacyCSIPropertyNamespace = "truenas-csi:"
)

// LegacyCSIPropertyKey returns the legacy-namespace twin of a canonical
// scale-csi:* key, and whether the key was canonical at all.
func LegacyCSIPropertyKey(key string) (string, bool) {
	suffix, ok := strings.CutPrefix(key, CSIPropertyNamespace)
	if !ok {
		return "", false
	}
	return LegacyCSIPropertyNamespace + suffix, true
}

// CanonicalCSIPropertyKey returns the canonical scale-csi:* twin of a legacy
// truenas-csi:* key, and whether the key was legacy-namespaced at all.
func CanonicalCSIPropertyKey(key string) (string, bool) {
	suffix, ok := strings.CutPrefix(key, LegacyCSIPropertyNamespace)
	if !ok {
		return "", false
	}
	return CSIPropertyNamespace + suffix, true
}

// isLocalCSIPropertySource mirrors the driver's source discipline: only an
// exact "local" marks a value as set directly on the dataset. Clone-inherited
// user properties report the ORIGIN SNAPSHOT NAME as their source (never the
// string "inherited"), and the TrueNAS 26.0 resource APIs report no source at
// all (empty string) — neither may count as local.
func isLocalCSIPropertySource(source string) bool {
	return strings.EqualFold(strings.TrimSpace(source), "local")
}

// normalizeCSIUserProperties folds legacy-namespaced CSI keys onto their
// canonical scale-csi:* twins, in place. It returns the RAW legacy entries
// that were present (nil when none), so callers that need the on-disk truth —
// the dataset namespace-migration sweep — still see exactly which legacy keys
// exist and with which value/source.
//
// Collision rule when BOTH spellings carry the same suffix: a LOCAL value
// beats a non-local one (an inherited canonical key must not shadow a local
// legacy stamp, and vice versa); on a tie the canonical key wins, because
// every write since the rename uses the canonical namespace, so the canonical
// value is the newer one.
func normalizeCSIUserProperties(props map[string]UserProperty) map[string]UserProperty {
	if len(props) == 0 {
		return nil
	}
	var legacy map[string]UserProperty
	for key, prop := range props {
		suffix, ok := strings.CutPrefix(key, LegacyCSIPropertyNamespace)
		if !ok {
			continue
		}
		if legacy == nil {
			legacy = make(map[string]UserProperty)
		}
		legacy[key] = prop
		delete(props, key)
		canonical := CSIPropertyNamespace + suffix
		if existing, exists := props[canonical]; exists {
			if isLocalCSIPropertySource(existing.Source) || !isLocalCSIPropertySource(prop.Source) {
				continue
			}
		}
		props[canonical] = prop
	}
	return legacy
}

// expandCSIPropertyRemovalKeys widens a removal key set so every canonical
// scale-csi:* key also removes its legacy truenas-csi:* spelling. Callers pass
// canonical keys and stale legacy stamps disappear with them; removing a key
// that does not exist is a no-op on the appliance (live-verified on 26.0:
// user_properties_update {remove:true} for an absent key succeeds silently).
// Keys already in the legacy namespace pass through UNCHANGED so the
// migration sweep can remove exactly the legacy spelling it just replaced.
func expandCSIPropertyRemovalKeys(keys []string) []string {
	expanded := make([]string, 0, len(keys)*2)
	seen := make(map[string]struct{}, len(keys)*2)
	add := func(key string) {
		if _, dup := seen[key]; !dup {
			seen[key] = struct{}{}
			expanded = append(expanded, key)
		}
	}
	for _, key := range keys {
		add(key)
		if legacyTwin, ok := LegacyCSIPropertyKey(key); ok {
			add(legacyTwin)
		}
	}
	return expanded
}
