// Package gorules holds go-critic "ruleguard" rules that encode scale-csi's
// own real bug classes, not generic style advice. This file is loaded by
// gocritic's ruleguard checker (see .golangci.yml, settings.gocritic.settings.
// ruleguard.rules) — it is never imported by application code and is excluded
// from normal lint reporting via linters.exclusions.paths, since a ruleguard
// rules file's "checker" functions are, by design, never called directly.
package gorules

import "github.com/quasilyte/go-ruleguard/dsl"

// rawUserPropertiesIndex bans direct indexing into a ZFS dataset/snapshot's
// UserProperties map anywhere outside pkg/driver/share.go's accessor helpers
// (datasetUserProperty, datasetUserPropertyHasValue, datasetUserPropertyProjection,
// datasetLocalUserProperty, datasetHasLocalUserProperty) and test files.
//
// Why this matters: ZFS user properties are INHERITED by clones and snapshots
// unless a value was set locally on that exact dataset. DeleteVolume once read
// ds.UserProperties[PropVolumeOriginSnapshot] directly instead of going
// through the source-checked datasetLocalUserProperty helper — so a
// clone-of-a-snapshot-of-a-clone could hand DeleteVolume the INHERITED
// identity of a different, still-live volume's origin snapshot, and the wrong
// snapshot could be torn down. A raw map index has no way to distinguish
// "set here" from "inherited from an ancestor"; only the share.go accessors
// (or an inline check of Property.Source, reviewed and nolint'ed) do.
//
// This is enforced in pkg/driver only: pkg/truenas owns the Dataset/Snapshot
// types and legitimately constructs/decodes the raw map (see .golangci.yml's
// pkg/truenas/ exclusion for this rule) — pkg/driver is where identity/trust
// decisions are made from already-decoded data.
//
// A genuinely reviewed exception (there are several: presence-only checks,
// snapshot-only properties that cannot be inherited from a dataset, or sites
// that already inline a Source=="local" check) should carry an explicit
// `//nolint:gocritic // reason` rather than a blanket suppression.
func rawUserPropertiesIndex(m dsl.Matcher) {
	m.Match(`$x.UserProperties[$_]`).
		Report(`RG-USERPROPS-RAW: raw ".UserProperties[key]" index bypasses the source-checked accessors in share.go (datasetUserProperty/datasetLocalUserProperty/datasetHasLocalUserProperty/datasetUserPropertyProjection). ZFS clones INHERIT user properties, so an unchecked read cannot tell a locally-set value from one inherited off a different volume's identity (the DeleteVolume clone-of-a-snapshot-of-a-clone bug). Use an accessor, or add //nolint:gocritic with a reason if this specific read is provably safe (e.g. a presence-only check, a snapshot-only property, or an inline Source=="local" check).`)
}
