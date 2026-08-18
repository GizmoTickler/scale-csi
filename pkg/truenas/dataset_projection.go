package truenas

import "slices"

// PROJECTION FIDELITY.
//
// Every pool.dataset.query the driver issues carries an `extra.properties`
// PROJECTION (datasetQueryProperties). TrueNAS 26.0 then returns the projected
// properties plus a small always-present core, and OMITS everything else — not
// as a null field the decoder could notice, but as an absent key that parses to
// the Go ZERO VALUE.
//
// That is a silent, type-safe way to make a predicate read a field the wire
// never sent. It has now happened twice:
//
//   - GF1 re-drill D-3 (2026-08-03, nas01 26.0.0-BETA.1): the encryption block
//     (`encrypted`, `locked`, `key_format`, `encryption_root`) was never
//     projected, so every "wire truth" encryption predicate evaluated on zeros
//     and failed OPEN on hardware — while every unit test passed, because
//     MockClient returned fully-populated structs.
//   - the same audit found `origin` unprojected, with real readers.
//
// projectDatasetLikePoolQuery is the structural fix for the TEST side: it takes
// a fully-populated dataset and strips it down to what the CURRENT projection
// would actually deliver, deriving that from datasetQueryProperties itself. A
// mock running in this mode therefore makes a predicate that depends on an
// unprojected field FAIL A UNIT TEST instead of shipping — and a micro-revert of
// the projection reproduces the hardware symptom exactly.
//
// The mapping below is grounded in the re-drill's measured shapes (report
// §"Root cause (measured)"), not in a reading of the middleware source:
//
//	always present, whatever the projection:
//	  id, name, pool, type, mountpoint, user_properties (and children, unmodeled)
//	present only when projected:
//	  used, available, quota, refquota, referenced, usedbysnapshots, reservation,
//	  refreservation, volsize, volblocksize, creation, origin
//	present only when the whole encryption property SET is projected (shape B vs C):
//	  encrypted, locked, key_loaded, key_format, encryption_root
//
// Dataset.EncryptionAlgorithm is a special case: NO wire decoder sets it
// (parseDataset, parseDatasetResource and rawDataset.toDataset all leave it
// alone), so on any real read it is always "". Only MockClient populates it.
// This mode zeroes it, which is the honest model and keeps any future reader of
// it from being validated by mock-only data.

// datasetProjectedProperty maps a Dataset field to the ZFS property name that
// must appear in the projection for the wire to carry it. Fields not listed here
// are either always present or handled explicitly below.
type datasetProjectedProperty struct {
	property string
	clear    func(*Dataset)
}

var datasetProjectedProperties = []datasetProjectedProperty{
	{"used", func(ds *Dataset) { ds.Used = DatasetProperty{} }},
	{"available", func(ds *Dataset) { ds.Available = DatasetProperty{} }},
	{"quota", func(ds *Dataset) { ds.Quota = DatasetProperty{} }},
	{"refquota", func(ds *Dataset) { ds.Refquota = DatasetProperty{} }},
	{"referenced", func(ds *Dataset) { ds.Referenced = DatasetProperty{} }},
	{"usedbysnapshots", func(ds *Dataset) { ds.Usedbysnapshots = DatasetProperty{} }},
	{"reservation", func(ds *Dataset) { ds.Reservation = DatasetProperty{} }},
	{"refreservation", func(ds *Dataset) { ds.Refreservation = DatasetProperty{} }},
	{"volsize", func(ds *Dataset) { ds.Volsize = DatasetProperty{} }},
	{"volblocksize", func(ds *Dataset) { ds.Volblocksize = DatasetProperty{} }},
	{"creation", func(ds *Dataset) { ds.Creation = DatasetProperty{} }},
	{"origin", func(ds *Dataset) { ds.Origin = DatasetProperty{} }},
	// Live-tunable quartet (MODIFY_VOLUME): projected on the pool.dataset.query
	// path only, and modeled here for the same reason as origin — a reader that
	// depends on them off a projection that does not carry them must fail a
	// unit test, not evaluate zero values on hardware.
	{"compression", func(ds *Dataset) { ds.Compression = DatasetProperty{} }},
	{"sync", func(ds *Dataset) { ds.Sync = DatasetProperty{} }},
	{"atime", func(ds *Dataset) { ds.Atime = DatasetProperty{} }},
	{"recordsize", func(ds *Dataset) { ds.Recordsize = DatasetProperty{} }},
}

// datasetEncryptionBlockProjected reports whether the projection asks for the
// WHOLE measured encryption property set. The re-drill measured the four names
// as a set and did not attribute individual response fields to individual
// property names, so anything less is modeled as "no encryption block" rather
// than guessing a partial shape.
func datasetEncryptionBlockProjected(projection []string) bool {
	for _, property := range datasetEncryptionQueryProperties {
		if !slices.Contains(projection, property) {
			return false
		}
	}
	return true
}

// projectDatasetLikePoolQuery returns a copy of ds carrying only the fields a
// real pool.dataset.query response would carry under the given projection. It
// never mutates its argument.
func projectDatasetLikePoolQuery(ds *Dataset, projection []string) *Dataset {
	if ds == nil {
		return nil
	}
	projected := *ds
	projected.UserProperties = make(map[string]UserProperty, len(ds.UserProperties))
	for key, property := range ds.UserProperties {
		projected.UserProperties[key] = property
	}
	for _, field := range datasetProjectedProperties {
		if !slices.Contains(projection, field.property) {
			field.clear(&projected)
		}
	}
	if !datasetEncryptionBlockProjected(projection) {
		projected.Encrypted = false
		projected.Locked = false
		projected.KeyLoaded = false
		projected.EncryptionRoot = ""
		projected.KeyFormat = ""
	}
	// Never delivered by any wire decoder — see the note above.
	projected.EncryptionAlgorithm = ""
	return &projected
}
