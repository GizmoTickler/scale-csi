package truenas

import (
	"bytes"
	"encoding/json"
	"strings"
)

type rawProperty struct {
	Value    interface{} `json:"value"`
	Parsed   interface{} `json:"parsed"`
	Rawvalue string      `json:"rawvalue"`
	Raw      string      `json:"raw"`
	// Source is a string on pool.dataset.query but an OBJECT {type,value} on
	// zfs.resource.query native properties (TrueNAS 26.0). The legacy
	// parseProperty dropped non-string sources; a plain string field instead
	// hard-failed the whole decode, silently degrading every managed-dataset
	// listing to the pool.dataset.query fallback since v1.3.0 (found live in
	// the 2026-07-31 drill; prod logged the fallback 125x/24h). tolerantString
	// restores the legacy drop-if-not-string semantics.
	Source tolerantString `json:"source"`
}

func (p rawProperty) toDatasetProperty() DatasetProperty {
	return DatasetProperty{
		Value:    p.Value,
		Parsed:   p.Parsed,
		Rawvalue: p.Rawvalue,
		Source:   string(p.Source),
	}
}

type rawUserProperty struct {
	Value  string
	Source string
	flat   bool
	object bool
}

type tolerantString string

func (value *tolerantString) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || data[0] != '"' {
		return nil
	}
	var decoded string
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*value = tolerantString(decoded)
	return nil
}

func (p *rawUserProperty) UnmarshalJSON(data []byte) error {
	*p = rawUserProperty{}
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}
	if data[0] == '"' {
		if err := json.Unmarshal(data, &p.Value); err != nil {
			return err
		}
		p.flat = true
		return nil
	}
	if data[0] != '{' {
		// The interface{} parsers ignore unsupported user-property values.
		return nil
	}
	var object struct {
		Value  tolerantString `json:"value"`
		Source tolerantString `json:"source"`
	}
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	p.Value = string(object.Value)
	p.Source = string(object.Source)
	p.object = true
	return nil
}

func (p rawUserProperty) toUserProperty() UserProperty {
	return UserProperty{Value: p.Value, Source: p.Source}
}

type rawSnapshot struct {
	Snapshot
	// SnapshotName is a pointer so present-but-empty is distinguishable from
	// absent: the legacy parseSnapshot overwrites Name whenever the key is
	// PRESENT (even with ""), and the typed decoder must match it exactly
	// (2026-07-31 differential-fuzz find).
	SnapshotName      *string                `json:"snapshot_name"`
	CreateTXGRaw      rawUnsigned            `json:"createtxg"`
	RawUserProperties map[string]interface{} `json:"user_properties"`
}

type rawUnsigned struct {
	value uint64
	valid bool
}

func (value *rawUnsigned) UnmarshalJSON(data []byte) error {
	var decoded interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	value.value, value.valid = unsignedInteger(decoded)
	return nil
}

func (snapshot *rawSnapshot) toSnapshot() *Snapshot {
	result := &snapshot.Snapshot
	if result.Properties == nil {
		result.Properties = make(map[string]interface{})
	}
	result.UserProperties = make(map[string]UserProperty)
	if result.ID == "" && strings.Contains(snapshot.Name, "@") {
		result.ID = snapshot.Name
	}
	if snapshot.SnapshotName != nil {
		result.Name = *snapshot.SnapshotName
	}
	if snapshot.CreateTXGRaw.valid {
		result.CreateTXG = snapshot.CreateTXGRaw.value
	}
	if result.ID == "" && result.Dataset != "" && result.Name != "" {
		result.ID = result.Dataset + "@" + result.Name
	}

	for key, property := range result.Properties {
		if !strings.Contains(key, ":") {
			continue
		}
		propertyMap, ok := property.(map[string]interface{})
		if !ok {
			continue
		}
		userProperty := UserProperty{}
		if value, ok := propertyMap["value"].(string); ok {
			userProperty.Value = value
		}
		if source, ok := propertyMap["source"].(string); ok {
			userProperty.Source = source
		}
		result.UserProperties[key] = userProperty
	}
	for key, property := range snapshot.RawUserProperties {
		switch value := property.(type) {
		case string:
			result.UserProperties[key] = UserProperty{Value: value}
		case map[string]interface{}:
			userProperty := UserProperty{}
			if propertyValue, ok := value["value"].(string); ok {
				userProperty.Value = propertyValue
			}
			if source, ok := value["source"].(string); ok {
				userProperty.Source = source
			}
			result.UserProperties[key] = userProperty
		}
	}
	return result
}

type rawDataset struct {
	Dataset
	Path       string                `json:"path"`
	Properties *rawDatasetProperties `json:"properties"`
	// Encryption identity (P-10, pool.dataset.query only): encryption_root is a
	// plain string, key_format a property dict. Both are decoded TOLERANTLY —
	// an unexpected shape yields "" instead of failing the whole response —
	// because these fields sit on the same decode as every other dataset in the
	// listing. They are assigned only on the pool.dataset.query path, keeping this
	// decoder exactly equivalent to parseDataset/parseDatasetResource (P-11:
	// zfs.resource.query carries no encryption fields at all).
	RawEncryptionRoot tolerantString             `json:"encryption_root"`
	RawKeyFormat      tolerantPropertyString     `json:"key_format"`
	RawUserProperties map[string]rawUserProperty `json:"user_properties"`
}

// tolerantPropertyString decodes a TrueNAS value that may be a property dict
// ({"value": "..."} — the P-10 key_format shape), a plain string, or anything
// else. Anything it cannot read becomes "", never a decode error: the same
// discipline as tolerantString, which exists because a hard-typed field turned
// one shape surprise into a silent fallback for every managed-dataset listing
// (2026-07-31).
type tolerantPropertyString string

func (value *tolerantPropertyString) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}
	if data[0] == '"' {
		var decoded string
		if err := json.Unmarshal(data, &decoded); err != nil {
			return nil
		}
		*value = tolerantPropertyString(decoded)
		return nil
	}
	if data[0] != '{' {
		return nil
	}
	var object struct {
		Value tolerantString `json:"value"`
	}
	if err := json.Unmarshal(data, &object); err != nil {
		return nil
	}
	*value = tolerantPropertyString(object.Value)
	return nil
}

type rawDatasetProperties struct {
	Used           rawProperty `json:"used"`
	Available      rawProperty `json:"available"`
	Quota          rawProperty `json:"quota"`
	Refquota       rawProperty `json:"refquota"`
	Reservation    rawProperty `json:"reservation"`
	Refreservation rawProperty `json:"refreservation"`
	Volsize        rawProperty `json:"volsize"`
	Volblocksize   rawProperty `json:"volblocksize"`
	Origin         rawProperty `json:"origin"`
	Creation       rawProperty `json:"creation"`
}

func (dataset *rawDataset) toDataset(resourceQuery bool) *Dataset {
	result := &dataset.Dataset
	// The path→name fallback is a zfs.resource.query shape concern only:
	// parseDatasetResource accepts name-or-path, but the legacy parseDataset
	// (pool.dataset.query) reads only "name". Keep the typed decoder exactly
	// equivalent to the parser it replaced (2026-07-31 differential-fuzz find).
	if resourceQuery && result.Name == "" {
		result.Name = dataset.Path
	}
	if resourceQuery && result.ID == "" {
		result.ID = result.Name
	}
	if !resourceQuery {
		// P-10/P-11: these fields exist only on pool.dataset.query, and
		// parseDatasetResource does not read them, so assigning them only here keeps
		// the two decoders deep-equal on both paths.
		result.EncryptionRoot = string(dataset.RawEncryptionRoot)
		result.KeyFormat = strings.ToUpper(strings.TrimSpace(string(dataset.RawKeyFormat)))
	}
	if resourceQuery && dataset.Properties != nil {
		result.Used = dataset.Properties.Used.toDatasetProperty()
		result.Available = dataset.Properties.Available.toDatasetProperty()
		result.Quota = dataset.Properties.Quota.toDatasetProperty()
		result.Refquota = dataset.Properties.Refquota.toDatasetProperty()
		result.Reservation = dataset.Properties.Reservation.toDatasetProperty()
		result.Refreservation = dataset.Properties.Refreservation.toDatasetProperty()
		result.Volsize = dataset.Properties.Volsize.toDatasetProperty()
		result.Volblocksize = dataset.Properties.Volblocksize.toDatasetProperty()
		result.Origin = dataset.Properties.Origin.toDatasetProperty()
		result.Creation = dataset.Properties.Creation.toDatasetProperty()
	}
	result.UserProperties = make(map[string]UserProperty)
	result.ResourceQuery = resourceQuery
	for key, property := range dataset.RawUserProperties {
		if property.object || (resourceQuery && property.flat) {
			result.UserProperties[key] = property.toUserProperty()
		}
	}
	return result
}

func rawSnapshotsToSnapshots(raw []*rawSnapshot, resourceQuery bool) []*Snapshot {
	snapshots := make([]*Snapshot, 0, len(raw))
	for _, item := range raw {
		if item == nil {
			continue
		}
		snapshot := item.toSnapshot()
		snapshot.ResourceQuery = resourceQuery
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

func rawDatasetsToDatasets(raw []*rawDataset, resourceQuery bool) []*Dataset {
	datasets := make([]*Dataset, 0, len(raw))
	for _, item := range raw {
		if item == nil {
			continue
		}
		datasets = append(datasets, item.toDataset(resourceQuery))
	}
	return datasets
}
