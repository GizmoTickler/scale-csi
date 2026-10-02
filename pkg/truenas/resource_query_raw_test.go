package truenas

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// zfsResourceQueryZvolRow is a zvol row as zfs.resource.query returned it
// live (TrueNAS 26.0): every property {"raw", "source": {...}, "value"}.
const zfsResourceQueryZvolRow = `{"createtxg": 3808963, "guid": 3898709091749277693, "name": "flashstor/scale-csi/pvc-3d848f4a-24f7-4fd8-9af5-316e8ad1f78d", "pool": "flashstor", "properties": {"refreservation": {"raw": "0", "source": {"type": "LOCAL", "value": null}, "value": 0}, "available": {"raw": "15051461210048", "source": {"type": "NONE", "value": null}, "value": 15051461210048}, "origin": {"raw": "none", "source": {"type": "NONE", "value": null}, "value": null}, "referenced": {"raw": "479375072", "source": {"type": "NONE", "value": null}, "value": 479375072}, "reservation": {"raw": "0", "source": {"type": "DEFAULT", "value": null}, "value": 0}, "used": {"raw": "479375072", "source": {"type": "NONE", "value": null}, "value": 479375072}, "usedbysnapshots": {"raw": "0", "source": {"type": "NONE", "value": null}, "value": 0}, "volblocksize": {"raw": "16384", "source": {"type": "DEFAULT", "value": null}, "value": 16384}, "volsize": {"raw": "21474836480", "source": {"type": "LOCAL", "value": null}, "value": 21474836480}}, "type": "VOLUME", "user_properties": {"scale-csi:managed_resource": "true", "scale-csi:provision_success": "true", "comments": "INHERIT"}, "children": []}`

// Both decoders keep zfs.resource.query's raw string, and only in Raw: the
// origin's raw "none" must not become an origin name.
func TestResourceQueryRowKeepsRaw(t *testing.T) {
	var typed []*rawDataset
	require.NoError(t, json.Unmarshal([]byte("["+zfsResourceQueryZvolRow+"]"), &typed))
	var generic interface{}
	require.NoError(t, json.Unmarshal([]byte(zfsResourceQueryZvolRow), &generic))
	reference, err := parseDatasetResource(generic)
	require.NoError(t, err)
	for name, ds := range map[string]*Dataset{"typed": rawDatasetsToDatasets(typed, true)[0], "reference": reference} {
		assert.Equal(t, "21474836480", ds.Volsize.Raw, name)
		assert.Equal(t, "15051461210048", ds.Available.Raw, name)
		assert.Empty(t, ds.Volsize.Rawvalue, name)
		assert.Empty(t, datasetPropertyString(ds.Origin), name)
	}
	assert.Equal(t, "21474836480", parseProperty(generic.(map[string]interface{})["properties"].(map[string]interface{})["volsize"]).Raw)
}
