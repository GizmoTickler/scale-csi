package truenas

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestISCSIExtentCreateParamsNilOmit proves the byte-identical contract: with no
// options the outgoing param map carries the historical hardcoded values and
// OMITS every GF-Sprint 4 field, so a default create is unchanged.
func TestISCSIExtentCreateParamsNilOmit(t *testing.T) {
	params := iscsiExtentCreateParams("name", "zvol/pool/vol", "comment", 512, true, "SSD")

	assert.Equal(t, 512, params["blocksize"])
	assert.Equal(t, true, params["pblocksize"])
	assert.Equal(t, true, params["insecure_tpc"], "historical hardcoded default must be preserved")
	assert.Equal(t, false, params["ro"])
	assert.Equal(t, "SSD", params["rpm"])

	_, hasThreshold := params["avail_threshold"]
	assert.False(t, hasThreshold, "avail_threshold must be omitted when unset")
	_, hasSerial := params["serial"]
	assert.False(t, hasSerial, "serial must be omitted when unset")
}

func TestISCSIExtentCreateParamsOverrides(t *testing.T) {
	insecure := false
	ro := true
	threshold := 80
	params := iscsiExtentCreateParams("name", "zvol/pool/vol", "comment", 4096, false, "SSD", ISCSIExtentCreateOptions{
		InsecureTpc:    &insecure,
		ReadOnly:       &ro,
		AvailThreshold: &threshold,
		Serial:         "abcdef0123456789",
	})

	assert.Equal(t, 4096, params["blocksize"])
	assert.Equal(t, false, params["pblocksize"])
	assert.Equal(t, false, params["insecure_tpc"])
	assert.Equal(t, true, params["ro"])
	assert.Equal(t, 80, params["avail_threshold"])
	assert.Equal(t, "abcdef0123456789", params["serial"])
}
