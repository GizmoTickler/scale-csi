package driver

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
)

// TestGetAccessibleTopology_Disabled tests that topology returns nil when disabled.
func TestGetAccessibleTopology_Disabled(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled: false,
					Zone:    "zone-a",
					Region:  "us-west-1",
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.Nil(t, result, "getAccessibleTopology should return nil when topology is disabled")
}

// TestGetAccessibleTopology_EnabledNoSegments tests that topology returns nil
// when enabled but no segments are configured.
func TestGetAccessibleTopology_EnabledNoSegments(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled:      true,
					Zone:         "",
					Region:       "",
					CustomLabels: nil,
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.Nil(t, result, "getAccessibleTopology should return nil when no segments are configured")
}

// TestGetAccessibleTopology_EnabledEmptyCustomLabels tests that topology returns nil
// when enabled but only empty custom labels are configured.
func TestGetAccessibleTopology_EnabledEmptyCustomLabels(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled:      true,
					Zone:         "",
					Region:       "",
					CustomLabels: make(map[string]string), // Empty map
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.Nil(t, result, "getAccessibleTopology should return nil when custom labels map is empty")
}

// TestGetAccessibleTopology_ZoneOnly tests topology with only zone configured.
func TestGetAccessibleTopology_ZoneOnly(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled: true,
					Zone:    "zone-a",
					Region:  "",
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.NotNil(t, result, "getAccessibleTopology should return topology when zone is set")
	assert.Len(t, result, 1, "Should have exactly one topology entry")
	assert.Equal(t, "zone-a", result[0].Segments["topology.kubernetes.io/zone"])
	_, hasRegion := result[0].Segments["topology.kubernetes.io/region"]
	assert.False(t, hasRegion, "Region should not be present when empty")
}

// TestGetAccessibleTopology_RegionOnly tests topology with only region configured.
func TestGetAccessibleTopology_RegionOnly(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled: true,
					Zone:    "",
					Region:  "us-west-1",
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.NotNil(t, result, "getAccessibleTopology should return topology when region is set")
	assert.Len(t, result, 1, "Should have exactly one topology entry")
	assert.Equal(t, "us-west-1", result[0].Segments["topology.kubernetes.io/region"])
	_, hasZone := result[0].Segments["topology.kubernetes.io/zone"]
	assert.False(t, hasZone, "Zone should not be present when empty")
}

// TestGetAccessibleTopology_ZoneAndRegion tests topology with both zone and region.
func TestGetAccessibleTopology_ZoneAndRegion(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled: true,
					Zone:    "zone-a",
					Region:  "us-west-1",
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.NotNil(t, result, "getAccessibleTopology should return topology")
	assert.Len(t, result, 1, "Should have exactly one topology entry")
	assert.Equal(t, "zone-a", result[0].Segments["topology.kubernetes.io/zone"])
	assert.Equal(t, "us-west-1", result[0].Segments["topology.kubernetes.io/region"])
}

// TestGetAccessibleTopology_CustomLabelsOnly tests topology with only custom labels.
func TestGetAccessibleTopology_CustomLabelsOnly(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled: true,
					Zone:    "",
					Region:  "",
					CustomLabels: map[string]string{
						"topology.kubernetes.io/rack": "rack-1",
						"custom.example.com/storage":  "fast",
					},
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.NotNil(t, result, "getAccessibleTopology should return topology with custom labels")
	assert.Len(t, result, 1, "Should have exactly one topology entry")
	assert.Equal(t, "rack-1", result[0].Segments["topology.kubernetes.io/rack"])
	assert.Equal(t, "fast", result[0].Segments["custom.example.com/storage"])
	_, hasZone := result[0].Segments["topology.kubernetes.io/zone"]
	assert.False(t, hasZone, "Zone should not be present when empty")
}

// TestGetAccessibleTopology_AllCombined tests topology with zone, region, and custom labels.
func TestGetAccessibleTopology_AllCombined(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled: true,
					Zone:    "zone-b",
					Region:  "eu-central-1",
					CustomLabels: map[string]string{
						"topology.kubernetes.io/rack":        "rack-5",
						"topology.kubernetes.io/datacenter":  "dc-1",
						"custom.mycompany.io/storage-tier":   "premium",
					},
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.NotNil(t, result, "getAccessibleTopology should return topology")
	assert.Len(t, result, 1, "Should have exactly one topology entry")

	segments := result[0].Segments
	assert.Equal(t, "zone-b", segments["topology.kubernetes.io/zone"])
	assert.Equal(t, "eu-central-1", segments["topology.kubernetes.io/region"])
	assert.Equal(t, "rack-5", segments["topology.kubernetes.io/rack"])
	assert.Equal(t, "dc-1", segments["topology.kubernetes.io/datacenter"])
	assert.Equal(t, "premium", segments["custom.mycompany.io/storage-tier"])

	// Total of 5 segments
	assert.Len(t, segments, 5, "Should have all 5 segments")
}

// TestGetAccessibleTopology_CustomLabelOverride tests that custom labels can override
// standard zone/region keys (though this is not recommended).
func TestGetAccessibleTopology_CustomLabelOverride(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled: true,
					Zone:    "zone-a",
					Region:  "us-west-1",
					CustomLabels: map[string]string{
						// This would override the zone - custom labels are applied after
						"topology.kubernetes.io/zone": "custom-zone",
					},
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.NotNil(t, result, "getAccessibleTopology should return topology")
	// Custom labels are added after zone/region, so they override
	assert.Equal(t, "custom-zone", result[0].Segments["topology.kubernetes.io/zone"])
	assert.Equal(t, "us-west-1", result[0].Segments["topology.kubernetes.io/region"])
}

// TestGetAccessibleTopology_TypeAssertion verifies the return type is correct.
func TestGetAccessibleTopology_TypeAssertion(t *testing.T) {
	d := &Driver{
		config: &Config{
			Node: NodeConfig{
				Topology: TopologyConfig{
					Enabled: true,
					Zone:    "zone-a",
				},
			},
		},
	}

	result := d.getAccessibleTopology()

	assert.NotNil(t, result)
	// Verify we get proper CSI Topology type
	_ = result // Use result to avoid unused variable warning
	assert.IsType(t, []*csi.Topology{}, result)
}
