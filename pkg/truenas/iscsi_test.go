package truenas

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Parser Tests - These test the internal parsing logic
// =============================================================================

func TestParseISCSITarget_AllFields(t *testing.T) {
	tests := []struct {
		name      string
		data      interface{}
		wantID    int
		wantName  string
		wantAlias string
		wantMode  string
		wantErr   bool
	}{
		{
			name: "full target with groups and auth",
			data: map[string]interface{}{
				"id":    float64(42),
				"name":  "iqn.2005-10.org.freenas.ctl:vol-001",
				"alias": "pvc-abc123",
				"mode":  "ISCSI",
				"groups": []interface{}{
					map[string]interface{}{
						"portal":     float64(1),
						"initiator":  float64(2),
						"authmethod": "CHAP",
						"auth":       float64(5),
					},
				},
			},
			wantID:    42,
			wantName:  "iqn.2005-10.org.freenas.ctl:vol-001",
			wantAlias: "pvc-abc123",
			wantMode:  "ISCSI",
		},
		{
			name: "target without alias",
			data: map[string]interface{}{
				"id":     float64(1),
				"name":   "iqn.2005-10.org.freenas.ctl:vol-002",
				"alias":  "",
				"mode":   "ISCSI",
				"groups": []interface{}{},
			},
			wantID:    1,
			wantName:  "iqn.2005-10.org.freenas.ctl:vol-002",
			wantAlias: "",
			wantMode:  "ISCSI",
		},
		{
			name: "target with multiple groups",
			data: map[string]interface{}{
				"id":    float64(3),
				"name":  "multi-group-target",
				"alias": "multi",
				"mode":  "ISCSI",
				"groups": []interface{}{
					map[string]interface{}{"portal": float64(1), "initiator": float64(1), "authmethod": "NONE"},
					map[string]interface{}{"portal": float64(2), "initiator": float64(2), "authmethod": "NONE"},
				},
			},
			wantID:    3,
			wantName:  "multi-group-target",
			wantAlias: "multi",
			wantMode:  "ISCSI",
		},
		{
			name:    "invalid data type",
			data:    "not a map",
			wantErr: true,
		},
		{
			name:    "nil data",
			data:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := parseISCSITarget(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, target)
			assert.Equal(t, tt.wantID, target.ID)
			assert.Equal(t, tt.wantName, target.Name)
			assert.Equal(t, tt.wantAlias, target.Alias)
			assert.Equal(t, tt.wantMode, target.Mode)
		})
	}
}

func TestParseISCSITarget_Groups(t *testing.T) {
	data := map[string]interface{}{
		"id":    float64(1),
		"name":  "test-target",
		"alias": "",
		"mode":  "ISCSI",
		"groups": []interface{}{
			map[string]interface{}{
				"portal":     float64(1),
				"initiator":  float64(2),
				"authmethod": "CHAP",
				"auth":       float64(10),
			},
			map[string]interface{}{
				"portal":     float64(3),
				"initiator":  float64(4),
				"authmethod": "NONE",
				// no auth field - should result in nil
			},
		},
	}

	target, err := parseISCSITarget(data)
	require.NoError(t, err)
	require.Len(t, target.Groups, 2)

	// First group with auth
	assert.Equal(t, 1, target.Groups[0].Portal)
	assert.Equal(t, 2, target.Groups[0].Initiator)
	assert.Equal(t, "CHAP", target.Groups[0].AuthMethod)
	require.NotNil(t, target.Groups[0].Auth)
	assert.Equal(t, 10, *target.Groups[0].Auth)

	// Second group without auth
	assert.Equal(t, 3, target.Groups[1].Portal)
	assert.Equal(t, 4, target.Groups[1].Initiator)
	assert.Equal(t, "NONE", target.Groups[1].AuthMethod)
	assert.Nil(t, target.Groups[1].Auth)
}

func TestParseISCSIExtent_AllFields(t *testing.T) {
	tests := []struct {
		name        string
		data        interface{}
		wantID      int
		wantName    string
		wantType    string
		wantDisk    string
		wantEnabled bool
		wantErr     bool
	}{
		{
			name: "full extent",
			data: map[string]interface{}{
				"id":           float64(10),
				"name":         "test-extent",
				"type":         "DISK",
				"disk":         "zvol/tank/vol1",
				"serial":       "abc123",
				"path":         "/dev/zvol/tank/vol1",
				"comment":      "test comment",
				"naa":          "0x60012340",
				"blocksize":    float64(512),
				"pblocksize":   true,
				"insecure_tpc": true,
				"xen":          false,
				"rpm":          "SSD",
				"ro":           false,
				"enabled":      true,
			},
			wantID:      10,
			wantName:    "test-extent",
			wantType:    "DISK",
			wantDisk:    "zvol/tank/vol1",
			wantEnabled: true,
		},
		{
			name: "minimal extent",
			data: map[string]interface{}{
				"id":      float64(5),
				"name":    "min-extent",
				"type":    "DISK",
				"disk":    "zvol/tank/min",
				"enabled": false,
			},
			wantID:      5,
			wantName:    "min-extent",
			wantType:    "DISK",
			wantDisk:    "zvol/tank/min",
			wantEnabled: false,
		},
		{
			name:    "invalid data type",
			data:    []int{1, 2, 3},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			extent, err := parseISCSIExtent(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, extent)
			assert.Equal(t, tt.wantID, extent.ID)
			assert.Equal(t, tt.wantName, extent.Name)
			assert.Equal(t, tt.wantType, extent.Type)
			assert.Equal(t, tt.wantDisk, extent.Disk)
			assert.Equal(t, tt.wantEnabled, extent.Enabled)
		})
	}
}

func TestParseISCSIExtent_AllProperties(t *testing.T) {
	data := map[string]interface{}{
		"id":           float64(10),
		"name":         "detailed-extent",
		"type":         "DISK",
		"disk":         "zvol/tank/vol1",
		"serial":       "SER123ABC",
		"path":         "/dev/zvol/tank/vol1",
		"comment":      "CSI managed volume",
		"naa":          "0x600120001234567890ABCDEF",
		"blocksize":    float64(4096),
		"pblocksize":   true,
		"insecure_tpc": false,
		"xen":          true,
		"rpm":          "7200",
		"ro":           true,
		"enabled":      true,
	}

	extent, err := parseISCSIExtent(data)
	require.NoError(t, err)

	assert.Equal(t, 10, extent.ID)
	assert.Equal(t, "detailed-extent", extent.Name)
	assert.Equal(t, "DISK", extent.Type)
	assert.Equal(t, "zvol/tank/vol1", extent.Disk)
	assert.Equal(t, "SER123ABC", extent.Serial)
	assert.Equal(t, "/dev/zvol/tank/vol1", extent.Path)
	assert.Equal(t, "CSI managed volume", extent.Comment)
	assert.Equal(t, "0x600120001234567890ABCDEF", extent.Naa)
	assert.Equal(t, 4096, extent.Blocksize)
	assert.True(t, extent.Pblocksize)
	assert.False(t, extent.InsecureTpc)
	assert.True(t, extent.Xen)
	assert.Equal(t, "7200", extent.Rpm)
	assert.True(t, extent.Ro)
	assert.True(t, extent.Enabled)
}

func TestParseISCSITargetExtent_AllFields(t *testing.T) {
	tests := []struct {
		name       string
		data       interface{}
		wantID     int
		wantTarget int
		wantExtent int
		wantLunID  int
		wantErr    bool
	}{
		{
			name: "full target-extent association",
			data: map[string]interface{}{
				"id":     float64(50),
				"target": float64(1),
				"extent": float64(2),
				"lunid":  float64(0),
			},
			wantID:     50,
			wantTarget: 1,
			wantExtent: 2,
			wantLunID:  0,
		},
		{
			name: "association with higher LUN",
			data: map[string]interface{}{
				"id":     float64(100),
				"target": float64(5),
				"extent": float64(10),
				"lunid":  float64(5),
			},
			wantID:     100,
			wantTarget: 5,
			wantExtent: 10,
			wantLunID:  5,
		},
		{
			name:    "invalid data",
			data:    "not a map",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			te, err := parseISCSITargetExtent(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, te)
			assert.Equal(t, tt.wantID, te.ID)
			assert.Equal(t, tt.wantTarget, te.Target)
			assert.Equal(t, tt.wantExtent, te.Extent)
			assert.Equal(t, tt.wantLunID, te.LunID)
		})
	}
}

func TestParseISCSIGlobalConfig(t *testing.T) {
	tests := []struct {
		name         string
		data         interface{}
		wantID       int
		wantBasename string
		wantErr      bool
	}{
		{
			name: "standard config",
			data: map[string]interface{}{
				"id":       float64(1),
				"basename": "iqn.2005-10.org.freenas.ctl",
			},
			wantID:       1,
			wantBasename: "iqn.2005-10.org.freenas.ctl",
		},
		{
			name: "custom basename",
			data: map[string]interface{}{
				"id":       float64(1),
				"basename": "iqn.2024-01.com.example:storage",
			},
			wantID:       1,
			wantBasename: "iqn.2024-01.com.example:storage",
		},
		{
			name:    "invalid data",
			data:    []string{"not", "a", "map"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseISCSIGlobalConfig(tt.data)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cfg)
			assert.Equal(t, tt.wantID, cfg.ID)
			assert.Equal(t, tt.wantBasename, cfg.Basename)
		})
	}
}

// =============================================================================
// MockClient iSCSI Tests - Integration tests using the mock client
// =============================================================================

func TestMockClient_ISCSITargetCreate_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	target, err := client.ISCSITargetCreate(ctx, "test-target", "alias", "ISCSI", []ISCSITargetGroup{
		{Portal: 1, Initiator: 1, AuthMethod: "NONE"},
	})

	require.NoError(t, err)
	require.NotNil(t, target)
	assert.Equal(t, 1, target.ID)
	assert.Equal(t, "test-target", target.Name)
	assert.Equal(t, "alias", target.Alias)
	assert.Equal(t, "ISCSI", target.Mode)
}

func TestMockClient_ISCSITargetCreate_MultipleTargets(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Create multiple targets
	targets := []struct {
		name  string
		alias string
	}{
		{"target-1", "alias-1"},
		{"target-2", "alias-2"},
		{"target-3", "alias-3"},
	}

	for i, tt := range targets {
		target, err := client.ISCSITargetCreate(ctx, tt.name, tt.alias, "ISCSI", nil)
		require.NoError(t, err)
		assert.Equal(t, i+1, target.ID)
		assert.Equal(t, tt.name, target.Name)
	}

	// Verify all were created
	for _, tt := range targets {
		found, err := client.ISCSITargetFindByName(ctx, tt.name)
		require.NoError(t, err)
		require.NotNil(t, found)
		assert.Equal(t, tt.name, found.Name)
	}
}

func TestMockClient_ISCSITargetDelete_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Create a target
	target, err := client.ISCSITargetCreate(ctx, "to-delete", "", "ISCSI", nil)
	require.NoError(t, err)

	// Verify it exists
	found, err := client.ISCSITargetFindByName(ctx, "to-delete")
	require.NoError(t, err)
	require.NotNil(t, found)

	// Delete it
	err = client.ISCSITargetDelete(ctx, target.ID, false)
	require.NoError(t, err)

	// Verify it's gone
	found, err = client.ISCSITargetFindByName(ctx, "to-delete")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_ISCSITargetDelete_Idempotent(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Delete non-existent target should not error
	err := client.ISCSITargetDelete(ctx, 999, false)
	require.NoError(t, err)

	// Delete same target twice
	target, _ := client.ISCSITargetCreate(ctx, "target", "", "ISCSI", nil)
	err = client.ISCSITargetDelete(ctx, target.ID, false)
	require.NoError(t, err)
	err = client.ISCSITargetDelete(ctx, target.ID, false)
	require.NoError(t, err)
}

func TestMockClient_ISCSITargetFindByName_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	found, err := client.ISCSITargetFindByName(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_ISCSITargetGet_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	target, _ := client.ISCSITargetCreate(ctx, "get-test", "alias", "ISCSI", nil)

	got, err := client.ISCSITargetGet(ctx, target.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, target.ID, got.ID)
	assert.Equal(t, "get-test", got.Name)
}

func TestMockClient_ISCSITargetGet_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	_, err := client.ISCSITargetGet(ctx, 999)
	require.Error(t, err)
}

func TestMockClient_ISCSIExtentCreate_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	extent, err := client.ISCSIExtentCreate(ctx, "test-extent", "zvol/tank/vol1", "comment", 512, "SSD")
	require.NoError(t, err)
	require.NotNil(t, extent)
	assert.Equal(t, 1, extent.ID)
	assert.Equal(t, "test-extent", extent.Name)
	assert.Equal(t, "zvol/tank/vol1", extent.Disk)
}

func TestMockClient_ISCSIExtentFindByName_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	_, err := client.ISCSIExtentCreate(ctx, "findme", "zvol/tank/findme", "", 512, "SSD")
	require.NoError(t, err)

	found, err := client.ISCSIExtentFindByName(ctx, "findme")
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, "findme", found.Name)
}

func TestMockClient_ISCSIExtentFindByName_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	found, err := client.ISCSIExtentFindByName(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_ISCSIExtentFindByDisk_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	diskPath := "zvol/tank/disk-test"
	_, err := client.ISCSIExtentCreate(ctx, "disk-extent", diskPath, "", 512, "SSD")
	require.NoError(t, err)

	found, err := client.ISCSIExtentFindByDisk(ctx, diskPath)
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, diskPath, found.Disk)
}

func TestMockClient_ISCSIExtentFindByDisk_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	found, err := client.ISCSIExtentFindByDisk(ctx, "zvol/nonexistent")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_ISCSIExtentDelete_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	extent, _ := client.ISCSIExtentCreate(ctx, "delete-me", "zvol/tank/delete", "", 512, "SSD")

	err := client.ISCSIExtentDelete(ctx, extent.ID, false, false)
	require.NoError(t, err)

	// Verify deleted
	found, err := client.ISCSIExtentFindByName(ctx, "delete-me")
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_ISCSIExtentGet_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	extent, _ := client.ISCSIExtentCreate(ctx, "get-extent", "zvol/tank/get", "", 512, "SSD")

	got, err := client.ISCSIExtentGet(ctx, extent.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, extent.ID, got.ID)
}

func TestMockClient_ISCSITargetExtentCreate_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	target, _ := client.ISCSITargetCreate(ctx, "target", "", "ISCSI", nil)
	extent, _ := client.ISCSIExtentCreate(ctx, "extent", "zvol/tank/vol", "", 512, "SSD")

	te, err := client.ISCSITargetExtentCreate(ctx, target.ID, extent.ID, 0)
	require.NoError(t, err)
	require.NotNil(t, te)
	assert.Equal(t, target.ID, te.Target)
	assert.Equal(t, extent.ID, te.Extent)
	assert.Equal(t, 0, te.LunID)
}

func TestMockClient_ISCSITargetExtentFind_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	target, _ := client.ISCSITargetCreate(ctx, "target", "", "ISCSI", nil)
	extent, _ := client.ISCSIExtentCreate(ctx, "extent", "zvol/tank/vol", "", 512, "SSD")
	_, err := client.ISCSITargetExtentCreate(ctx, target.ID, extent.ID, 0)
	require.NoError(t, err)

	found, err := client.ISCSITargetExtentFind(ctx, target.ID, extent.ID)
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, target.ID, found.Target)
	assert.Equal(t, extent.ID, found.Extent)
}

func TestMockClient_ISCSITargetExtentFind_NotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	found, err := client.ISCSITargetExtentFind(ctx, 999, 888)
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_ISCSITargetExtentFindByTarget_Multiple(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	target, _ := client.ISCSITargetCreate(ctx, "target", "", "ISCSI", nil)
	extent1, _ := client.ISCSIExtentCreate(ctx, "extent1", "zvol/tank/vol1", "", 512, "SSD")
	extent2, _ := client.ISCSIExtentCreate(ctx, "extent2", "zvol/tank/vol2", "", 512, "SSD")
	extent3, _ := client.ISCSIExtentCreate(ctx, "extent3", "zvol/tank/vol3", "", 512, "SSD")

	_, _ = client.ISCSITargetExtentCreate(ctx, target.ID, extent1.ID, 0)
	_, _ = client.ISCSITargetExtentCreate(ctx, target.ID, extent2.ID, 1)
	_, _ = client.ISCSITargetExtentCreate(ctx, target.ID, extent3.ID, 2)

	results, err := client.ISCSITargetExtentFindByTarget(ctx, target.ID)
	require.NoError(t, err)
	assert.Len(t, results, 3)
}

func TestMockClient_ISCSITargetExtentFindByExtent_Multiple(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	target1, _ := client.ISCSITargetCreate(ctx, "target1", "", "ISCSI", nil)
	target2, _ := client.ISCSITargetCreate(ctx, "target2", "", "ISCSI", nil)
	extent, _ := client.ISCSIExtentCreate(ctx, "extent", "zvol/tank/vol", "", 512, "SSD")

	_, _ = client.ISCSITargetExtentCreate(ctx, target1.ID, extent.ID, 0)
	_, _ = client.ISCSITargetExtentCreate(ctx, target2.ID, extent.ID, 0)

	results, err := client.ISCSITargetExtentFindByExtent(ctx, extent.ID)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestMockClient_ISCSITargetExtentDelete_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	target, _ := client.ISCSITargetCreate(ctx, "target", "", "ISCSI", nil)
	extent, _ := client.ISCSIExtentCreate(ctx, "extent", "zvol/tank/vol", "", 512, "SSD")
	te, _ := client.ISCSITargetExtentCreate(ctx, target.ID, extent.ID, 0)

	err := client.ISCSITargetExtentDelete(ctx, te.ID, false)
	require.NoError(t, err)

	// Verify deleted
	found, err := client.ISCSITargetExtentFind(ctx, target.ID, extent.ID)
	require.NoError(t, err)
	assert.Nil(t, found)
}

func TestMockClient_ISCSITargetExtentGet_Success(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	target, _ := client.ISCSITargetCreate(ctx, "target", "", "ISCSI", nil)
	extent, _ := client.ISCSIExtentCreate(ctx, "extent", "zvol/tank/vol", "", 512, "SSD")
	te, _ := client.ISCSITargetExtentCreate(ctx, target.ID, extent.ID, 0)

	got, err := client.ISCSITargetExtentGet(ctx, te.ID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, te.ID, got.ID)
}

func TestMockClient_ISCSIGlobalConfigGet(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	config, err := client.ISCSIGlobalConfigGet(ctx)
	require.NoError(t, err)
	require.NotNil(t, config)
	assert.Equal(t, "iqn.2005-10.org.freenas.ctl", config.Basename)
}

// =============================================================================
// Full Workflow Tests
// =============================================================================

func TestMockClient_ISCSIFullWorkflow(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Step 1: Create iSCSI target
	target, err := client.ISCSITargetCreate(ctx, "pvc-workflow-target", "kubernetes-pvc", "ISCSI", []ISCSITargetGroup{
		{Portal: 1, Initiator: 1, AuthMethod: "NONE"},
	})
	require.NoError(t, err)
	require.NotNil(t, target)

	// Step 2: Create iSCSI extent
	extent, err := client.ISCSIExtentCreate(ctx, "pvc-workflow-extent", "zvol/tank/k8s/volumes/pvc-workflow", "CSI managed", 512, "SSD")
	require.NoError(t, err)
	require.NotNil(t, extent)

	// Step 3: Associate target with extent
	te, err := client.ISCSITargetExtentCreate(ctx, target.ID, extent.ID, 0)
	require.NoError(t, err)
	require.NotNil(t, te)

	// Verify the full chain exists
	foundTarget, err := client.ISCSITargetFindByName(ctx, "pvc-workflow-target")
	require.NoError(t, err)
	require.NotNil(t, foundTarget)

	foundExtent, err := client.ISCSIExtentFindByDisk(ctx, "zvol/tank/k8s/volumes/pvc-workflow")
	require.NoError(t, err)
	require.NotNil(t, foundExtent)

	foundTE, err := client.ISCSITargetExtentFind(ctx, foundTarget.ID, foundExtent.ID)
	require.NoError(t, err)
	require.NotNil(t, foundTE)

	// Cleanup in reverse order
	err = client.ISCSITargetExtentDelete(ctx, te.ID, false)
	require.NoError(t, err)

	err = client.ISCSIExtentDelete(ctx, extent.ID, false, false)
	require.NoError(t, err)

	err = client.ISCSITargetDelete(ctx, target.ID, false)
	require.NoError(t, err)

	// Verify everything is cleaned up
	foundTarget, _ = client.ISCSITargetFindByName(ctx, "pvc-workflow-target")
	assert.Nil(t, foundTarget)

	foundExtent, _ = client.ISCSIExtentFindByDisk(ctx, "zvol/tank/k8s/volumes/pvc-workflow")
	assert.Nil(t, foundExtent)
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestMockClient_ISCSIConcurrentTargetCreation(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 20

	// Concurrent target creation
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("concurrent-target-%d", idx)
			_, err := client.ISCSITargetCreate(ctx, name, "", "ISCSI", nil)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Verify all targets were created
	for i := 0; i < numGoroutines; i++ {
		name := fmt.Sprintf("concurrent-target-%d", i)
		found, err := client.ISCSITargetFindByName(ctx, name)
		assert.NoError(t, err)
		assert.NotNil(t, found, "target %s should exist", name)
	}
}

func TestMockClient_ISCSIConcurrentExtentCreation(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 20

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("concurrent-extent-%d", idx)
			disk := fmt.Sprintf("zvol/tank/concurrent-%d", idx)
			_, err := client.ISCSIExtentCreate(ctx, name, disk, "", 512, "SSD")
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	// Verify all extents were created
	for i := 0; i < numGoroutines; i++ {
		name := fmt.Sprintf("concurrent-extent-%d", i)
		found, err := client.ISCSIExtentFindByName(ctx, name)
		assert.NoError(t, err)
		assert.NotNil(t, found, "extent %s should exist", name)
	}
}

func TestMockClient_ISCSIConcurrentReadWrite(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	// Pre-populate some targets
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("rw-target-%d", i)
		_, _ = client.ISCSITargetCreate(ctx, name, "", "ISCSI", nil)
	}

	var wg sync.WaitGroup

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("rw-target-%d", idx%10)
			_, _ = client.ISCSITargetFindByName(ctx, name)
		}(i)
	}

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("new-rw-target-%d", idx)
			_, _ = client.ISCSITargetCreate(ctx, name, "", "ISCSI", nil)
		}(i)
	}

	wg.Wait()
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestISCSITargetGroup_AuthPointer(t *testing.T) {
	// Test that auth pointer is correctly handled
	authValue := 5
	group := ISCSITargetGroup{
		Portal:     1,
		Initiator:  1,
		AuthMethod: "CHAP",
		Auth:       &authValue,
	}

	assert.Equal(t, 1, group.Portal)
	assert.Equal(t, 1, group.Initiator)
	assert.Equal(t, "CHAP", group.AuthMethod)
	assert.NotNil(t, group.Auth)
	assert.Equal(t, 5, *group.Auth)

	// Test nil auth
	groupNoAuth := ISCSITargetGroup{
		Portal:     2,
		Initiator:  3,
		AuthMethod: "NONE",
		Auth:       nil,
	}
	assert.Equal(t, 2, groupNoAuth.Portal)
	assert.Equal(t, 3, groupNoAuth.Initiator)
	assert.Equal(t, "NONE", groupNoAuth.AuthMethod)
	assert.Nil(t, groupNoAuth.Auth)
}

func TestParseISCSITarget_EmptyGroups(t *testing.T) {
	data := map[string]interface{}{
		"id":     float64(1),
		"name":   "no-groups",
		"alias":  "",
		"mode":   "ISCSI",
		"groups": []interface{}{},
	}

	target, err := parseISCSITarget(data)
	require.NoError(t, err)
	assert.Len(t, target.Groups, 0)
}

func TestParseISCSITarget_MissingFields(t *testing.T) {
	// Test that missing fields don't cause panic
	data := map[string]interface{}{
		"id": float64(1),
		// missing name, alias, mode, groups
	}

	target, err := parseISCSITarget(data)
	require.NoError(t, err)
	assert.Equal(t, 1, target.ID)
	assert.Equal(t, "", target.Name)
	assert.Equal(t, "", target.Alias)
	assert.Equal(t, "", target.Mode)
	assert.Nil(t, target.Groups)
}

func TestParseISCSIExtent_MissingFields(t *testing.T) {
	data := map[string]interface{}{
		"id": float64(1),
		// missing all other fields
	}

	extent, err := parseISCSIExtent(data)
	require.NoError(t, err)
	assert.Equal(t, 1, extent.ID)
	assert.Equal(t, "", extent.Name)
	assert.Equal(t, "", extent.Type)
	assert.False(t, extent.Enabled)
}

func TestParseISCSITargetExtent_MissingFields(t *testing.T) {
	data := map[string]interface{}{
		"id": float64(1),
		// missing target, extent, lunid
	}

	te, err := parseISCSITargetExtent(data)
	require.NoError(t, err)
	assert.Equal(t, 1, te.ID)
	assert.Equal(t, 0, te.Target)
	assert.Equal(t, 0, te.Extent)
	assert.Equal(t, 0, te.LunID)
}
