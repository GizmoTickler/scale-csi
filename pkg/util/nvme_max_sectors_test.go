package util

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type maxSectorsVectorController struct {
	Name      string         `json:"name"`
	State     string         `json:"state"`
	Transport string         `json:"transport"`
	Paths     map[string]int `json:"paths"`
}

type maxSectorsVectorSubsystem struct {
	Name        string                       `json:"name"`
	NQN         string                       `json:"nqn"`
	Heads       map[string]int               `json:"heads"`
	Controllers []maxSectorsVectorController `json:"controllers"`
}

type maxSectorsVector struct {
	Name       string                      `json:"name"`
	Policy     string                      `json:"policy"`
	Transport  string                      `json:"transport"`
	MDTS       *int                        `json:"mdts"`
	NQN        string                      `json:"nqn"`
	Subsystems []maxSectorsVectorSubsystem `json:"subsystems"`
	Unwritable []string                    `json:"unwritable"`
	Expect     struct {
		Error       bool           `json:"error"`
		MDTSQueried bool           `json:"mdtsQueried"`
		LimitKB     int            `json:"limitKB"`
		Skipped     bool           `json:"skipped"`
		Waiting     []string       `json:"waiting"`
		After       map[string]int `json:"after"`
	} `json:"expect"`
}

// fakeMaxSectorsSysfs builds a sysfs tree the way the kernel lays it out:
// controllers under class/nvme, linked by name from their subsystem; the
// multipath head directly under the subsystem; each path device (a hidden
// gendisk) under its controller. It returns the nvme-subsystem class root and
// a map from device name to its queue directory.
type fakeMaxSectorsSysfs struct {
	root          string
	subsystemRoot string
	queues        map[string]string
}

func newFakeMaxSectorsSysfs(t *testing.T) *fakeMaxSectorsSysfs {
	t.Helper()
	root := t.TempDir()
	f := &fakeMaxSectorsSysfs{root: root, subsystemRoot: filepath.Join(root, "class", "nvme-subsystem"), queues: map[string]string{}}
	require.NoError(t, os.MkdirAll(f.subsystemRoot, 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "class", "nvme"), 0o750))
	return f
}

func (f *fakeMaxSectorsSysfs) subsystem(t *testing.T, name, nqn string) string {
	t.Helper()
	dir := filepath.Join(f.subsystemRoot, name)
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "subsysnqn"), []byte(nqn+"\n"), 0o600))
	return dir
}

func (f *fakeMaxSectorsSysfs) disk(t *testing.T, parent, name string, kb int, writable bool) {
	t.Helper()
	queue := filepath.Join(parent, name, "queue")
	require.NoError(t, os.MkdirAll(queue, 0o750))
	attr := filepath.Join(queue, "max_sectors_kb")
	if writable {
		require.NoError(t, os.WriteFile(attr, []byte(strconv.Itoa(kb)+"\n"), 0o600))
	} else {
		// A directory can be neither read as a value nor opened for writing,
		// even by root.
		require.NoError(t, os.MkdirAll(attr, 0o750))
	}
	f.queues[name] = queue
}

func (f *fakeMaxSectorsSysfs) controller(t *testing.T, subsysDir, name, state, transport string) string {
	t.Helper()
	dir := filepath.Join(f.root, "class", "nvme", name)
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "state"), []byte(state+"\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "transport"), []byte(transport+"\n"), 0o600))
	require.NoError(t, os.Symlink(dir, filepath.Join(subsysDir, name)))
	return dir
}

func (f *fakeMaxSectorsSysfs) value(t *testing.T, device string) int {
	t.Helper()
	return readMaxSectorsKB(f.queues[device])
}

func loadMaxSectorsVectors(t *testing.T) []maxSectorsVector {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "nvme_max_sectors_vectors.json"))
	require.NoError(t, err)
	var file struct {
		Vectors []maxSectorsVector `json:"vectors"`
	}
	require.NoError(t, json.Unmarshal(data, &file))
	require.NotEmpty(t, file.Vectors)
	return file.Vectors
}

func stubMDTS(t *testing.T, mdts *int) *[]string {
	t.Helper()
	original := readNVMeControllerMDTS
	t.Cleanup(func() { readNVMeControllerMDTS = original })
	var mu sync.Mutex
	asked := []string{}
	readNVMeControllerMDTS = func(_ context.Context, controller string) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, controller)
		if mdts == nil {
			return 0, errors.New("id-ctrl failed")
		}
		return *mdts, nil
	}
	return &asked
}

// TestNVMeMaxSectorsVectors runs the shared vectors the Rust node agent runs
// too (rust/scale-csi-node/src/max_sectors.rs).
func TestNVMeMaxSectorsVectors(t *testing.T) {
	for _, vector := range loadMaxSectorsVectors(t) {
		t.Run(vector.Name, func(t *testing.T) {
			f := newFakeMaxSectorsSysfs(t)
			unwritable := map[string]bool{}
			for _, name := range vector.Unwritable {
				unwritable[name] = true
			}
			for _, subsys := range vector.Subsystems {
				dir := f.subsystem(t, subsys.Name, subsys.NQN)
				for name, kb := range subsys.Heads {
					f.disk(t, dir, name, kb, !unwritable[name])
				}
				for _, ctrl := range subsys.Controllers {
					ctrlDir := f.controller(t, dir, ctrl.Name, ctrl.State, ctrl.Transport)
					for name, kb := range ctrl.Paths {
						f.disk(t, ctrlDir, name, kb, !unwritable[name])
					}
				}
			}
			asked := stubMDTS(t, vector.MDTS)
			policy, err := ParseNVMeMaxSectorsPolicy(vector.Policy)
			require.NoError(t, err)
			nqn := vector.NQN
			if nqn == "" {
				nqn = "nqn.test:vol"
			}

			result, err := clampNVMeoFMaxSectorsAt(context.Background(), f.subsystemRoot, nqn, NVMeMaxSectorsClampOptions{Policy: policy, Transport: vector.Transport})
			if vector.Expect.Error {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrNVMeMaxSectorsClamp)
			} else {
				require.NoError(t, err)
				assert.Equal(t, vector.Expect.LimitKB, result.LimitKB, "limit (%s)", result)
				assert.Equal(t, vector.Expect.Skipped, result.Skipped != "", "skipped (%s)", result)
				assert.Equal(t, vector.Expect.Waiting, result.Waiting)
			}
			assert.Equal(t, vector.Expect.MDTSQueried, len(*asked) > 0, "MDTS queried: %v", *asked)
			assert.LessOrEqual(t, len(*asked), 1, "one controller answers for the subsystem")

			got := map[string]int{}
			for name := range f.queues {
				got[name] = f.value(t, name)
			}
			assert.Equal(t, vector.Expect.After, got)
		})
	}
}

func TestNVMeMaxSectorsWaitsForLatePaths(t *testing.T) {
	f := newFakeMaxSectorsSysfs(t)
	dir := f.subsystem(t, "nvme-subsys3", "nqn.late")
	f.disk(t, dir, "nvme3n1", 32768, true)
	first := f.controller(t, dir, "nvme3", "live", "tcp")
	f.disk(t, first, "nvme3c3n1", 32768, true)
	// Three more paths are live but their namespace scan has not created the
	// path devices yet: they appear while the clamp waits.
	late := make([]string, 0, 3)
	for _, name := range []string{"nvme4", "nvme5", "nvme6"} {
		late = append(late, f.controller(t, dir, name, "live", "tcp"))
	}
	zero := 0
	stubMDTS(t, &zero)

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(150 * time.Millisecond)
		for i, ctrlDir := range late {
			name := "nvme3c" + strconv.Itoa(4+i) + "n1"
			queue := filepath.Join(ctrlDir, name, "queue")
			_ = os.MkdirAll(queue, 0o750)
			_ = os.WriteFile(filepath.Join(queue, "max_sectors_kb"), []byte("32768\n"), 0o600)
		}
	}()

	result, err := clampNVMeoFMaxSectorsAt(context.Background(), f.subsystemRoot, "nqn.late", NVMeMaxSectorsClampOptions{PathWait: 5 * time.Second})
	<-done
	require.NoError(t, err)
	require.Len(t, result.Devices, 5, "%s", result)
	for _, device := range result.Devices {
		assert.Equal(t, 4096, device.AfterKB, device.Name)
	}
	for i, ctrlDir := range late {
		name := "nvme3c" + strconv.Itoa(4+i) + "n1"
		assert.Equal(t, 4096, readMaxSectorsKB(filepath.Join(ctrlDir, name, "queue")), name)
	}
	assert.Equal(t, 5, result.Changed())
}

func TestNVMeMaxSectorsLatePathTimesOut(t *testing.T) {
	f := newFakeMaxSectorsSysfs(t)
	dir := f.subsystem(t, "nvme-subsys3", "nqn.late")
	f.disk(t, dir, "nvme3n1", 32768, true)
	first := f.controller(t, dir, "nvme3", "live", "tcp")
	f.disk(t, first, "nvme3c3n1", 32768, true)
	f.controller(t, dir, "nvme4", "live", "tcp")
	zero := 0
	stubMDTS(t, &zero)

	start := time.Now()
	_, err := clampNVMeoFMaxSectorsAt(context.Background(), f.subsystemRoot, "nqn.late", NVMeMaxSectorsClampOptions{PathWait: 200 * time.Millisecond})
	require.ErrorIs(t, err, ErrNVMeMaxSectorsClamp)
	assert.Contains(t, err.Error(), "nvme4")
	assert.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond, "waited the whole budget")
	assert.Equal(t, 32768, f.value(t, "nvme3n1"), "nothing written when the clamp cannot cover every live path")
}

func TestNVMeMaxSectorsWaitHonorsCancel(t *testing.T) {
	f := newFakeMaxSectorsSysfs(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := clampNVMeoFMaxSectorsAt(ctx, f.subsystemRoot, "nqn.absent", NVMeMaxSectorsClampOptions{PathWait: time.Minute})
	require.ErrorIs(t, err, ErrNVMeMaxSectorsClamp)
	assert.Contains(t, err.Error(), "canceled")
}

func TestNVMeMaxSectorsIsIdempotent(t *testing.T) {
	f := newFakeMaxSectorsSysfs(t)
	dir := f.subsystem(t, "nvme-subsys0", "nqn.again")
	f.disk(t, dir, "nvme0n1", 32768, true)
	ctrl := f.controller(t, dir, "nvme0", "live", "tcp")
	f.disk(t, ctrl, "nvme0c0n1", 32768, true)
	zero := 0
	asked := stubMDTS(t, &zero)

	first, err := clampNVMeoFMaxSectorsAt(context.Background(), f.subsystemRoot, "nqn.again", NVMeMaxSectorsClampOptions{})
	require.NoError(t, err)
	assert.Equal(t, 2, first.Changed())
	second, err := clampNVMeoFMaxSectorsAt(context.Background(), f.subsystemRoot, "nqn.again", NVMeMaxSectorsClampOptions{})
	require.NoError(t, err)
	assert.Equal(t, 0, second.Changed(), "a re-stage finds everything in place")
	assert.Len(t, *asked, 1, "a converged subsystem costs no id-ctrl")
	assert.Contains(t, second.String(), "nvme0c0n1 4096->4096")
}

func TestNVMeMaxSectorsSummaryHasBeforeAndAfter(t *testing.T) {
	f := newFakeMaxSectorsSysfs(t)
	dir := f.subsystem(t, "nvme-subsys0", "nqn.log")
	f.disk(t, dir, "nvme0n1", 32768, true)
	ctrl := f.controller(t, dir, "nvme0", "live", "tcp")
	f.disk(t, ctrl, "nvme0c0n1", 32768, true)
	zero := 0
	stubMDTS(t, &zero)

	result, err := clampNVMeoFMaxSectorsAt(context.Background(), f.subsystemRoot, "nqn.log", NVMeMaxSectorsClampOptions{Transport: "tcp"})
	require.NoError(t, err)
	assert.Equal(t, "policy=auto transport=tcp mdts=0 limit=4096KiB changed=2 devices=[nvme0c0n1 32768->4096, nvme0n1 32768->4096]", result.String())
}

func TestParseNVMeMaxSectorsPolicy(t *testing.T) {
	for raw, want := range map[string]NVMeMaxSectorsPolicy{
		"":       {Mode: NVMeMaxSectorsAuto},
		"auto":   {Mode: NVMeMaxSectorsAuto},
		" Auto ": {Mode: NVMeMaxSectorsAuto},
		"off":    {Mode: NVMeMaxSectorsOff},
		"OFF":    {Mode: NVMeMaxSectorsOff},
		"0":      {Mode: NVMeMaxSectorsOff},
		"false":  {Mode: NVMeMaxSectorsOff},
		"4":      {Mode: NVMeMaxSectorsFixed, LimitKB: 4},
		"4096":   {Mode: NVMeMaxSectorsFixed, LimitKB: 4096},
	} {
		got, err := ParseNVMeMaxSectorsPolicy(raw)
		require.NoError(t, err, raw)
		assert.Equal(t, want, got, raw)
	}
	for _, bad := range []string{"-1", "3", "4k", "4096KiB", "on", "1.5", "true"} {
		_, err := ParseNVMeMaxSectorsPolicy(bad)
		assert.Error(t, err, bad)
	}
}

func TestParseNVMeIDCtrlMDTS(t *testing.T) {
	mdts, err := parseNVMeIDCtrlMDTS([]byte(`{"vid":0,"ssvid":0,"sn":"abc","mdts":0,"cntlid":1,"fguid":"","subnqn":"nqn.x"}`))
	require.NoError(t, err)
	assert.Equal(t, 0, mdts)
	mdts, err = parseNVMeIDCtrlMDTS([]byte(`{"mdts":5,"tnvmcap":340282366920938463463374607431768211455}`))
	require.NoError(t, err, "a 128-bit capacity elsewhere in the document is skipped, not parsed")
	assert.Equal(t, 5, mdts)
	for _, bad := range []string{`{}`, `{"mdts":-1}`, `not json`, `{"mdts":"0"}`} {
		_, err := parseNVMeIDCtrlMDTS([]byte(bad))
		assert.Error(t, err, bad)
	}
}
