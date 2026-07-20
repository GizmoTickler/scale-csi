package main

import "testing"

func FuzzParseCgroupMemoryLimit(f *testing.F) {
	f.Add("memory.max", []byte("104857600\n"), false)
	f.Add("memory.max", []byte("max\n"), false)
	f.Add("memory.limit_in_bytes", []byte("9223372036854771712\n"), true)
	f.Add("memory.max", []byte("not-a-number"), false)
	f.Fuzz(func(t *testing.T, path string, contents []byte, cgroupV1 bool) {
		limit, finite, err := parseCgroupMemoryLimit(path, contents, cgroupV1)
		if limit < 0 {
			t.Fatalf("negative limit %d (finite=%v, err=%v)", limit, finite, err)
		}
		if err == nil && finite && limit == 0 {
			t.Fatal("finite cgroup limit must be positive")
		}
	})
}
