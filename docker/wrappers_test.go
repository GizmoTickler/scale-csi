package docker_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestISCSIAdmNSenterUsesHostPIDOneAndPreservesProcessResults(t *testing.T) {
	result := runWrapper(t, "iscsiadm", []string{
		"ISCSIADM_HOST_STRATEGY=nsenter",
		"ISCSIADM_HOST_PATH=/usr/local/sbin/iscsiadm",
	}, "-m", "node", "argument with spaces")

	wantArgv := []string{
		"-t", "1", "--mount", "--net", "--",
		"/usr/local/sbin/iscsiadm", "-m", "node", "argument with spaces",
	}
	if !reflect.DeepEqual(result.argv, wantArgv) {
		t.Fatalf("nsenter argv = %#v, want %#v", result.argv, wantArgv)
	}
	if result.exitCode != 23 {
		t.Fatalf("exit code = %d, want 23", result.exitCode)
	}
	if result.stderr != "host-command-stderr" {
		t.Fatalf("stderr = %q, want exact host stderr", result.stderr)
	}
}

func TestNVMeNSenterUsesHostPIDOne(t *testing.T) {
	result := runWrapper(t, "nvme", []string{
		"NVME_HOST_STRATEGY=nsenter",
		"NVME_HOST_PATH=/usr/bin/nvme",
	}, "connect", "--nqn", "nqn.2026-07.example:test")

	wantArgv := []string{
		"-t", "1", "--mount", "--net", "--",
		"/usr/bin/nvme", "connect", "--nqn", "nqn.2026-07.example:test",
	}
	if !reflect.DeepEqual(result.argv, wantArgv) {
		t.Fatalf("nsenter argv = %#v, want %#v", result.argv, wantArgv)
	}
}

type wrapperResult struct {
	argv     []string
	stderr   string
	exitCode int
}

func runWrapper(t *testing.T, name string, environment []string, args ...string) wrapperResult {
	t.Helper()

	tempDir := t.TempDir()
	argvLog := filepath.Join(tempDir, "argv.log")
	fakeNSenter := filepath.Join(tempDir, "nsenter")
	fakeCommand := `#!/bin/sh
printf '%s\n' "$@" > "$WRAPPER_ARGV_LOG"
printf '%s' "${WRAPPER_STDERR:-}" >&2
exit "${WRAPPER_EXIT_CODE:-0}"
`
	if err := os.WriteFile(fakeNSenter, []byte(fakeCommand), 0o755); err != nil {
		t.Fatalf("write fake nsenter: %v", err)
	}

	wrapperPath, err := filepath.Abs(name)
	if err != nil {
		t.Fatalf("resolve wrapper path: %v", err)
	}
	cmd := exec.Command(wrapperPath, args...)
	cmd.Env = append(os.Environ(),
		"PATH="+tempDir+":/usr/bin:/bin",
		"WRAPPER_ARGV_LOG="+argvLog,
		"WRAPPER_STDERR=host-command-stderr",
		"WRAPPER_EXIT_CODE=23",
	)
	cmd.Env = append(cmd.Env, environment...)
	output, runErr := cmd.Output()
	if len(output) != 0 {
		t.Fatalf("stdout = %q, want empty passthrough", output)
	}

	exitCode := 0
	var exitErr *exec.ExitError
	if runErr != nil {
		if !errors.As(runErr, &exitErr) {
			t.Fatalf("run wrapper: %v", runErr)
		}
		exitCode = exitErr.ExitCode()
	}

	argvBytes, err := os.ReadFile(argvLog)
	if err != nil {
		t.Fatalf("read argv log: %v", err)
	}
	argv := strings.Split(strings.TrimSuffix(string(argvBytes), "\n"), "\n")

	stderr := ""
	if exitErr != nil {
		stderr = string(exitErr.Stderr)
	}
	return wrapperResult{argv: argv, stderr: stderr, exitCode: exitCode}
}
