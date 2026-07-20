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

func TestNVMeAutoExecutesRequestedCommandOnce(t *testing.T) {
	tests := []struct {
		name          string
		probeExitCode string
		wantExecutor  string
		wantArgv      []string
	}{
		{
			name:          "chroot probe succeeds",
			probeExitCode: "0",
			wantExecutor:  "chroot",
			wantArgv: []string{
				"/host", "/usr/bin/nvme", "connect", "--nqn", "nqn.2026-07.example:auto",
			},
		},
		{
			name:          "chroot probe fails",
			probeExitCode: "31",
			wantExecutor:  "nsenter",
			wantArgv: []string{
				"-t", "1", "--mount", "--net", "--",
				"/usr/bin/nvme", "connect", "--nqn", "nqn.2026-07.example:auto",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runWrapper(t, "nvme", []string{
				"NVME_HOST_STRATEGY=auto",
				"NVME_HOST_PATH=/usr/bin/nvme",
				"WRAPPER_CHROOT_PROBE_EXIT_CODE=" + tt.probeExitCode,
			}, "connect", "--nqn", "nqn.2026-07.example:auto")

			if result.probeInvocations != 1 {
				t.Fatalf("probe invocations = %d, want 1", result.probeInvocations)
			}
			if result.realInvocations != 1 {
				t.Fatalf("requested-command invocations = %d, want 1", result.realInvocations)
			}
			if result.executor != tt.wantExecutor {
				t.Fatalf("executor = %q, want %q", result.executor, tt.wantExecutor)
			}
			if !reflect.DeepEqual(result.argv, tt.wantArgv) {
				t.Fatalf("%s argv = %#v, want %#v", tt.wantExecutor, result.argv, tt.wantArgv)
			}
			if result.exitCode != 23 {
				t.Fatalf("exit code = %d, want 23", result.exitCode)
			}
			if result.stderr != "host-command-stderr" {
				t.Fatalf("stderr = %q, want exact host stderr", result.stderr)
			}
		})
	}
}

func TestUmountAcceptsLazyFlagWithoutGetoptsError(t *testing.T) {
	result := runWrapper(t, "umount", []string{"USE_HOST_MOUNT_TOOLS=1"}, "-l", "/var/lib/kubelet/test target")

	wantArgv := []string{"/host", "/bin/umount", "-l", "/var/lib/kubelet/test target"}
	if !reflect.DeepEqual(result.argv, wantArgv) {
		t.Fatalf("chroot argv = %#v, want %#v", result.argv, wantArgv)
	}
	if result.stderr != "host-command-stderr" {
		t.Fatalf("stderr = %q, want exact host stderr without getopts diagnostics", result.stderr)
	}
}

type wrapperResult struct {
	argv             []string
	stderr           string
	exitCode         int
	executor         string
	probeInvocations int
	realInvocations  int
}

func runWrapper(t *testing.T, name string, environment []string, args ...string) wrapperResult {
	t.Helper()

	tempDir := t.TempDir()
	argvLog := filepath.Join(tempDir, "argv.log")
	invocationLog := filepath.Join(tempDir, "invocations.log")
	fakeNSenter := filepath.Join(tempDir, "nsenter")
	fakeCommand := `#!/bin/sh
executor=${0##*/}
if [ "$executor" = "chroot" ] && [ "$#" -eq 3 ] && [ "$3" = "--version" ]; then
  printf 'probe:%s\n' "$executor" >> "$WRAPPER_INVOCATION_LOG"
  printf 'probe-stderr' >&2
  exit "${WRAPPER_CHROOT_PROBE_EXIT_CODE:-1}"
fi
printf 'real:%s\n' "$executor" >> "$WRAPPER_INVOCATION_LOG"
printf '%s\n' "$@" > "$WRAPPER_ARGV_LOG"
printf '%s' "${WRAPPER_STDERR:-}" >&2
exit "${WRAPPER_EXIT_CODE:-0}"
`
	if err := os.WriteFile(fakeNSenter, []byte(fakeCommand), 0o755); err != nil {
		t.Fatalf("write fake nsenter: %v", err)
	}
	fakeChroot := filepath.Join(tempDir, "chroot")
	if err := os.WriteFile(fakeChroot, []byte(fakeCommand), 0o755); err != nil {
		t.Fatalf("write fake chroot: %v", err)
	}

	wrapperPath, err := filepath.Abs(name)
	if err != nil {
		t.Fatalf("resolve wrapper path: %v", err)
	}
	cmd := exec.Command(wrapperPath, args...)
	cmd.Env = append(os.Environ(),
		"PATH="+tempDir+":/usr/bin:/bin",
		"WRAPPER_ARGV_LOG="+argvLog,
		"WRAPPER_INVOCATION_LOG="+invocationLog,
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
	invocationBytes, err := os.ReadFile(invocationLog)
	if err != nil {
		t.Fatalf("read invocation log: %v", err)
	}
	var executor string
	var probeInvocations, realInvocations int
	for _, invocation := range strings.Split(strings.TrimSpace(string(invocationBytes)), "\n") {
		switch {
		case strings.HasPrefix(invocation, "probe:"):
			probeInvocations++
		case strings.HasPrefix(invocation, "real:"):
			realInvocations++
			executor = strings.TrimPrefix(invocation, "real:")
		}
	}

	stderr := ""
	if exitErr != nil {
		stderr = string(exitErr.Stderr)
	}
	return wrapperResult{
		argv:             argv,
		stderr:           stderr,
		exitCode:         exitCode,
		executor:         executor,
		probeInvocations: probeInvocations,
		realInvocations:  realInvocations,
	}
}
