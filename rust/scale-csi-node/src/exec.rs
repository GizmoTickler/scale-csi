//! Host commands, with the Go driver's hardening (`pkg/util/iscsi.go` HardenCmd,
//! `pkg/util/config.go` commandContext):
//!
//! - each command runs in its own process group, and a timeout kills the whole
//!   group, so the host binary behind a `bash -> nsenter` wrapper dies with it;
//! - after the command exits, its output pipes get a bounded grace: a surviving
//!   grandchild that still holds them cannot hang the caller;
//! - a command killed by a signal, or whose pipes never closed, is *wedged*: its
//!   output is untrustworthy and must never be read as "already connected",
//!   "not found" and the like;
//! - the deadline is the smaller of the command's own timeout and what is left
//!   of the RPC's, floored at 5 s, and nothing starts once the RPC's has passed.

use std::io;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use tokio::process::Command;

/// How long the output pipes may stay open after the command has exited.
const PIPE_GRACE: Duration = Duration::from_secs(5);
/// No command is given less than this, whatever is left of the RPC.
const MIN_COMMAND_TIME: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct Output {
    /// The exit code; `None` when the command was killed by a signal.
    pub code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    /// Killed (by the deadline or any signal), or the pipes outlived the
    /// command: never trust the text.
    pub wedged: bool,
    pub timed_out: bool,
}

impl Output {
    pub fn success(&self) -> bool {
        self.code == Some(0) && !self.wedged
    }

    /// stdout and stderr together, as a failure message shows them.
    pub fn combined(&self) -> String {
        let mut text = String::from_utf8_lossy(&self.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&self.stderr));
        text
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// The command's own configured timeout.
    pub timeout: Duration,
    /// The RPC's deadline, if it has one.
    pub rpc_deadline: Option<Instant>,
}

/// The time a command may take, or `None` when the RPC's deadline has passed and
/// the command must not start.
pub fn command_budget(limits: Limits, now: Instant) -> Option<Duration> {
    match limits.rpc_deadline {
        None => Some(limits.timeout),
        Some(deadline) => {
            let left = deadline.checked_duration_since(now).filter(|d| !d.is_zero())?;
            Some(limits.timeout.min(left).max(MIN_COMMAND_TIME))
        }
    }
}

/// Runs `program args`. `c_locale` sets `LC_ALL=C` (for tools whose output text
/// is matched: nvme, iscsiadm).
pub async fn run(program: &str, args: &[&str], limits: Limits, c_locale: bool) -> io::Result<Output> {
    let Some(budget) = command_budget(limits, Instant::now()) else {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("deadline passed before running {program}"),
        ));
    };
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    if c_locale {
        command.env("LC_ALL", "C");
    }
    let mut child = command.spawn()?;
    let pgid = child.id().map(|id| id as libc::pid_t);
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    let out_reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf).await;
        buf
    });
    let err_reader = tokio::spawn(async move {
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf).await;
        buf
    });

    let mut timed_out = false;
    let status = match tokio::time::timeout(budget, child.wait()).await {
        Ok(status) => status?,
        Err(_) => {
            timed_out = true;
            kill_group(pgid);
            child.wait().await?
        }
    };
    // A command that exits can still leave its group holding the pipes.
    let mut wedged = timed_out;
    let aborts = [out_reader.abort_handle(), err_reader.abort_handle()];
    let (stdout, stderr) = match tokio::time::timeout(PIPE_GRACE, async { (out_reader.await, err_reader.await) }).await
    {
        Ok((out, err)) => (out.unwrap_or_default(), err.unwrap_or_default()),
        Err(_) => {
            wedged = true;
            kill_group(pgid);
            aborts.iter().for_each(|a| a.abort());
            (Vec::new(), Vec::new())
        }
    };
    let code = status.code();
    if code.is_none() {
        wedged = true;
    }
    Ok(Output {
        code,
        stdout,
        stderr,
        wedged,
        timed_out,
    })
}

fn kill_group(pgid: Option<libc::pid_t>) {
    if let Some(pgid) = pgid.filter(|p| *p > 0) {
        // ESRCH (the group is already gone) is the same as success here.
        // SAFETY: kill(2) with a negative pid signals that process group only.
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(timeout: Duration) -> Limits {
        Limits {
            timeout,
            rpc_deadline: None,
        }
    }

    #[test]
    fn the_budget_is_the_smaller_deadline_floored_at_five_seconds() {
        let now = Instant::now();
        let l = |t: u64, left: Option<u64>| Limits {
            timeout: Duration::from_secs(t),
            rpc_deadline: left.map(|s| now + Duration::from_secs(s)),
        };
        assert_eq!(command_budget(l(30, None), now), Some(Duration::from_secs(30)));
        assert_eq!(command_budget(l(30, Some(10)), now), Some(Duration::from_secs(10)));
        assert_eq!(command_budget(l(30, Some(2)), now), Some(MIN_COMMAND_TIME));
        assert_eq!(command_budget(l(3, Some(60)), now), Some(MIN_COMMAND_TIME));
        let passed = Limits {
            timeout: Duration::from_secs(30),
            rpc_deadline: Some(now),
        };
        assert_eq!(
            command_budget(passed, now + Duration::from_millis(1)),
            None,
            "a passed deadline never starts a command"
        );
    }

    #[tokio::test]
    async fn output_and_exit_codes() {
        let out = run(
            "sh",
            &["-c", "echo hi; echo err >&2; exit 3"],
            limits(Duration::from_secs(10)),
            false,
        )
        .await
        .unwrap();
        assert_eq!(out.code, Some(3));
        assert_eq!(out.stdout, b"hi\n");
        assert_eq!(out.stderr, b"err\n");
        assert!(!out.wedged && !out.success());
        let ok = run(
            "sh",
            &["-c", "printf %s \"$LC_ALL\""],
            limits(Duration::from_secs(10)),
            true,
        )
        .await
        .unwrap();
        assert!(ok.success());
        assert_eq!(ok.stdout, b"C");
    }

    #[tokio::test]
    async fn a_timeout_kills_the_whole_group_and_is_wedged() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("grandchild-survived");
        // The grandchild would write the marker after 2 s if it outlived the kill.
        let script = format!("(sleep 2; touch {}) & sleep 30", marker.display());
        let started = Instant::now();
        let out = run("sh", &["-c", &script], limits(Duration::from_millis(200)), false)
            .await
            .unwrap();
        assert!(out.timed_out && out.wedged && !out.success());
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "took {:?}",
            started.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert!(!marker.exists(), "the grandchild was not killed with its group");
    }

    #[tokio::test]
    async fn a_grandchild_holding_the_pipes_cannot_hang_the_caller() {
        // The command exits at once; a detached grandchild in another session keeps
        // stdout open for 20 s.
        let started = Instant::now();
        let out = run(
            "sh",
            &["-c", "setsid sleep 20 & echo started"],
            limits(Duration::from_secs(10)),
            false,
        )
        .await
        .unwrap();
        let took = started.elapsed();
        assert!(
            took >= PIPE_GRACE && took < PIPE_GRACE + Duration::from_secs(3),
            "took {took:?}"
        );
        assert!(
            out.wedged,
            "output of a command whose pipes never closed is not trusted"
        );
        assert_eq!(out.code, Some(0));
    }

    #[tokio::test]
    async fn a_command_killed_by_a_signal_is_wedged() {
        let out = run("sh", &["-c", "kill -9 $$"], limits(Duration::from_secs(10)), false)
            .await
            .unwrap();
        assert_eq!(out.code, None);
        assert!(out.wedged && !out.timed_out);
    }
}
