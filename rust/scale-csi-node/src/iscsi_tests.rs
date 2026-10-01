//! The iSCSI initiator layer: iscsiadm's argv and exit-code semantics through
//! a scripted runner, Go's parsers through vectors generated from the Go
//! functions, the node database through the open-iscsi fixture the Go tests
//! use, and sysfs lookups through a fake tree.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::exec::{Limits, Output};
use crate::iscsi::{
    Credentials, InfoError, Iscsi, Session, canonical_portal, go_duration, node_record_files, normalize_scsi_wwid,
    parse_sessions, same_portal, set_record_param, split_portal, write_node_record_secret,
};
use crate::mount::Runner;

/// (exit code, output, wedged)
type Reply = (Option<i32>, &'static str, bool);

#[derive(Default)]
struct Script {
    replies: Mutex<Vec<Reply>>,
    calls: Mutex<Vec<String>>,
}

impl Script {
    fn new(replies: Vec<Reply>) -> Arc<Self> {
        Arc::new(Script {
            replies: Mutex::new(replies.into_iter().rev().collect()),
            calls: Mutex::default(),
        })
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl Runner for Script {
    async fn run(&self, program: &str, args: &[&str], _: Limits) -> std::io::Result<Output> {
        self.calls.lock().unwrap().push(format!("{program} {}", args.join(" ")));
        let (code, text, wedged) = self.replies.lock().unwrap().pop().expect("an unscripted command ran");
        Ok(Output {
            code,
            stdout: text.into(),
            stderr: Vec::new(),
            wedged,
            timed_out: false,
        })
    }
}

fn initiator(script: &Arc<Script>, root: &Path) -> Iscsi {
    let mut iscsi = Iscsi::new(script.clone(), Duration::from_secs(10));
    iscsi.sysfs = root.join("sys");
    iscsi.dev = root.join("dev");
    iscsi.node_db_roots = vec![root.join("etc-iscsi"), root.join("var-lib-iscsi")];
    iscsi.multipathd_sockets = vec![root.join("run/multipathd.sock")];
    iscsi
}

/// Generated from Go parseISCSISessionLines.
#[test]
fn session_lines_parse_as_go_does() {
    let session = |id: &str, portal: &str, iqn: &str| Session {
        portal: portal.into(),
        iqn: iqn.into(),
        id: id.into(),
    };
    let cases: Vec<(&str, Option<Session>)> = vec![
        (
            "tcp: [1] 192.0.2.10:3260,1 iqn.2005-10.org.freenas.ctl:pvc-1 (non-flash)",
            Some(session("1", "192.0.2.10:3260", "iqn.2005-10.org.freenas.ctl:pvc-1")),
        ),
        (
            "tcp: [12] [2001:db8::1]:3260,1 iqn.2005-10.org.freenas.ctl:pvc-2",
            Some(session("12", "[2001:db8::1]:3260", "iqn.2005-10.org.freenas.ctl:pvc-2")),
        ),
        (
            "  tcp:  [3]   192.0.2.11:3260,12  iqn.x:y  ",
            Some(session("3", "192.0.2.11:3260", "iqn.x:y")),
        ),
        ("tcp: [4]  ,1 iqn.a:b", Some(session("4", " ", "iqn.a:b"))),
        ("tcp: [5] ,1 iqn.a:b", None),
        ("tcp: [6] a b,1 iqn.a:b", Some(session("6", "a b", "iqn.a:b"))),
        ("tcp: [7] 192.0.2.10:3260,1 eui.x", None),
        ("tcp: [8] 192.0.2.10:3260,x iqn.a:b", None),
        ("tcp:[9] 192.0.2.10:3260,1 iqn.a:b", None),
        ("iser: [10] 192.0.2.10:3260,1 iqn.a:b", None),
        ("tcp: [11] 192.0.2.10:3260,1 iqn.", None),
        ("tcp: [x] 192.0.2.10:3260,1 iqn.a:b", None),
        (
            "tcp:\t[13]\t192.0.2.10:3260,1\tiqn.a:b",
            Some(session("13", "192.0.2.10:3260", "iqn.a:b")),
        ),
        ("tcp: [14] 192.0.2.10:3260,1,2 iqn.a:b", None),
        (
            "tcp: [15] 192.0.2.10:3260,1 iqn.a:b,c d",
            Some(session("15", "192.0.2.10:3260", "iqn.a:b,c")),
        ),
        ("tcp: [16] 192.0.2.10:3260,1iqn.a:b", None),
        ("tcp: [17] 192.0.2.10:3260, 1 iqn.a:b", None),
    ];
    for (line, want) in cases {
        assert_eq!(parse_sessions(line).into_iter().next(), want, "{line:?}");
    }
    let many = parse_sessions("tcp: [1] 192.0.2.10:3260,1 iqn.a:b\n\ngarbage\ntcp: [2] 192.0.2.11:3260,1 iqn.a:b\n");
    assert_eq!(many.len(), 2);
}

/// Generated from Go canonicalISCSIPortalForComparison.
#[test]
fn portals_compare_as_go_does() {
    for (input, want) in [
        ("", ":3260"),
        (" 192.0.2.10:3260 ", "192.0.2.10:3260"),
        ("1.2.3.4:", "1.2.3.4:"),
        ("192.0.2.10", "192.0.2.10:3260"),
        ("192.0.2.10:03260", "192.0.2.10:03260"),
        ("192.0.2.10:3260", "192.0.2.10:3260"),
        ("2001:db8::1", "[2001:db8::1]:3260"),
        (":3260", ":3260"),
        ("::", "[::]:3260"),
        ("::ffff:192.0.2.1", "192.0.2.1:3260"),
        ("HOST:3260", "host:3260"),
        ("Host.Example:3260", "host.example:3260"),
        ("[ HOST ]:3260", "host:3260"),
        ("[ ]:", ":"),
        ("[1.2.3.4]:3260", "1.2.3.4:3260"),
        ("[2001:0db8:0000:0000:0000:0000:0000:0001]:3260", "[2001:db8::1]:3260"),
        ("[2001:db8:0:0:1:0:0:1]:3260", "[2001:db8::1:0:0:1]:3260"),
        ("[2001:db8::0:1]:3260", "[2001:db8::1]:3260"),
        ("[2001:db8::1]", "[2001:db8::1]:3260"),
        ("[2001:db8::1]:3260", "[2001:db8::1]:3260"),
        ("[::1.2.3.4]:3260", "[::102:304]:3260"),
        ("[::ffff:192.0.2.1]:3260", "192.0.2.1:3260"),
        ("[A:B::C]:99", "[a:b::c]:99"),
        ("[[]]", ":3260"),
        ("[]", ":3260"),
        ("[fe80::1%eth0]:3260", "[fe80::1%eth0]:3260"),
        ("[x:3260", "[x:3260"),
        ("] [] [", ":3260"),
        ("a [b] c", "a b c:3260"),
        ("a:b:c", "a:b:c"),
        ("host.example", "host.example:3260"),
        ("host:", "host:"),
        ("x]:3260", "x]:3260"),
    ] {
        assert_eq!(canonical_portal(input), want, "{input:?}");
    }
    assert!(same_portal("192.0.2.10", "192.0.2.10:3260"));
    assert!(same_portal("[2001:0db8::0001]:3260", "[2001:db8::1]:3260"));
    assert!(!same_portal("192.0.2.10:3260", "192.0.2.11:3260"));
}

/// Generated from Go normalizeSCSIWWID and splitISCSIPortal.
#[test]
fn wwids_and_record_portals_as_go_does() {
    for (input, want) in [
        ("", ""),
        (" naa.x ", "3x"),
        ("36589cfc", "36589cfc"),
        ("NAA.6589", "36589"),
        ("T10.a b", "1a_b"),
        ("eui.0123", "20123"),
        ("naa.", "3"),
        ("naa.6589cfc0000002", "36589cfc0000002"),
        ("t10.ATA  disk 1", "1ATA__disk_1"),
    ] {
        assert_eq!(normalize_scsi_wwid(input), want, "{input:?}");
    }
    for (input, host, port) in [
        (" h:1 ", "h", "1"),
        ("192.0.2.10", "192.0.2.10", "3260"),
        ("192.0.2.10:3260", "192.0.2.10", "3260"),
        ("2001:db8::1", "2001:db8::1", "3260"),
        ("[2001:db8::1]", "2001:db8::1", "3260"),
        ("[2001:db8::1]:3260", "2001:db8::1", "3260"),
        ("h: 2", "h", "2"),
    ] {
        assert_eq!(split_portal(input), (host.to_string(), port.to_string()), "{input:?}");
    }
    assert_eq!(go_duration(Duration::from_secs(60)), "1m0s");
    assert_eq!(go_duration(Duration::from_secs(2)), "2s");
    assert_eq!(go_duration(Duration::from_millis(500)), "500ms");
    assert_eq!(go_duration(Duration::from_millis(1500)), "1.5s");
}

/// Generated from Go setISCSINodeRecordParamText: the record text Go writes is
/// the text this writes, so either can read the other's records.
#[test]
fn node_record_text_as_go_writes_it() {
    let p = "node.session.auth.password";
    for (text, name, value, want) in [
        ("", p, "secretsecret1", "node.session.auth.password = secretsecret1\n"),
        (
            "node.name = x\n",
            p,
            "s",
            "node.name = x\nnode.session.auth.password = s\n",
        ),
        (
            "node.name = x",
            p,
            "s",
            "node.name = x\nnode.session.auth.password = s\n",
        ),
        (
            "a = 1\nnode.session.auth.password = old\nb = 2\nnode.session.auth.password=older\n",
            p,
            "new",
            "a = 1\nnode.session.auth.password = new\nb = 2\nnode.session.auth.password = new\n",
        ),
        (
            "# node.session.auth.password = c\n  node.session.auth.password   =  x  \n\n",
            p,
            "y",
            "# node.session.auth.password = c\nnode.session.auth.password = y\n\n",
        ),
        (
            "node.session.auth.password_in = z\n",
            p,
            "y",
            "node.session.auth.password_in = z\nnode.session.auth.password = y\n",
        ),
        ("\n\n", "k", "v", "k = v\n"),
        ("k = 1\n\n\n", "k", "v", "k = v\n\n\n"),
        ("x\nk\n", "k", "v", "x\nk\nk = v\n"),
    ] {
        assert_eq!(set_record_param(text, name, value), want, "{text:?}");
    }
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
            // Looser than open-iscsi creates them: the write must tighten.
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
    }
}

/// The node database open-iscsi 2.1.11 really writes, the Go node's fixture.
fn fixture_db() -> (tempfile::TempDir, PathBuf) {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../pkg/util/testdata/iscsi-node-db/tree");
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("etc-iscsi");
    copy_tree(&fixture, &root);
    (dir, root)
}

const FASTPATH: &str = "iqn.2005-10.org.freenas.ctl:pvc-fastpath";
const DISCOVERED: &str = "iqn.2005-10.org.freenas.ctl:pvc-discovered";

#[test]
fn node_records_in_both_open_iscsi_layouts() {
    let (dir, root) = fixture_db();
    let roots = vec![root.clone(), dir.path().join("var-lib-iscsi")];
    let names = |portal: &str, iqn: &str| -> Vec<String> {
        node_record_files(&roots, portal, iqn)
            .unwrap()
            .into_iter()
            .map(|r| r.path.strip_prefix(&root).unwrap().display().to_string())
            .collect()
    };
    // `-o new` without a tag: the flat file.
    assert_eq!(
        names("192.0.2.40:3260", FASTPATH),
        [format!("nodes/{FASTPATH}/192.0.2.40,3260")]
    );
    assert_eq!(
        names("192.0.2.40", FASTPATH),
        [format!("nodes/{FASTPATH}/192.0.2.40,3260")]
    );
    // IPv6 records carry no brackets.
    assert_eq!(
        names("[2001:db8::31]:3260", FASTPATH),
        [format!("nodes/{FASTPATH}/2001:db8::31,3260")]
    );
    // A discovery's record: one file per iface.
    assert_eq!(
        names("192.0.2.40:3260", DISCOVERED),
        [
            format!("nodes/{DISCOVERED}/192.0.2.40,3260,1/default"),
            format!("nodes/{DISCOVERED}/192.0.2.40,3260,1/iface1")
        ]
    );
    assert!(
        node_record_files(&roots, "192.0.2.99:3260", FASTPATH).is_err(),
        "no record: fail closed"
    );
    assert!(
        node_record_files(&roots, "192.0.2.40:3261", FASTPATH).is_err(),
        "port is part of the name"
    );
    for bad in ["", "iqn.x/../y", "iqn..x"] {
        assert!(node_record_files(&roots, "192.0.2.40:3260", bad).is_err(), "{bad:?}");
    }
}

#[test]
fn a_secret_is_written_into_every_record_at_0600() {
    let (dir, root) = fixture_db();
    let roots = vec![root.clone(), dir.path().join("var-lib-iscsi")];
    let untouched =
        std::fs::read_to_string(root.join(format!("nodes/{DISCOVERED}/192.0.2.41,3260,1/default"))).unwrap();
    write_node_record_secret(
        &roots,
        "192.0.2.40:3260",
        DISCOVERED,
        "node.session.auth.password",
        "abcdefghijkl",
    )
    .unwrap();
    for iface in ["default", "iface1"] {
        let path = root.join(format!("nodes/{DISCOVERED}/192.0.2.40,3260,1/{iface}"));
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            text.lines()
                .filter(|l| l.trim() == "node.session.auth.password = abcdefghijkl")
                .count(),
            1,
            "{iface}: {text}"
        );
        assert!(text.contains("node.name = "), "the rest of the record is kept");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600,
            "{iface}"
        );
    }
    assert_eq!(
        std::fs::read_to_string(root.join(format!("nodes/{DISCOVERED}/192.0.2.41,3260,1/default"))).unwrap(),
        untouched,
        "another portal's record is not touched"
    );
    let stray: Vec<_> = std::fs::read_dir(&root)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(".scale-csi-node-"))
        .collect();
    assert!(stray.is_empty(), "no temporary file is left in the database root");
    for (value, why) in [
        (" abcdefghijkl", "whitespace"),
        ("abcdef\nghijkl", "newline"),
        ("abcdef#ghijkl", "'#'"),
    ] {
        let err = write_node_record_secret(&roots, "192.0.2.40:3260", FASTPATH, "node.session.auth.password", value)
            .unwrap_err();
        let text = format!("{err:#}");
        assert!(text.contains(why) && !text.contains("ghijkl"), "{text}");
    }
}

fn running_as_root() -> bool {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() == 0 }
}

/// A record that cannot be seen or written fails the write, never a partial
/// success; a write that stopped part way says so and never names the value.
#[test]
fn node_record_writes_fail_closed() {
    if running_as_root() {
        eprintln!("running as root: permissions do not apply; skipping");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("etc-iscsi");
    let target = root.join("nodes/iqn.x:t");
    std::fs::create_dir_all(target.join("192.0.2.40,3260,1")).unwrap();
    std::fs::write(target.join("192.0.2.40,3260"), "node.name = iqn.x:t\n").unwrap();
    std::fs::write(target.join("192.0.2.40,3260,1/default"), "node.name = iqn.x:t\n").unwrap();
    let roots = vec![root.clone()];
    // The tagged directory is read-only: its record cannot be replaced.
    std::fs::set_permissions(target.join("192.0.2.40,3260,1"), std::fs::Permissions::from_mode(0o555)).unwrap();
    let err = write_node_record_secret(
        &roots,
        "192.0.2.40:3260",
        "iqn.x:t",
        "node.session.auth.password",
        "abcdefghijkl",
    )
    .unwrap_err();
    let text = format!("{err:#}");
    assert!(
        text.contains("applied to 1 of 2 node records") && text.contains("partially updated"),
        "{text}"
    );
    assert!(!text.contains("abcdefghijkl"), "{text}");
    // A target directory that cannot be read hides records: fail closed.
    std::fs::set_permissions(target.join("192.0.2.40,3260,1"), std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o000)).unwrap();
    let err = node_record_files(&roots, "192.0.2.40:3260", "iqn.x:t").unwrap_err();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        format!("{err:#}").contains("failed to read iSCSI node database directory"),
        "{err:#}"
    );
}

#[tokio::test]
async fn iscsiadm_exit_codes_and_text() {
    let root = tempfile::tempdir().unwrap();
    // --login: 15 and "already present" are success; text is never read from
    // a wedged command.
    for (reply, ok) in [
        ((Some(0), "", false), true),
        ((Some(15), "", false), true),
        ((Some(1), "session already present", false), true),
        ((None, "already present", true), false),
        (
            (
                Some(1),
                "iscsiadm: initiator reported error (8 - connection timed out)",
                false,
            ),
            false,
        ),
    ] {
        let script = Script::new(vec![reply]);
        let got = initiator(&script, root.path())
            .login("192.0.2.30:3260", "iqn.x:t", &[], None)
            .await;
        assert_eq!(got.is_ok(), ok, "{reply:?}: {got:?}");
        assert_eq!(
            script.calls(),
            ["iscsiadm -m node -T iqn.x:t -p 192.0.2.30:3260 --login"]
        );
    }
    // A session the snapshot shows is not logged in again.
    let script = Script::new(vec![]);
    let listed = [Session {
        portal: "192.0.2.30:3260".into(),
        iqn: "iqn.x:t".into(),
        id: "1".into(),
    }];
    initiator(&script, root.path())
        .login("192.0.2.30", "iqn.x:t", &listed, None)
        .await
        .unwrap();
    assert!(script.calls().is_empty());

    // --logout: 21 and the "not logged in" texts are success; busy is retried
    // three times; the node record is deleted after a logout.
    let busy = (Some(1), "Device or resource busy", false);
    let deleted = (Some(0), "", false);
    for (replies, ok, logouts) in [
        (vec![(Some(0), "", false), deleted], true, 1),
        (vec![(Some(21), "", false), deleted], true, 1),
        (
            vec![(Some(1), "iscsiadm: No matching sessions found", false), deleted],
            true,
            1,
        ),
        (vec![busy, busy, (Some(0), "", false), deleted], true, 3),
        (vec![busy, busy, busy], false, 3),
        (vec![(Some(1), "internal error", false)], false, 1),
        (vec![(None, "No matching sessions", true)], false, 1),
    ] {
        let script = Script::new(replies.clone());
        let got = initiator(&script, root.path())
            .logout("192.0.2.30:3260", "iqn.x:t")
            .await;
        assert_eq!(got.is_ok(), ok, "{replies:?}: {got:?}");
        let calls = script.calls();
        assert_eq!(
            calls.iter().filter(|c| c.ends_with("--logout")).count(),
            logouts,
            "{replies:?}"
        );
        assert_eq!(
            calls
                .iter()
                .any(|c| *c == "iscsiadm -m node -T iqn.x:t -p 192.0.2.30:3260 -o delete"),
            ok,
            "{calls:?}"
        );
    }

    // -m session: 21 is no sessions; another failure is an error.
    let script = Script::new(vec![(Some(21), "iscsiadm: No active sessions.", false)]);
    assert!(
        initiator(&script, root.path())
            .list_sessions(None)
            .await
            .unwrap()
            .is_empty()
    );
    let script = Script::new(vec![(Some(1), "", false)]);
    assert!(initiator(&script, root.path()).list_sessions(None).await.is_err());
    // -o new: a record that exists already is fine.
    let script = Script::new(vec![(Some(6), "iscsiadm: node record already exists", false)]);
    initiator(&script, root.path())
        .ensure_node_record("192.0.2.30:3260", "iqn.x:t", None)
        .await
        .unwrap();
    assert_eq!(
        script.calls(),
        ["iscsiadm -m node -o new -T iqn.x:t -p 192.0.2.30:3260"]
    );
}

/// The two passwords never reach any argv; the method and the user names do,
/// as in the Go node. A failure names the parameter, never iscsiadm's output.
#[tokio::test]
async fn chap_passwords_never_reach_argv() {
    let (dir, root) = fixture_db();
    let creds = Credentials {
        username: "chap-user".into(),
        password: "s3cret-Pass12".into(),
        mutual_username: "target-user".into(),
        mutual_password: "s3cret-Mutual".into(),
        mutual: true,
    };
    let script = Script::new(vec![(Some(0), "", false); 3]);
    let mut iscsi = initiator(&script, dir.path());
    iscsi.node_db_roots = vec![root.clone()];
    iscsi
        .configure_chap("192.0.2.40:3260", FASTPATH, &creds, None)
        .await
        .unwrap();
    let calls = script.calls();
    assert_eq!(
        calls,
        [
            format!(
                "iscsiadm -m node -T {FASTPATH} -p 192.0.2.40:3260 -o update -n node.session.auth.authmethod -v CHAP"
            ),
            format!(
                "iscsiadm -m node -T {FASTPATH} -p 192.0.2.40:3260 -o update -n node.session.auth.username -v chap-user"
            ),
            format!(
                "iscsiadm -m node -T {FASTPATH} -p 192.0.2.40:3260 -o update -n node.session.auth.username_in -v target-user"
            ),
        ]
    );
    assert!(calls.iter().all(|c| !c.contains("s3cret")), "{calls:?}");
    let record = std::fs::read_to_string(root.join(format!("nodes/{FASTPATH}/192.0.2.40,3260"))).unwrap();
    assert!(
        record.contains("node.session.auth.password = s3cret-Pass12\n"),
        "{record}"
    );
    assert!(
        record.contains("node.session.auth.password_in = s3cret-Mutual\n"),
        "{record}"
    );

    // iscsiadm echoes the argv (a wrapper might): the error does not.
    let script = Script::new(vec![
        (Some(0), "", false),
        (Some(7), "echo: -v chap-user s3cret", false),
    ]);
    let mut iscsi = initiator(&script, dir.path());
    iscsi.node_db_roots = vec![root];
    let err = iscsi
        .configure_chap("192.0.2.40:3260", FASTPATH, &creds, None)
        .await
        .unwrap_err();
    assert_eq!(
        format!("{err:#}"),
        "failed to set node param node.session.auth.username (exit status 7)"
    );
}

fn touch(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, "").unwrap();
}

/// A device's session through its sysfs ancestry, as Go reads it: a partition
/// is its disk, a dm map its first iSCSI slave; a disk with no session above
/// it is positively local, an unreadable one unknown.
#[test]
fn device_identity_through_sysfs() {
    let root = tempfile::tempdir().unwrap();
    let iscsi = initiator(&Script::new(vec![]), root.path());
    let sys = root.path().join("sys");
    let dev = root.path().join("dev");
    let link = |name: &str, target: &Path| {
        std::fs::create_dir_all(target).unwrap();
        std::fs::create_dir_all(sys.join("block").join(name)).unwrap();
        std::os::unix::fs::symlink(target, sys.join("block").join(name).join("device")).unwrap();
        touch(&dev.join(name));
    };
    // sdb on session 4; sda a local disk; sdc's partition sdc1.
    link("sdb", &sys.join("devices/platform/host3/session4/target3:0:0/3:0:0:0"));
    std::fs::write(sys.join("block/sdb/device/wwid"), "naa.6589\n").unwrap();
    touch(&sys.join("class/iscsi_session/session4/targetname"));
    std::fs::write(sys.join("class/iscsi_session/session4/targetname"), "iqn.x:t\n").unwrap();
    link("sda", &sys.join("devices/pci0000:00/ata1/host0/target0:0:0/0:0:0:0"));
    std::fs::create_dir_all(sys.join("devices/block/sdc/sdc1")).unwrap();
    touch(&sys.join("devices/block/sdc/sdc1/partition"));
    std::fs::create_dir_all(sys.join("class/block")).unwrap();
    std::os::unix::fs::symlink(sys.join("devices/block/sdc/sdc1"), sys.join("class/block/sdc1")).unwrap();
    link("sdc", &sys.join("devices/platform/host3/session4/target3:0:0/3:0:0:1"));
    touch(&dev.join("sdc1"));
    let listed = [Session {
        portal: "192.0.2.30:3260".into(),
        iqn: "iqn.x:t".into(),
        id: "4".into(),
    }];
    let dev_path = |n: &str| dev.join(n).to_string_lossy().into_owned();
    let found = iscsi.info_from_device(&dev_path("sdb"), &listed).unwrap();
    assert_eq!(found, ("192.0.2.30:3260".to_string(), "iqn.x:t".to_string()));
    assert_eq!(
        iscsi.info_from_device(&dev_path("sdc1"), &listed).unwrap().1,
        "iqn.x:t",
        "a partition is its disk"
    );
    assert!(matches!(
        iscsi.info_from_device(&dev_path("sda"), &listed),
        Err(InfoError::NotIscsi(_))
    ));
    assert!(matches!(
        iscsi.info_from_device(&dev_path("sdz"), &listed),
        Err(InfoError::Unknown(_))
    ));
    assert!(
        matches!(
            iscsi.info_from_device(&dev_path("sdb"), &[]),
            Err(InfoError::Unknown(_))
        ),
        "a session iscsiadm does not list is unknown"
    );

    // dm-0 over sdb (iSCSI), dm-1 over sda (local), dm-2 over an unknown disk.
    for (dm, slaves, uuid) in [
        ("dm-0", vec!["sda", "sdb"], "mpath-36589"),
        ("dm-1", vec!["sda"], "LVM-x"),
        ("dm-2", vec!["sda", "sdq"], "mpath-3x"),
    ] {
        for slave in slaves {
            std::fs::create_dir_all(sys.join("block").join(dm).join("slaves").join(slave)).unwrap();
        }
        touch(&sys.join("block").join(dm).join("dm/uuid"));
        std::fs::write(sys.join("block").join(dm).join("dm/uuid"), uuid).unwrap();
        touch(&dev.join(dm));
    }
    assert_eq!(iscsi.info_from_device(&dev_path("dm-0"), &listed).unwrap().1, "iqn.x:t");
    assert!(matches!(
        iscsi.info_from_device(&dev_path("dm-1"), &listed),
        Err(InfoError::NotIscsi(_))
    ));
    assert!(matches!(
        iscsi.info_from_device(&dev_path("dm-2"), &listed),
        Err(InfoError::Unknown(_))
    ));

    // Multipath identity and ownership.
    std::fs::write(sys.join("block/dm-0/dm/name"), "mpatha\n").unwrap();
    std::fs::create_dir_all(dev.join("mapper")).unwrap();
    std::os::unix::fs::symlink("../dm-0", dev.join("mapper/mpatha")).unwrap();
    assert_eq!(iscsi.scsi_wwid(&dev_path("sdb")).unwrap(), "36589");
    assert_eq!(
        iscsi.find_multipath_device("naa.6589").unwrap(),
        dev_path("mapper/mpatha")
    );
    assert_eq!(iscsi.multipath_wwid(&dev_path("mapper/mpatha")).unwrap(), "36589");
    assert!(
        iscsi.multipath_wwid(&dev_path("sdb")).is_err(),
        "a component path is not a map"
    );
    assert!(
        iscsi.multipath_wwid(&dev_path("dm-1")).is_err(),
        "an LVM volume is not a map"
    );
    assert!(iscsi.check_multipath_ownership(&dev_path("sdb")).is_ok());
    std::fs::create_dir_all(sys.join("block/sdb/holders/dm-0")).unwrap();
    assert!(
        iscsi.check_multipath_ownership(&dev_path("sdb")).is_err(),
        "held by dm-0"
    );
    assert!(iscsi.is_likely_iscsi_device(&dev_path("sdb")) && iscsi.is_likely_iscsi_device(&dev_path("dm-0")));
    for no in ["dm-1", "sda1", "sd", "sdA", "nvme0n1"] {
        assert!(!iscsi.is_likely_iscsi_device(&dev_path(no)), "{no}");
    }
    assert!(iscsi.check_multipath_prerequisites().is_err());
    touch(&dev.join("mapper/control"));
    assert!(iscsi.check_multipath_prerequisites().is_err(), "no multipathd socket");
    touch(&root.path().join("run/multipathd.sock"));
    assert!(iscsi.check_multipath_prerequisites().is_ok());
}

/// A LUN's device is found on the SCSI host of the exact session, never by
/// LUN number alone.
#[test]
fn devices_are_found_by_session() {
    let root = tempfile::tempdir().unwrap();
    let iscsi = initiator(&Script::new(vec![]), root.path());
    let sys = root.path().join("sys");
    let dev = root.path().join("dev");
    for (host, session, disk, iqn) in [(3, 4, "sdb", "iqn.x:a"), (5, 6, "sdc", "iqn.x:b")] {
        std::fs::create_dir_all(sys.join(format!("class/iscsi_host/host{host}/device/session{session}"))).unwrap();
        std::fs::create_dir_all(sys.join(format!("class/scsi_device/{host}:0:0:0/device/block/{disk}"))).unwrap();
        touch(&dev.join(disk));
        touch(&sys.join(format!("class/iscsi_session/session{session}/targetname")));
        std::fs::write(
            sys.join(format!("class/iscsi_session/session{session}/targetname")),
            iqn,
        )
        .unwrap();
    }
    let dev_path = |n: &str| dev.join(n).to_string_lossy().into_owned();
    assert_eq!(iscsi.find_device_for_session("4", 0).unwrap(), dev_path("sdb"));
    assert_eq!(iscsi.find_device_for_session("6", 0).unwrap(), dev_path("sdc"));
    assert!(iscsi.find_device_for_session("6", 1).is_err(), "no LUN 1");
    assert!(iscsi.find_device_for_session("9", 0).is_err());
    assert_eq!(iscsi.find_device_by_iqn("iqn.x:b", 0).unwrap(), dev_path("sdc"));
    assert!(iscsi.find_device_by_iqn("iqn.x:none", 0).is_err());
}
