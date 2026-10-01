//! Kubernetes Events written to the API, the way the Go node's client-go
//! recorder writes them (`record.NewBroadcaster` in `pkg/driver/events.go`):
//! the same Event object (name, namespace, involved object, source, reporting
//! controller and instance), a repeat of an identical event patches the
//! existing Event's count instead of creating another, and each object gets at
//! most a burst of 25 events, then one every 5 minutes.
//!
//! Recording never blocks or fails a CSI RPC: `warning` puts the event on a
//! bounded queue and returns; one task writes the queue to the API. An event
//! that does not fit on the queue, is rate limited, or that the API refuses is
//! dropped and counted in `scale_csi_events_dropped_total`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::{debug, warn};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::events::{Events, LogEvents, ObjectRef};
use crate::kube_api::{self, ApiError, EventApi};
use crate::metrics::Metrics;

/// Events waiting for the API; more are dropped.
pub const QUEUE_CAPACITY: usize = 256;
/// An identical event within this long of the last one patches its count.
pub const AGGREGATE_WINDOW: Duration = Duration::from_secs(600);
/// Events and objects remembered for aggregation and rate limiting
/// (client-go's LRU size).
pub const CACHE_ENTRIES: usize = 4096;
/// Events an object may get at once (client-go's spam filter burst) ...
pub const SPAM_BURST: f64 = 25.0;
/// ... and how often it gets one more (client-go: 1/300 per second).
pub const SPAM_REFILL: Duration = Duration::from_secs(300);

pub const EVENT_TYPE_WARNING: &str = "Warning";

/// An object name's longest (a DNS subdomain).
const MAX_NAME_LENGTH: usize = 253;

pub const DROP_QUEUE_FULL: &str = "queue_full";
pub const DROP_RATE_LIMITED: &str = "rate_limited";
pub const DROP_API_ERROR: &str = "api_error";
pub const DROP_CLOSED: &str = "closed";

/// Who records the events: the driver name and the host, as the Go node's
/// corev1.EventSource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub component: String,
    pub host: String,
}

/// One event as recorded, before it is written.
#[derive(Debug, Clone)]
pub struct Pending {
    pub object: ObjectRef,
    pub event_type: &'static str,
    pub reason: String,
    pub message: String,
    /// Wall clock time, for the Event's name and timestamps.
    pub at: SystemTime,
    /// Monotonic time, for the aggregation window and the rate limit.
    pub seen: Instant,
}

impl Pending {
    pub fn warning(object: &ObjectRef, reason: &str, message: &str) -> Self {
        Pending {
            object: object.clone(),
            event_type: EVENT_TYPE_WARNING,
            reason: reason.into(),
            message: message.into(),
            at: SystemTime::now(),
            seen: Instant::now(),
        }
    }
}

/// The namespace an Event about the object goes to: the object's, else
/// "default" (client-go makeEvent).
pub fn event_namespace(object: &ObjectRef) -> &str {
    match object.namespace() {
        "" => "default",
        namespace => namespace,
    }
}

/// client-go's GenerateEventName: the object's name and the time in hex
/// nanoseconds; where that is not a valid object name (too long, or the
/// object's name has characters a name may not, like a static PV's volume
/// handle), client-go takes a UUID, the node a hash of it (64 hex digits).
pub fn event_name(object: &ObjectRef, at: SystemTime) -> String {
    use sha2::{Digest, Sha256};
    let nanos = at.duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or_default();
    let name = format!("{}.{:x}", object.name(), nanos);
    if name.len() <= MAX_NAME_LENGTH && crate::kube_api::is_dns1123_subdomain(&name) {
        return name;
    }
    Sha256::digest(name.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// metav1.Time's JSON form: RFC 3339 in UTC, whole seconds.
pub fn rfc3339(at: SystemTime) -> String {
    let secs = at.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or_default();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Howard Hinnant's civil_from_days.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// The core/v1 Event client-go's recorder creates for the event.
pub fn event_object(source: &Source, event: &Pending, name: &str, count: u32, first: SystemTime) -> Value {
    let object = &event.object;
    let mut involved = json!({
        "kind": object.kind(),
        "name": object.name(),
        "apiVersion": object.api_version(),
    });
    if !object.namespace().is_empty() {
        involved["namespace"] = json!(object.namespace());
    }
    json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {
            "name": name,
            "namespace": event_namespace(object),
        },
        "involvedObject": involved,
        "reason": event.reason,
        "message": event.message,
        "source": {
            "component": source.component,
            "host": source.host,
        },
        "firstTimestamp": rfc3339(first),
        "lastTimestamp": rfc3339(event.at),
        "count": count,
        "type": event.event_type,
        "reportingComponent": source.component,
        "reportingInstance": source.host,
    })
}

/// The patch client-go sends for a repeat: the new count, last timestamp and
/// message.
pub fn repeat_patch(event: &Pending, count: u32) -> Value {
    json!({
        "count": count,
        "lastTimestamp": rfc3339(event.at),
        "message": event.message,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EventKey {
    object: ObjectRef,
    event_type: &'static str,
    reason: String,
    message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ObjectKey {
    object: ObjectRef,
    event_type: &'static str,
}

#[derive(Debug, Clone)]
struct Written {
    name: String,
    count: u32,
    first: SystemTime,
    last_seen: Instant,
}

#[derive(Debug, Clone)]
struct Bucket {
    tokens: f64,
    refilled: Instant,
}

/// What to write for an event.
#[derive(Debug, Clone, PartialEq)]
pub struct Write {
    pub namespace: String,
    /// The Event's name.
    pub name: String,
    /// Its count once written.
    pub count: u32,
    /// When the first of its events happened.
    pub first: SystemTime,
    pub op: Op,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    /// A new Event object.
    Create(Value),
    /// A repeat: this patch, or if the Event is gone, the whole Event again.
    Patch { patch: Value, recreate: Value },
}

/// client-go's EventCorrelator, the parts the node needs: the spam filter
/// and the identical-event count.
pub struct Correlator {
    source: Source,
    written: HashMap<EventKey, Written>,
    buckets: HashMap<ObjectKey, Bucket>,
}

impl Correlator {
    pub fn new(source: Source) -> Self {
        Correlator {
            source,
            written: HashMap::new(),
            buckets: HashMap::new(),
        }
    }

    /// Takes one of the object's tokens; false when it has none left.
    fn allow(&mut self, event: &Pending) -> bool {
        let key = ObjectKey {
            object: event.object.clone(),
            event_type: event.event_type,
        };
        if !self.buckets.contains_key(&key) && self.buckets.len() >= CACHE_ENTRIES {
            evict_oldest(&mut self.buckets, |b| b.refilled);
        }
        let bucket = self.buckets.entry(key).or_insert(Bucket {
            tokens: SPAM_BURST,
            refilled: event.seen,
        });
        let elapsed = event.seen.saturating_duration_since(bucket.refilled);
        let refill = elapsed.as_secs_f64() / SPAM_REFILL.as_secs_f64();
        if refill > 0.0 {
            bucket.tokens = (bucket.tokens + refill).min(SPAM_BURST);
            bucket.refilled = event.seen;
        }
        if bucket.tokens < 1.0 {
            return false;
        }
        bucket.tokens -= 1.0;
        true
    }

    /// What to write for the event, or None when the object is rate limited.
    pub fn plan(&mut self, event: &Pending) -> Option<Write> {
        if !self.allow(event) {
            return None;
        }
        let namespace = event_namespace(&event.object).to_string();
        match self.written.get(&key_of(event)) {
            Some(last) if event.seen.saturating_duration_since(last.last_seen) <= AGGREGATE_WINDOW => {
                let count = last.count + 1;
                Some(Write {
                    namespace,
                    name: last.name.clone(),
                    count,
                    first: last.first,
                    op: Op::Patch {
                        patch: repeat_patch(event, count),
                        recreate: event_object(&self.source, event, &last.name, count, last.first),
                    },
                })
            }
            _ => {
                let name = event_name(&event.object, event.at);
                let body = event_object(&self.source, event, &name, 1, event.at);
                Some(Write {
                    namespace,
                    name,
                    count: 1,
                    first: event.at,
                    op: Op::Create(body),
                })
            }
        }
    }

    /// The API took the write: later repeats count from it.
    pub fn record(&mut self, event: &Pending, write: &Write) {
        let key = key_of(event);
        if !self.written.contains_key(&key) && self.written.len() >= CACHE_ENTRIES {
            evict_oldest(&mut self.written, |w| w.last_seen);
        }
        self.written.insert(
            key,
            Written {
                name: write.name.clone(),
                count: write.count,
                first: write.first,
                last_seen: event.seen,
            },
        );
    }
}

fn key_of(event: &Pending) -> EventKey {
    EventKey {
        object: event.object.clone(),
        event_type: event.event_type,
        reason: event.reason.clone(),
        message: event.message.clone(),
    }
}

fn evict_oldest<K: Clone + Eq + std::hash::Hash, V>(map: &mut HashMap<K, V>, at: impl Fn(&V) -> Instant) {
    if let Some(oldest) = map.iter().min_by_key(|(_, v)| at(v)).map(|(k, _)| k.clone()) {
        map.remove(&oldest);
    }
}

/// Writes one event; Err names why it was dropped.
pub async fn write<A: EventApi>(api: &A, correlator: &mut Correlator, event: &Pending) -> Result<(), &'static str> {
    let Some(planned) = correlator.plan(event) else {
        return Err(DROP_RATE_LIMITED);
    };
    let namespace = &planned.namespace;
    let result = match &planned.op {
        Op::Create(body) => api.create(namespace, body.to_string().into_bytes()).await,
        Op::Patch { patch, recreate } => match api
            .patch(namespace, &planned.name, patch.to_string().into_bytes())
            .await
        {
            // Gone (an Event lives an hour): create it again, as client-go does.
            Err(ApiError::NotFound) => api.create(namespace, recreate.to_string().into_bytes()).await,
            other => other,
        },
    };
    match result {
        Ok(()) => {
            correlator.record(event, &planned);
            Ok(())
        }
        Err(e) => {
            warn!(
                "event {} {} on {} not written: {e}",
                event.event_type, event.reason, event.object
            );
            Err(DROP_API_ERROR)
        }
    }
}

/// Writes the queue to the API until every sender is gone.
pub async fn run<A: EventApi>(mut queue: mpsc::Receiver<Pending>, api: A, source: Source, metrics: Arc<Metrics>) {
    let mut correlator = Correlator::new(source);
    while let Some(event) = queue.recv().await {
        if let Err(reason) = write(&api, &mut correlator, &event).await {
            metrics.record_event_dropped(reason);
        }
    }
}

/// The sink in a cluster: queues events for `run`.
pub struct KubeEvents {
    queue: mpsc::Sender<Pending>,
    metrics: Arc<Metrics>,
    dropped: AtomicU64,
}

impl KubeEvents {
    /// The sink and the queue's receiving end.
    pub fn queue(capacity: usize, metrics: Arc<Metrics>) -> (Self, mpsc::Receiver<Pending>) {
        let (queue, rx) = mpsc::channel(capacity);
        (
            KubeEvents {
                queue,
                metrics,
                dropped: AtomicU64::new(0),
            },
            rx,
        )
    }
}

impl Events for KubeEvents {
    fn warning(&self, object: &ObjectRef, reason: &str, message: &str) {
        debug!("event Warning {reason} on {object}: {message}");
        let reason_dropped = match self.queue.try_send(Pending::warning(object, reason, message)) {
            Ok(()) => return,
            Err(mpsc::error::TrySendError::Full(_)) => DROP_QUEUE_FULL,
            Err(mpsc::error::TrySendError::Closed(_)) => DROP_CLOSED,
        };
        self.metrics.record_event_dropped(reason_dropped);
        // The first drop and then every power of two, not one line per event.
        let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        if n.is_power_of_two() {
            warn!("event Warning {reason} on {object} dropped ({reason_dropped}, {n} so far): {message}");
        }
    }
}

/// Which sink `sink` chose.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkKind {
    Log,
    Kubernetes,
}

/// The node's event sink: the API when the agent runs in a cluster (a service
/// account token and KUBERNETES_SERVICE_HOST/PORT), else the log. Must be
/// called on the runtime, which runs the writer.
pub fn sink(
    sa_dir: &Path,
    host: Option<String>,
    port: Option<String>,
    source: Source,
    metrics: Arc<Metrics>,
) -> (Arc<dyn Events>, SinkKind) {
    match kube_api::InCluster::from_env(sa_dir, host, port) {
        Ok(Some(cluster)) => {
            let (events, queue) = KubeEvents::queue(QUEUE_CAPACITY, metrics.clone());
            tokio::spawn(run(queue, kube_api::Client::new(cluster), source, metrics));
            (Arc::new(events), SinkKind::Kubernetes)
        }
        Ok(None) => {
            debug!("not in a Kubernetes cluster: events go to the log only");
            (Arc::new(LogEvents), SinkKind::Log)
        }
        Err(e) => {
            warn!("no Kubernetes API for events ({e:#}): events go to the log only");
            (Arc::new(LogEvents), SinkKind::Log)
        }
    }
}
