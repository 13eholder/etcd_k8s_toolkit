use crate::{
    client::ClientBundle,
    config::{
        EtcdLeasePoolSpec, ResourceSpec, RetryPolicy as ConfigRetryPolicy, RuntimeConfig,
        WorkloadSpec,
    },
    heartbeat::{HeartbeatItem, HeartbeatItemKind, NodeHeartbeatPlanner},
    lease::{KeepAliveHandle, LeaseEvent, LeasePool, LeasePoolConfig},
    metrics::{
        self, DropReason, HeartbeatKind, Operation, Outcome, PoolName, ResourceName, Rpc,
        TxnResult, TxnShape,
    },
    pb::etcdserverpb::{CompactionRequest, LeaseGrantRequest, RangeRequest, TxnResponse},
    workload::{
        CasDecision, CasLoop, KeyState, ResourceOperation, RetryPolicy as CasRetryPolicy,
        TxnAttempt, build_compact_revision_txn,
    },
};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::{self, Instant as TokioInstant},
};

#[derive(Debug)]
pub enum WorkItem {
    Txn {
        resource: usize,
        key: Vec<u8>,
        operation: ResourceOperation,
        value: Vec<u8>,
        pool: Option<String>,
    },
    Range(RangeRequest),
    Heartbeat(HeartbeatItem),
    Bootstrap {
        item: HeartbeatItem,
        done: oneshot::Sender<Result<(), String>>,
    },
    Status,
    CompactTick {
        observed_revision: i64,
        previous_revision: Option<i64>,
        expected_version: i64,
    },
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ScheduleStats {
    pub scheduled: u64,
    pub dropped: u64,
}

pub struct Scheduler {
    workload: WorkloadSpec,
    nodes: usize,
    rate_scale: f64,
    time_scale: f64,
    seed: u64,
}
impl Scheduler {
    pub fn new(config: &RuntimeConfig) -> Self {
        Self {
            workload: config.workload.clone(),
            nodes: config.nodes as usize,
            rate_scale: config.rate_scale,
            time_scale: config.time_scale,
            seed: config.seed,
        }
    }
    pub fn duration(&self) -> Duration {
        Duration::from_secs_f64(
            self.workload
                .timeline
                .iter()
                .map(|s| s.duration_s)
                .sum::<f64>()
                * self.time_scale,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    App,
    Range,
    MasterLease { resource: usize, ordinal: u64 },
    NodeLease(usize),
    NodeStatus { node: usize, tick: u64 },
    Status(u64),
    Compact(u64),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Scheduled {
    at: Duration,
    rank: u8,
    sequence: u64,
    source: Source,
}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.rank.cmp(&self.rank))
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct RateSegment {
    offset: Duration,
    duration: Duration,
    count: u64,
    emitted: u64,
}
struct RateCursor {
    segments: Vec<RateSegment>,
    index: usize,
    ordinal: u64,
}
impl RateCursor {
    fn new(
        workload: &WorkloadSpec,
        rate_scale: f64,
        app: bool,
        time_scale: f64,
        seed: u64,
        tag: u64,
    ) -> Self {
        let mut offset = Duration::ZERO;
        let mut segments = Vec::with_capacity(workload.timeline.len());
        for (index, segment) in workload.timeline.iter().enumerate() {
            let duration = Duration::from_secs_f64(segment.duration_s * time_scale);
            let rate = if app {
                segment.application_txn_rps * rate_scale
            } else {
                segment.range_rps
            };
            let expected = rate.max(0.0) * duration.as_secs_f64();
            let base = expected.floor() as u64;
            let count = base
                + u64::from(expected.fract() + deterministic_unit(seed ^ tag, index as u64) >= 1.0);
            segments.push(RateSegment {
                offset,
                duration,
                count,
                emitted: 0,
            });
            offset = offset.saturating_add(duration);
        }
        Self {
            segments,
            index: 0,
            ordinal: 0,
        }
    }
    fn next(&mut self) -> Option<(Duration, u64)> {
        loop {
            let segment = self.segments.get_mut(self.index)?;
            if segment.emitted < segment.count {
                segment.emitted += 1;
                self.ordinal = self.ordinal.wrapping_add(1);
                let at = segment.offset.saturating_add(
                    segment
                        .duration
                        .mul_f64(segment.emitted as f64 / segment.count as f64),
                );
                return Some((at, self.ordinal - 1));
            }
            self.index += 1;
        }
    }
    fn total(&self) -> u64 {
        self.segments.iter().map(|s| s.count).sum()
    }
    fn retarget_total(&mut self, total: u64, seed: u64) {
        let current = self.total();
        if current == total || current == 0 {
            return;
        }
        let exact: Vec<f64> = self
            .segments
            .iter()
            .map(|segment| total as f64 * segment.count as f64 / current as f64)
            .collect();
        let mut assigned = 0u64;
        for (segment, value) in self.segments.iter_mut().zip(&exact) {
            segment.count = value.floor() as u64;
            segment.emitted = 0;
            assigned += segment.count;
        }
        let mut order: Vec<usize> = (0..self.segments.len()).collect();
        order.sort_by(|a, b| {
            exact[*b]
                .fract()
                .partial_cmp(&exact[*a].fract())
                .unwrap_or(Ordering::Equal)
                .then_with(|| splitmix(seed ^ *a as u64).cmp(&splitmix(seed ^ *b as u64)))
        });
        for index in order
            .into_iter()
            .take(total.saturating_sub(assigned) as usize)
        {
            self.segments[index].count += 1;
        }
        self.index = 0;
        self.ordinal = 0;
    }
}

fn application_operation_total(workload: &WorkloadSpec, rate_scale: f64, time_scale: f64) -> u64 {
    let weight: f64 = workload
        .resources
        .iter()
        .filter(|resource| resource.retry_policy != ConfigRetryPolicy::MasterLease)
        .map(|resource| resource.create_weight + resource.update_weight + resource.delete_weight)
        .sum();
    (weight * rate_scale * time_scale).round() as u64
}

struct ResourceQuota {
    spec_index: usize,
    remaining: [u64; 3],
    deficits: [f64; 3],
    total_weight: f64,
    next_id: u64,
    present: Vec<bool>,
    present_cursor: usize,
}
struct LifecycleProducer {
    resources: Vec<ResourceQuota>,
    resource_deficits: Vec<f64>,
    total_weight: f64,
}
impl LifecycleProducer {
    fn new(workload: &WorkloadSpec, total: u64, seed: u64) -> Self {
        let total_weight: f64 = workload
            .resources
            .iter()
            .filter(|resource| resource.retry_policy != ConfigRetryPolicy::MasterLease)
            .map(|r| r.create_weight + r.update_weight + r.delete_weight)
            .sum();
        let mut exact = Vec::with_capacity(workload.resources.len());
        let mut floors = 0u64;
        for spec in &workload.resources {
            if spec.retry_policy == ConfigRetryPolicy::MasterLease {
                exact.push(0.0);
                continue;
            }
            let weight = spec.create_weight + spec.update_weight + spec.delete_weight;
            let value = if total_weight > 0.0 {
                total as f64 * weight / total_weight
            } else {
                0.0
            };
            floors += value.floor() as u64;
            exact.push(value);
        }
        let mut quotas: Vec<u64> = exact.iter().map(|value| value.floor() as u64).collect();
        let mut left = total.saturating_sub(floors);
        let mut order: Vec<usize> = (0..exact.len()).collect();
        order.sort_by(|a, b| {
            exact[*b]
                .fract()
                .partial_cmp(&exact[*a].fract())
                .unwrap_or(Ordering::Equal)
                .then_with(|| splitmix(seed ^ *a as u64).cmp(&splitmix(seed ^ *b as u64)))
        });
        for index in order {
            if left == 0 {
                break;
            }
            quotas[index] += 1;
            left -= 1;
        }
        let mut resources = Vec::with_capacity(workload.resources.len());
        for (index, spec) in workload.resources.iter().enumerate() {
            if spec.retry_policy == ConfigRetryPolicy::MasterLease {
                continue;
            }
            let weight = spec.create_weight + spec.update_weight + spec.delete_weight;
            let quota = quotas[index];
            let op_weights = [spec.create_weight, spec.update_weight, spec.delete_weight];
            let mut remaining = [0; 3];
            let mut assigned = 0;
            for op in 0..3 {
                let value = if weight > 0.0 {
                    quota as f64 * op_weights[op] / weight
                } else {
                    0.0
                };
                remaining[op] = value.floor() as u64;
                assigned += remaining[op];
            }
            let mut op_left = quota.saturating_sub(assigned);
            for op in 0..3 {
                if op_left > 0 && op_weights[op] > 0.0 {
                    remaining[op] += 1;
                    op_left -= 1;
                }
            }
            let create_overflow = remaining[0].saturating_sub(spec.unique_keys);
            remaining[0] -= create_overflow;
            if op_weights[1] > 0.0 {
                remaining[1] += create_overflow;
            } else {
                remaining[2] += create_overflow;
            }
            resources.push(ResourceQuota {
                spec_index: index,
                remaining,
                deficits: [0.0; 3],
                total_weight: weight,
                next_id: 0,
                present: vec![false; spec.unique_keys as usize],
                present_cursor: splitmix(seed ^ index as u64) as usize,
            });
        }
        Self {
            resource_deficits: vec![0.0; resources.len()],
            resources,
            total_weight,
        }
    }
    fn next(&mut self, specs: &[ResourceSpec]) -> Option<(usize, ResourceOperation, u64)> {
        let mut best: Option<(usize, f64)> = None;
        for (index, resource) in self.resources.iter_mut().enumerate() {
            let eligible = (resource.remaining[0] > 0
                && resource.next_id < resource.present.len() as u64)
                || (resource.remaining[1] > 0 && resource.present.iter().any(|p| *p))
                || (resource.remaining[2] > 0 && resource.present.iter().any(|p| *p));
            if !eligible {
                continue;
            }
            self.resource_deficits[index] += resource.total_weight;
            if best.map_or(true, |(_, deficit)| self.resource_deficits[index] > deficit) {
                best = Some((index, self.resource_deficits[index]));
            }
        }
        let (index, _) = best?;
        self.resource_deficits[index] -= self.total_weight;
        let resource = &mut self.resources[index];
        let spec = &specs[resource.spec_index];
        let weights = [spec.create_weight, spec.update_weight, spec.delete_weight];
        for op in 0..3 {
            resource.deficits[op] += weights[op];
        }
        let op = (0..3)
            .filter(|op| {
                resource.remaining[*op] > 0
                    && match *op {
                        0 => resource.next_id < resource.present.len() as u64,
                        _ => resource.present.iter().any(|p| *p),
                    }
            })
            .max_by(|a, b| {
                resource.deficits[*a]
                    .partial_cmp(&resource.deficits[*b])
                    .unwrap_or(Ordering::Equal)
            })?;
        resource.deficits[op] -= resource.total_weight;
        resource.remaining[op] -= 1;
        let id = match op {
            0 => {
                let id = resource.next_id;
                resource.next_id += 1;
                resource.present[id as usize] = true;
                id
            }
            1 => next_present_id(resource)?,
            _ => {
                let id = next_present_id(resource)?;
                resource.present[id as usize] = false;
                id
            }
        };
        Some((
            resource.spec_index,
            match op {
                0 => ResourceOperation::Create,
                1 => ResourceOperation::GuaranteedUpdate,
                _ => ResourceOperation::ConditionalDelete,
            },
            id,
        ))
    }
}
fn next_present_id(resource: &mut ResourceQuota) -> Option<u64> {
    let len = resource.present.len();
    if len == 0 {
        return None;
    }
    let start = resource.present_cursor % len;
    for offset in 0..len {
        let id = (start + offset) % len;
        if resource.present[id] {
            resource.present_cursor = (id + 1) % len;
            return Some(id as u64);
        }
    }
    None
}
struct RangeProducer {
    deficits: Vec<f64>,
    total: f64,
}
impl RangeProducer {
    fn new(weights: impl Iterator<Item = f64>) -> Self {
        let deficits: Vec<f64> = weights.collect();
        Self {
            total: deficits.iter().sum(),
            deficits,
        }
    }
    fn next(&mut self, weights: &[f64]) -> Option<usize> {
        if self.deficits.is_empty() || self.total <= 0.0 {
            return None;
        }
        for (d, w) in self.deficits.iter_mut().zip(weights) {
            *d += *w;
        }
        let index = self
            .deficits
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
            .map(|(i, _)| i)?;
        self.deficits[index] -= self.total;
        Some(index)
    }
}

fn deterministic_unit(seed: u64, n: u64) -> f64 {
    splitmix(seed.wrapping_add(n)) as f64 / u64::MAX as f64
}
fn splitmix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
fn stable_shard(key: &[u8], workers: usize) -> usize {
    if workers == 0 {
        0
    } else {
        key.iter()
            .fold(0u64, |hash, byte| splitmix(hash ^ *byte as u64)) as usize
            % workers
    }
}
fn resource_key(resource: &ResourceSpec, id: u64) -> Vec<u8> {
    resource
        .key_pattern
        .replace("{ns}", "bench")
        .replace("{id}", &format!("{id:05}"))
        .into_bytes()
}
fn resource_value(resource: &ResourceSpec, id: u64, n: u64) -> Vec<u8> {
    let mut value = vec![0; resource.value_size_bytes];
    let marker = splitmix(id ^ n);
    for (i, byte) in value.iter_mut().enumerate() {
        *byte = marker.rotate_left((i % 63) as u32) as u8;
    }
    value
}
fn prefix_end(mut key: Vec<u8>) -> Vec<u8> {
    for i in (0..key.len()).rev() {
        if key[i] != 0xff {
            key[i] += 1;
            key.truncate(i + 1);
            return key;
        }
    }
    vec![0]
}
fn range_request(spec: &crate::config::RangeSpec, n: u64) -> RangeRequest {
    let key = spec
        .key_pattern
        .replace("{ns}", "bench")
        .replace("{id}", &format!("{:05}", n % 1000))
        .into_bytes();
    RangeRequest {
        key: key.clone(),
        range_end: if spec.kind == "prefix" {
            prefix_end(key)
        } else {
            Vec::new()
        },
        limit: spec.limit as i64,
        count_only: spec.count_only,
        serializable: false,
        keys_only: false,
        ..Default::default()
    }
}
fn node_status_phase(node: usize, nodes: usize, interval: Duration) -> Duration {
    interval.mul_f64((node + 1) as f64 / nodes as f64)
}

fn heartbeat_interval(
    seed: u64,
    node: usize,
    tick: u64,
    base: Duration,
    jitter_ratio: f64,
) -> Duration {
    let nanos = base.as_nanos() as u128;
    let jitter = (nanos as f64 * jitter_ratio).round() as u128;
    if jitter == 0 {
        return base;
    }
    let mut z = seed
        .wrapping_add((node as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .wrapping_add(tick.rotate_left(17));
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    let span = jitter.saturating_mul(2).saturating_add(1);
    let offset = (z as u128 % span) as i128 - jitter as i128;
    Duration::from_nanos((nanos as i128 + offset).max(1) as u64)
}
struct SharedPool {
    pool: Mutex<LeasePool>,
    keep_alive: Option<Arc<KeepAliveHandle>>,
}

pub async fn run(config: RuntimeConfig, client: ClientBundle) -> Result<ScheduleStats, String> {
    if config.queue_capacity < config.workers {
        return Err(format!(
            "queue_capacity ({}) must be at least workers ({})",
            config.queue_capacity, config.workers
        ));
    }
    let scheduler = Scheduler::new(&config);
    let queue_capacity = config.queue_capacity.max(1);
    let workers_count = config.workers.max(1);
    let mut senders = Vec::with_capacity(workers_count);
    let mut workers = Vec::with_capacity(workers_count);
    let latest_revision = Arc::new(AtomicI64::new(0));
    let cas_conflicts = Arc::new(AtomicU64::new(config.workload.cas_conflicts));
    let lease_epoch = TokioInstant::now();
    let mut stats = ScheduleStats::default();
    let mut previous_compact_revision = None;
    let pools: Arc<HashMap<String, Arc<SharedPool>>> = Arc::new(
        config
            .workload
            .etcd_lease_pools
            .iter()
            .map(|spec| {
                let pool = lease_pool(spec);
                let pool_name =
                    PoolName::new(spec.name.clone()).expect("validated lease pool label");
                let handle = pool
                    .start_keep_alive(client.lease_client(), pool_name)
                    .map(Arc::new);
                (
                    spec.name.clone(),
                    Arc::new(SharedPool {
                        pool: Mutex::new(pool),
                        keep_alive: handle,
                    }),
                )
            })
            .collect(),
    );
    let base_capacity = queue_capacity / workers_count;
    let extra_capacity = queue_capacity % workers_count;
    for worker_index in 0..workers_count {
        let capacity = base_capacity + if worker_index < extra_capacity { 1 } else { 0 };
        let (tx, rx) = mpsc::channel(capacity.max(1));
        senders.push(tx);
        workers.push(tokio::spawn(worker_loop(
            rx,
            client.clone(),
            config.workload.clone(),
            config.operation_timeout,
            pools.clone(),
            latest_revision.clone(),
            lease_epoch,
            cas_conflicts.clone(),
        )));
    }
    let planner = NodeHeartbeatPlanner::new(crate::heartbeat::HeartbeatConfig {
        nodes: scheduler.nodes,
        lease_interval: Duration::from_secs_f64(
            scheduler.workload.nodes.lease_interval_s * scheduler.time_scale,
        ),
        lease_jitter: scheduler.workload.nodes.lease_jitter,
        status_interval: Duration::from_secs_f64(
            scheduler.workload.nodes.status_interval_s * scheduler.time_scale,
        ),
        seed: scheduler.seed,
    });
    let started = TokioInstant::now();
    let mut ctrl_c = Box::pin(tokio::signal::ctrl_c());
    let mut bootstrap_interrupted = false;
    let mut bootstrap_acks = Vec::with_capacity(scheduler.nodes.saturating_mul(3));
    'bootstrap_send: for item in planner.bootstrap() {
        let shard = stable_shard(item.key.as_bytes(), workers_count);
        let (done_tx, done_rx) = oneshot::channel();
        let resource = if matches!(item.kind, HeartbeatItemKind::NodeLeaseCreate) {
            ResourceName::new("node_leases")
        } else {
            ResourceName::new("nodes")
        };
        let operation = if matches!(
            item.kind,
            HeartbeatItemKind::NodeCreate | HeartbeatItemKind::NodeLeaseCreate
        ) {
            Operation::Create
        } else {
            Operation::Update
        };
        metrics::record_scheduled(operation, &resource.unwrap());
        stats.scheduled += 1;
        let send_result = tokio::select! {
            _ = &mut ctrl_c => {
                bootstrap_interrupted = true;
                break 'bootstrap_send;
            }
            result = senders[shard].send(WorkItem::Bootstrap { item, done: done_tx }) => result,
        };
        send_result.map_err(|_| "bootstrap worker closed".to_string())?;
        bootstrap_acks.push(done_rx);
    }
    if !bootstrap_interrupted {
        'bootstrap_ack: for done_rx in bootstrap_acks {
            let result = tokio::select! {
                _ = &mut ctrl_c => {
                    bootstrap_interrupted = true;
                    break 'bootstrap_ack;
                }
                result = done_rx => result,
            };
            match result {
                Ok(Err(error)) => return Err(format!("bootstrap: {error}")),
                Err(_) => return Err("bootstrap worker closed".to_string()),
                Ok(Ok(())) => {}
            }
        }
    }
    if !bootstrap_allows_main_load(bootstrap_interrupted) {
        drain_workers(senders, workers, pools, config.operation_timeout).await;
        return Ok(stats);
    }
    let duration = scheduler.duration();
    let mut app = RateCursor::new(
        &scheduler.workload,
        scheduler.rate_scale,
        true,
        scheduler.time_scale,
        scheduler.seed,
        0xA1,
    );
    app.retarget_total(
        application_operation_total(
            &scheduler.workload,
            scheduler.rate_scale,
            scheduler.time_scale,
        ),
        scheduler.seed ^ 0xA1,
    );
    let mut ranges = RateCursor::new(
        &scheduler.workload,
        1.0,
        false,
        scheduler.time_scale,
        scheduler.seed,
        0xB2,
    );
    let mut lifecycle = LifecycleProducer::new(&scheduler.workload, app.total(), scheduler.seed);
    let mut range_producer = RangeProducer::new(scheduler.workload.ranges.iter().map(|r| r.weight));
    let weights: Vec<f64> = scheduler.workload.ranges.iter().map(|r| r.weight).collect();
    let mut heap = BinaryHeap::new();
    let mut sequence = 0u64;
    let push = |at: Duration,
                rank: u8,
                source: Source,
                heap: &mut BinaryHeap<Scheduled>,
                sequence: &mut u64| {
        heap.push(Scheduled {
            at,
            rank,
            sequence: *sequence,
            source,
        });
        *sequence = sequence.wrapping_add(1);
    };
    if let Some((at, _)) = app.next() {
        push(at, 1, Source::App, &mut heap, &mut sequence);
    }
    if let Some((at, _)) = ranges.next() {
        push(at, 2, Source::Range, &mut heap, &mut sequence);
    }
    let status_interval =
        Duration::from_secs_f64(scheduler.workload.nodes.status_interval_s * scheduler.time_scale);
    let status_ticks = if status_interval.is_zero() {
        0
    } else {
        duration.as_nanos() / status_interval.as_nanos()
    } as u64;
    for node in 0..scheduler.nodes {
        let lease_at = heartbeat_interval(
            scheduler.seed,
            node,
            0,
            Duration::from_secs_f64(
                scheduler.workload.nodes.lease_interval_s * scheduler.time_scale,
            ),
            scheduler.workload.nodes.lease_jitter,
        );
        if lease_at <= duration {
            push(
                lease_at,
                0,
                Source::NodeLease(node),
                &mut heap,
                &mut sequence,
            );
        }
        if status_ticks > 0 {
            let status_at = node_status_phase(node, scheduler.nodes, status_interval);
            push(
                status_at,
                0,
                Source::NodeStatus { node, tick: 0 },
                &mut heap,
                &mut sequence,
            );
        }
    }
    let master_lease_interval =
        Duration::from_secs_f64(scheduler.workload.master_lease_interval_s * scheduler.time_scale);
    for (resource, spec) in scheduler.workload.resources.iter().enumerate() {
        if spec.retry_policy == ConfigRetryPolicy::MasterLease {
            let count =
                (spec.create_weight + spec.update_weight + spec.delete_weight).round() as u64;
            if count > 0 && master_lease_interval <= duration {
                push(
                    master_lease_interval,
                    0,
                    Source::MasterLease {
                        resource,
                        ordinal: 0,
                    },
                    &mut heap,
                    &mut sequence,
                );
            }
        }
    }
    let status_total = scheduler.workload.status_count.saturating_sub(1);
    if status_total > 0 {
        push(
            duration.mul_f64(1.0 / status_total as f64),
            3,
            Source::Status(1),
            &mut heap,
            &mut sequence,
        );
    }
    let compact_interval =
        Duration::from_secs_f64(scheduler.workload.compact_interval_s * scheduler.time_scale);
    if compact_interval <= duration {
        push(
            compact_interval,
            4,
            Source::Compact(0),
            &mut heap,
            &mut sequence,
        );
    }
    let mut ctrl_c = Box::pin(tokio::signal::ctrl_c());
    while let Some(event) = heap.pop() {
        let deadline = started + event.at;
        tokio::select! { _ = time::sleep_until(deadline) => {}, _ = &mut ctrl_c => { break; } }
        match event.source {
            Source::App => {
                if let Some((at, _)) = app.next() {
                    push(at, 1, Source::App, &mut heap, &mut sequence);
                }
                if let Some((resource, operation, id)) =
                    lifecycle.next(&scheduler.workload.resources)
                {
                    let spec = &scheduler.workload.resources[resource];
                    let key = resource_key(spec, id);
                    let label =
                        ResourceName::try_from(spec.name.as_str()).map_err(|e| e.to_string())?;
                    metrics::record_scheduled(operation_metric(operation), &label);
                    stats.scheduled += 1;
                    let item = WorkItem::Txn {
                        resource,
                        key: key.clone(),
                        operation,
                        value: resource_value(spec, id, event.sequence),
                        pool: spec.lease_pool.clone(),
                    };
                    match senders[stable_shard(&key, workers_count)].try_send(item) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            stats.dropped += 1;
                            metrics::record_dropped(DropReason::Backpressure);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            stats.dropped += 1;
                            metrics::record_dropped(DropReason::QueueClosed);
                        }
                    }
                }
            }
            Source::Range => {
                if let Some((at, _)) = ranges.next() {
                    push(at, 2, Source::Range, &mut heap, &mut sequence);
                }
                if let Some(index) = range_producer.next(&weights) {
                    let request = range_request(&scheduler.workload.ranges[index], event.sequence);
                    metrics::record_scheduled(
                        Operation::Read,
                        &ResourceName::new("range").unwrap(),
                    );
                    stats.scheduled += 1;
                    match senders[stable_shard(&request.key, workers_count)]
                        .try_send(WorkItem::Range(request))
                    {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            stats.dropped += 1;
                            metrics::record_dropped(DropReason::Backpressure);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {}
                    }
                }
            }
            Source::MasterLease { resource, ordinal } => {
                let spec = &scheduler.workload.resources[resource];
                let operation = if ordinal == 0 {
                    ResourceOperation::Create
                } else {
                    ResourceOperation::GuaranteedUpdate
                };
                let key = resource_key(spec, 0);
                let label =
                    ResourceName::try_from(spec.name.as_str()).map_err(|e| e.to_string())?;
                metrics::record_scheduled(operation_metric(operation), &label);
                stats.scheduled += 1;
                let item = WorkItem::Txn {
                    resource,
                    key: key.clone(),
                    operation,
                    value: resource_value(spec, 0, ordinal),
                    pool: spec.lease_pool.clone(),
                };
                match senders[stable_shard(&key, workers_count)].try_send(item) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::Backpressure);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::QueueClosed);
                    }
                }
                let count =
                    (spec.create_weight + spec.update_weight + spec.delete_weight).round() as u64;
                let next_ordinal = ordinal + 1;
                if next_ordinal < count {
                    push(
                        master_lease_interval.mul_f64((next_ordinal + 1) as f64),
                        0,
                        Source::MasterLease {
                            resource,
                            ordinal: next_ordinal,
                        },
                        &mut heap,
                        &mut sequence,
                    );
                }
            }
            Source::NodeLease(node) => {
                let item = HeartbeatItem {
                    node,
                    kind: HeartbeatItemKind::NodeLeaseRenew,
                    key: format!("/registry/leases/kube-node-lease/node-{node:04}"),
                    at: event.at,
                };
                let key = item.key.clone();
                metrics::record_scheduled(
                    Operation::Update,
                    &ResourceName::new("node_leases").unwrap(),
                );
                stats.scheduled += 1;
                match senders[stable_shard(key.as_bytes(), workers_count)]
                    .try_send(WorkItem::Heartbeat(item))
                {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::Backpressure);
                    }
                    Err(_) => {}
                }
                let next = event.at.saturating_add(heartbeat_interval(
                    scheduler.seed,
                    node,
                    event.sequence,
                    Duration::from_secs_f64(
                        scheduler.workload.nodes.lease_interval_s * scheduler.time_scale,
                    ),
                    scheduler.workload.nodes.lease_jitter,
                ));
                if next <= duration {
                    push(next, 0, Source::NodeLease(node), &mut heap, &mut sequence);
                }
            }
            Source::NodeStatus { node, tick } => {
                let item = HeartbeatItem {
                    node,
                    kind: HeartbeatItemKind::NodeStatus,
                    key: format!("/registry/minions/node-{node:04}"),
                    at: event.at,
                };
                let key = item.key.clone();
                metrics::record_scheduled(Operation::Update, &ResourceName::new("nodes").unwrap());
                stats.scheduled += 1;
                match senders[stable_shard(key.as_bytes(), workers_count)]
                    .try_send(WorkItem::Heartbeat(item))
                {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::Backpressure);
                    }
                    Err(_) => {}
                }
                let next_tick = tick + 1;
                if next_tick < status_ticks {
                    push(
                        event.at.saturating_add(status_interval),
                        0,
                        Source::NodeStatus {
                            node,
                            tick: next_tick,
                        },
                        &mut heap,
                        &mut sequence,
                    );
                }
            }
            Source::Status(index) => {
                if index <= status_total {
                    metrics::record_scheduled(
                        Operation::Read,
                        &ResourceName::new("status").unwrap(),
                    );
                    stats.scheduled += 1;
                    let shard = index as usize % workers_count;
                    match senders[shard].try_send(WorkItem::Status) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            stats.dropped += 1;
                            metrics::record_dropped(DropReason::Backpressure);
                        }
                        Err(_) => {}
                    }
                    if index < status_total {
                        push(
                            duration.mul_f64((index + 1) as f64 / status_total as f64),
                            3,
                            Source::Status(index + 1),
                            &mut heap,
                            &mut sequence,
                        );
                    }
                }
            }
            Source::Compact(tick) => {
                let observed = latest_revision.load(AtomicOrdering::Relaxed);
                let previous = previous_compact_revision.replace(observed);
                let compact_key = ResourceName::new("compact_rev_key").unwrap();
                let compact = ResourceName::new("compact").unwrap();
                metrics::record_scheduled(
                    if tick == 0 {
                        Operation::Create
                    } else {
                        Operation::Update
                    },
                    &compact_key,
                );
                stats.scheduled += 1;
                if previous.is_some_and(|revision| revision > 0) {
                    metrics::record_scheduled(Operation::Compact, &compact);
                    stats.scheduled += 1;
                }
                let item = WorkItem::CompactTick {
                    observed_revision: observed,
                    previous_revision: previous.filter(|revision| *revision > 0),
                    expected_version: tick as i64,
                };
                match senders[stable_shard(b"compact_rev_key", workers_count)].try_send(item) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        stats.dropped += 1;
                        metrics::record_dropped(DropReason::Backpressure);
                    }
                    Err(_) => {}
                }
                let next = event.at.saturating_add(compact_interval);
                if next <= duration {
                    push(next, 4, Source::Compact(tick + 1), &mut heap, &mut sequence);
                }
            }
        }
    }
    drain_workers(senders, workers, pools, config.operation_timeout).await;
    Ok(stats)
}
fn bootstrap_allows_main_load(interrupted: bool) -> bool {
    !interrupted
}

fn update_revision(latest_revision: &AtomicI64, revision: i64) {
    let previous = latest_revision.fetch_max(revision, AtomicOrdering::Relaxed);
    metrics::set_revision(previous.max(revision));
}

async fn drain_workers(
    senders: Vec<mpsc::Sender<WorkItem>>,
    workers: Vec<tokio::task::JoinHandle<Result<(), String>>>,
    pools: Arc<HashMap<String, Arc<SharedPool>>>,
    timeout: Duration,
) {
    drop(senders);
    let drain_deadline = TokioInstant::now() + timeout;
    for worker in workers {
        let remaining = drain_deadline.saturating_duration_since(TokioInstant::now());
        let _ = time::timeout(remaining, worker).await;
    }
    if let Ok(pools) = Arc::try_unwrap(pools) {
        for (_, pool) in pools {
            if let Ok(mut pool) = Arc::try_unwrap(pool) {
                if let Some(handle) = pool.keep_alive.take() {
                    if let Ok(handle) = Arc::try_unwrap(handle) {
                        handle.close().await;
                    }
                }
            }
        }
    }
}

fn lease_pool(spec: &EtcdLeasePoolSpec) -> LeasePool {
    LeasePool::new(LeasePoolConfig {
        object_ttl: Duration::from_secs_f64(spec.object_ttl_s),
        reuse_window: Duration::from_secs_f64(spec.reuse_s),
        max_objects: spec.max_objects.min(u32::MAX as u64) as u32,
        keep_alive: spec.keep_alive,
    })
}
async fn worker_loop(
    mut receiver: mpsc::Receiver<WorkItem>,
    client: ClientBundle,
    workload: WorkloadSpec,
    timeout: Duration,
    pools: Arc<HashMap<String, Arc<SharedPool>>>,
    latest_revision: Arc<AtomicI64>,
    lease_epoch: TokioInstant,
    cas_conflicts: Arc<AtomicU64>,
) -> Result<(), String> {
    let mut states = HashMap::new();
    while let Some(item) = receiver.recv().await {
        match item {
            WorkItem::Txn {
                resource,
                key,
                operation,
                value,
                pool,
            } => {
                if let Some(spec) = workload.resources.get(resource) {
                    if let Ok(label) = ResourceName::try_from(spec.name.as_str()) {
                        let operation_started = Instant::now();
                        let lease_id = match pool.as_deref().and_then(|name| pools.get(name)) {
                            Some(shared) => match acquire_lease(
                                shared,
                                &client,
                                lease_epoch,
                                timeout,
                                latest_revision.clone(),
                            )
                            .await
                            {
                                Ok(id) => id,
                                Err(outcome) => {
                                    metrics::record_operation(
                                        operation_metric(operation),
                                        &label,
                                        outcome,
                                    );
                                    continue;
                                }
                            },
                            None => 0,
                        };
                        let remaining_timeout = timeout.saturating_sub(operation_started.elapsed());
                        let inject_conflict = operation == ResourceOperation::GuaranteedUpdate
                            && cas_conflicts
                                .fetch_update(
                                    AtomicOrdering::Relaxed,
                                    AtomicOrdering::Relaxed,
                                    |remaining| remaining.checked_sub(1),
                                )
                                .is_ok();
                        let _ = execute_txn(
                            &client,
                            &label,
                            &mut states,
                            key,
                            value,
                            operation,
                            inject_conflict,
                            retry_policy(spec),
                            remaining_timeout,
                            lease_id,
                            latest_revision.clone(),
                        )
                        .await;
                    }
                }
            }
            WorkItem::Range(request) => {
                let start = Instant::now();
                metrics::add_inflight(Rpc::Range);
                let result = client.range(request.clone()).await;
                metrics::remove_inflight(Rpc::Range);
                metrics::record_request_duration(Rpc::Range, start.elapsed());
                metrics::record_request_bytes(
                    &ResourceName::new("range").unwrap(),
                    request.key.len() + request.range_end.len(),
                );
                let label = ResourceName::new("range").unwrap();
                let outcome = match result {
                    Ok(response) => {
                        if let Some(header) = response.header.as_ref() {
                            update_revision(&latest_revision, header.revision);
                        }
                        metrics::record_request(Rpc::Range, Outcome::Success);
                        Outcome::Success
                    }
                    Err(_) => {
                        metrics::record_request(Rpc::Range, Outcome::GrpcError);
                        Outcome::GrpcError
                    }
                };
                metrics::record_operation(Operation::Read, &label, outcome);
            }
            WorkItem::Bootstrap { item, done } => {
                let result = execute_heartbeat(
                    &client,
                    &mut states,
                    item,
                    timeout,
                    workload.nodes.status_retry_limit,
                    workload.nodes.lease_retry_limit,
                    latest_revision.clone(),
                )
                .await;
                let _ = done.send(result);
            }
            WorkItem::Heartbeat(item) => {
                let _ = execute_heartbeat(
                    &client,
                    &mut states,
                    item,
                    timeout,
                    workload.nodes.status_retry_limit,
                    workload.nodes.lease_retry_limit,
                    latest_revision.clone(),
                )
                .await;
            }
            WorkItem::Status => {
                let start = Instant::now();
                metrics::add_inflight(Rpc::Status);
                let result = client.status().await;
                metrics::remove_inflight(Rpc::Status);
                metrics::record_request_duration(Rpc::Status, start.elapsed());
                metrics::record_request_bytes(&ResourceName::new("status").unwrap(), 0);
                let label = ResourceName::new("status").unwrap();
                let outcome = match result {
                    Ok(response) => {
                        if let Some(header) = response.header.as_ref() {
                            update_revision(&latest_revision, header.revision);
                        }
                        metrics::record_request(Rpc::Status, Outcome::Success);
                        Outcome::Success
                    }
                    Err(_) => {
                        metrics::record_request(Rpc::Status, Outcome::GrpcError);
                        Outcome::GrpcError
                    }
                };
                metrics::record_operation(Operation::Read, &label, outcome);
            }
            WorkItem::CompactTick {
                observed_revision,
                previous_revision,
                expected_version,
            } => {
                execute_compact_tick(
                    &client,
                    observed_revision,
                    previous_revision,
                    expected_version,
                    latest_revision.clone(),
                )
                .await;
            }
        }
    }
    Ok(())
}
async fn execute_compact_tick(
    client: &ClientBundle,
    observed_revision: i64,
    previous_revision: Option<i64>,
    expected_version: i64,
    latest_revision: Arc<AtomicI64>,
) {
    let coordination_revision = previous_revision.unwrap_or(observed_revision);
    let value = coordination_revision.to_string();
    let label = ResourceName::new("compact_rev_key").unwrap();
    let operation = if expected_version == 0 {
        Operation::Create
    } else {
        Operation::Update
    };
    let start = Instant::now();
    metrics::add_inflight(Rpc::Txn);
    let result = client
        .txn(build_compact_revision_txn(
            value.as_bytes(),
            expected_version,
        ))
        .await;
    metrics::remove_inflight(Rpc::Txn);
    metrics::record_request_duration(Rpc::Txn, start.elapsed());
    metrics::record_request_bytes(&label, b"compact_rev_key".len() + value.len());
    let response = match result {
        Ok(response) => {
            metrics::record_request(Rpc::Txn, Outcome::Success);
            response
        }
        Err(_) => {
            metrics::record_request(Rpc::Txn, Outcome::GrpcError);
            metrics::record_operation(operation, &label, Outcome::GrpcError);
            return;
        }
    };
    if let Some(header) = response.header.as_ref() {
        update_revision(&latest_revision, header.revision);
    }
    metrics::record_txn(
        &label,
        TxnShape::CompactRevision,
        if response.succeeded {
            TxnResult::Committed
        } else {
            TxnResult::Conflict
        },
    );
    if !response.succeeded {
        metrics::record_operation(operation, &label, Outcome::GrpcError);
        return;
    }
    metrics::record_operation(operation, &label, Outcome::Success);

    let Some(revision) = previous_revision else {
        return;
    };
    let compact = ResourceName::new("compact").unwrap();
    let start = Instant::now();
    metrics::add_inflight(Rpc::Compact);
    let result = client
        .compact(CompactionRequest {
            revision,
            physical: false,
        })
        .await;
    metrics::remove_inflight(Rpc::Compact);
    metrics::record_request_duration(Rpc::Compact, start.elapsed());
    metrics::record_request_bytes(&compact, 8);
    let outcome = match result {
        Ok(response) => {
            if let Some(header) = response.header.as_ref() {
                update_revision(&latest_revision, header.revision);
            }
            metrics::record_request(Rpc::Compact, Outcome::Success);
            Outcome::Success
        }
        Err(_) => {
            metrics::record_request(Rpc::Compact, Outcome::GrpcError);
            Outcome::GrpcError
        }
    };
    metrics::record_compact(outcome);
    metrics::record_operation(Operation::Compact, &compact, outcome);
}
async fn acquire_lease(
    shared: &SharedPool,
    client: &ClientBundle,
    started: TokioInstant,
    timeout: Duration,
    latest_revision: Arc<AtomicI64>,
) -> Result<i64, Outcome> {
    let now = started.elapsed();
    let deadline = Instant::now() + timeout;
    let mut pool = shared.pool.lock().await;
    let mut id = None;
    let mut failure = None;
    for event in pool.acquire_at(now) {
        match event {
            LeaseEvent::Expire { lease_id } => {
                if let Some(handle) = &shared.keep_alive {
                    handle.unregister(lease_id).await;
                }
            }
            LeaseEvent::Reuse { lease_id } => {
                let active = match &shared.keep_alive {
                    Some(handle) => handle.is_active(lease_id).await,
                    None => true,
                };
                if lease_id == 0 || !active {
                    pool.invalidate(lease_id);
                    failure = Some(Outcome::Timeout);
                } else {
                    id = Some(lease_id);
                }
            }
            LeaseEvent::Grant { ttl } => loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    metrics::record_request(Rpc::LeaseGrant, Outcome::Timeout);
                    failure = Some(Outcome::Timeout);
                    break;
                }
                let start = Instant::now();
                metrics::add_inflight(Rpc::LeaseGrant);
                let response = time::timeout(
                    remaining,
                    client.lease_grant(LeaseGrantRequest {
                        ttl: ttl.as_secs() as i64,
                        id: 0,
                    }),
                )
                .await;
                metrics::remove_inflight(Rpc::LeaseGrant);
                metrics::record_request_duration(Rpc::LeaseGrant, start.elapsed());
                metrics::record_request_bytes(&ResourceName::new("lease").unwrap(), 16);
                match response {
                    Ok(Ok(response)) if response.id != 0 => {
                        id = Some(response.id);
                        pool.record_grant(response.id, now);
                        metrics::record_request(Rpc::LeaseGrant, Outcome::Success);
                        if let Some(header) = response.header {
                            update_revision(&latest_revision, header.revision);
                        }
                        if let Some(handle) = &shared.keep_alive {
                            handle.register(response.id, ttl).await;
                        }
                        break;
                    }
                    Ok(Ok(_)) => {
                        metrics::record_request(Rpc::LeaseGrant, Outcome::GrpcError);
                        failure = Some(Outcome::GrpcError);
                        break;
                    }
                    Ok(Err(error)) => {
                        metrics::record_request(Rpc::LeaseGrant, Outcome::GrpcError);
                        failure = Some(Outcome::GrpcError);
                        if !error.is_retriable() {
                            break;
                        }
                        let delay = Duration::from_millis(10)
                            .min(deadline.saturating_duration_since(Instant::now()));
                        if delay.is_zero() {
                            break;
                        }
                        time::sleep(delay).await;
                    }
                    Err(_) => {
                        metrics::record_request(Rpc::LeaseGrant, Outcome::Timeout);
                        failure = Some(Outcome::Timeout);
                        break;
                    }
                }
            },
        }
    }
    id.ok_or_else(|| failure.unwrap_or(Outcome::GrpcError))
}
fn retry_policy(spec: &ResourceSpec) -> CasRetryPolicy {
    match spec.retry_policy {
        ConfigRetryPolicy::CreateOnly => CasRetryPolicy {
            max_attempts: 1,
            delay: Duration::ZERO,
            jitter_ratio: 0.0,
        },
        ConfigRetryPolicy::GuaranteedUpdate
        | ConfigRetryPolicy::ConditionalDelete
        | ConfigRetryPolicy::MasterLease => CasRetryPolicy::generic_controller(),
        ConfigRetryPolicy::Event => CasRetryPolicy::event(),
    }
}
fn operation_metric(op: ResourceOperation) -> Operation {
    match op {
        ResourceOperation::Create => Operation::Create,
        ResourceOperation::GuaranteedUpdate => Operation::Update,
        ResourceOperation::ConditionalDelete => Operation::Delete,
    }
}
fn shape(op: ResourceOperation) -> TxnShape {
    match op {
        ResourceOperation::Create => TxnShape::Create,
        ResourceOperation::GuaranteedUpdate => TxnShape::Update,
        ResourceOperation::ConditionalDelete => TxnShape::Delete,
    }
}

async fn execute_txn(
    client: &ClientBundle,
    label: &ResourceName,
    states: &mut HashMap<Vec<u8>, KeyState>,
    key: Vec<u8>,
    value: Vec<u8>,
    operation: ResourceOperation,
    mut inject_conflict: bool,
    policy: CasRetryPolicy,
    timeout: Duration,
    lease_id: i64,
    latest_revision: Arc<AtomicI64>,
) -> Outcome {
    let deadline = Instant::now() + timeout;
    let mut outer_attempt = 0u32;
    let mut current_value = value;
    let mut state = states.get(&key).cloned().unwrap_or_default();
    if !matches!(operation, ResourceOperation::Create) && state.mod_revision().is_none() {
        metrics::record_txn(label, shape(operation), TxnResult::Conflict);
        metrics::record_operation(operation_metric(operation), label, Outcome::GrpcError);
        return Outcome::GrpcError;
    }
    loop {
        if Instant::now() >= deadline {
            metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
            return Outcome::Timeout;
        }
        outer_attempt = outer_attempt.saturating_add(1);
        let mut attempt = match operation {
            ResourceOperation::Create => TxnAttempt::create(key.clone(), current_value.clone()),
            ResourceOperation::GuaranteedUpdate => {
                let revision = state.mod_revision().unwrap_or(0);
                let expected_revision = if inject_conflict {
                    inject_conflict = false;
                    revision.saturating_sub(1)
                } else {
                    revision
                };
                TxnAttempt::update(key.clone(), current_value.clone(), expected_revision)
            }
            ResourceOperation::ConditionalDelete => {
                TxnAttempt::delete(key.clone(), state.mod_revision().unwrap_or(0))
            }
        };
        let mut cas = CasLoop::new(
            attempt.clone(),
            CasRetryPolicy::generic_controller(),
            deadline,
        );
        loop {
            let request = attempt.request_with_lease(lease_id);
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
                return Outcome::Timeout;
            }
            let start = Instant::now();
            metrics::add_inflight(Rpc::Txn);
            let response = time::timeout(remaining, client.txn(request)).await;
            metrics::remove_inflight(Rpc::Txn);
            metrics::record_request_duration(Rpc::Txn, start.elapsed());
            metrics::record_request_bytes(label, current_value.len() + key.len());
            let response: TxnResponse = match response {
                Ok(Ok(response)) => {
                    if let Some(header) = response.header.as_ref() {
                        update_revision(&latest_revision, header.revision);
                    }
                    metrics::record_request(Rpc::Txn, Outcome::Success);
                    response
                }
                Err(_) => {
                    metrics::record_request(Rpc::Txn, Outcome::Timeout);
                    metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
                    return Outcome::Timeout;
                }
                Ok(Err(error)) => {
                    metrics::record_request(Rpc::Txn, Outcome::GrpcError);
                    if !error.is_retriable() || outer_attempt >= policy.max_attempts {
                        metrics::record_operation(
                            operation_metric(operation),
                            label,
                            Outcome::GrpcError,
                        );
                        return Outcome::GrpcError;
                    }
                    let delay =
                        policy.delay_for(stable_shard(&key, usize::MAX) as u64, outer_attempt);
                    time::sleep(delay.min(deadline.saturating_duration_since(Instant::now())))
                        .await;
                    break;
                }
            };
            metrics::record_txn(
                label,
                shape(operation),
                if response.succeeded {
                    TxnResult::Committed
                } else {
                    TxnResult::Conflict
                },
            );
            match cas.observe(&response, Instant::now()) {
                CasDecision::Succeeded { state: next, .. } => {
                    if operation == ResourceOperation::ConditionalDelete {
                        states.remove(&key);
                    } else {
                        states.insert(key.clone(), next);
                    }
                    metrics::record_operation(operation_metric(operation), label, Outcome::Success);
                    return Outcome::Success;
                }
                CasDecision::CreateCollision { latest, .. } => {
                    states.insert(key.clone(), latest);
                    metrics::record_operation(
                        operation_metric(operation),
                        label,
                        Outcome::GrpcError,
                    );
                    return Outcome::GrpcError;
                }
                CasDecision::Retry { latest, .. } => {
                    metrics::record_cas_retry(label, operation_metric(operation));
                    state = latest.clone();
                    current_value = if operation == ResourceOperation::ConditionalDelete {
                        Vec::new()
                    } else {
                        resource_value_for_retry(&current_value, attempt.number)
                    };
                    let Some(next) = cas.retry_with_value(current_value.clone(), &latest) else {
                        metrics::record_operation(
                            operation_metric(operation),
                            label,
                            Outcome::GrpcError,
                        );
                        return Outcome::GrpcError;
                    };
                    attempt = next.clone();
                }
                CasDecision::Exhausted { .. } => {
                    metrics::record_operation(operation_metric(operation), label, Outcome::Timeout);
                    return Outcome::Timeout;
                }
            }
        }
    }
}
fn resource_value_for_retry(value: &[u8], attempt: u32) -> Vec<u8> {
    let mut out = value.to_vec();
    if let Some(first) = out.first_mut() {
        *first = first.wrapping_add(attempt as u8);
    }
    out
}
async fn execute_heartbeat(
    client: &ClientBundle,
    states: &mut HashMap<Vec<u8>, KeyState>,
    item: HeartbeatItem,
    timeout: Duration,
    status_retry_limit: u32,
    lease_retry_limit: u32,
    latest_revision: Arc<AtomicI64>,
) -> Result<(), String> {
    let (resource, kind) = match item.kind {
        HeartbeatItemKind::NodeCreate
        | HeartbeatItemKind::NodeReady
        | HeartbeatItemKind::NodeStatus => (
            "nodes",
            if matches!(item.kind, HeartbeatItemKind::NodeStatus) {
                HeartbeatKind::NodeStatus
            } else {
                HeartbeatKind::Node
            },
        ),
        HeartbeatItemKind::NodeLeaseCreate | HeartbeatItemKind::NodeLeaseRenew => {
            ("node_leases", HeartbeatKind::NodeLease)
        }
    };
    let operation = if matches!(
        item.kind,
        HeartbeatItemKind::NodeCreate | HeartbeatItemKind::NodeLeaseCreate
    ) {
        ResourceOperation::Create
    } else {
        ResourceOperation::GuaranteedUpdate
    };
    let spec = ResourceSpec {
        name: resource.into(),
        key_pattern: item.key.clone(),
        unique_keys: 1,
        create_weight: 1.0,
        update_weight: 1.0,
        delete_weight: 0.0,
        value_size_bytes: if resource == "nodes" { 2048 } else { 256 },
        retry_policy: ConfigRetryPolicy::GuaranteedUpdate,
        lease_pool: None,
    };
    let label = ResourceName::try_from(resource).map_err(|e| e.to_string())?;
    let mut policy = if matches!(item.kind, HeartbeatItemKind::NodeStatus) {
        CasRetryPolicy::node_status()
    } else if matches!(item.kind, HeartbeatItemKind::NodeLeaseRenew) {
        CasRetryPolicy::node_lease()
    } else {
        CasRetryPolicy {
            max_attempts: 1,
            delay: Duration::ZERO,
            jitter_ratio: 0.0,
        }
    };
    if matches!(item.kind, HeartbeatItemKind::NodeStatus) {
        policy.max_attempts = status_retry_limit.max(1);
    } else if matches!(item.kind, HeartbeatItemKind::NodeLeaseRenew) {
        policy.max_attempts = lease_retry_limit.max(1);
    }
    let outcome = execute_txn(
        client,
        &label,
        states,
        item.key.into_bytes(),
        resource_value(&spec, item.node as u64, item.at.as_nanos() as u64),
        operation,
        false,
        policy,
        timeout,
        0,
        latest_revision,
    )
    .await;
    metrics::record_node_heartbeat(kind, outcome);
    if outcome != Outcome::Success {
        return Err(format!("heartbeat {kind:?} failed: {outcome:?}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lazy_cursor_has_bounded_state() {
        let workload = WorkloadSpec {
            name: "x".into(),
            pod_size_bytes: 4096,
            nodes: crate::config::NodeSpec {
                count: 1,
                status_interval_s: 300.0,
                lease_interval_s: 10.0,
                lease_jitter: 0.0,
                status_retry_limit: 5,
                lease_retry_limit: 5,
            },
            timeline: vec![crate::config::TimelineSegment {
                duration_s: 10.0,
                application_txn_rps: 1000.0,
                range_rps: 0.0,
            }],
            resources: vec![ResourceSpec {
                name: "pods".into(),
                key_pattern: "/p/{id}".into(),
                unique_keys: 10000,
                create_weight: 1.0,
                update_weight: 1.0,
                delete_weight: 0.0,
                value_size_bytes: 4096,
                retry_policy: ConfigRetryPolicy::GuaranteedUpdate,
                lease_pool: None,
            }],
            ranges: vec![],
            etcd_lease_pools: vec![],
            status_count: 0,
            compact_interval_s: 300.0,
            master_lease_interval_s: 10.0,
            cas_conflicts: 0,
        };
        let cursor = RateCursor::new(&workload, 1.0, true, 1.0, 1, 1);
        assert_eq!(cursor.total(), 10000);
    }
    #[test]
    fn stable_routing_is_repeatable() {
        assert_eq!(stable_shard(b"/k", 7), stable_shard(b"/k", 7));
    }
    #[test]
    fn prefix_end_handles_ff() {
        assert_eq!(prefix_end(vec![0xff]), vec![0]);
        assert_eq!(prefix_end(vec![b'a', 0xff]), vec![b'b']);
    }
    #[test]
    fn present_keys_rotate_deterministically() {
        let mut resource = ResourceQuota {
            spec_index: 0,
            remaining: [0, 0, 0],
            deficits: [0.0; 3],
            total_weight: 0.0,
            next_id: 3,
            present: vec![true, true, true],
            present_cursor: 0,
        };
        assert_eq!(next_present_id(&mut resource), Some(0));
        assert_eq!(next_present_id(&mut resource), Some(1));
        assert_eq!(next_present_id(&mut resource), Some(2));
        assert_eq!(next_present_id(&mut resource), Some(0));
    }
    #[test]
    fn embedded_ranges_scan_resource_prefixes() {
        let workload = WorkloadSpec::from_json(include_str!("../workloads/density.json")).unwrap();
        let pods = workload
            .ranges
            .iter()
            .find(|range| range.key_pattern == "/registry/pods/bench-{ns}/")
            .unwrap();
        let request = range_request(pods, 42);
        assert_eq!(request.key, b"/registry/pods/bench-bench/");
        assert_eq!(request.range_end, prefix_end(request.key.clone()));

        let minions = workload
            .ranges
            .iter()
            .find(|range| range.key_pattern == "/registry/minions/")
            .unwrap();
        let request = range_request(minions, 42);
        assert_eq!(request.key, b"/registry/minions/");
        assert_eq!(request.range_end, prefix_end(request.key.clone()));
    }

    #[test]
    fn application_quotas_are_exact_and_exclude_masterlease() {
        let workload = WorkloadSpec::from_json(include_str!("../workloads/density.json")).unwrap();
        let total = application_operation_total(&workload, 1.0, 1.0);
        assert_eq!(total, 514_085);
        assert_eq!(application_operation_total(&workload, 1.0, 0.001), 514);
        let producer = LifecycleProducer::new(&workload, total, 1);
        assert!(producer.resources.iter().all(|quota| {
            workload.resources[quota.spec_index].retry_policy != ConfigRetryPolicy::MasterLease
        }));
        assert_eq!(
            producer
                .resources
                .iter()
                .flat_map(|quota| quota.remaining)
                .sum::<u64>(),
            total
        );
        assert!(producer.resources.iter().all(|quota| {
            quota.remaining[0] <= workload.resources[quota.spec_index].unique_keys
        }));
    }

    #[test]
    fn node_status_phases_are_uniform_and_repeatable() {
        let interval = Duration::from_secs(300);
        let phases: Vec<_> = (0..4)
            .map(|node| node_status_phase(node, 4, interval))
            .collect();
        assert_eq!(
            phases,
            vec![
                Duration::from_secs(75),
                Duration::from_secs(150),
                Duration::from_secs(225),
                Duration::from_secs(300),
            ]
        );
    }

    #[test]
    fn interrupted_bootstrap_blocks_main_load() {
        assert!(!bootstrap_allows_main_load(true));
        assert!(bootstrap_allows_main_load(false));
    }
}
