use serde::Serialize;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct RunTargets {
    pub bootstrap: u64,
    pub application: ApplicationTargets,
    pub range_requests: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ApplicationTargets {
    pub creates: u64,
    pub updates: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AccountingInvariants {
    /// `offered == accepted + dropped + unissued`.
    pub offered_equals_accepted_plus_dropped_plus_unissued: bool,
    /// `accepted == completed + unfinished`.
    pub accepted_equals_completed_plus_unfinished: bool,
    /// Expanded form of the completed equation.
    pub accepted_equals_succeeded_plus_failed_plus_unfinished: bool,
    /// `result_unknown` is a subset of `failed`.
    pub result_unknown_subset_failed: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkItemReport {
    pub work_class: String,
    pub target: Option<u64>,
    /// Materialized dispatch attempts for this class.
    pub offered: u64,
    /// Items successfully put into a worker queue.
    pub accepted: u64,
    /// `succeeded + failed`; unknown results are failures.
    pub completed: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub result_unknown: u64,
    /// `dropped_full + dropped_closed`.
    pub dropped: u64,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    /// Accepted items whose execution was canceled or lost before a result.
    pub unfinished: u64,
    /// Offered dispatches canceled before acceptance or drop classification.
    pub unissued: u64,
    /// `target - succeeded` when a finite target exists.
    pub target_deficit: Option<u64>,
    /// First bounded diagnostic for this fixed class.
    pub first_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunReport {
    pub achieved: bool,
    pub interrupted: bool,
    pub schedule_complete: bool,
    /// The scheduler report intentionally excludes config/connect/status/preflight.
    pub scope: &'static str,
    pub termination_error: Option<String>,
    pub targets: RunTargets,
    pub work_items: Vec<WorkItemReport>,
    pub accounting_invariants: AccountingInvariants,
}
