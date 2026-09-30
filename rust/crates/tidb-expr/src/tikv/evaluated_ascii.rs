// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Closed ready-argument caller sharing one scoped pool across its fixed operations.
//! Legacy public ASCII capabilities retain their names and ASCII-only value API.
//!
//! The real C4 worker is the only computation path. Native children/transcode
//! precede this value boundary; original return coercion follows it. Public
//! native-only capabilities bind a scope without installing statement lifetimes
//! or propagating through existing business-context wrappers automatically.
//!
//! PINNED ACCOUNTING CONTRACT: PoolArcAllocation requires an independent
//! caller-specific allocation-request receipt for the exact payload/compiler.
//! A config-Arc receipt or this proxy's own size does not measure PoolCore.
//! Revalidate that external basis after layout/toolchain changes; the ledger
//! is conditional accounting, NOT a portable or whole-process byte cap.
//! Creation reservations also do not measure factory transient high water.
//! The pool owns its control block, slot slab and boxed workers. Caller-owned
//! handle/scope storage and native coercion allocations are outside this ledger;
//! ready input/driver temporaries belong to C4's separate per-call allowance.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tidb_datatype::tikv_compat::value::{from_scalar, BridgeError, ValueMetadata};
use tidb_datatype::{Datum, DatumKind, Time};
use tidb_query_datatype::{codec::data_type::ScalarValueRef, EvalType};
use tidb_query_expr::local::{
    prepare_evaluated_bytes, CompileLimits, ComputedBytesMetadata, ComputedIeee754BitsMetadata,
    ComputedInt, ComputedIntMetadata, ComputedValue, EvaluatedArgs, EvaluatedBytesOp,
    EvaluatedBytesWorker, ExecutionLimits, LocalCompileContext,
};

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::runtime_failure::{ExpressionRuntimeFailure, ExpressionRuntimeFailurePhase};
use crate::context::{BlockEncryptionMode, ErrorLevel, SessionTimeZone};
use crate::{Columns, EvalError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerErrorKind {
    Policy,
    Resource,
    Closed,
    Poisoned,
    Contract,
}

/// TiDB-only configuration/lifecycle failure; no KV type is exposed here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AsciiOwnerError {
    kind: OwnerErrorKind,
    message: &'static str,
}

impl AsciiOwnerError {
    pub(super) fn kind(&self) -> OwnerErrorKind {
        self.kind
    }

    fn new(kind: OwnerErrorKind, message: &'static str) -> Self {
        Self { kind, message }
    }
    fn resource(message: &'static str) -> Self {
        Self::new(OwnerErrorKind::Resource, message)
    }
    fn closed() -> Self {
        Self::new(OwnerErrorKind::Closed, "ASCII execution epoch is closed")
    }
    fn poisoned() -> Self {
        Self::new(
            OwnerErrorKind::Poisoned,
            "ASCII owner accounting is poisoned",
        )
    }
    fn contract(message: &'static str) -> Self {
        Self::new(OwnerErrorKind::Contract, message)
    }
}

impl fmt::Display for AsciiOwnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message)
    }
}
impl std::error::Error for AsciiOwnerError {}

/// Private structured handoff. Actual C4 failures capture their known phase at
/// the producing call; adapter failures never impersonate a LocalError.
#[derive(Debug)]
pub(super) enum AsciiBoundaryError {
    Frontend(EvalError),
    Kernel(ExpressionRuntimeFailure),
    Metadata(BridgeError),
    Owner(AsciiOwnerError),
    Scope {
        kind: ScopeFailureKind,
        reason: &'static str,
    },
}

impl AsciiBoundaryError {
    fn into_eval_error(self) -> EvalError {
        match self {
            Self::Frontend(error) => error,
            Self::Kernel(failure) => EvalError::ExpressionRuntimeFailure(failure),
            Self::Metadata(error) => {
                EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_bridge(error))
            }
            Self::Owner(error) => {
                EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_owner(error))
            }
            Self::Scope { kind, reason } => EvalError::ExpressionAdapterFailure(
                ExpressionAdapterFailure::from_scope(kind, reason),
            ),
        }
    }
}

impl From<AsciiOwnerError> for AsciiBoundaryError {
    fn from(error: AsciiOwnerError) -> Self {
        Self::Owner(error)
    }
}

/// Explicit, immutable limits for the closed ASCII worker; no default policy.
///
/// The pool ledger uses conditional retained/request-size accounting for a
/// validated, fixed compiler/layout cohort. It is not a physical-heap cap, a
/// factory transient-peak measurement, or an allocation/OOM recovery guarantee.
/// Creation reservations are allowances, not measured construction peaks.
/// Caller handles/scopes, native coercion and native error-carrier allocations
/// are outside this ledger; driver temporaries have a separate call allowance.
#[derive(Clone, Copy, Debug)]
pub struct AsciiPoolPolicy {
    max_workers: usize,
    max_creating: usize,
    max_pool_bytes: usize,
    worker_retained_cap: usize,
    creation_reservation: usize,
    max_steps: u64,
    max_frame_depth: usize,
    max_call_retained_bytes: usize,
}

impl AsciiPoolPolicy {
    /// Checks explicit limits and the conditional control-storage charge.
    ///
    /// This does not prepare a worker or certify physical heap usage, factory
    /// transients or OOM recovery. See the accounting exclusions on this type.
    /// Zero worker/creating slots are valid for a dormant binding; admission
    /// occurs only when an evaluated value demands the closed ASCII worker.
    ///
    /// # Errors
    /// Returns a native configuration/resource error for inconsistent limits or
    /// an overflowing/excessive control-storage charge.
    pub fn checked(
        max_workers: usize,
        max_creating: usize,
        max_pool_bytes: usize,
        worker_retained_cap: usize,
        creation_reservation: usize,
        max_steps: u64,
        max_frame_depth: usize,
        max_call_retained_bytes: usize,
    ) -> Result<Self, AsciiOwnerError> {
        if max_creating > max_workers || creation_reservation < worker_retained_cap {
            return Err(AsciiOwnerError::new(
                OwnerErrorKind::Policy,
                "ASCII creation slots/reservation contradict worker limits",
            ));
        }
        // Extent checks precede allocation. Zero available slots are valid:
        // merely binding an unused scope must not perform runtime admission.
        let base = base_charge(max_workers)?;
        if base > max_pool_bytes {
            return Err(AsciiOwnerError::resource(
                "ASCII owner control budget exceeded",
            ));
        }
        Ok(Self {
            max_workers,
            max_creating,
            max_pool_bytes,
            worker_retained_cap,
            creation_reservation,
            max_steps,
            max_frame_depth,
            max_call_retained_bytes,
        })
    }

    fn execution_limits(self) -> ExecutionLimits {
        ExecutionLimits {
            max_steps: self.max_steps,
            max_frame_depth: self.max_frame_depth,
            max_active_tasks: 0, // this exact closed recipe has no Host provider
            max_retained_bytes: self.max_call_retained_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SlotToken {
    index: usize,
    serial: u64,
    epoch: u64,
}

enum Slot {
    Empty,
    Creating(SlotToken),
    Leased(SlotToken),
    Idle {
        token: SlotToken,
        worker: Box<EvaluatedBytesWorker>,
        observed_bytes: usize,
    },
    Retiring {
        token: SlotToken,
        charge: usize,
        uncertain: bool,
    },
}

struct PoolState {
    slots: Vec<Slot>,
    next_epoch: u64,
    next_serial: u64,
    base_bytes: usize,
    reserved_bytes: usize,
    factory_attempts: u64,
    factory_successes: u64,
    retired: u64,
}

struct PoolCore {
    policy: AsciiPoolPolicy,
    // Zero means closed. A single atomic word is the epoch publication point.
    epoch: AtomicU64,
    poisoned: AtomicBool,
    uncertain: AtomicUsize,
    state: Mutex<PoolState>,
}

// ACCOUNTING ONLY, no pointer casts or layout-dependent access. This mirrors
// the pinned Arc allocation-request convention, not a portable std ABI. It
// MUST be checked against an independent actual PoolCore allocation receipt.
// In particular, comparing this proxy's size with itself proves nothing.
#[repr(C, align(2))]
struct PoolArcAllocation {
    _strong: AtomicUsize,
    _weak: AtomicUsize,
    _data: PoolCore,
}

fn base_charge(capacity: usize) -> Result<usize, AsciiOwnerError> {
    capacity
        .checked_mul(mem::size_of::<Slot>())
        .and_then(|slots| slots.checked_add(mem::size_of::<PoolArcAllocation>()))
        .ok_or_else(|| AsciiOwnerError::resource("ASCII control/container extent overflow"))
}

/// Reservation diagnostics, not a simultaneous measurement of leased workers
/// owned by other threads. The Arc basis always requires external pinned-cohort
/// validation; the boolean below does not certify or invalidate such a receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PoolSnapshot {
    live: usize,
    idle: usize,
    creating: usize,
    retiring: usize,
    uncertain: usize,
    reserved_bytes: usize,
    base_bytes: usize,
    idle_observed_bytes: usize,
    factory_attempts: u64,
    factory_successes: u64,
    retired: u64,
    caller_arc_measurement_required: bool,
}

/// Cloneable, synchronized accounting root for explicit ASCII executions.
///
/// Clones share outstanding creation/lease/retirement charges across epochs.
/// Accounting is conditional on the fixed-pin allocation-request/retained-size
/// basis, not a physical-heap or transient-peak/OOM guarantee. Caller handles,
/// scopes, native coercion and native error-carrier allocations are excluded;
/// see [`AsciiPoolPolicy`]. This owner installs no SQL or statement lifecycle.
#[derive(Clone)]
pub struct AsciiPoolOwner {
    core: Arc<PoolCore>,
}

/// Cloneable, synchronized token for one epoch of an [`AsciiPoolOwner`].
///
/// Cloning does not begin an execution or clone a worker. The lifecycle owner
/// must explicitly close this epoch; dropping a borrowed token does not do so.
#[derive(Clone)]
pub struct AsciiExecution {
    core: Arc<PoolCore>,
    epoch: u64,
}

impl AsciiPoolOwner {
    /// Allocates the accounting root and slot container, not an ASCII worker.
    ///
    /// Uses only the caller's checked policy. Its conditional fixed-pin request
    /// accounting has the exclusions and non-guarantees on [`AsciiPoolPolicy`];
    /// notably ordinary Arc/Box allocations do not promise OOM recovery.
    ///
    /// # Errors
    /// Returns a native resource error if the slot reservation fails or its
    /// observed container capacity exceeds the control-storage allowance.
    pub fn new(policy: AsciiPoolPolicy) -> Result<Self, AsciiOwnerError> {
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(policy.max_workers)
            .map_err(|_| AsciiOwnerError::resource("ASCII slot-container allocation failed"))?;
        let base_bytes = base_charge(slots.capacity())?;
        if base_bytes > policy.max_pool_bytes {
            return Err(AsciiOwnerError::resource(
                "ASCII actual container budget exceeded",
            ));
        }
        slots.resize_with(policy.max_workers, || Slot::Empty);
        Ok(Self {
            core: Arc::new(PoolCore {
                policy,
                epoch: AtomicU64::new(0),
                poisoned: AtomicBool::new(false),
                uncertain: AtomicUsize::new(0),
                state: Mutex::new(PoolState {
                    slots,
                    next_epoch: 0,
                    next_serial: 0,
                    base_bytes,
                    reserved_bytes: base_bytes,
                    factory_attempts: 0,
                    factory_successes: 0,
                    retired: 0,
                }),
            }),
        })
    }

    /// Begins a checked, non-reused epoch on this same accounting root.
    ///
    /// Invalidates older epochs without forgiving their outstanding charges.
    /// No worker is prepared here; this is a lifecycle operation, not a row
    /// entrypoint. The caller owns the matching [`AsciiExecution::close`].
    ///
    /// # Errors
    /// Returns a native lifecycle/contract error if the owner is poisoned or
    /// the epoch counter is exhausted.
    pub fn begin_execution(&self) -> Result<AsciiExecution, AsciiOwnerError> {
        let epoch = {
            let mut state = self.core.lock()?;
            let Some(epoch) = state.next_epoch.checked_add(1) else {
                self.core.poison();
                return Err(AsciiOwnerError::contract("ASCII epoch exhausted"));
            };
            state.next_epoch = epoch;
            self.core.epoch.store(epoch, Ordering::SeqCst);
            epoch
        };
        // Old leased/creating/retiring charges survive on this SAME root.
        self.core.retire_old_idle();
        Ok(AsciiExecution {
            core: Arc::clone(&self.core),
            epoch,
        })
    }

    fn snapshot(&self) -> Result<PoolSnapshot, AsciiOwnerError> {
        self.core.snapshot()
    }
}

impl PoolCore {
    fn poison(&self) {
        self.poisoned.store(true, Ordering::SeqCst);
        self.epoch.store(0, Ordering::SeqCst);
    }

    fn lock(&self) -> Result<MutexGuard<'_, PoolState>, AsciiOwnerError> {
        let state = self.state.lock().map_err(|_| {
            self.poison();
            AsciiOwnerError::poisoned()
        })?;
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(AsciiOwnerError::poisoned());
        }
        Ok(state)
    }

    // Recovery is disposal-only. No admission uses a poisoned mutex's contents.
    fn cleanup_lock(&self) -> MutexGuard<'_, PoolState> {
        self.state.lock().unwrap_or_else(|error| {
            self.poison();
            error.into_inner()
        })
    }

    fn check_epoch(&self, epoch: u64) -> Result<(), AsciiOwnerError> {
        if self.state.is_poisoned() || self.poisoned.load(Ordering::SeqCst) {
            // A cached lease need not acquire the state mutex again. Observe
            // its sticky poison directly, then permanently close this root.
            self.poison();
            Err(AsciiOwnerError::poisoned())
        } else if epoch == 0 || self.epoch.load(Ordering::SeqCst) != epoch {
            Err(AsciiOwnerError::closed())
        } else if {
            // Timing-only one-shot rendezvous for the structural snapshot-race
            // regression. No production callback or PoolCore field is added.
            #[cfg(test)]
            tests::after_epoch_read_for_test();
            self.uncertain.load(Ordering::SeqCst) != 0
        } {
            Err(AsciiOwnerError::resource(
                "ASCII uncertain retirement debt remains",
            ))
        } else if self.epoch.load(Ordering::SeqCst) != epoch {
            Err(AsciiOwnerError::closed())
        } else if self.state.is_poisoned() || self.poisoned.load(Ordering::SeqCst) {
            self.poison();
            Err(AsciiOwnerError::poisoned())
        } else {
            // Epochs are root-local, never reused and checked against wrap.
            // Equal reads bracket the debt observation; final sticky-poison
            // checks establish that this same instant was eligible. No pool
            // mutex is held across a kernel or native callback.
            Ok(())
        }
    }

    fn snapshot(&self) -> Result<PoolSnapshot, AsciiOwnerError> {
        let state = self.lock()?;
        let mut out = PoolSnapshot {
            live: 0,
            idle: 0,
            creating: 0,
            retiring: 0,
            uncertain: self.uncertain.load(Ordering::SeqCst),
            reserved_bytes: state.reserved_bytes,
            base_bytes: state.base_bytes,
            idle_observed_bytes: 0,
            factory_attempts: state.factory_attempts,
            factory_successes: state.factory_successes,
            retired: state.retired,
            caller_arc_measurement_required: true,
        };
        // Count is bounded by the checked, allocated slot extent.
        for slot in &state.slots {
            match slot {
                Slot::Empty => {}
                Slot::Creating(_) => out.creating += 1,
                Slot::Leased(_) => out.live += 1,
                Slot::Retiring { .. } => out.retiring += 1,
                Slot::Idle { observed_bytes, .. } => {
                    out.idle += 1;
                    out.idle_observed_bytes = out
                        .idle_observed_bytes
                        .checked_add(*observed_bytes)
                        .ok_or_else(|| AsciiOwnerError::resource("ASCII observation overflow"))?;
                }
            }
        }
        Ok(out)
    }

    fn release_creation(&self, token: SlotToken, had_worker: bool) {
        let mut state = self.cleanup_lock();
        if !matches!(state.slots.get(token.index), Some(Slot::Creating(t)) if *t == token) {
            self.poison();
            return;
        }
        let Some(bytes) = state
            .reserved_bytes
            .checked_sub(self.policy.creation_reservation)
        else {
            self.poison();
            return;
        };
        let retired = if had_worker {
            state.retired.checked_add(1)
        } else {
            Some(state.retired)
        };
        let Some(retired) = retired else {
            self.poison();
            return;
        };
        state.slots[token.index] = Slot::Empty;
        state.reserved_bytes = bytes;
        state.retired = retired;
    }

    fn start_retirement(&self, token: SlotToken, uncertain: bool) -> bool {
        let mut state = self.cleanup_lock();
        if !matches!(state.slots.get(token.index), Some(Slot::Leased(t)) if *t == token) {
            self.poison();
            return false;
        }
        if uncertain {
            let Some(next) = self.uncertain.load(Ordering::SeqCst).checked_add(1) else {
                self.poison();
                return false;
            };
            self.uncertain.store(next, Ordering::SeqCst);
        }
        state.slots[token.index] = Slot::Retiring {
            token,
            charge: self.policy.worker_retained_cap,
            uncertain,
        };
        true
    }

    fn finish_retirement(&self, token: SlotToken) {
        let mut state = self.cleanup_lock();
        let Some(Slot::Retiring {
            token: stored,
            charge,
            uncertain,
        }) = state.slots.get(token.index)
        else {
            self.poison();
            return;
        };
        if *stored != token {
            self.poison();
            return;
        }
        let Some(bytes) = state.reserved_bytes.checked_sub(*charge) else {
            self.poison();
            return;
        };
        let Some(retired) = state.retired.checked_add(1) else {
            self.poison();
            return;
        };
        if *uncertain {
            let Some(next) = self.uncertain.load(Ordering::SeqCst).checked_sub(1) else {
                self.poison();
                return;
            };
            self.uncertain.store(next, Ordering::SeqCst);
        }
        state.slots[token.index] = Slot::Empty;
        state.reserved_bytes = bytes;
        state.retired = retired;
    }

    fn retire_old_idle(self: &Arc<Self>) {
        loop {
            let detached = {
                let mut state = self.cleanup_lock();
                let current = self.epoch.load(Ordering::SeqCst);
                let Some(index) = state.slots.iter().position(
                    |slot| matches!(slot, Slot::Idle { token, .. } if token.epoch != current),
                ) else {
                    return;
                };
                let Slot::Idle { token, worker, .. } =
                    mem::replace(&mut state.slots[index], Slot::Empty)
                else {
                    unreachable!("matched idle slot");
                };
                // The allocation stays charged while detached; no Vec of
                // retired workers is allocated and no destructor runs locked.
                state.slots[index] = Slot::Retiring {
                    token,
                    charge: self.policy.worker_retained_cap,
                    uncertain: false,
                };
                Retirement {
                    core: Arc::clone(self),
                    token,
                    worker: Some(worker),
                    recorded: true,
                }
            };
            drop(detached);
        }
    }
}

impl AsciiExecution {
    /// Creates an affine scope without checkout, compilation or admission.
    ///
    /// A closed epoch is refused only when a value demands its worker. Scopes
    /// can move between threads but cannot share their mutable worker state.
    pub fn scope(&self) -> AsciiScope {
        AsciiScope {
            execution: self.clone(),
            lease: RefCell::new(None),
            busy: Cell::new(false),
            poisoned: Cell::new(false),
        }
    }

    /// Idempotently closes this epoch, never a newer epoch on the same root.
    ///
    /// Existing live leases/creations remain charged until their disposal.
    /// Only the execution's lifecycle owner, not a borrowing operator, should
    /// close it. This does not wait for outstanding native work to finish.
    pub fn close(&self) {
        {
            let _state = self.core.cleanup_lock();
            if self.core.epoch.load(Ordering::SeqCst) == self.epoch {
                self.core.epoch.store(0, Ordering::SeqCst);
            }
        }
        self.core.retire_old_idle();
    }

    // Split reservation/preparation is a closed internal seam, useful for
    // deterministic race tests. No caller supplies a replacement factory.
    fn checkout(&self) -> Result<Checkout, AsciiOwnerError> {
        self.checkout_for(EvaluatedBytesOp::Ascii)
    }

    fn checkout_for(&self, operation: EvaluatedBytesOp) -> Result<Checkout, AsciiOwnerError> {
        // Bounded eviction even if other threads continually refill idle slots.
        // Every operation shares this same root, epoch and reservation ledger.
        let mut evictions_left = self.core.policy.max_workers;
        loop {
            let mut state = self.core.lock()?;
            self.core.check_epoch(self.epoch)?;
            if let Some(index) = state.slots.iter().position(|slot| {
                matches!(slot, Slot::Idle { token, worker, .. }
                    if token.epoch == self.epoch && worker.operation() == operation)
            }) {
                let Slot::Idle { token, worker, .. } =
                    mem::replace(&mut state.slots[index], Slot::Empty)
                else {
                    unreachable!("matched idle slot");
                };
                state.slots[index] = Slot::Leased(token);
                return Ok(Checkout::Idle(AsciiLease {
                    core: Arc::clone(&self.core),
                    token,
                    worker: Some(worker),
                }));
            }
            let creating = state
                .slots
                .iter()
                .filter(|slot| matches!(slot, Slot::Creating(_)))
                .count();
            if creating >= self.core.policy.max_creating {
                return Err(AsciiOwnerError::resource(
                    "ASCII creating-worker limit exceeded",
                ));
            }
            let empty = state
                .slots
                .iter()
                .position(|slot| matches!(slot, Slot::Empty));
            let bytes = state
                .reserved_bytes
                .checked_add(self.core.policy.creation_reservation)
                .filter(|bytes| *bytes <= self.core.policy.max_pool_bytes);
            if let (Some(index), Some(bytes)) = (empty, bytes) {
                let serial = state
                    .next_serial
                    .checked_add(1)
                    .ok_or_else(|| AsciiOwnerError::resource("ASCII slot serial exhausted"))?;
                let token = SlotToken {
                    index,
                    serial,
                    epoch: self.epoch,
                };
                state.next_serial = serial;
                state.reserved_bytes = bytes;
                state.slots[index] = Slot::Creating(token);
                return Ok(Checkout::Create(Creation {
                    core: Arc::clone(&self.core),
                    token,
                    operation,
                    worker: None,
                    active: true,
                }));
            }
            if evictions_left != 0 {
                if let Some(index) = state.slots.iter().position(|slot| {
                    matches!(slot, Slot::Idle { token, worker, .. }
                        if token.epoch == self.epoch && worker.operation() != operation)
                }) {
                    let Slot::Idle { token, worker, .. } =
                        mem::replace(&mut state.slots[index], Slot::Empty)
                    else {
                        unreachable!("matched idle slot");
                    };
                    // Reuse the real retirement path; do not release bytes or
                    // the slot until the detached worker is actually destroyed.
                    state.slots[index] = Slot::Leased(token);
                    let retired = AsciiLease {
                        core: Arc::clone(&self.core),
                        token,
                        worker: Some(worker),
                    };
                    drop(state);
                    drop(retired);
                    evictions_left -= 1;
                    continue;
                }
            }
            return Err(AsciiOwnerError::resource(if empty.is_none() {
                "ASCII worker-slot limit exceeded"
            } else {
                "ASCII owner reservation budget exceeded"
            }));
        }
    }
}

enum Checkout {
    Idle(AsciiLease),
    Create(Creation),
}

impl Checkout {
    fn ready(self) -> Result<AsciiLease, AsciiBoundaryError> {
        match self {
            Self::Idle(lease) => {
                lease.validate()?;
                Ok(lease)
            }
            Self::Create(creation) => creation.prepare(),
        }
    }
}

struct Creation {
    core: Arc<PoolCore>,
    token: SlotToken,
    operation: EvaluatedBytesOp,
    worker: Option<Box<EvaluatedBytesWorker>>,
    active: bool,
}

impl Creation {
    fn prepare(mut self) -> Result<AsciiLease, AsciiBoundaryError> {
        self.build_worker()?;
        self.publish()
    }

    // Keeping these two closed phases separate permits a test to close an
    // epoch AFTER real preparation but BEFORE publication. Neither phase
    // accepts a factory, callback, program, context or substituted worker.
    fn build_worker(&mut self) -> Result<(), AsciiBoundaryError> {
        if self.worker.is_some() {
            return Err(AsciiOwnerError::contract("ASCII creation already prepared").into());
        }
        {
            let mut state = self.core.lock()?;
            self.core.check_epoch(self.token.epoch)?;
            if !matches!(state.slots.get(self.token.index), Some(Slot::Creating(t)) if *t == self.token)
            {
                self.core.poison();
                return Err(AsciiOwnerError::contract("ASCII creation token changed").into());
            }
            state.factory_attempts = state
                .factory_attempts
                .checked_add(1)
                .ok_or_else(|| AsciiOwnerError::resource("ASCII factory counter exhausted"))?;
        }
        // The full creating reservation predates ALL factory/prewarm/Box work.
        let worker = prepare_evaluated_bytes(
            self.operation,
            LocalCompileContext {
                limits: CompileLimits {
                    // Three ready arguments plus a call, or a two-call unary
                    // predicate over one ready argument (depth three).
                    max_nodes: 4,
                    max_depth: 3,
                },
            },
            self.core.policy.execution_limits(),
            self.core.policy.worker_retained_cap,
        )
        .map_err(|error| {
            AsciiBoundaryError::Kernel(ExpressionRuntimeFailure::from_ascii_local(
                error,
                Some(ExpressionRuntimeFailurePhase::Prepare),
            ))
        })?;
        self.worker = Some(Box::new(worker));
        let worker = self.worker.as_ref().expect("just prepared worker");
        let observed = worker
            .retained_storage()
            .map_err(|error| {
                AsciiBoundaryError::Kernel(ExpressionRuntimeFailure::from_ascii_local(
                    error,
                    Some(ExpressionRuntimeFailurePhase::Observe),
                ))
            })?
            .total_bytes();
        if worker.operation() != self.operation
            || !worker.is_healthy()
            || observed > self.core.policy.worker_retained_cap
        {
            return Err(
                AsciiOwnerError::contract("ASCII factory published unhealthy storage").into(),
            );
        }
        {
            let mut state = self.core.lock()?;
            // Count real successful factory returns, including one invalidated
            // by a concurrent close. Do not label a cache lookup as preparation.
            state.factory_successes = state
                .factory_successes
                .checked_add(1)
                .ok_or_else(|| AsciiOwnerError::resource("ASCII factory counter exhausted"))?;
        }
        Ok(())
    }

    fn publish(mut self) -> Result<AsciiLease, AsciiBoundaryError> {
        let worker = self.worker.as_ref().ok_or(AsciiBoundaryError::Scope {
            kind: ScopeFailureKind::Contract,
            reason: "ASCII publication before preparation",
        })?;
        let observed = worker
            .retained_storage()
            .map_err(|error| {
                AsciiBoundaryError::Kernel(ExpressionRuntimeFailure::from_ascii_local(
                    error,
                    Some(ExpressionRuntimeFailurePhase::Observe),
                ))
            })?
            .total_bytes();
        if !worker.is_healthy() || observed > self.core.policy.worker_retained_cap {
            return Err(AsciiOwnerError::contract("ASCII publication is not healthy").into());
        }
        {
            let mut state = self.core.lock()?;
            self.core.check_epoch(self.token.epoch)?;
            if !matches!(state.slots.get(self.token.index), Some(Slot::Creating(t)) if *t == self.token)
            {
                self.core.poison();
                return Err(AsciiOwnerError::contract("ASCII publication token changed").into());
            }
            let bytes = state
                .reserved_bytes
                .checked_sub(self.core.policy.creation_reservation)
                .and_then(|bytes| bytes.checked_add(self.core.policy.worker_retained_cap))
                .ok_or_else(|| {
                    AsciiOwnerError::contract("ASCII publication reservation changed")
                })?;
            state.reserved_bytes = bytes;
            state.slots[self.token.index] = Slot::Leased(self.token);
        }
        // All factory temporaries are gone before F is exchanged for W.
        self.active = false;
        Ok(AsciiLease {
            core: Arc::clone(&self.core),
            token: self.token,
            worker: self.worker.take(),
        })
    }
}

impl Drop for Creation {
    fn drop(&mut self) {
        if self.active {
            let had_worker = self.worker.is_some();
            drop(self.worker.take()); // before returning even a single credit
            self.core.release_creation(self.token, had_worker);
        }
    }
}

struct AsciiLease {
    core: Arc<PoolCore>,
    token: SlotToken,
    worker: Option<Box<EvaluatedBytesWorker>>,
}

impl AsciiLease {
    fn validate(&self) -> Result<usize, AsciiBoundaryError> {
        let worker = self.worker.as_ref().ok_or(AsciiBoundaryError::Scope {
            kind: ScopeFailureKind::Contract,
            reason: "missing ASCII worker",
        })?;
        let observed = worker
            .retained_storage()
            .map_err(|error| {
                AsciiBoundaryError::Kernel(ExpressionRuntimeFailure::from_ascii_local(
                    error,
                    Some(ExpressionRuntimeFailurePhase::Observe),
                ))
            })?
            .total_bytes();
        if !worker.is_healthy() || observed > self.core.policy.worker_retained_cap {
            return Err(AsciiOwnerError::contract("ASCII worker ownership is not healthy").into());
        }
        self.core.check_epoch(self.token.epoch)?;
        Ok(observed)
    }

    fn detach_retirement(&mut self) -> Option<Retirement> {
        let worker = self.worker.take()?;
        let uncertain = worker.retained_storage().map_or(true, |storage| {
            storage.total_bytes() > self.core.policy.worker_retained_cap
        });
        let recorded = self.core.start_retirement(self.token, uncertain);
        Some(Retirement {
            core: Arc::clone(&self.core),
            token: self.token,
            worker: Some(worker),
            recorded,
        })
    }

    fn into_retirement(mut self) -> Retirement {
        self.detach_retirement().expect("owned lease has a worker")
    }

    fn return_to_pool(mut self) {
        if std::thread::panicking() {
            return;
        }
        let Ok(observed_bytes) = self.validate() else {
            return;
        };
        {
            let Ok(mut state) = self.core.lock() else {
                return;
            };
            if self.core.check_epoch(self.token.epoch).is_err() {
                return;
            }
            if !matches!(state.slots.get(self.token.index), Some(Slot::Leased(t)) if *t == self.token)
            {
                self.core.poison();
                return;
            }
            let worker = self.worker.take().expect("validated owned worker");
            state.slots[self.token.index] = Slot::Idle {
                token: self.token,
                worker,
                observed_bytes,
            };
        }
        // Drop sees no worker. The unique Box moved into the same root/epoch.
    }
}

impl Drop for AsciiLease {
    fn drop(&mut self) {
        // Default destruction NEVER recycles. Only explicit normal scope
        // completion may choose return_to_pool after all health checks.
        drop(self.detach_retirement());
    }
}

struct Retirement {
    core: Arc<PoolCore>,
    token: SlotToken,
    worker: Option<Box<EvaluatedBytesWorker>>,
    recorded: bool,
}

impl Drop for Retirement {
    fn drop(&mut self) {
        drop(self.worker.take()); // no pool lock; byte/slot debt is still live
        if self.recorded {
            self.core.finish_retirement(self.token);
        }
    }
}

/// Affine worker scope. RefCell/Cell intentionally make this Send, not Sync.
/// It contains no native Columns/row/SQL descriptor or invocation value.
///
/// A healthy worker is reused within the scope and returned to its same-epoch
/// pool on ordinary drop. Unwind poison is sticky: it never permits native
/// replay or silently creates a replacement execution.
pub struct AsciiScope {
    execution: AsciiExecution,
    lease: RefCell<Option<AsciiLease>>,
    busy: Cell<bool>,
    poisoned: Cell<bool>,
}

impl AsciiScope {
    /// Lexically binds a capability while preserving native Columns behavior.
    ///
    /// An already active scope on `native` wins, including its execution token
    /// and unwind guard. If discovery itself panics, only the requested scope
    /// can be quarantined; an undisclosed native scope is not known. Otherwise
    /// this scope is used when no active scope exists. Binding itself does not
    /// check out or prepare a worker, perform admission, or supply defaults.
    /// The sized borrowing wrapper also supports existing `C: Columns` callers;
    /// neither `native` nor the callback needs `Send`, `Sync` or `'static`.
    ///
    /// Put this call INSIDE the caller's panic catcher and encompass native
    /// child/return work: the guard poisons the effective scope on unwind but
    /// does not catch it. This API alone does not wire business forwarders or
    /// statement lifetimes. Callback/handle/coercion/error-carrier allocations
    /// are outside the conditional fixed-pin pool ledger; neither this binding
    /// nor that ledger guarantees physical heap, transient peaks or OOM recovery.
    pub fn with_columns<'a, R>(
        &'a self,
        native: &'a dyn Columns,
        body: impl FnOnce(&ScopedAsciiColumns<'a, 'a>) -> R,
    ) -> R {
        // Capability discovery is a native virtual call and can itself unwind.
        // Protect the requested scope until the effective guard is armed; a
        // scope the getter fails to disclose cannot be identified here.
        let mut discovery_guard = NativeGuard::new(self);
        let scope = native.evaluated_ascii_scope().unwrap_or(self);
        // This guard must be INSIDE the existing caller's panic catcher. It
        // catches no panic itself; Drop marks poison while unwinding.
        let mut guard = NativeGuard::new(scope);
        discovery_guard.disarm();
        let scoped = ScopedAsciiColumns { native, scope };
        let result = body(&scoped);
        guard.disarm();
        result
    }

    /// Computes ASCII from one already evaluated native value using only the
    /// existing closed C4 worker, including for SQL NULL.
    ///
    /// This is a value/coercion boundary, NOT a SQL frontend: the caller must
    /// already have evaluated children, checked arity, and applied the native
    /// context-dependent argument casts/transcoding. This method performs the
    /// existing final byte coercion, invokes C4, and materializes its owned Int
    /// or NULL; the caller still owns native return-type coercion afterwards.
    /// No policy, execution, native fallback or general evaluator is synthesized.
    ///
    /// Native byte-coercion and error-carrier allocations are outside the pool
    /// ledger. Its fixed-pin request/retained accounting is conditional, not a
    /// physical-heap cap, factory transient-peak or allocation/OOM guarantee.
    /// Use [`Self::with_columns`] to guard surrounding native child/return work.
    ///
    /// # Errors
    /// Original frontend coercion errors precede runtime admission. Actual C4
    /// errors preserve their cause and known phase; pool/scope/bridge errors
    /// have the distinct native adapter origin. No failure is replayed natively.
    pub fn evaluate_value(&self, value: &Datum) -> Result<Datum, EvalError> {
        evaluate_ascii_value(self, value).map_err(AsciiBoundaryError::into_eval_error)
    }

    fn poison(&self) {
        self.poisoned.set(true);
        if let Ok(mut parked) = self.lease.try_borrow_mut() {
            let lease = parked.take();
            drop(parked);
            drop(lease);
        }
        // Busy invocations own their lease outside the cell; their armed guard
        // handles disposal. Never panic again in an unwind cleanup path.
    }
}

impl Drop for AsciiScope {
    fn drop(&mut self) {
        let lease = self.lease.get_mut().take();
        if let Some(lease) = lease {
            if !self.poisoned.get() && !self.busy.get() && !std::thread::panicking() {
                lease.return_to_pool();
            } else {
                drop(lease);
            }
        }
    }
}

struct NativeGuard<'a> {
    scope: &'a AsciiScope,
    armed: bool,
}
impl<'a> NativeGuard<'a> {
    fn new(scope: &'a AsciiScope) -> Self {
        Self { scope, armed: true }
    }
    fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for NativeGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.scope.poison();
        }
    }
}

struct Invocation<'a> {
    scope: &'a AsciiScope,
    lease: Option<AsciiLease>,
    armed: bool,
}

impl<'a> Invocation<'a> {
    fn enter(scope: &'a AsciiScope) -> Result<Self, AsciiBoundaryError> {
        if scope.poisoned.get() {
            return Err(AsciiBoundaryError::Scope {
                kind: ScopeFailureKind::Poisoned,
                reason: "ASCII scope is poisoned",
            });
        }
        if scope.busy.get() {
            return Err(AsciiBoundaryError::Scope {
                kind: ScopeFailureKind::Reentry,
                reason: "reentrant ASCII runtime borrow",
            });
        }
        let mut parked = scope
            .lease
            .try_borrow_mut()
            .map_err(|_| AsciiBoundaryError::Scope {
                kind: ScopeFailureKind::Reentry,
                reason: "ASCII scope cell is already borrowed",
            })?;
        let lease = parked.take();
        scope.busy.set(true);
        Ok(Self {
            scope,
            lease,
            armed: true,
        })
    }

    fn run(&mut self, ready: ReadyAsciiBytes) -> Result<ComputedInt, AsciiBoundaryError> {
        require_computed_int(self.run_for(EvaluatedBytesOp::Ascii, ready)?)
    }

    fn run_for(
        &mut self,
        operation: EvaluatedBytesOp,
        ready: ReadyAsciiBytes,
    ) -> Result<ComputedValue, AsciiBoundaryError> {
        self.run_args(operation, EvaluatedArgs::Bytes(ready.0))
    }

    fn run_args(
        &mut self,
        operation: EvaluatedBytesOp,
        ready: EvaluatedArgs,
    ) -> Result<ComputedValue, AsciiBoundaryError> {
        if let Some(lease) = self.lease.as_ref() {
            lease.validate()?;
            if lease.worker.as_ref().expect("validated worker").operation() != operation {
                // This affine scope has one cache entry. Replacing its operation
                // destroys the old worker before any new creating reservation.
                drop(self.lease.take());
            }
        }
        if self.lease.is_none() {
            self.lease = Some(self.scope.execution.checkout_for(operation)?.ready()?);
        }
        let lease = self.lease.as_mut().expect("checked out worker");
        lease.validate()?;
        // No cell/pool borrow or native callback enters the C4 driver.
        let worker = lease.worker.as_mut().expect("validated worker");
        if worker.operation() != operation {
            return Err(AsciiOwnerError::contract("closed Bytes worker operation mismatch").into());
        }
        // Test-only facade-entry observation, not a substitute for C4's real
        // function-pointer witness. No observer is passed into the worker.
        #[cfg(test)]
        tests::before_eval_one_for_test(worker.kernel_invocations());
        let result = worker.eval_args(ready);
        #[cfg(test)]
        tests::after_eval_one_for_test(worker.kernel_invocations());
        result.map_err(|error| {
            AsciiBoundaryError::Kernel(ExpressionRuntimeFailure::from_ascii_local(
                error,
                Some(ExpressionRuntimeFailurePhase::Invoke),
            ))
        })
    }

    fn finish<T>(mut self, result: Result<T, AsciiBoundaryError>) -> Result<T, AsciiBoundaryError> {
        let postflight = self
            .lease
            .as_ref()
            .map_or(Ok(()), |lease| lease.validate().map(|_| ()));
        let result = match (result, postflight) {
            (Err(primary), secondary) => {
                if secondary.is_err() {
                    self.scope.poisoned.set(true);
                    drop(self.lease.take());
                }
                Err(primary) // never replace the original owned engine error
            }
            (Ok(_), Err(error)) => {
                self.scope.poisoned.set(true);
                drop(self.lease.take());
                Err(error)
            }
            (Ok(value), Ok(())) => Ok(value),
        };
        let restore = if let Some(lease) = self.lease.take() {
            match self.scope.lease.try_borrow_mut() {
                Ok(mut parked) if parked.is_none() => {
                    *parked = Some(lease);
                    Ok(())
                }
                _ => {
                    self.scope.poisoned.set(true);
                    drop(lease);
                    Err(AsciiBoundaryError::Scope {
                        kind: ScopeFailureKind::Contract,
                        reason: "ASCII lease restore conflict",
                    })
                }
            }
        } else {
            Ok(())
        };
        self.scope.busy.set(false);
        self.armed = false;
        match result {
            Err(primary) => Err(primary),
            Ok(value) => restore.map(|()| value),
        }
    }
}

impl Drop for Invocation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.scope.poisoned.set(true);
            drop(self.lease.take()); // before a surviving caller catches unwind
            self.scope.busy.set(false);
        }
    }
}

struct ReadyAsciiBytes(Option<Vec<u8>>);

struct NativeComputedInt {
    value: Option<i64>,
    metadata: ValueMetadata,
}

fn coerce_ready(value: &Datum) -> Result<ReadyAsciiBytes, AsciiBoundaryError> {
    crate::coerce::coerce_str_bytes(value)
        .map(ReadyAsciiBytes)
        .map_err(AsciiBoundaryError::Frontend)
}

fn eval_ready(
    scope: &AsciiScope,
    ready: ReadyAsciiBytes,
) -> Result<NativeComputedInt, AsciiBoundaryError> {
    let mut invocation = Invocation::enter(scope)?;
    let result = invocation.run(ready);
    let computed = invocation.finish(result)?;
    Ok(own_computed_int(computed))
}

fn own_computed_int(computed: ComputedInt) -> NativeComputedInt {
    // C4's actual generated output, including NULL, owns this identity. The
    // operand's kind/collation and native return FieldType are not consulted.
    let metadata = match computed.metadata() {
        ComputedIntMetadata::OwnSignedInt => ValueMetadata {
            kind: DatumKind::Int,
            string_collation: None,
            decimal_declared_shape: None,
        },
    };
    NativeComputedInt {
        value: computed.into_option(),
        metadata,
    }
}

fn result_kind_error() -> AsciiBoundaryError {
    AsciiBoundaryError::Scope {
        kind: ScopeFailureKind::Contract,
        reason: "closed Bytes result kind mismatch",
    }
}

fn require_computed_int(computed: ComputedValue) -> Result<ComputedInt, AsciiBoundaryError> {
    match computed {
        ComputedValue::Int(value) => Ok(value),
        ComputedValue::Bytes(_) | ComputedValue::Ieee754Bits(_) => Err(result_kind_error()),
    }
}

impl NativeComputedInt {
    fn into_datum(self) -> Result<Datum, AsciiBoundaryError> {
        from_scalar(
            ScalarValueRef::Int(self.value.as_ref()),
            EvalType::Int,
            &self.metadata,
        )
        .map_err(AsciiBoundaryError::Metadata)
    }
}

pub(super) fn evaluate_ascii_value(
    scope: &AsciiScope,
    value: &Datum,
) -> Result<Datum, AsciiBoundaryError> {
    // Also cover a direct private helper's coercion/materialization. The outer
    // with_columns guard covers native child/return work beyond this function.
    let mut guard = NativeGuard::new(scope);
    let result = (|| eval_ready(scope, coerce_ready(value)?)?.into_datum())();
    guard.disarm(); // ordinary Result::Err is not an unwind
    result
}

// Only the isolated one-shot path owns a close. A borrowed execution above a
// temporary operation scope must remain open for its actual lifecycle owner.
struct OneShotAsciiExecution(AsciiExecution);

impl Drop for OneShotAsciiExecution {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Native-owned computed output, not an operand identity or SQL descriptor.
/// Byte results deliberately leave text/binary packing to the original frontend.
pub(crate) enum EvaluatedBytesResult {
    Int(Datum),
    Bytes(Option<Vec<u8>>),
    // A separate owned carrier: never ordinary Bytes, SQL Int or NotNan Real.
    Ieee754Bits(Option<u64>),
}

impl EvaluatedBytesResult {
    pub(crate) fn into_int_datum(self) -> Result<Datum, EvalError> {
        match self {
            Self::Int(value) => Ok(value),
            Self::Bytes(_) | Self::Ieee754Bits(_) => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Pack only a computed boolean carrier; do not recalculate its truth.
    pub(crate) fn into_boolean_datum(self) -> Result<Datum, EvalError> {
        match self {
            Self::Int(value @ (Datum::Int(0) | Datum::Int(1) | Datum::Null)) => Ok(value),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    /// The signed C carrier owns the result bits, not the frontend SQL flag.
    /// Negative carriers are valid unsigned bitwise answers, never overflows.
    pub(crate) fn into_uint_bits_datum(self) -> Result<Datum, EvalError> {
        match self.into_int_datum()? {
            Datum::Int(bits) => Ok(Datum::UInt(bits as u64)),
            Datum::Null => Ok(Datum::Null),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    pub(crate) fn into_bytes(self) -> Result<Option<Vec<u8>>, EvalError> {
        match self {
            Self::Bytes(value) => Ok(value),
            Self::Int(_) | Self::Ieee754Bits(_) => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Pack a non-null real result without a local constant or NULL fallback.
    pub(crate) fn into_nonnull_real_datum(self) -> Result<Datum, EvalError> {
        match self {
            Self::Ieee754Bits(Some(bits)) => Ok(Datum::Real(f64::from_bits(bits))),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Only the explicitly owned IEEE carrier can supply raw float bits.
    pub(crate) fn into_ieee754_bits(self) -> Result<Option<u64>, EvalError> {
        match self {
            Self::Ieee754Bits(value) => Ok(value),
            Self::Int(_) | Self::Bytes(_) => Err(result_kind_error().into_eval_error()),
        }
    }
}

fn materialize_computed(
    operation: EvaluatedBytesOp,
    computed: ComputedValue,
) -> Result<EvaluatedBytesResult, AsciiBoundaryError> {
    match (operation, computed) {
        (
            EvaluatedBytesOp::Ascii
            | EvaluatedBytesOp::Length
            | EvaluatedBytesOp::BitLength
            | EvaluatedBytesOp::Crc32
            | EvaluatedBytesOp::CharLength
            | EvaluatedBytesOp::CharLengthUtf8
            | EvaluatedBytesOp::BitCount
            | EvaluatedBytesOp::BitNeg
            | EvaluatedBytesOp::BitAnd
            | EvaluatedBytesOp::BitOr
            | EvaluatedBytesOp::BitXor
            | EvaluatedBytesOp::LeftShift
            | EvaluatedBytesOp::RightShift
            | EvaluatedBytesOp::UnaryNot
            | EvaluatedBytesOp::IsNull
            | EvaluatedBytesOp::IsTrue
            | EvaluatedBytesOp::IsFalse
            | EvaluatedBytesOp::IsTrueWithNull
            | EvaluatedBytesOp::IsNotNull
            | EvaluatedBytesOp::IsNotTrue
            | EvaluatedBytesOp::IsNotFalse
            | EvaluatedBytesOp::LogicalAnd
            | EvaluatedBytesOp::LogicalOr
            | EvaluatedBytesOp::LogicalXor
            | EvaluatedBytesOp::InetAton
            | EvaluatedBytesOp::SignRaw
            | EvaluatedBytesOp::IsIpv4Nullable
            | EvaluatedBytesOp::IsIpv6Nullable
            | EvaluatedBytesOp::IsIpv4CompatNullable
            | EvaluatedBytesOp::IsIpv4MappedNullable,
            ComputedValue::Int(value),
        ) => own_computed_int(value)
            .into_datum()
            .map(EvaluatedBytesResult::Int),
        (
            EvaluatedBytesOp::LTrim
            | EvaluatedBytesOp::RTrim
            | EvaluatedBytesOp::UnHex
            | EvaluatedBytesOp::Reverse
            | EvaluatedBytesOp::ReverseUtf8
            | EvaluatedBytesOp::Quote
            | EvaluatedBytesOp::HexInt
            | EvaluatedBytesOp::HexStr
            | EvaluatedBytesOp::Bin
            | EvaluatedBytesOp::Left
            | EvaluatedBytesOp::LeftUtf8
            | EvaluatedBytesOp::Right
            | EvaluatedBytesOp::RightUtf8
            | EvaluatedBytesOp::Replace
            | EvaluatedBytesOp::Md5
            | EvaluatedBytesOp::Sha1
            | EvaluatedBytesOp::InetNtoa
            | EvaluatedBytesOp::Inet6Aton
            | EvaluatedBytesOp::Inet6Ntoa,
            ComputedValue::Bytes(value),
        ) => {
            match value.metadata() {
                ComputedBytesMetadata::OwnBytes => {}
            }
            Ok(EvaluatedBytesResult::Bytes(value.into_option()))
        }
        (
            EvaluatedBytesOp::AsinRaw
            | EvaluatedBytesOp::AcosRaw
            | EvaluatedBytesOp::SqrtRaw
            | EvaluatedBytesOp::RadiansRaw
            | EvaluatedBytesOp::DegreesRaw
            | EvaluatedBytesOp::PiRaw,
            ComputedValue::Ieee754Bits(value),
        ) => {
            match value.metadata() {
                ComputedIeee754BitsMetadata::OwnIeee754Bits => {}
            }
            Ok(EvaluatedBytesResult::Ieee754Bits(value.into_option()))
        }
        _ => Err(result_kind_error()),
    }
}

fn evaluate_scoped_args(
    operation: EvaluatedBytesOp,
    scope: &AsciiScope,
    coerce: impl FnOnce() -> Result<EvaluatedArgs, EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult) -> Result<Datum, EvalError>,
) -> Result<Datum, AsciiBoundaryError> {
    let mut guard = NativeGuard::new(scope);
    let result = (|| {
        // Frontend coercion runs exactly once, before taking/replacing a lease.
        let ready = coerce().map_err(AsciiBoundaryError::Frontend)?;
        let mut invocation = Invocation::enter(scope)?;
        let result = invocation.run_args(operation, ready);
        let computed = invocation.finish(result)?;
        // No worker/cell/mutex borrow surrounds original native result packing.
        pack(materialize_computed(operation, computed)?).map_err(AsciiBoundaryError::Frontend)
    })();
    guard.disarm(); // ordinary Result::Err is never an unwind or native replay
    result
}

/// One closed operation router, sharing the legacy-named ASCII capabilities.
/// Neither frontend callback enters C4: coercion precedes admission, and native
/// packing follows the exclusive invocation. Both remain under the scope guard.
pub(crate) fn evaluate_args_in(
    operation: EvaluatedBytesOp,
    ctx: &dyn Columns,
    coerce: impl FnOnce() -> Result<EvaluatedArgs, EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    if let Some(scope) = ctx.evaluated_ascii_scope() {
        return evaluate_scoped_args(operation, scope, coerce, pack)
            .map_err(AsciiBoundaryError::into_eval_error);
    }
    if let Some(execution) = ctx.evaluated_ascii_execution() {
        return evaluate_scoped_args(operation, &execution.scope(), coerce, pack)
            .map_err(AsciiBoundaryError::into_eval_error);
    }

    let result = (|| {
        // No capability: preserve frontend precedence even before pool creation.
        let ready = coerce().map_err(AsciiBoundaryError::Frontend)?;
        // One explicit experimental policy for all closed fixed-arity recipes.
        // Retained/request allowances are not physical heap/factory-peak bounds.
        // A worker's retained cap must not become a maximum SQL string length.
        let policy = AsciiPoolPolicy::checked(1, 1, 8 << 20, 1 << 20, 2 << 20, 64, 16, usize::MAX)?;
        let owner = AsciiPoolOwner::new(policy)?;
        let execution = OneShotAsciiExecution(owner.begin_execution()?);
        // The scope/guard drop before the owned closer, including on unwind.
        let scope = execution.0.scope();
        evaluate_scoped_args(operation, &scope, || Ok(ready), pack)
    })();
    result.map_err(AsciiBoundaryError::into_eval_error)
}

/// Lowers the frontend's explicit demand record to the existing Int2 driver.
/// Invalid markers are adapter contract errors, never kernel calls or SQL NULL.
pub(crate) fn evaluate_logical_in(
    function: crate::LogicalFunction,
    arguments: crate::LogicalArgs,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    use crate::{LogicalArgs, LogicalFunction};
    let (left, right) = match arguments {
        LogicalArgs::Both(left, right) => (left, right),
        LogicalArgs::UndemandedRight { left } => {
            if !matches!(
                (function, left),
                (LogicalFunction::And, Some(false)) | (LogicalFunction::Or, Some(true))
            ) {
                return Err(AsciiBoundaryError::Scope {
                    kind: ScopeFailureKind::Contract,
                    reason: "invalid undemanded logical right argument",
                }
                .into_eval_error());
            }
            // This is an irrelevant representative, NOT an evaluated RHS.
            // Only the validated absorbing left values make it safe to supply.
            (left, Some(false))
        }
    };
    let operation = match function {
        LogicalFunction::And => EvaluatedBytesOp::LogicalAnd,
        LogicalFunction::Or => EvaluatedBytesOp::LogicalOr,
        LogicalFunction::Xor => EvaluatedBytesOp::LogicalXor,
    };
    evaluate_args_in(
        operation,
        ctx,
        || {
            Ok(EvaluatedArgs::Int2(
                left.map(i64::from),
                right.map(i64::from),
            ))
        },
        EvaluatedBytesResult::into_boolean_datum,
    )
}

/// Compatible single-Bytes entry; all shapes use the same context/pool driver.
pub(crate) fn evaluate_bytes_in(
    operation: EvaluatedBytesOp,
    ctx: &dyn Columns,
    coerce: impl FnOnce() -> Result<Option<Vec<u8>>, EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult) -> Result<Datum, EvalError>,
) -> Result<Datum, EvalError> {
    evaluate_args_in(operation, ctx, || coerce().map(EvaluatedArgs::Bytes), pack)
}

/// Compatible ASCII-only entry into the shared closed operation router.
pub(crate) fn evaluate_ascii_in(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    evaluate_bytes_in(
        EvaluatedBytesOp::Ascii,
        ctx,
        || crate::coerce::coerce_str_bytes(value),
        EvaluatedBytesResult::into_int_datum,
    )
}

/// Opaque, sized lexical Columns binding created by [`AsciiScope::with_columns`].
///
/// Borrows the original native context and effective scope, with no ownership
/// or `'static` requirement on that context. Ordinary methods forward to the
/// original context; only the two ASCII capability methods are overridden.
/// The wrapper cannot share the scope between threads and does not establish
/// business-wrapper propagation or statement/executor lifecycle ownership.
pub struct ScopedAsciiColumns<'native, 'scope> {
    native: &'native dyn Columns,
    scope: &'scope AsciiScope,
}

// One local forwarding list, not a general context/delegation framework.
// Forward the overridden method itself, NEVER reconstruct its default body.
macro_rules! forward_columns {
    ($(fn $name:ident(&$this:ident $(, $arg:ident: $ty:ty)*) $(-> $ret:ty)?;)*) => {
        $(fn $name(&$this $(, $arg: $ty)*) $(-> $ret)? {
            $this.native.$name($($arg),*)
        })*
    };
}

impl Columns for ScopedAsciiColumns<'_, '_> {
    fn evaluated_ascii_scope(&self) -> Option<&AsciiScope> {
        Some(self.scope)
    }

    fn evaluated_ascii_execution(&self) -> Option<&AsciiExecution> {
        Some(&self.scope.execution)
    }

    forward_columns! {
        fn get(&self, path: &[String]) -> Option<Datum>;
        fn context_id(&self) -> u64;
        fn use_plan_cache(&self) -> bool;
        fn skip_plan_cache_for_comparison(&self, constant: &crate::constant::Constant, target: &str);
        fn enable_vectorized_expression(&self) -> bool;
        fn param_value(&self, order: usize) -> Result<Datum, EvalError>;
        fn current_insert_value(&self, offset: usize) -> Result<Option<Datum>, EvalError>;
        fn get_param_value(&self, idx: usize) -> Result<Datum, EvalError>;
        fn bounded_staleness_safe_time(&self) -> Option<Time>;
        fn connection_charset_info(&self) -> (&str, &str);
        fn no_unsigned_subtraction(&self) -> bool;
        fn now(&self) -> Option<(i64, u32, i32)>;
        fn cast_time_to_year_through_concat(&self) -> bool;
        fn sysdate_is_now(&self) -> bool;
        fn current_database(&self) -> Option<String>;
        fn current_user(&self) -> Option<String>;
        fn login_user(&self) -> Option<String>;
        fn current_role(&self) -> Option<String>;
        fn current_resource_group(&self) -> Option<String>;
        fn connection_id(&self) -> Option<u64>;
        fn tidb_decode_key(&self, input: &[u8]) -> Vec<u8>;
        fn acquire_advisory_lock(&self, name: &str, timeout: Duration) -> Result<bool, EvalError>;
        fn advisory_lock_owner(&self, name: &str) -> Result<Option<u64>, EvalError>;
        fn release_advisory_lock(&self, name: &str) -> Result<bool, EvalError>;
        fn release_all_advisory_locks(&self) -> Result<usize, EvalError>;
        fn found_rows(&self) -> Option<u64>;
        fn current_tso(&self) -> i64;
        fn ddl_owner_info(&self) -> Result<bool, EvalError>;
        fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum>;
        fn tidb_info(&self) -> String;
        fn block_encryption_mode(&self) -> BlockEncryptionMode;
        fn division_by_zero_level(&self) -> ErrorLevel;
        fn truncate_level(&self) -> ErrorLevel;
        fn type_flags(&self) -> tidb_datatype::ConversionFlags;
        fn strict_sql_mode(&self) -> bool;
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError>;
        fn handle_group_concat_cut(&self, message: &str) -> Result<(), EvalError>;
        fn handle_sleep_incorrect_argument(&self) -> Result<(), EvalError>;
        fn sleep_for(&self, duration: Duration) -> bool;
        fn append_warning(&self, code: u16, message: &str);
        fn append_note(&self, code: u16, message: &str);
        fn warning_count(&self) -> usize;
        fn truncate_warnings(&self, bookmark: usize);
        fn take_warnings_since(&self, bookmark: usize) -> Vec<(u16, String)>;
        fn max_allowed_packet(&self) -> u64;
        fn handle_allowed_packet_overflowed(&self, expr_name: &str) -> Result<(), EvalError>;
        fn date_modes(&self) -> tidb_datatype::DateModes;
        fn handle_division_by_zero(&self) -> Result<(), EvalError>;
        fn get_uservar(&self, name: &str) -> Option<Datum>;
        fn set_uservar(&self, name: &str, value: Datum);
        fn row_count(&self) -> Option<i64>;
        fn last_insert_id(&self) -> Option<u64>;
        fn set_last_insert_id(&self, value: u64);
        fn time_zone(&self) -> SessionTimeZone;
        fn like_default_escape(&self) -> u8;
        fn default_week_format(&self) -> i64;
        fn windowing_use_high_precision(&self) -> bool;
        fn div_precision_increment(&self) -> u32;
        fn rand_next(&self) -> Option<f64>;
        fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64>;
        fn sequence_nextval(&self, path: &[String]) -> Result<Datum, EvalError>;
        fn sequence_lastval(&self, path: &[String]) -> Result<Datum, EvalError>;
        fn sequence_setval(&self, path: &[String], value: i64) -> Result<Datum, EvalError>;
    }
}

#[cfg(test)]
#[path = "evaluated_ascii_tests.rs"]
mod tests;
