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
    prepare_evaluated_bytes, CompileLimits, ComputedBytesMetadata, ComputedDecimalDivisionMetadata,
    ComputedDecimalFastMetadata, ComputedDecimalMetadata, ComputedIeee754BitsMetadata, ComputedInt,
    ComputedInt128Metadata, ComputedIntMetadata, ComputedJsonReportMetadata,
    ComputedNativeVectorMetadata, ComputedUncompressMetadata, ComputedValue, EvaluatedArgs,
    EvaluatedBytesOp, EvaluatedBytesWorker, EvaluatedSqlFailureKind, ExecutionLimits,
    JsonReportOutcome, LocalCompileContext, NativeDecimalDivisionDisposition, UncompressOutcome,
};
use tidb_query_expr::{
    BinaryArithmeticErrorKind, BinaryArithmeticOperation, ComparisonOp, NativeDecimalFastOutcome,
    NativeDecimalFastValue, NativeLikeInvocation,
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
                    // Widen only the exact closed recipes needing additional
                    // argument nodes. All retain the original depth allowance.
                    max_nodes: match self.operation {
                        EvaluatedBytesOp::RegexpSubstrNative
                        | EvaluatedBytesOp::IntDivDecimalSignedNative
                        | EvaluatedBytesOp::IntDivDecimalUnsignedNative => 6,
                        EvaluatedBytesOp::RegexpInstrNative
                        | EvaluatedBytesOp::RegexpReplaceNative => 7,
                        EvaluatedBytesOp::LpadBytesNative
                        | EvaluatedBytesOp::RpadBytesNative
                        | EvaluatedBytesOp::LpadUtf8Native
                        | EvaluatedBytesOp::RpadUtf8Native
                        | EvaluatedBytesOp::Insert
                        | EvaluatedBytesOp::InsertUtf8Native
                        | EvaluatedBytesOp::Locate3Native => 5,
                        _ => 4,
                    },
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
        let result = worker.eval_args_reported(ready);
        #[cfg(test)]
        tests::after_eval_one_for_test(worker.kernel_invocations());
        result.map_err(|report| {
            // Only C4's sealed receipt for this operation's actual generated
            // call authorizes the existing native SQL error carrier. Neither
            // an input value nor an error code/text substitutes for that proof.
            if operation == EvaluatedBytesOp::AbsIntNative
                && report.operation() == Some(operation)
                && matches!(
                    report.sql_failure(),
                    Some(EvaluatedSqlFailureKind::AbsSignedOverflow)
                )
            {
                return AsciiBoundaryError::Frontend(EvalError::IntOverflow);
            }
            if matches!(
                operation,
                EvaluatedBytesOp::ConvNative | EvaluatedBytesOp::ConvBinaryLiteralNative
            ) && report.operation() == Some(operation)
                && matches!(
                    report.sql_failure(),
                    Some(EvaluatedSqlFailureKind::ConvUnsignedOverflow)
                )
            {
                // The authenticated kernel owns the exact digits, including
                // stripping the source sign. Do not reparse the native input.
                let Some(digits) = report.conv_overflow_digits() else {
                    return AsciiBoundaryError::Scope {
                        kind: ScopeFailureKind::Contract,
                        reason: "CONV overflow receipt lacks its digit payload",
                    };
                };
                return AsciiBoundaryError::Frontend(EvalError::DataOutOfRange {
                    value: "BIGINT UNSIGNED",
                    expression: digits.to_owned(),
                });
            }
            if report.operation() == Some(operation) {
                if matches!(
                    operation,
                    EvaluatedBytesOp::AesEncrypt128CbcNative
                        | EvaluatedBytesOp::AesEncrypt192CbcNative
                        | EvaluatedBytesOp::AesEncrypt256CbcNative
                        | EvaluatedBytesOp::AesEncrypt128OfbNative
                        | EvaluatedBytesOp::AesEncrypt192OfbNative
                        | EvaluatedBytesOp::AesEncrypt256OfbNative
                        | EvaluatedBytesOp::AesEncrypt128CfbNative
                        | EvaluatedBytesOp::AesEncrypt192CfbNative
                        | EvaluatedBytesOp::AesEncrypt256CfbNative
                        | EvaluatedBytesOp::AesDecrypt128CbcNative
                        | EvaluatedBytesOp::AesDecrypt192CbcNative
                        | EvaluatedBytesOp::AesDecrypt256CbcNative
                        | EvaluatedBytesOp::AesDecrypt128OfbNative
                        | EvaluatedBytesOp::AesDecrypt192OfbNative
                        | EvaluatedBytesOp::AesDecrypt256OfbNative
                        | EvaluatedBytesOp::AesDecrypt128CfbNative
                        | EvaluatedBytesOp::AesDecrypt192CfbNative
                        | EvaluatedBytesOp::AesDecrypt256CfbNative
                ) {
                    // This accessor authenticates the actual short-IV cause,
                    // exact opcode/profile and this invocation's kernel witness.
                    // Cipher failures are successful NULL values, not this cause.
                    if let Some(cause) = report.native_aes_error() {
                        return AsciiBoundaryError::Frontend(EvalError::IncorrectArguments(
                            cause.to_string(),
                        ));
                    }
                }
                let message = match (operation, report.sql_failure()) {
                    (
                        EvaluatedBytesOp::PeriodAddNative,
                        Some(EvaluatedSqlFailureKind::PeriodAddIncorrectArguments),
                    ) => Some("Incorrect arguments to period_add"),
                    (
                        EvaluatedBytesOp::PeriodDiffNative,
                        Some(EvaluatedSqlFailureKind::PeriodDiffIncorrectArguments),
                    ) => Some("Incorrect arguments to period_diff"),
                    _ => None,
                };
                if let Some(message) = message {
                    return AsciiBoundaryError::Frontend(EvalError::IncorrectArguments(
                        message.to_owned(),
                    ));
                }
                match (operation, report.sql_failure()) {
                    (
                        EvaluatedBytesOp::UuidToBinParseNative,
                        Some(EvaluatedSqlFailureKind::UuidToBinWhitespace),
                    ) => {
                        return AsciiBoundaryError::Frontend(EvalError::Unsupported(
                            "invalid UUID_TO_BIN whitespace",
                        ));
                    }
                    (
                        EvaluatedBytesOp::UuidToBinParseNative,
                        Some(EvaluatedSqlFailureKind::UuidToBinInvalid),
                    ) => {
                        return AsciiBoundaryError::Frontend(EvalError::Unsupported(
                            "invalid UUID for UUID_TO_BIN",
                        ));
                    }
                    (
                        EvaluatedBytesOp::UuidVersionNative,
                        Some(EvaluatedSqlFailureKind::UuidVersionInvalid),
                    ) => {
                        return AsciiBoundaryError::Frontend(EvalError::Unsupported(
                            "invalid UUID for UUID_VERSION",
                        ));
                    }
                    (
                        EvaluatedBytesOp::UuidTimestampNative,
                        Some(EvaluatedSqlFailureKind::UuidTimestampInvalid),
                    ) => {
                        return AsciiBoundaryError::Frontend(EvalError::Unsupported(
                            "invalid UUID for UUID_TIMESTAMP",
                        ));
                    }
                    (
                        EvaluatedBytesOp::BinToUuidNative,
                        Some(EvaluatedSqlFailureKind::BinToUuidInvalidLength),
                    ) => {
                        // The receipt owns the exact rejected byte payload;
                        // never re-read or revalidate the frontend argument.
                        let Some(input) = report.bin_to_uuid_input() else {
                            return AsciiBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "BIN_TO_UUID length receipt lacks its input payload",
                            };
                        };
                        return AsciiBoundaryError::Frontend(EvalError::WrongValueForType {
                            value_class: "string",
                            value: String::from_utf8_lossy(input).into_owned(),
                            function: "bin_to_uuid",
                        });
                    }
                    (
                        EvaluatedBytesOp::AddIntSsNative
                        | EvaluatedBytesOp::AddIntSuNative
                        | EvaluatedBytesOp::AddIntUsNative
                        | EvaluatedBytesOp::AddIntUuNative
                        | EvaluatedBytesOp::SubIntSsNative
                        | EvaluatedBytesOp::SubIntSuNative
                        | EvaluatedBytesOp::SubIntUsNative
                        | EvaluatedBytesOp::SubIntUuNative
                        | EvaluatedBytesOp::SubIntSuForcedNative
                        | EvaluatedBytesOp::SubIntUsForcedNative
                        | EvaluatedBytesOp::SubIntUuForcedNative
                        | EvaluatedBytesOp::MulIntSignedNative
                        | EvaluatedBytesOp::MulIntUnsignedNative
                        | EvaluatedBytesOp::IntDivIntSsNative
                        | EvaluatedBytesOp::IntDivIntUsNative
                        | EvaluatedBytesOp::IntDivIntSuNative
                        | EvaluatedBytesOp::IntDivIntUuNative
                        | EvaluatedBytesOp::AddRealNative
                        | EvaluatedBytesOp::SubRealNative
                        | EvaluatedBytesOp::MulRealNative
                        | EvaluatedBytesOp::ModRealNative
                        | EvaluatedBytesOp::DivRealNative
                        | EvaluatedBytesOp::AddDecimalNative
                        | EvaluatedBytesOp::SubDecimalNative
                        | EvaluatedBytesOp::MulDecimalNative,
                        Some(EvaluatedSqlFailureKind::BinaryArithmeticNative),
                    ) => {
                        let Some(cause) = report.native_binary_arithmetic_error() else {
                            return AsciiBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "binary arithmetic failure receipt lacks its native cause",
                            };
                        };
                        if operation == EvaluatedBytesOp::ModRealNative
                            && (cause.operation != BinaryArithmeticOperation::Modulo
                                || cause.kind != BinaryArithmeticErrorKind::FloatOverflow)
                        {
                            return AsciiBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "native modulo failure receipt has an unexpected cause",
                            };
                        }
                        if operation == EvaluatedBytesOp::DivRealNative
                            && (cause.operation != BinaryArithmeticOperation::Divide
                                || cause.kind != BinaryArithmeticErrorKind::FloatOverflow)
                        {
                            return AsciiBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "native division failure receipt has an unexpected cause",
                            };
                        }
                        if matches!(operation,
                            EvaluatedBytesOp::IntDivIntSsNative | EvaluatedBytesOp::IntDivIntUsNative
                                | EvaluatedBytesOp::IntDivIntSuNative | EvaluatedBytesOp::IntDivIntUuNative)
                            && (cause.operation != BinaryArithmeticOperation::IntDivide
                                || cause.kind != BinaryArithmeticErrorKind::IntOverflow)
                        {
                            return AsciiBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "native integer division failure receipt has an unexpected cause",
                            };
                        }
                        return AsciiBoundaryError::Frontend(match cause.kind {
                            BinaryArithmeticErrorKind::IntOverflow => EvalError::IntOverflow,
                            BinaryArithmeticErrorKind::FloatOverflow => EvalError::FloatOverflow,
                            BinaryArithmeticErrorKind::DecimalOverflow => {
                                EvalError::DecimalOverflow
                            }
                        });
                    }
                    (
                        EvaluatedBytesOp::AddInt128SignedLegacy
                        | EvaluatedBytesOp::AddInt128UnsignedLegacy
                        | EvaluatedBytesOp::AddInt128RejectLeftLegacy
                        | EvaluatedBytesOp::AddInt128RejectRightLegacy
                        | EvaluatedBytesOp::SubInt128SignedLegacy
                        | EvaluatedBytesOp::SubInt128UnsignedLegacy
                        | EvaluatedBytesOp::SubInt128RejectLeftLegacy
                        | EvaluatedBytesOp::SubInt128RejectRightLegacy
                        | EvaluatedBytesOp::MulInt128SignedLegacy
                        | EvaluatedBytesOp::MulInt128UnsignedLegacy,
                        Some(EvaluatedSqlFailureKind::BinaryArithmeticLegacy),
                    ) => {
                        let Some(cause) = report.legacy_binary_arithmetic_error() else {
                            return AsciiBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "binary arithmetic failure receipt lacks its legacy cause",
                            };
                        };
                        let expression = match cause.operation {
                            BinaryArithmeticOperation::Add => "ADD",
                            BinaryArithmeticOperation::Subtract => "SUBTRACT",
                            BinaryArithmeticOperation::Multiply => "MULTIPLY",
                            BinaryArithmeticOperation::Modulo => {
                                return AsciiBoundaryError::Scope {
                                    kind: ScopeFailureKind::Contract,
                                    reason: "legacy modulo has no arithmetic SQL failure",
                                };
                            }
                            BinaryArithmeticOperation::Divide | BinaryArithmeticOperation::IntDivide => {
                                return AsciiBoundaryError::Scope {
                                    kind: ScopeFailureKind::Contract,
                                    reason: "legacy division has no integer arithmetic SQL failure",
                                };
                            }
                        };
                        return AsciiBoundaryError::Frontend(EvalError::DataOutOfRange {
                            value: if cause.unsigned {
                                "BIGINT UNSIGNED"
                            } else {
                                "BIGINT"
                            },
                            expression: expression.to_owned(),
                        });
                    }
                    (
                        EvaluatedBytesOp::UnaryMinusIntNative
                        | EvaluatedBytesOp::UnaryMinusUIntNative,
                        Some(EvaluatedSqlFailureKind::UnaryMinusNative),
                    ) => {
                        let Some(cause) = report.native_unary_minus_error() else {
                            return AsciiBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "unary-minus failure receipt lacks its native cause",
                            };
                        };
                        // Render the authenticated source bits, not a new negation.
                        // Signed MIN retains the original double minus in its text.
                        let expression = if cause.unsigned {
                            format!("-{}", cause.bits)
                        } else {
                            format!("-{}", cause.bits as i64)
                        };
                        return AsciiBoundaryError::Frontend(EvalError::DataOutOfRange {
                            value: "BIGINT",
                            expression,
                        });
                    }
                    (
                        EvaluatedBytesOp::RegexpLikeNative
                        | EvaluatedBytesOp::RegexpSubstrNative
                        | EvaluatedBytesOp::RegexpInstrNative
                        | EvaluatedBytesOp::RegexpReplaceNative,
                        Some(EvaluatedSqlFailureKind::RegexpNative),
                    ) => {
                        let Some(cause) = report.native_regexp_error() else {
                            return AsciiBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "regexp failure receipt lacks its native cause",
                            };
                        };
                        return AsciiBoundaryError::Frontend(crate::regexp::native_regexp_error(
                            cause,
                        ));
                    }
                    (
                        EvaluatedBytesOp::VecFromTextNative
                        | EvaluatedBytesOp::VecL1DistanceNative
                        | EvaluatedBytesOp::VecL2DistanceNative
                        | EvaluatedBytesOp::VecNegativeInnerProductNative
                        | EvaluatedBytesOp::VecCosineDistanceNative
                        | EvaluatedBytesOp::AddVectorNative
                        | EvaluatedBytesOp::SubVectorNative
                        | EvaluatedBytesOp::MulVectorNative,
                        Some(EvaluatedSqlFailureKind::VectorNative),
                    ) => {
                        // Render only the actual typed cause. Do not parse the
                        // input again or recompute vector dimensions here.
                        let Some(cause) = report.native_vector_error() else {
                            return AsciiBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "vector failure receipt lacks its native cause",
                            };
                        };
                        return AsciiBoundaryError::Frontend(EvalError::Vector(cause.to_string()));
                    }
                    _ => {}
                }
            }
            AsciiBoundaryError::Kernel(ExpressionRuntimeFailure::from_ascii_local(
                report.into_error(),
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

pub(crate) fn native_time_result_contract_error() -> EvalError {
    result_kind_error().into_eval_error()
}

fn require_computed_int(computed: ComputedValue) -> Result<ComputedInt, AsciiBoundaryError> {
    match computed {
        ComputedValue::Int(value) => Ok(value),
        ComputedValue::Bytes(_)
        | ComputedValue::Ieee754Bits(_)
        | ComputedValue::Decimal(_)
        | ComputedValue::Int128(_)
        | ComputedValue::Uncompress(_)
        | ComputedValue::JsonReport(_)
        | ComputedValue::NativeVector(_)
        | ComputedValue::DecimalFast(_)
        | ComputedValue::DecimalDivision(_) => Err(result_kind_error()),
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
    // Decoder outcomes are not ordinary Bytes; warnings remain native packing.
    Uncompress(UncompressOutcome),
    // A parser report is distinct even when its payload is Bytes, Int or NULL.
    JsonReport(JsonReportOutcome),
    // Separate owned carriers: never ordinary Bytes or a narrower SQL Int.
    Ieee754Bits(Option<u64>),
    Int128(Option<i128>),
    Decimal {
        value: Option<tidb_datatype::Decimal>,
        // Supplied only by C4's ceil/floor decimal result; no native rounding
        // or range check is permitted when selecting the existing Int view.
        checked_i64_view: Option<i64>,
    },
    // Division carries the actual kernel disposition, not a reconstructed status.
    DecimalDivision {
        value: Option<tidb_datatype::Decimal>,
        disposition: NativeDecimalDivisionDisposition,
    },
    // The actual aligned vector, never ordinary Bytes or an input descriptor.
    NativeVector(Option<tidb_datatype::VectorFloat32>),
    // The decoder has already distinguished Unsupported, SQL NULL and a value.
    DecimalFast(NativeDecimalFastOutcome),
}

impl EvaluatedBytesResult {
    pub(crate) fn into_int_datum(self) -> Result<Datum, EvalError> {
        match self {
            Self::Int(value) => Ok(value),
            Self::Bytes(_)
            | Self::Uncompress(_)
            | Self::JsonReport(_)
            | Self::Ieee754Bits(_)
            | Self::Int128(_)
            | Self::NativeVector(_)
            | Self::DecimalFast(_)
            | Self::Decimal { .. }
            | Self::DecimalDivision { .. } => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Reconstruct the DATE representation from the worker's actual core bits.
    /// The signed carrier preserves all 64 bits, including the high year bit.
    pub(crate) fn into_date_core_datum(self) -> Result<Datum, EvalError> {
        match self.into_int_datum()? {
            Datum::Null => Ok(Datum::Null),
            Datum::Int(bits) => Time::new(
                tidb_datatype::CoreTime::from_raw(bits as u64),
                tidb_datatype::TimeType::Date,
                0,
            )
            .map(Datum::Time)
            .map_err(|_| result_kind_error().into_eval_error()),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Pack only a computed boolean carrier; do not recalculate its truth.
    pub(crate) fn into_boolean_datum(self) -> Result<Datum, EvalError> {
        match self {
            Self::Int(value @ (Datum::Int(0) | Datum::Int(1) | Datum::Null)) => Ok(value),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Project an already checked boolean result for the original non-NULL API.
    pub(crate) fn into_nonnull_bool(self) -> Result<bool, EvalError> {
        match self {
            Self::Int(Datum::Int(0)) => Ok(false),
            Self::Int(Datum::Int(1)) => Ok(true),
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
            Self::Int(_)
            | Self::Uncompress(_)
            | Self::JsonReport(_)
            | Self::Ieee754Bits(_)
            | Self::Int128(_)
            | Self::NativeVector(_)
            | Self::DecimalFast(_)
            | Self::Decimal { .. }
            | Self::DecimalDivision { .. } => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Reconstruct identity exclusively from the worker's actual nullable bytes.
    pub(crate) fn into_identity_datum(self) -> Result<Datum, EvalError> {
        super::identity_value::decode(self.into_bytes()?)
    }

    /// Represent only the worker's computed JSON text as native BinaryJSON.
    /// JSON `null` is a present document; only absent computed bytes are SQL NULL.
    pub(crate) fn into_json_datum(self) -> Result<Datum, EvalError> {
        let Some(bytes) = self.into_bytes()? else {
            return Ok(Datum::Null);
        };
        let text = std::str::from_utf8(&bytes).map_err(|_| {
            AsciiBoundaryError::Scope {
                kind: ScopeFailureKind::Contract,
                reason: "computed JSON text is not UTF-8",
            }
            .into_eval_error()
        })?;
        tidb_datatype::BinaryJSON::parse(text)
            .map(Datum::Json)
            .map_err(|_| EvalError::Json(crate::JsonError::InvalidText))
    }

    /// Move only the kernel's decoded outcome; do not inspect input or bytes.
    pub(crate) fn into_uncompress(self) -> Result<UncompressOutcome, EvalError> {
        match self {
            Self::Uncompress(outcome) => Ok(outcome),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Move only the kernel's typed report; do not parse or inspect the input.
    pub(crate) fn into_json_report(self) -> Result<JsonReportOutcome, EvalError> {
        match self {
            Self::JsonReport(outcome) => Ok(outcome),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Require the computed bytes themselves; NULL is not an empty result.
    pub(crate) fn into_nonnull_bytes(self) -> Result<Vec<u8>, EvalError> {
        match self {
            Self::Bytes(Some(value)) => Ok(value),
            _ => Err(result_kind_error().into_eval_error()),
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
            Self::Int(_)
            | Self::Bytes(_)
            | Self::Uncompress(_)
            | Self::JsonReport(_)
            | Self::Int128(_)
            | Self::NativeVector(_)
            | Self::DecimalFast(_)
            | Self::Decimal { .. }
            | Self::DecimalDivision { .. } => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Move the kernel-decoded outcome without parsing its private wire format.
    pub(crate) fn into_decimal_fast_outcome(self) -> Result<NativeDecimalFastOutcome, EvalError> {
        match self {
            Self::DecimalFast(outcome) => Ok(outcome),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Move the computed aligned vector without parsing or revalidation.
    pub(crate) fn into_native_vector_datum(self) -> Result<Datum, EvalError> {
        match self {
            Self::NativeVector(value) => Ok(value.map_or(Datum::Null, Datum::VectorFloat32)),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Preserve the complete legacy integer carrier without a Datum detour.
    pub(crate) fn into_int128(self) -> Result<Option<i128>, EvalError> {
        match self {
            Self::Int128(value) => Ok(value),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    pub(crate) fn into_decimal_datum(self) -> Result<Datum, EvalError> {
        match self {
            Self::Decimal { value, .. } => Ok(value.map_or(Datum::Null, Datum::Decimal)),
            _ => Err(result_kind_error().into_eval_error()),
        }
    }

    /// Select only the checked view already owned by the decimal C4 result.
    /// An out-of-range view retains the exact computed decimal, as before.
    pub(crate) fn into_decimal_or_int_datum(self) -> Result<Datum, EvalError> {
        match self {
            Self::Decimal {
                value: None,
                checked_i64_view: None,
            } => Ok(Datum::Null),
            Self::Decimal {
                value: Some(value),
                checked_i64_view,
            } => Ok(checked_i64_view.map_or(Datum::Decimal(value), Datum::Int)),
            _ => Err(result_kind_error().into_eval_error()),
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
            | EvaluatedBytesOp::IsIpv4MappedNullable
            | EvaluatedBytesOp::OrdNative
            | EvaluatedBytesOp::UncompressedLengthNative
            | EvaluatedBytesOp::StrcmpNative
            | EvaluatedBytesOp::Locate2Native
            | EvaluatedBytesOp::Locate3Native
            | EvaluatedBytesOp::Locate3BytesExtNative
            | EvaluatedBytesOp::Locate3Utf8ExtNative
            | EvaluatedBytesOp::FindInSetNative
            | EvaluatedBytesOp::FindInSetPreparedNative
            | EvaluatedBytesOp::FieldBytesNative
            | EvaluatedBytesOp::FieldIntNative
            | EvaluatedBytesOp::FieldRealNative
            | EvaluatedBytesOp::JsonValidTextNative
            | EvaluatedBytesOp::JsonValidBinaryNative
            | EvaluatedBytesOp::JsonValidOtherNative
            | EvaluatedBytesOp::YearCoreNative
            | EvaluatedBytesOp::MonthCoreNative
            | EvaluatedBytesOp::DayOfMonthCoreNative
            | EvaluatedBytesOp::QuarterCoreNative
            | EvaluatedBytesOp::HourTextNative
            | EvaluatedBytesOp::MinuteTextNative
            | EvaluatedBytesOp::SecondTextNative
            | EvaluatedBytesOp::HourNanosNative
            | EvaluatedBytesOp::MinuteNanosNative
            | EvaluatedBytesOp::SecondNanosNative
            | EvaluatedBytesOp::TimeToSecTextNative
            | EvaluatedBytesOp::PeriodAddNative
            | EvaluatedBytesOp::PeriodDiffNative
            | EvaluatedBytesOp::DayOfWeekTextNative
            | EvaluatedBytesOp::WeekdayTextNative
            | EvaluatedBytesOp::DayOfYearTextNative
            | EvaluatedBytesOp::DateDiffTextNative
            | EvaluatedBytesOp::DateDiffNullNative
            | EvaluatedBytesOp::DateDiffCoreNative
            | EvaluatedBytesOp::DateCoreNative
            | EvaluatedBytesOp::DateCorePredicateLegacy
            | EvaluatedBytesOp::ToDaysTextNative
            | EvaluatedBytesOp::ToSecondsTextNative
            | EvaluatedBytesOp::TsoLogicalNative
            | EvaluatedBytesOp::WeekTextNative
            | EvaluatedBytesOp::YearWeekTextNative
            | EvaluatedBytesOp::WeekOfYearTextNative
            | EvaluatedBytesOp::WeekNullNative
            | EvaluatedBytesOp::WeekCoreNative
            | EvaluatedBytesOp::DateFormatMissingNative
            | EvaluatedBytesOp::IsUuidNative
            | EvaluatedBytesOp::UuidVersionNative
            | EvaluatedBytesOp::TidbShardNative
            | EvaluatedBytesOp::VitessHashNative
            | EvaluatedBytesOp::VecDimsNative
            | EvaluatedBytesOp::LikeNative
            | EvaluatedBytesOp::IlikeNative
            | EvaluatedBytesOp::LikeLegacyNative
            | EvaluatedBytesOp::LikeNullIntNative
            | EvaluatedBytesOp::LikeMissingLegacyNative
            | EvaluatedBytesOp::RegexpLikeNative
            | EvaluatedBytesOp::RegexpInstrNative
            | EvaluatedBytesOp::RegexpLikeLegacyCiNative
            | EvaluatedBytesOp::RegexpLikeLegacyBinNative
            | EvaluatedBytesOp::RegexpNullIntNative
            | EvaluatedBytesOp::RegexpMissingLegacyNative
            | EvaluatedBytesOp::UnaryPlusIntNative
            | EvaluatedBytesOp::UnaryMinusIntNative
            | EvaluatedBytesOp::UnaryMinusUIntNative
            | EvaluatedBytesOp::UnaryNullNative
            | EvaluatedBytesOp::AddIntSsNative
            | EvaluatedBytesOp::AddIntSuNative
            | EvaluatedBytesOp::AddIntUsNative
            | EvaluatedBytesOp::AddIntUuNative
            | EvaluatedBytesOp::SubIntSsNative
            | EvaluatedBytesOp::SubIntSuNative
            | EvaluatedBytesOp::SubIntUsNative
            | EvaluatedBytesOp::SubIntUuNative
            | EvaluatedBytesOp::SubIntSuForcedNative
            | EvaluatedBytesOp::SubIntUsForcedNative
            | EvaluatedBytesOp::SubIntUuForcedNative
            | EvaluatedBytesOp::MulIntSignedNative
            | EvaluatedBytesOp::MulIntUnsignedNative
            | EvaluatedBytesOp::ModIntSsNative
            | EvaluatedBytesOp::ModIntSuNative
            | EvaluatedBytesOp::ModIntUsNative
            | EvaluatedBytesOp::ModIntUuNative
            | EvaluatedBytesOp::IntDivIntSsNative
            | EvaluatedBytesOp::IntDivIntUsNative
            | EvaluatedBytesOp::IntDivIntSuNative
            | EvaluatedBytesOp::IntDivIntUuNative
            | EvaluatedBytesOp::IntDivDecimalLegacy
            | EvaluatedBytesOp::MicrosecondNative
            | EvaluatedBytesOp::MicrosecondLegacy
            | EvaluatedBytesOp::BinaryArithmeticNullNative
            | EvaluatedBytesOp::BinaryArithmeticMissingLegacy
            | EvaluatedBytesOp::CompareIntSsNative(_)
            | EvaluatedBytesOp::CompareIntSuNative(_)
            | EvaluatedBytesOp::CompareIntUsNative(_)
            | EvaluatedBytesOp::CompareIntUuNative(_)
            | EvaluatedBytesOp::CompareInt128Legacy(_)
            | EvaluatedBytesOp::CompareRealNative(_)
            | EvaluatedBytesOp::CompareRealLegacy(_)
            | EvaluatedBytesOp::CompareDecimalNative(_)
            | EvaluatedBytesOp::CompareBytesNative(_)
            | EvaluatedBytesOp::CompareVectorNative(_)
            | EvaluatedBytesOp::CompareTimeCoreNative(_)
            | EvaluatedBytesOp::CompareDurationNative(_)
            | EvaluatedBytesOp::CompareJsonNative(_)
            | EvaluatedBytesOp::CompareNullNative
            | EvaluatedBytesOp::CompareMissingLegacy
            | EvaluatedBytesOp::GroupingBitAndNative
            | EvaluatedBytesOp::GroupingNumericCmpNative
            | EvaluatedBytesOp::GroupingNumericSetNative
            | EvaluatedBytesOp::GroupingNullNative
            | EvaluatedBytesOp::JsonContainsSerdeNative
            | EvaluatedBytesOp::JsonContainsPathSerdeNative
            | EvaluatedBytesOp::JsonOverlapsSerdeNative
            | EvaluatedBytesOp::JsonMemberOfSerdeNative
            | EvaluatedBytesOp::JsonLengthSerdeNative
            | EvaluatedBytesOp::JsonLengthPathSerdeNative
            | EvaluatedBytesOp::JsonPathExistsSerdeNative
            | EvaluatedBytesOp::JsonMemberOfBinaryLegacy
            | EvaluatedBytesOp::JsonPredicateNullNative
            | EvaluatedBytesOp::JsonPredicateMissingLegacy
            | EvaluatedBytesOp::AbsIntNative
            | EvaluatedBytesOp::AbsUIntNative
            | EvaluatedBytesOp::CeilIntNative
            | EvaluatedBytesOp::FloorIntNative
            | EvaluatedBytesOp::RoundIntNative
            | EvaluatedBytesOp::RoundIntWithScaleNative
            | EvaluatedBytesOp::TruncateIntNative
            | EvaluatedBytesOp::TruncateUIntNative
            | EvaluatedBytesOp::TruncateIntUnsignedScaleNative
            | EvaluatedBytesOp::MathNullWitnessNative,
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
            | EvaluatedBytesOp::Inet6Ntoa
            | EvaluatedBytesOp::SpaceNative
            | EvaluatedBytesOp::RepeatNative
            | EvaluatedBytesOp::ToBase64Native
            | EvaluatedBytesOp::FromBase64Native
            | EvaluatedBytesOp::FromBase64ValueNative
            | EvaluatedBytesOp::Lower
            | EvaluatedBytesOp::Upper
            | EvaluatedBytesOp::LowerUtf8Ready
            | EvaluatedBytesOp::UpperUtf8Ready
            | EvaluatedBytesOp::LowerAsciiNative
            | EvaluatedBytesOp::UpperAsciiNative
            | EvaluatedBytesOp::Sha2Native
            | EvaluatedBytesOp::TrimBothNative
            | EvaluatedBytesOp::TrimLeadingNative
            | EvaluatedBytesOp::TrimTrailingNative
            | EvaluatedBytesOp::SubstringIndexSignedNative
            | EvaluatedBytesOp::SubstringIndexUnsignedNative
            | EvaluatedBytesOp::LpadBytesNative
            | EvaluatedBytesOp::RpadBytesNative
            | EvaluatedBytesOp::LpadUtf8Native
            | EvaluatedBytesOp::RpadUtf8Native
            | EvaluatedBytesOp::Insert
            | EvaluatedBytesOp::InsertUtf8Native
            | EvaluatedBytesOp::Substring2BytesNative
            | EvaluatedBytesOp::Substring2Utf8Native
            | EvaluatedBytesOp::Substring3BytesNative
            | EvaluatedBytesOp::Substring3Utf8Native
            | EvaluatedBytesOp::Substring2BytesLegacy
            | EvaluatedBytesOp::Substring2Utf8Legacy
            | EvaluatedBytesOp::Substring3BytesLegacy
            | EvaluatedBytesOp::Substring3Utf8Legacy
            | EvaluatedBytesOp::OctInt
            | EvaluatedBytesOp::OctStringNative
            | EvaluatedBytesOp::ConcatNative
            | EvaluatedBytesOp::ConcatWsNative
            | EvaluatedBytesOp::EltNative
            | EvaluatedBytesOp::MakeSetNative
            | EvaluatedBytesOp::ExportSetNative
            | EvaluatedBytesOp::CharNative
            | EvaluatedBytesOp::ConvNative
            | EvaluatedBytesOp::ConvBinaryLiteralNative
            | EvaluatedBytesOp::ConvLegacy
            | EvaluatedBytesOp::CompressGoNative
            | EvaluatedBytesOp::JsonQuoteNative
            | EvaluatedBytesOp::MonthNameTextNative
            | EvaluatedBytesOp::GetFormatNative
            | EvaluatedBytesOp::GetFormatNullNative
            | EvaluatedBytesOp::DayNameTextNative
            | EvaluatedBytesOp::WeekDateTextNative
            | EvaluatedBytesOp::PasswordNative
            | EvaluatedBytesOp::Sm3Native
            | EvaluatedBytesOp::MakeDateNative
            | EvaluatedBytesOp::FromDaysNative
            | EvaluatedBytesOp::SecToTimeNative
            | EvaluatedBytesOp::DateFormatTextNative
            | EvaluatedBytesOp::DateFormatCoreNative
            | EvaluatedBytesOp::DateFormatNullNative
            | EvaluatedBytesOp::DurationTextProbeNative
            | EvaluatedBytesOp::TimeFormatTextNative
            | EvaluatedBytesOp::LastDayTextNative
            | EvaluatedBytesOp::UuidToBinParseNative
            | EvaluatedBytesOp::UuidToBinSwapNative
            | EvaluatedBytesOp::BinToUuidNative
            | EvaluatedBytesOp::TranslateUtf8Native
            | EvaluatedBytesOp::TranslateBinaryNative
            | EvaluatedBytesOp::TranslateNullNative
            | EvaluatedBytesOp::SqlEncodeNative
            | EvaluatedBytesOp::SqlDecodeNative
            | EvaluatedBytesOp::SqlCryptNullNative
            | EvaluatedBytesOp::AesEncrypt128EcbNative
            | EvaluatedBytesOp::AesEncrypt192EcbNative
            | EvaluatedBytesOp::AesEncrypt256EcbNative
            | EvaluatedBytesOp::AesEncrypt128CbcNative
            | EvaluatedBytesOp::AesEncrypt192CbcNative
            | EvaluatedBytesOp::AesEncrypt256CbcNative
            | EvaluatedBytesOp::AesEncrypt128OfbNative
            | EvaluatedBytesOp::AesEncrypt192OfbNative
            | EvaluatedBytesOp::AesEncrypt256OfbNative
            | EvaluatedBytesOp::AesEncrypt128CfbNative
            | EvaluatedBytesOp::AesEncrypt192CfbNative
            | EvaluatedBytesOp::AesEncrypt256CfbNative
            | EvaluatedBytesOp::AesDecrypt128EcbNative
            | EvaluatedBytesOp::AesDecrypt192EcbNative
            | EvaluatedBytesOp::AesDecrypt256EcbNative
            | EvaluatedBytesOp::AesDecrypt128CbcNative
            | EvaluatedBytesOp::AesDecrypt192CbcNative
            | EvaluatedBytesOp::AesDecrypt256CbcNative
            | EvaluatedBytesOp::AesDecrypt128OfbNative
            | EvaluatedBytesOp::AesDecrypt192OfbNative
            | EvaluatedBytesOp::AesDecrypt256OfbNative
            | EvaluatedBytesOp::AesDecrypt128CfbNative
            | EvaluatedBytesOp::AesDecrypt192CfbNative
            | EvaluatedBytesOp::AesDecrypt256CfbNative
            | EvaluatedBytesOp::AesNullNative
            | EvaluatedBytesOp::JsonArraySerdeNative
            | EvaluatedBytesOp::JsonObjectSerdeNative
            | EvaluatedBytesOp::JsonKeysSerdeNative
            | EvaluatedBytesOp::JsonKeysPathSerdeNative
            | EvaluatedBytesOp::JsonPrettySerdeNative
            | EvaluatedBytesOp::JsonOutputNullNative
            | EvaluatedBytesOp::JsonExtractSerdeNative
            | EvaluatedBytesOp::JsonSearchSerdeNative
            | EvaluatedBytesOp::JsonInsertSerdeNative
            | EvaluatedBytesOp::JsonSetSerdeNative
            | EvaluatedBytesOp::JsonReplaceSerdeNative
            | EvaluatedBytesOp::JsonRemoveSerdeNative
            | EvaluatedBytesOp::JsonArrayAppendSerdeNative
            | EvaluatedBytesOp::JsonArrayInsertSerdeNative
            | EvaluatedBytesOp::JsonReplaceRawLegacy
            | EvaluatedBytesOp::JsonArrayAppendRawLegacy
            | EvaluatedBytesOp::JsonArrayAppendEmptyLegacy
            | EvaluatedBytesOp::JsonValueAbsentLegacy
            | EvaluatedBytesOp::JsonUnquoteTextNative
            | EvaluatedBytesOp::JsonUnquoteBinaryNative
            | EvaluatedBytesOp::UtcDateNative
            | EvaluatedBytesOp::UtcTimestampNative
            | EvaluatedBytesOp::CurrentTimeWithoutFspNative
            | EvaluatedBytesOp::CurrentTimeWithFspNative
            | EvaluatedBytesOp::UtcTimeWithoutFspNative
            | EvaluatedBytesOp::UtcTimeWithFspNative
            | EvaluatedBytesOp::UtcTimeNullNative
            | EvaluatedBytesOp::NowNative
            | EvaluatedBytesOp::CurrentDateNative
            | EvaluatedBytesOp::SysdateNative
            | EvaluatedBytesOp::JsonMergeSerdeNative
            | EvaluatedBytesOp::JsonMergePatchSerdeNative
            | EvaluatedBytesOp::JsonMergePatchRawLegacy
            | EvaluatedBytesOp::WeightStringNative
            | EvaluatedBytesOp::WeightStringCharNative
            | EvaluatedBytesOp::WeightStringBinaryNative
            | EvaluatedBytesOp::WeightStringNumericNative
            | EvaluatedBytesOp::FormatLocaleNative
            | EvaluatedBytesOp::AnyValueNative
            | EvaluatedBytesOp::NameConstNative
            | EvaluatedBytesOp::IntDivDecimalSignedNative
            | EvaluatedBytesOp::IntDivDecimalUnsignedNative
            | EvaluatedBytesOp::TidbParseTsoNative
            | EvaluatedBytesOp::TimeDiffTextNative
            | EvaluatedBytesOp::TimeNative
            | EvaluatedBytesOp::AddTimeNative
            | EvaluatedBytesOp::SubTimeNative
            | EvaluatedBytesOp::TimeAddRightDatetimeNative
            | EvaluatedBytesOp::TimestampAddNative
            | EvaluatedBytesOp::TimestampAddPrefixNullNative
            | EvaluatedBytesOp::DateLiteralNative
            | EvaluatedBytesOp::TimestampLiteralNative
            | EvaluatedBytesOp::ConvertTzNative
            | EvaluatedBytesOp::Timestamp1Native
            | EvaluatedBytesOp::Timestamp2BaseNative
            | EvaluatedBytesOp::Timestamp2AddNative
            | EvaluatedBytesOp::TimestampNullNative
            | EvaluatedBytesOp::UnixTimestampNowNative
            | EvaluatedBytesOp::UnixTimestampNullNative
            | EvaluatedBytesOp::UnixTimestampParseNative
            | EvaluatedBytesOp::UnixTimestampValueNative
            | EvaluatedBytesOp::UnixTimestampIntLegacy
            | EvaluatedBytesOp::UnixTimestampDecLegacy
            | EvaluatedBytesOp::FromUnixTimeNumericNative
            | EvaluatedBytesOp::FromUnixTimeTextNative
            | EvaluatedBytesOp::FromUnixTimeLocalNative
            | EvaluatedBytesOp::FromUnixTimeLegacy
            | EvaluatedBytesOp::FromUnixTimeNullNative
            | EvaluatedBytesOp::IfNullHeadNative
            | EvaluatedBytesOp::IfNullFinishNative
            | EvaluatedBytesOp::IfHeadNative
            | EvaluatedBytesOp::IfFinishNative
            | EvaluatedBytesOp::FormatBytesNative
            | EvaluatedBytesOp::FormatNanoTimeNative
            | EvaluatedBytesOp::VecAsTextNative
            | EvaluatedBytesOp::RegexpSubstrNative
            | EvaluatedBytesOp::RegexpReplaceNative
            | EvaluatedBytesOp::RegexpNullBytesNative
            | EvaluatedBytesOp::UnaryPlusBytesNative,
            ComputedValue::Bytes(value),
        ) => {
            match value.metadata() {
                ComputedBytesMetadata::OwnBytes => {}
            }
            Ok(EvaluatedBytesResult::Bytes(value.into_option()))
        }
        (
            EvaluatedBytesOp::VecFromTextNative
            | EvaluatedBytesOp::AddVectorNative
            | EvaluatedBytesOp::SubVectorNative
            | EvaluatedBytesOp::MulVectorNative,
            ComputedValue::NativeVector(value),
        ) => {
            match value.metadata() {
                ComputedNativeVectorMetadata::OwnNativeVector => {}
            }
            Ok(EvaluatedBytesResult::NativeVector(value.into_value()))
        }
        (EvaluatedBytesOp::UncompressNative, ComputedValue::Uncompress(value)) => {
            match value.metadata() {
                ComputedUncompressMetadata::OwnUncompress => {}
            }
            Ok(EvaluatedBytesResult::Uncompress(value.into_outcome()))
        }
        (
            EvaluatedBytesOp::JsonTypeTextNative
            | EvaluatedBytesOp::JsonTypeBinaryNative
            | EvaluatedBytesOp::JsonDepthNative
            | EvaluatedBytesOp::JsonStorageFreeNative
            | EvaluatedBytesOp::JsonStorageSizeNative,
            ComputedValue::JsonReport(value),
        ) => {
            match value.metadata() {
                ComputedJsonReportMetadata::OwnJsonReport => {}
            }
            Ok(EvaluatedBytesResult::JsonReport(value.into_outcome()))
        }
        (
            EvaluatedBytesOp::AsinRaw
            | EvaluatedBytesOp::AcosRaw
            | EvaluatedBytesOp::SqrtRaw
            | EvaluatedBytesOp::RadiansRaw
            | EvaluatedBytesOp::DegreesRaw
            | EvaluatedBytesOp::PiRaw
            | EvaluatedBytesOp::ExpGoNative
            | EvaluatedBytesOp::Log10GoNative
            | EvaluatedBytesOp::LnNative
            | EvaluatedBytesOp::LogNative
            | EvaluatedBytesOp::Log2Native
            | EvaluatedBytesOp::PowNative
            | EvaluatedBytesOp::SinGoNative
            | EvaluatedBytesOp::CosGoNative
            | EvaluatedBytesOp::TanGoNative
            | EvaluatedBytesOp::CotGoNative
            | EvaluatedBytesOp::AtanGoNative
            | EvaluatedBytesOp::Atan2GoNative
            | EvaluatedBytesOp::SinLibmLegacy
            | EvaluatedBytesOp::CosLibmLegacy
            | EvaluatedBytesOp::CotLibmLegacy
            | EvaluatedBytesOp::AtanLibmLegacy
            | EvaluatedBytesOp::Atan2LibmLegacy
            | EvaluatedBytesOp::AbsRealNative
            | EvaluatedBytesOp::CeilRealNative
            | EvaluatedBytesOp::FloorRealNative
            | EvaluatedBytesOp::RoundRealNative
            | EvaluatedBytesOp::TruncateRealNative
            | EvaluatedBytesOp::RoundRealLegacy
            | EvaluatedBytesOp::RoundDecimalLegacy
            | EvaluatedBytesOp::MakeTimePartsNative
            | EvaluatedBytesOp::VecL1DistanceNative
            | EvaluatedBytesOp::VecL2DistanceNative
            | EvaluatedBytesOp::VecNegativeInnerProductNative
            | EvaluatedBytesOp::VecCosineDistanceNative
            | EvaluatedBytesOp::VecL2NormNative
            | EvaluatedBytesOp::VecRealNullNative
            | EvaluatedBytesOp::UnaryPlusBitsNative
            | EvaluatedBytesOp::UnaryMinusBitsNative
            | EvaluatedBytesOp::AddRealNative
            | EvaluatedBytesOp::SubRealNative
            | EvaluatedBytesOp::MulRealNative
            | EvaluatedBytesOp::AddRealLegacy
            | EvaluatedBytesOp::SubRealLegacy
            | EvaluatedBytesOp::MulRealLegacy
            | EvaluatedBytesOp::ModRealNative
            | EvaluatedBytesOp::ModRealLegacy
            | EvaluatedBytesOp::DivRealNative
            | EvaluatedBytesOp::DivRealLegacy,
            ComputedValue::Ieee754Bits(value),
        ) => {
            match value.metadata() {
                ComputedIeee754BitsMetadata::OwnIeee754Bits => {}
            }
            Ok(EvaluatedBytesResult::Ieee754Bits(value.into_option()))
        }
        (
            EvaluatedBytesOp::AbsDecimalNative
            | EvaluatedBytesOp::CeilDecimalNative
            | EvaluatedBytesOp::FloorDecimalNative
            | EvaluatedBytesOp::RoundDecimalNative
            | EvaluatedBytesOp::TruncateDecimalNative
            | EvaluatedBytesOp::UuidTimestampNative
            | EvaluatedBytesOp::UnaryPlusDecimalNative
            | EvaluatedBytesOp::UnaryMinusDecimalNative
            | EvaluatedBytesOp::UnaryMinusIntConstantNative
            | EvaluatedBytesOp::UnaryMinusUIntConstantNative
            | EvaluatedBytesOp::AddDecimalNative
            | EvaluatedBytesOp::SubDecimalNative
            | EvaluatedBytesOp::MulDecimalNative
            | EvaluatedBytesOp::AddDecimalLegacy
            | EvaluatedBytesOp::SubDecimalLegacy
            | EvaluatedBytesOp::MulDecimalLegacy
            | EvaluatedBytesOp::ModDecimalNative,
            ComputedValue::Decimal(value),
        ) => {
            match value.metadata() {
                ComputedDecimalMetadata::OwnDecimal => {}
            }
            let checked_i64_view = value.checked_i64_view();
            let value = value
                .into_option()
                .map(|value| tidb_datatype::Decimal::try_from_shared_math(&value, usize::MAX))
                .transpose()
                .map_err(|error| {
                    AsciiBoundaryError::Frontend(super::math_decimal_bridge_error(error))
                })?;
            Ok(EvaluatedBytesResult::Decimal {
                value,
                checked_i64_view,
            })
        }
        (
            EvaluatedBytesOp::DivDecimalNative | EvaluatedBytesOp::DivDecimalLegacy,
            ComputedValue::DecimalDivision(report),
        ) => {
            match report.metadata() {
                ComputedDecimalDivisionMetadata::OwnDecimalDivision => {}
            }
            let (value, disposition) = report.into_parts();
            match (disposition, value.as_ref()) {
                (NativeDecimalDivisionDisposition::ZeroDivisor, None)
                | (
                    NativeDecimalDivisionDisposition::Ok
                    | NativeDecimalDivisionDisposition::Truncated
                    | NativeDecimalDivisionDisposition::Overflow,
                    Some(_),
                ) => {}
                _ => {
                    return Err(AsciiBoundaryError::Scope {
                        kind: ScopeFailureKind::Contract,
                        reason: "decimal division disposition contradicts its computed value",
                    });
                }
            }
            let value = value
                .map(|value| tidb_datatype::Decimal::try_from_shared_math(&value, usize::MAX))
                .transpose()
                .map_err(|error| {
                    AsciiBoundaryError::Frontend(super::math_decimal_bridge_error(error))
                })?;
            Ok(EvaluatedBytesResult::DecimalDivision { value, disposition })
        }
        (
            EvaluatedBytesOp::RoundInt128Legacy
            | EvaluatedBytesOp::AddInt128SignedLegacy
            | EvaluatedBytesOp::AddInt128UnsignedLegacy
            | EvaluatedBytesOp::AddInt128RejectLeftLegacy
            | EvaluatedBytesOp::AddInt128RejectRightLegacy
            | EvaluatedBytesOp::SubInt128SignedLegacy
            | EvaluatedBytesOp::SubInt128UnsignedLegacy
            | EvaluatedBytesOp::SubInt128RejectLeftLegacy
            | EvaluatedBytesOp::SubInt128RejectRightLegacy
            | EvaluatedBytesOp::MulInt128SignedLegacy
            | EvaluatedBytesOp::MulInt128UnsignedLegacy
            | EvaluatedBytesOp::ModInt128Legacy
            | EvaluatedBytesOp::IntDivInt128Legacy,
            ComputedValue::Int128(value),
        ) => {
            match value.metadata() {
                ComputedInt128Metadata::OwnInt128 => {}
            }
            Ok(EvaluatedBytesResult::Int128(value.into_option()))
        }
        (
            EvaluatedBytesOp::AddDecimalFastNative
            | EvaluatedBytesOp::SubDecimalFastNative
            | EvaluatedBytesOp::MulDecimalFastNative,
            ComputedValue::DecimalFast(value),
        ) => {
            match value.metadata() {
                ComputedDecimalFastMetadata::OwnDecimalFast => {}
            }
            Ok(EvaluatedBytesResult::DecimalFast(value.into_outcome()))
        }
        _ => Err(result_kind_error()),
    }
}

fn evaluate_scoped_args<T>(
    scope: &AsciiScope,
    prepare: impl FnOnce() -> Result<(EvaluatedBytesOp, EvaluatedArgs), EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult, &AsciiScope) -> Result<T, EvalError>,
) -> Result<T, AsciiBoundaryError> {
    let mut guard = NativeGuard::new(scope);
    let result = (|| {
        // Frontend coercion and closed recipe selection happen exactly once,
        // before taking/replacing a lease, under the original scope guard.
        let (operation, ready) = prepare().map_err(AsciiBoundaryError::Frontend)?;
        let mut invocation = Invocation::enter(scope)?;
        let result = invocation.run_args(operation, ready);
        let computed = invocation.finish(result)?;
        // No worker/cell/mutex borrow surrounds original native result packing.
        pack(materialize_computed(operation, computed)?, scope)
            .map_err(AsciiBoundaryError::Frontend)
    })();
    guard.disarm(); // ordinary Result::Err is never an unwind or native replay
    result
}

pub(crate) fn evaluate_prepared_args_in<T>(
    ctx: &dyn Columns,
    prepare: impl FnOnce() -> Result<(EvaluatedBytesOp, EvaluatedArgs), EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    route_prepared_args_in(ctx, prepare, |computed, _scope| pack(computed))
}

/// Lend the selected authority to a dependent stage after the first lease has
/// finished and its result is owned. The existing guard and one-shot owner
/// remain alive through this callback; no worker borrow crosses it.
///
/// Bind directly rather than rediscovering a possibly different scope through
/// `with_columns`. Callers must use these columns for their dependent stage.
pub(crate) fn evaluate_prepared_args_scoped_in<T>(
    ctx: &dyn Columns,
    prepare: impl FnOnce() -> Result<(EvaluatedBytesOp, EvaluatedArgs), EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult, &dyn Columns) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    route_prepared_args_in(ctx, prepare, |computed, scope| {
        let columns = ScopedAsciiColumns { native: ctx, scope };
        pack(computed, &columns)
    })
}

fn route_prepared_args_in<T>(
    ctx: &dyn Columns,
    prepare: impl FnOnce() -> Result<(EvaluatedBytesOp, EvaluatedArgs), EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult, &AsciiScope) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    if let Some(scope) = ctx.evaluated_ascii_scope() {
        return evaluate_scoped_args(scope, prepare, pack)
            .map_err(AsciiBoundaryError::into_eval_error);
    }
    if let Some(execution) = ctx.evaluated_ascii_execution() {
        return evaluate_scoped_args(&execution.scope(), prepare, pack)
            .map_err(AsciiBoundaryError::into_eval_error);
    }

    let result = (|| {
        // No capability: preserve frontend precedence even before pool creation.
        let ready = prepare().map_err(AsciiBoundaryError::Frontend)?;
        // One explicit experimental policy for all closed fixed-arity recipes.
        // Retained/request allowances are not physical heap/factory-peak bounds.
        // A worker's retained cap must not become a maximum SQL string length.
        let policy = AsciiPoolPolicy::checked(1, 1, 8 << 20, 1 << 20, 2 << 20, 64, 16, usize::MAX)?;
        let owner = AsciiPoolOwner::new(policy)?;
        let execution = OneShotAsciiExecution(owner.begin_execution()?);
        // The scope/guard drop before the owned closer, including on unwind.
        let scope = execution.0.scope();
        evaluate_scoped_args(&scope, || Ok(ready), pack)
    })();
    result.map_err(AsciiBoundaryError::into_eval_error)
}

/// One closed operation router, sharing the legacy-named ASCII capabilities.
/// Neither frontend callback enters C4: coercion precedes admission, and native
/// packing follows the exclusive invocation. Existing scopes guard both; the
/// no-capability route retains its original preparation-before-pool precedence.
/// Only the native packed result is generic; arguments and lifecycle stay closed.
pub(crate) fn evaluate_args_in<T>(
    operation: EvaluatedBytesOp,
    ctx: &dyn Columns,
    coerce: impl FnOnce() -> Result<EvaluatedArgs, EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    evaluate_prepared_args_in(ctx, || Ok((operation, coerce()?)), pack)
}

/// The original legacy integer signatures have distinct signedness and
/// operand-rejection policies; callers select only their actual signature.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyIntegerArithmetic {
    AddSigned,
    AddUnsigned,
    AddRejectLeft,
    AddRejectRight,
    SubSigned,
    SubUnsigned,
    SubRejectLeft,
    SubRejectRight,
    MulSigned,
    MulUnsigned,
    /// All four legacy wire labels share the full-i128 remainder profile.
    Modulo,
    /// Legacy integer DIV keeps its original full-i128 ordinary quotient policy.
    IntDivide,
}

/// Missing children and an actually evaluated SQL NULL have distinct recipes.
/// A non-NULL witness is a contract error, never an arithmetic operand.
#[derive(Debug)]
pub enum LegacyBinaryArgs<T> {
    Missing,
    NullWitness(Option<i64>),
    Values(T, T),
}

fn arithmetic_null_witness(value: Option<i64>) -> Result<EvaluatedArgs, EvalError> {
    if value.is_some() {
        return Err(AsciiBoundaryError::Scope {
            kind: ScopeFailureKind::Contract,
            reason: "binary arithmetic NULL witness contains a value",
        }
        .into_eval_error());
    }
    Ok(EvaluatedArgs::NullWitness(None))
}

fn legacy_comparison_result(computed: EvaluatedBytesResult) -> Result<Option<i128>, EvalError> {
    match computed.into_boolean_datum()? {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(i128::from(value))),
        _ => Err(result_kind_error().into_eval_error()),
    }
}

/// Evaluate legacy binary JSON membership without substituting serde equality.
/// Values carry the actual target and document; caller-side array representation
/// validation and its original SQL error precedence remain with the caller.
pub fn eval_legacy_json_member_of_in(
    args: LegacyBinaryArgs<tidb_datatype::BinaryJSON>,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::JsonPredicateMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::JsonPredicateNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(target, document) => Ok((
                EvaluatedBytesOp::JsonMemberOfBinaryLegacy,
                super::prepare_json_binary_pair_args(&target, &document)?,
            )),
        },
        legacy_comparison_result,
    )
}

// A raw result owns its original type byte and payload. Never decode, validate
// or render it: an empty-pair append may intentionally return malformed raw data.
fn legacy_json_output_result(
    computed: EvaluatedBytesResult,
) -> Result<Option<tidb_datatype::BinaryJSON>, EvalError> {
    let Some(mut bytes) = computed.into_bytes()? else {
        return Ok(None);
    };
    if bytes.is_empty() {
        return Err(AsciiBoundaryError::Scope {
            kind: ScopeFailureKind::Contract,
            reason: "computed raw JSON result lacks a type byte",
        }
        .into_eval_error());
    }
    let type_code = bytes.remove(0);
    Ok(Some(tidb_datatype::BinaryJSON::from_encoded_parts(
        type_code, bytes,
    )))
}

/// Replace over the actual raw document and already-demanded ordered pairs.
/// Original child errors stay with the caller; only transport is prepared here.
pub fn eval_legacy_json_replace_in(
    document: &tidb_datatype::BinaryJSON,
    paths: &[tidb_datatype::JSONPathExpression],
    values: &[tidb_datatype::BinaryJSON],
    ctx: &dyn Columns,
) -> Result<Option<tidb_datatype::BinaryJSON>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || {
            let args = tidb_query_expr::local::prepare_json_raw_paths_values_args(
                (document.type_code(), document.value()),
                paths
                    .iter()
                    .map(|path| (path.legs(), path.could_match_multiple_values())),
                values
                    .iter()
                    .map(|value| (value.type_code(), value.value())),
            )
            .map_err(|error| {
                EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                    error, None,
                ))
            })?;
            Ok((EvaluatedBytesOp::JsonReplaceRawLegacy, args))
        },
        legacy_json_output_result,
    )
}

/// Execute exactly one append pair; `None` means the actual zero-pair identity,
/// not an absent legacy value. Demand another pair only after this returns Some.
pub fn eval_legacy_json_array_append_step_in(
    document: &tidb_datatype::BinaryJSON,
    pair: Option<(
        &tidb_datatype::JSONPathExpression,
        &tidb_datatype::BinaryJSON,
    )>,
    ctx: &dyn Columns,
) -> Result<Option<tidb_datatype::BinaryJSON>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || {
            let raw_document = (document.type_code(), document.value());
            let (operation, args) = match pair {
                Some((path, value)) => (
                    EvaluatedBytesOp::JsonArrayAppendRawLegacy,
                    tidb_query_expr::local::prepare_json_raw_paths_values_args(
                        raw_document,
                        std::iter::once((path.legs(), path.could_match_multiple_values())),
                        std::iter::once((value.type_code(), value.value())),
                    ),
                ),
                None => (
                    EvaluatedBytesOp::JsonArrayAppendEmptyLegacy,
                    tidb_query_expr::local::prepare_json_raw_identity_args(raw_document),
                ),
            };
            let args = args.map_err(|error| {
                EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                    error, None,
                ))
            })?;
            Ok((operation, args))
        },
        legacy_json_output_result,
    )
}

/// Preserve an actually observed legacy no-value outcome without claiming a
/// SQL NULL witness; missing/type/decoding outcomes share this legacy boundary.
pub fn eval_legacy_json_output_none_in(
    ctx: &dyn Columns,
) -> Result<Option<tidb_datatype::BinaryJSON>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || {
            Ok((
                EvaluatedBytesOp::JsonValueAbsentLegacy,
                EvaluatedArgs::NoArgs,
            ))
        },
        legacy_json_output_result,
    )
}

/// Merge-patch the caller's actual ordered raw JSON operands. Child demand and
/// its original errors remain with the caller; semantic codec failures are the
/// worker's computed absent result, not transport failures.
pub fn eval_legacy_json_merge_patch_in(
    values: &[tidb_datatype::BinaryJSON],
    ctx: &dyn Columns,
) -> Result<Option<tidb_datatype::BinaryJSON>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || {
            let mut raw_values = Vec::new();
            raw_values
                .try_reserve_exact(values.len())
                .map_err(|error| {
                    EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                        tidb_query_expr::local::LocalError::ResourceLimit(
                            format!("raw JSON operand references allocation failed: {error}")
                                .into(),
                        ),
                        None,
                    ))
                })?;
            raw_values.extend(
                values
                    .iter()
                    .map(|value| (value.type_code(), value.value())),
            );
            let args = tidb_query_expr::local::prepare_json_raw_values_args(&raw_values).map_err(
                |error| {
                    EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                        error, None,
                    ))
                },
            )?;
            Ok((EvaluatedBytesOp::JsonMergePatchRawLegacy, args))
        },
        legacy_json_output_result,
    )
}

/// Project legacy MICROSECOND from the actual nullable nanoseconds. FSP and
/// duration parsing remain with the caller; even NULL reaches this fixed worker.
pub fn eval_legacy_microsecond_in(
    value: Option<i64>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    evaluate_args_in(
        EvaluatedBytesOp::MicrosecondLegacy,
        ctx,
        || Ok(EvaluatedArgs::Int(value)),
        |computed| match computed.into_int_datum()? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(value)),
            _ => Err(result_kind_error().into_eval_error()),
        },
    )
}

/// Evaluate the legacy DATE predicate over the actual nullable temporal core.
pub fn eval_legacy_date_in(core: Option<u64>, ctx: &dyn Columns) -> Result<Option<i64>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || {
            Ok((
                EvaluatedBytesOp::DateCorePredicateLegacy,
                EvaluatedArgs::TimeCoreBits(core),
            ))
        },
        |computed| match computed.into_boolean_datum()? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(value)),
            _ => Err(result_kind_error().into_eval_error()),
        },
    )
}

/// Compare the actual full-width legacy integer pair under the caller's scope.
pub fn eval_legacy_integer_comparison_in(
    operation: ComparisonOp,
    args: LegacyBinaryArgs<i128>,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::CompareMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::CompareNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => Ok((
                EvaluatedBytesOp::CompareInt128Legacy(operation),
                EvaluatedArgs::Int1282(Some(left), Some(right)),
            )),
        },
        legacy_comparison_result,
    )
}

/// Preserve legacy total ordering over the actual raw IEEE operands.
pub fn eval_legacy_real_comparison_in(
    operation: ComparisonOp,
    args: LegacyBinaryArgs<f64>,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::CompareMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::CompareNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => Ok((
                EvaluatedBytesOp::CompareRealLegacy(operation),
                EvaluatedArgs::Ieee754Bits2 {
                    left: super::ReadyIeee754Arg::Value(Some(left.to_bits())),
                    right: super::ReadyIeee754Arg::Value(Some(right.to_bits())),
                },
            )),
        },
        legacy_comparison_result,
    )
}

/// Resolve the legacy collation once, only for an actual demanded byte pair.
pub fn eval_legacy_bytes_comparison_in(
    operation: ComparisonOp,
    args: LegacyBinaryArgs<Vec<u8>>,
    collation_id: i32,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::CompareMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::CompareNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => {
                // Preserve the registry's global-enabled decision and unknown-ID
                // fallback; neither may be replaced with the raw SQL ID.
                let collation = match tidb_datatype::get_collator_by_id(collation_id) {
                    tidb_datatype::Collator::DerivedBinary => super::NativeCollation::Binary,
                    tidb_datatype::Collator::New(collation) => collation.native_policy(),
                };
                Ok((
                    EvaluatedBytesOp::CompareBytesNative(operation),
                    EvaluatedArgs::CollatedBytes2 {
                        left: Some(left),
                        right: Some(right),
                        collation,
                    },
                ))
            }
        },
        legacy_comparison_result,
    )
}

/// Adapt real decimal owners losslessly before the comparison worker is admitted.
pub fn eval_legacy_decimal_comparison_in(
    operation: ComparisonOp,
    args: LegacyBinaryArgs<tidb_datatype::Decimal>,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::CompareMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::CompareNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => Ok((
                EvaluatedBytesOp::CompareDecimalNative(operation),
                EvaluatedArgs::Decimal2 {
                    left: Some(super::prepare_math_decimal(&left)?),
                    right: Some(super::prepare_math_decimal(&right)?),
                },
            )),
        },
        legacy_comparison_result,
    )
}

/// Forward actual legacy temporal cores without host comparison or repacking.
pub fn eval_legacy_time_comparison_in(
    operation: ComparisonOp,
    args: LegacyBinaryArgs<Time>,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::CompareMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::CompareNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => Ok((
                EvaluatedBytesOp::CompareTimeCoreNative(operation),
                EvaluatedArgs::TimeCoreBits2(
                    Some(left.core_time().raw()),
                    Some(right.core_time().raw()),
                ),
            )),
        },
        legacy_comparison_result,
    )
}

/// Evaluate only the caller's explicit legacy integer signature and presence.
/// Original i128 operands reach the worker without narrowing or host arithmetic.
pub fn eval_legacy_integer_arithmetic_in(
    profile: LegacyIntegerArithmetic,
    args: LegacyBinaryArgs<i128>,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::BinaryArithmeticMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::BinaryArithmeticNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => {
                let operation = match profile {
                    LegacyIntegerArithmetic::AddSigned => EvaluatedBytesOp::AddInt128SignedLegacy,
                    LegacyIntegerArithmetic::AddUnsigned => {
                        EvaluatedBytesOp::AddInt128UnsignedLegacy
                    }
                    LegacyIntegerArithmetic::AddRejectLeft => {
                        EvaluatedBytesOp::AddInt128RejectLeftLegacy
                    }
                    LegacyIntegerArithmetic::AddRejectRight => {
                        EvaluatedBytesOp::AddInt128RejectRightLegacy
                    }
                    LegacyIntegerArithmetic::SubSigned => EvaluatedBytesOp::SubInt128SignedLegacy,
                    LegacyIntegerArithmetic::SubUnsigned => {
                        EvaluatedBytesOp::SubInt128UnsignedLegacy
                    }
                    LegacyIntegerArithmetic::SubRejectLeft => {
                        EvaluatedBytesOp::SubInt128RejectLeftLegacy
                    }
                    LegacyIntegerArithmetic::SubRejectRight => {
                        EvaluatedBytesOp::SubInt128RejectRightLegacy
                    }
                    LegacyIntegerArithmetic::MulSigned => EvaluatedBytesOp::MulInt128SignedLegacy,
                    LegacyIntegerArithmetic::MulUnsigned => {
                        EvaluatedBytesOp::MulInt128UnsignedLegacy
                    }
                    LegacyIntegerArithmetic::Modulo => EvaluatedBytesOp::ModInt128Legacy,
                    LegacyIntegerArithmetic::IntDivide => EvaluatedBytesOp::IntDivInt128Legacy,
                };
                Ok((operation, EvaluatedArgs::Int1282(Some(left), Some(right))))
            }
        },
        |computed| match computed {
            EvaluatedBytesResult::Int(Datum::Null) => Ok(None),
            value => value.into_int128(),
        },
    )
}

/// Legacy REAL arithmetic retains raw IEEE values and its original NULL demand.
pub fn eval_legacy_real_arithmetic_in(
    operation: BinaryArithmeticOperation,
    args: LegacyBinaryArgs<f64>,
    ctx: &dyn Columns,
) -> Result<Option<f64>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::BinaryArithmeticMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::BinaryArithmeticNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => {
                let operation = match operation {
                    BinaryArithmeticOperation::Add => EvaluatedBytesOp::AddRealLegacy,
                    BinaryArithmeticOperation::Subtract => EvaluatedBytesOp::SubRealLegacy,
                    BinaryArithmeticOperation::Multiply => EvaluatedBytesOp::MulRealLegacy,
                    BinaryArithmeticOperation::Modulo => EvaluatedBytesOp::ModRealLegacy,
                    BinaryArithmeticOperation::Divide => EvaluatedBytesOp::DivRealLegacy,
                    BinaryArithmeticOperation::IntDivide => {
                        return Err(AsciiBoundaryError::Scope {
                            kind: ScopeFailureKind::Contract,
                            reason: "integer division is not a legacy REAL arithmetic profile",
                        }
                        .into_eval_error());
                    }
                };
                Ok((
                    operation,
                    EvaluatedArgs::Ieee754Bits2 {
                        left: super::ReadyIeee754Arg::Value(Some(left.to_bits())),
                        right: super::ReadyIeee754Arg::Value(Some(right.to_bits())),
                    },
                ))
            }
        },
        |computed| match computed {
            EvaluatedBytesResult::Int(Datum::Null) => Ok(None),
            value => Ok(value.into_ieee754_bits()?.map(f64::from_bits)),
        },
    )
}

/// Convert only actual demanded decimal layouts; the worker owns arithmetic and
/// the legacy warning-result policy. Representation failures remain failures.
/// Division requires [`eval_legacy_decimal_division_in`] and explicit precision.
pub fn eval_legacy_decimal_arithmetic_in(
    operation: BinaryArithmeticOperation,
    args: LegacyBinaryArgs<tidb_datatype::Decimal>,
    ctx: &dyn Columns,
) -> Result<Option<tidb_datatype::Decimal>, EvalError> {
    let operation = match operation {
        BinaryArithmeticOperation::Add => EvaluatedBytesOp::AddDecimalLegacy,
        BinaryArithmeticOperation::Subtract => EvaluatedBytesOp::SubDecimalLegacy,
        BinaryArithmeticOperation::Multiply => EvaluatedBytesOp::MulDecimalLegacy,
        BinaryArithmeticOperation::Modulo => EvaluatedBytesOp::ModDecimalNative,
        BinaryArithmeticOperation::IntDivide => {
            return Err(AsciiBoundaryError::Scope {
                kind: ScopeFailureKind::Contract,
                reason: "integer division is not a legacy decimal arithmetic profile",
            }
            .into_eval_error());
        }
        BinaryArithmeticOperation::Divide => {
            return Err(AsciiBoundaryError::Scope {
                kind: ScopeFailureKind::Contract,
                reason: "decimal division requires an explicit fraction increment",
            }
            .into_eval_error());
        }
    };
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::BinaryArithmeticMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::BinaryArithmeticNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => {
                let left = super::prepare_math_decimal(&left)?;
                let right = super::prepare_math_decimal(&right)?;
                Ok((
                    operation,
                    EvaluatedArgs::Decimal2 {
                        left: Some(left),
                        right: Some(right),
                    },
                ))
            }
        },
        |computed| match computed {
            EvaluatedBytesResult::Int(Datum::Null) => Ok(None),
            EvaluatedBytesResult::Decimal { value, .. } => Ok(value),
            _ => Err(result_kind_error().into_eval_error()),
        },
    )
}

fn decimal_intdiv_view(value: &tidb_datatype::Decimal) -> tidb_query_expr::NativeIdentityRef<'_> {
    tidb_query_expr::NativeIdentityRef::Decimal {
        negative: value.is_negative(),
        scale: value.scale(),
        storage_scale: value.storage_scale(),
        declared_shape: value.declared_shape(),
        coefficient: value.coefficient_bytes(),
    }
}

fn encode_decimal_intdiv_operand(
    view: tidb_query_expr::NativeIdentityRef<'_>,
) -> Result<Vec<u8>, EvalError> {
    tidb_query_expr::encode_native_identity(view).map_err(|error| match error {
        tidb_query_expr::NativeIdentityFrameError::Invalid => result_kind_error().into_eval_error(),
        tidb_query_expr::NativeIdentityFrameError::Capacity => {
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                tidb_query_expr::local::LocalError::ResourceLimit(
                    "native decimal INTDIV frame allocation or size failed".into(),
                ),
                None,
            ))
        }
    })
}

fn finish_decimal_integer_division(
    computed: EvaluatedBytesResult,
    unsigned: bool,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let bytes = computed
        .into_bytes()?
        .ok_or_else(|| result_kind_error().into_eval_error())?;
    let report = tidb_query_expr::decode_native_intdiv_report(&bytes)
        .ok_or_else(|| result_kind_error().into_eval_error())?;
    // The worker supplied the actual bounded-division warning, including its
    // original text. A strict warning must win over any later integer overflow.
    if let Some(warning) = report.warning {
        ctx.handle_truncate(warning)?;
    }
    match report.outcome {
        tidb_query_expr::NativeIntDivOutcome::ZeroDivisor => {
            ctx.handle_division_by_zero()?;
            Ok(Datum::Null)
        }
        tidb_query_expr::NativeIntDivOutcome::Value(bits) => Ok(if unsigned {
            Datum::UInt(bits as u64)
        } else {
            Datum::Int(bits)
        }),
        tidb_query_expr::NativeIntDivOutcome::IntOverflow => Err(EvalError::IntOverflow),
    }
}

/// Native Decimal DIV: capture only demanded raw precision reads, then run one
/// closed worker. The shared classifier and worker own all arithmetic policy.
pub(crate) fn eval_decimal_integer_division_in(
    left: &tidb_datatype::Decimal,
    right: &tidb_datatype::Decimal,
    unsigned: bool,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || {
            let left = decimal_intdiv_view(left);
            let right = decimal_intdiv_view(right);
            // Borrowed raw views avoid validating UTF-8 or allocating frames
            // before the original RHS-zero check and conditional getter reads.
            let needs_probe = tidb_query_expr::native_intdiv_needs_probe(left, right)
                .ok_or_else(|| result_kind_error().into_eval_error())?;
            let probe = if needs_probe {
                Some(i64::from(ctx.div_precision_increment()))
            } else {
                None
            };
            let needs_fallback = tidb_query_expr::native_intdiv_needs_fallback(left, right, probe)
                .ok_or_else(|| result_kind_error().into_eval_error())?;
            let fallback = if needs_fallback {
                Some(i64::from(ctx.div_precision_increment()))
            } else {
                None
            };
            Ok((
                if unsigned {
                    EvaluatedBytesOp::IntDivDecimalUnsignedNative
                } else {
                    EvaluatedBytesOp::IntDivDecimalSignedNative
                },
                EvaluatedArgs::BytesIntIntBytes(
                    Some(encode_decimal_intdiv_operand(left)?),
                    probe,
                    fallback,
                    Some(encode_decimal_intdiv_operand(right)?),
                ),
            ))
        },
        |computed| finish_decimal_integer_division(computed, unsigned, ctx),
    )
}

/// Legacy exact Decimal DIV keeps raw operands until the worker's RHS-zero
/// check, then returns only its actual signed i64 result widened to i128.
pub fn eval_legacy_decimal_integer_division_in(
    args: LegacyBinaryArgs<tidb_datatype::Decimal>,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::BinaryArithmeticMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::BinaryArithmeticNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => Ok((
                EvaluatedBytesOp::IntDivDecimalLegacy,
                EvaluatedArgs::Bytes2(
                    Some(encode_decimal_intdiv_operand(decimal_intdiv_view(&left))?),
                    Some(encode_decimal_intdiv_operand(decimal_intdiv_view(&right))?),
                ),
            )),
        },
        |computed| match computed.into_int_datum()? {
            Datum::Null => Ok(None),
            Datum::Int(value) => Ok(Some(i128::from(value))),
            _ => Err(result_kind_error().into_eval_error()),
        },
    )
}

/// Evaluate legacy decimal division with the caller's unmodified precision.
/// The worker owns arithmetic and its status; legacy packing keeps the actual
/// value without applying native warning policy or substituting precision zero.
pub fn eval_legacy_decimal_division_in(
    args: LegacyBinaryArgs<tidb_datatype::Decimal>,
    frac_increment: u32,
    ctx: &dyn Columns,
) -> Result<Option<tidb_datatype::Decimal>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyBinaryArgs::Missing => Ok((
                EvaluatedBytesOp::BinaryArithmeticMissingLegacy,
                EvaluatedArgs::NoArgs,
            )),
            LegacyBinaryArgs::NullWitness(value) => Ok((
                EvaluatedBytesOp::BinaryArithmeticNullNative,
                arithmetic_null_witness(value)?,
            )),
            LegacyBinaryArgs::Values(left, right) => Ok((
                EvaluatedBytesOp::DivDecimalLegacy,
                EvaluatedArgs::DecimalDivision {
                    left: Some(super::prepare_math_decimal(&left)?),
                    right: Some(super::prepare_math_decimal(&right)?),
                    frac_increment,
                },
            )),
        },
        |computed| match computed {
            EvaluatedBytesResult::Int(Datum::Null) => Ok(None),
            EvaluatedBytesResult::DecimalDivision { value, .. } => Ok(value),
            _ => Err(result_kind_error().into_eval_error()),
        },
    )
}

/// Lossless fast-input layout adaptation plus the same closed worker lifecycle.
/// Unsupported is a computed outcome, never a substitute for a bridge failure.
pub(crate) fn eval_arithmetic_decimal_fast_in(
    operation: BinaryArithmeticOperation,
    left: Option<NativeDecimalFastValue>,
    right: Option<NativeDecimalFastValue>,
    ctx: &dyn Columns,
) -> Result<NativeDecimalFastOutcome, EvalError> {
    let operation = match operation {
        BinaryArithmeticOperation::Add => EvaluatedBytesOp::AddDecimalFastNative,
        BinaryArithmeticOperation::Subtract => EvaluatedBytesOp::SubDecimalFastNative,
        BinaryArithmeticOperation::Multiply => EvaluatedBytesOp::MulDecimalFastNative,
        BinaryArithmeticOperation::Modulo => {
            return Err(AsciiBoundaryError::Scope {
                kind: ScopeFailureKind::Contract,
                reason: "modulo is unsupported by the decimal fast contract",
            }
            .into_eval_error());
        }
        BinaryArithmeticOperation::Divide | BinaryArithmeticOperation::IntDivide => {
            return Err(AsciiBoundaryError::Scope {
                kind: ScopeFailureKind::Contract,
                reason: "division is unsupported by the decimal fast contract",
            }
            .into_eval_error());
        }
    };
    evaluate_args_in(
        operation,
        ctx,
        || {
            let left = left
                .map(|value| {
                    tidb_query_datatype::codec::mysql::Decimal::try_from_native_fast(
                        value,
                        usize::MAX,
                    )
                })
                .transpose()
                .map_err(super::math_decimal_bridge_error)?;
            let right = right
                .map(|value| {
                    tidb_query_datatype::codec::mysql::Decimal::try_from_native_fast(
                        value,
                        usize::MAX,
                    )
                })
                .transpose()
                .map_err(super::math_decimal_bridge_error)?;
            Ok(EvaluatedArgs::Decimal2 { left, right })
        },
        EvaluatedBytesResult::into_decimal_fast_outcome,
    )
}

/// Actual legacy LIKE presence, distinct from a nullable value's SQL result.
/// Values retain their original bytes; the kernel owns UTF-8 and folding policy.
#[derive(Debug)]
pub enum LegacyLikeArgs {
    Missing,
    NullWitness(Option<i64>),
    Values {
        target: Vec<u8>,
        pattern: Vec<u8>,
        escape: u8,
    },
}

/// Evaluate legacy LIKE under the caller's scope without native matching.
/// Missing children and an actual NULL witness enter their own closed recipes;
/// a non-NULL witness is refused before worker admission.
pub fn eval_legacy_like_in(
    case_insensitive: bool,
    args: LegacyLikeArgs,
    ctx: &dyn Columns,
) -> Result<Option<i128>, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match args {
            LegacyLikeArgs::Missing => Ok((
                EvaluatedBytesOp::LikeMissingLegacyNative,
                EvaluatedArgs::NoArgs,
            )),
            LegacyLikeArgs::NullWitness(value) => {
                if value.is_some() {
                    return Err(AsciiBoundaryError::Scope {
                        kind: ScopeFailureKind::Contract,
                        reason: "legacy LIKE NULL witness contains a value",
                    }
                    .into_eval_error());
                }
                Ok((
                    EvaluatedBytesOp::LikeNullIntNative,
                    EvaluatedArgs::NullWitness(None),
                ))
            }
            LegacyLikeArgs::Values {
                target,
                pattern,
                escape,
            } => Ok((
                EvaluatedBytesOp::LikeLegacyNative,
                EvaluatedArgs::Like {
                    invocation: NativeLikeInvocation::legacy(case_insensitive),
                    text: Some(target),
                    pattern: Some(pattern),
                    escape: Some(i64::from(escape)),
                },
            )),
        },
        |computed| match computed.into_boolean_datum()? {
            Datum::Int(value) => Ok(Some(i128::from(value))),
            Datum::Null => Ok(None),
            _ => Err(result_kind_error().into_eval_error()),
        },
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RegexpFunction {
    Like,
    Substr,
    Instr,
    Replace,
    LegacyCi,
    LegacyBin,
    LegacyMissing,
}

/// The historical coprocessor distinguishes an absent child from SQL NULL.
/// A collation decision is supplied only when both actual operands are non-NULL.
#[derive(Debug)]
pub enum RegexpLegacyInput {
    Missing,
    Values {
        text: Option<Vec<u8>>,
        pattern: Option<Vec<u8>>,
        case_insensitive: Option<bool>,
    },
}

/// Narrow ready-input bridge for the historical policies. Recipe selection
/// shares the original guarded preparation and no-capability precedence.
pub fn eval_regexp_legacy_ready_in(
    ctx: &dyn Columns,
    input: RegexpLegacyInput,
) -> Result<Datum, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || match input {
            RegexpLegacyInput::Missing => Ok((
                EvaluatedBytesOp::RegexpMissingLegacyNative,
                EvaluatedArgs::NoArgs,
            )),
            RegexpLegacyInput::Values {
                text,
                pattern,
                case_insensitive,
            } => {
                if text.is_none() || pattern.is_none() {
                    return Ok((
                        EvaluatedBytesOp::RegexpNullIntNative,
                        EvaluatedArgs::NullWitness(None),
                    ));
                }
                let Some(case_insensitive) = case_insensitive else {
                    return Err(AsciiBoundaryError::Scope {
                        kind: ScopeFailureKind::Contract,
                        reason: "legacy regexp values lack their collation decision",
                    }
                    .into_eval_error());
                };
                let operation = if case_insensitive {
                    EvaluatedBytesOp::RegexpLikeLegacyCiNative
                } else {
                    EvaluatedBytesOp::RegexpLikeLegacyBinNative
                };
                Ok((operation, EvaluatedArgs::Bytes2(text, pattern)))
            }
        },
        EvaluatedBytesResult::into_boolean_datum,
    )
}

/// Select only a function's fixed recipe or its actual NULL witness. The worker
/// validates every non-NULL argument role; no suffix values are manufactured.
pub(crate) fn evaluate_regexp_in(
    function: RegexpFunction,
    ctx: &dyn Columns,
    coerce: impl FnOnce() -> Result<EvaluatedArgs, EvalError>,
) -> Result<Datum, EvalError> {
    evaluate_prepared_args_in(
        ctx,
        || {
            let ready = coerce()?;
            let operation = if function != RegexpFunction::LegacyMissing
                && matches!(&ready, EvaluatedArgs::NullWitness(None))
            {
                match function {
                    RegexpFunction::Substr | RegexpFunction::Replace => {
                        EvaluatedBytesOp::RegexpNullBytesNative
                    }
                    _ => EvaluatedBytesOp::RegexpNullIntNative,
                }
            } else {
                match function {
                    RegexpFunction::Like => EvaluatedBytesOp::RegexpLikeNative,
                    RegexpFunction::Substr => EvaluatedBytesOp::RegexpSubstrNative,
                    RegexpFunction::Instr => EvaluatedBytesOp::RegexpInstrNative,
                    RegexpFunction::Replace => EvaluatedBytesOp::RegexpReplaceNative,
                    RegexpFunction::LegacyCi => EvaluatedBytesOp::RegexpLikeLegacyCiNative,
                    RegexpFunction::LegacyBin => EvaluatedBytesOp::RegexpLikeLegacyBinNative,
                    RegexpFunction::LegacyMissing => EvaluatedBytesOp::RegexpMissingLegacyNative,
                }
            };
            Ok((operation, ready))
        },
        |computed| match function {
            RegexpFunction::Like
            | RegexpFunction::LegacyCi
            | RegexpFunction::LegacyBin
            | RegexpFunction::LegacyMissing => computed.into_boolean_datum(),
            RegexpFunction::Instr => computed.into_int_datum(),
            RegexpFunction::Substr | RegexpFunction::Replace => Ok(computed
                .into_bytes()?
                .map_or(Datum::Null, Datum::new_string)),
        },
    )
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
