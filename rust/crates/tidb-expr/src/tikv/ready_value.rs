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

//! Ready-argument bridge backed by an executor-lane operation→worker cache.
//!
//! The real C4 worker is the only computation path. Native children/transcode
//! precede this value boundary; original return coercion follows it. Each lane
//! exclusively owns its prepared workers; no session or statement pool exists.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
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

/// Private structured handoff. Actual C4 failures capture their known phase at
/// the producing call; adapter failures never impersonate a LocalError.
#[derive(Debug)]
pub(super) enum ReadyValueBoundaryError {
    Frontend(EvalError),
    Kernel(ExpressionRuntimeFailure),
    Metadata(BridgeError),
    Scope {
        kind: ScopeFailureKind,
        reason: &'static str,
    },
}

impl ReadyValueBoundaryError {
    fn into_eval_error(self) -> EvalError {
        match self {
            Self::Frontend(error) => error,
            Self::Kernel(failure) => EvalError::ExpressionRuntimeFailure(failure),
            Self::Metadata(error) => {
                EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_bridge(error))
            }
            Self::Scope { kind, reason } => EvalError::ExpressionAdapterFailure(
                ExpressionAdapterFailure::from_scope(kind, reason),
            ),
        }
    }
}

const READY_VALUE_CACHE_WORKER_RETAINED_CAP: usize = 1 << 20;

fn cache_compile_limits(operation: EvaluatedBytesOp) -> CompileLimits {
    CompileLimits {
        max_nodes: match operation {
            EvaluatedBytesOp::RegexpSubstrNative
            | EvaluatedBytesOp::IntDivDecimalSignedNative
            | EvaluatedBytesOp::IntDivDecimalUnsignedNative => 6,
            EvaluatedBytesOp::RegexpInstrNative | EvaluatedBytesOp::RegexpReplaceNative => 7,
            EvaluatedBytesOp::LpadBytesNative
            | EvaluatedBytesOp::RpadBytesNative
            | EvaluatedBytesOp::LpadUtf8Native
            | EvaluatedBytesOp::RpadUtf8Native
            | EvaluatedBytesOp::Insert
            | EvaluatedBytesOp::InsertUtf8Native
            | EvaluatedBytesOp::Locate3Native
            | EvaluatedBytesOp::ConvertUsingNative
            | EvaluatedBytesOp::DateArithmeticHeadNative
            | EvaluatedBytesOp::DateArithmeticDurationHeadNative => 5,
            _ => 4,
        },
        max_depth: 3,
    }
}

fn prepare_cache_worker(
    operation: EvaluatedBytesOp,
) -> Result<Box<EvaluatedBytesWorker>, ReadyValueBoundaryError> {
    let worker = prepare_evaluated_bytes(
        operation,
        LocalCompileContext {
            limits: cache_compile_limits(operation),
        },
        ExecutionLimits::default(),
        READY_VALUE_CACHE_WORKER_RETAINED_CAP,
    )
    .map_err(|error| {
        ReadyValueBoundaryError::Kernel(ExpressionRuntimeFailure::from_local_eval(
            error,
            Some(ExpressionRuntimeFailurePhase::Prepare),
        ))
    })?;
    let observed = worker
        .retained_storage()
        .map_err(|error| {
            ReadyValueBoundaryError::Kernel(ExpressionRuntimeFailure::from_local_eval(
                error,
                Some(ExpressionRuntimeFailurePhase::Observe),
            ))
        })?
        .total_bytes();
    if worker.operation() != operation
        || !worker.is_healthy()
        || observed > READY_VALUE_CACHE_WORKER_RETAINED_CAP
    {
        return Err(ReadyValueBoundaryError::Scope {
            kind: ScopeFailureKind::Contract,
            reason: "ready-value cache factory published unhealthy storage",
        });
    }
    Ok(Box::new(worker))
}

/// Executor-lane-owned operation cache. It is Send but deliberately neither
/// Sync nor Clone: one movable evaluation lane has exclusive access to every
/// prepared worker and drops all workers with that lane.
pub struct ReadyValueCache {
    workers: RefCell<HashMap<EvaluatedBytesOp, Box<EvaluatedBytesWorker>>>,
    busy: Cell<bool>,
    poisoned: Cell<bool>,
}

impl Default for ReadyValueCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadyValueCache {
    /// Creates an empty lane cache; no worker is prepared before first demand.
    #[must_use]
    pub fn new() -> Self {
        Self {
            workers: RefCell::new(HashMap::new()),
            busy: Cell::new(false),
            poisoned: Cell::new(false),
        }
    }

    /// Binds this cache to a native evaluation context for one lane task.
    /// An already bound cache wins, and unwind poisons only that effective lane.
    pub fn with_columns<'a, R>(
        &'a self,
        native: &'a dyn Columns,
        body: impl FnOnce(&ScopedReadyValueColumns<'a, 'a>) -> R,
    ) -> R {
        let cache = native.ready_value_cache().unwrap_or(self);
        let mut guard = CacheNativeGuard::new(cache);
        let columns = ScopedReadyValueColumns { native, cache };
        let result = body(&columns);
        guard.disarm();
        result
    }

    fn run_args(
        &self,
        operation: EvaluatedBytesOp,
        ready: EvaluatedArgs,
    ) -> Result<ComputedValue, ReadyValueBoundaryError> {
        if self.poisoned.get() {
            return Err(ReadyValueBoundaryError::Scope {
                kind: ScopeFailureKind::Poisoned,
                reason: "ready-value cache is poisoned",
            });
        }
        if self.busy.replace(true) {
            return Err(ReadyValueBoundaryError::Scope {
                kind: ScopeFailureKind::Reentry,
                reason: "reentrant ready-value cache borrow",
            });
        }
        let worker = match self.workers.try_borrow_mut() {
            Ok(mut workers) => workers
                .remove(&operation)
                .map(Ok)
                .unwrap_or_else(|| prepare_cache_worker(operation)),
            Err(_) => Err(ReadyValueBoundaryError::Scope {
                kind: ScopeFailureKind::Reentry,
                reason: "ready-value cache map is already borrowed",
            }),
        };
        let worker = match worker {
            Ok(worker) => worker,
            Err(error) => {
                self.busy.set(false);
                return Err(error);
            }
        };
        let mut invocation = CacheInvocation {
            cache: self,
            operation,
            worker: Some(worker),
            armed: true,
        };
        let result = eval_worker_args(
            invocation.worker.as_mut().expect("cache worker").as_mut(),
            operation,
            ready,
        );
        invocation.finish(result)
    }

    fn poison(&self) {
        self.poisoned.set(true);
        if let Ok(mut workers) = self.workers.try_borrow_mut() {
            workers.clear();
        }
    }

    /// Number of prepared operations retained by this lane.
    #[must_use]
    pub fn prepared_worker_count(&self) -> usize {
        self.workers.borrow().len()
    }
}

struct CacheNativeGuard<'a> {
    cache: &'a ReadyValueCache,
    armed: bool,
}

impl<'a> CacheNativeGuard<'a> {
    fn new(cache: &'a ReadyValueCache) -> Self {
        Self { cache, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CacheNativeGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.cache.poison();
        }
    }
}

struct CacheInvocation<'a> {
    cache: &'a ReadyValueCache,
    operation: EvaluatedBytesOp,
    worker: Option<Box<EvaluatedBytesWorker>>,
    armed: bool,
}

impl<'a> CacheInvocation<'a> {
    fn finish<T>(
        mut self,
        result: Result<T, ReadyValueBoundaryError>,
    ) -> Result<T, ReadyValueBoundaryError> {
        let healthy = self.worker.as_ref().is_some_and(|worker| {
            worker.operation() == self.operation
                && worker.is_healthy()
                && worker.retained_storage().is_ok_and(|storage| {
                    storage.total_bytes() <= READY_VALUE_CACHE_WORKER_RETAINED_CAP
                })
        });
        if !healthy {
            self.cache.poisoned.set(true);
            drop(self.worker.take());
            self.cache.busy.set(false);
            self.armed = false;
            return match result {
                Err(primary) => Err(primary),
                Ok(_) => Err(ReadyValueBoundaryError::Scope {
                    kind: ScopeFailureKind::Contract,
                    reason: "ready-value cache worker failed postflight",
                }),
            };
        }
        let worker = self.worker.take().expect("healthy cache worker");
        let restore = match self.cache.workers.try_borrow_mut() {
            Ok(mut workers) if !workers.contains_key(&self.operation) => {
                workers.insert(self.operation, worker);
                Ok(())
            }
            _ => {
                self.cache.poisoned.set(true);
                Err(ReadyValueBoundaryError::Scope {
                    kind: ScopeFailureKind::Contract,
                    reason: "ready-value cache restore conflict",
                })
            }
        };
        self.cache.busy.set(false);
        self.armed = false;
        match result {
            Err(primary) => Err(primary),
            Ok(value) => restore.map(|()| value),
        }
    }
}

impl Drop for CacheInvocation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.cache.poisoned.set(true);
            drop(self.worker.take());
            self.cache.busy.set(false);
        }
    }
}

fn eval_worker_args(
    worker: &mut EvaluatedBytesWorker,
    operation: EvaluatedBytesOp,
    ready: EvaluatedArgs,
) -> Result<ComputedValue, ReadyValueBoundaryError> {
    if worker.operation() != operation {
        return Err(ReadyValueBoundaryError::Scope {
            kind: ScopeFailureKind::Contract,
            reason: "closed Bytes worker operation mismatch",
        });
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
            return ReadyValueBoundaryError::Frontend(EvalError::IntOverflow);
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
                return ReadyValueBoundaryError::Scope {
                    kind: ScopeFailureKind::Contract,
                    reason: "CONV overflow receipt lacks its digit payload",
                };
            };
            return ReadyValueBoundaryError::Frontend(EvalError::DataOutOfRange {
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
                    return ReadyValueBoundaryError::Frontend(EvalError::IncorrectArguments(
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
                return ReadyValueBoundaryError::Frontend(EvalError::IncorrectArguments(
                    message.to_owned(),
                ));
            }
            match (operation, report.sql_failure()) {
                (
                    EvaluatedBytesOp::UuidToBinParseNative,
                    Some(EvaluatedSqlFailureKind::UuidToBinWhitespace),
                ) => {
                    return ReadyValueBoundaryError::Frontend(EvalError::Unsupported(
                        "invalid UUID_TO_BIN whitespace",
                    ));
                }
                (
                    EvaluatedBytesOp::UuidToBinParseNative,
                    Some(EvaluatedSqlFailureKind::UuidToBinInvalid),
                ) => {
                    return ReadyValueBoundaryError::Frontend(EvalError::Unsupported(
                        "invalid UUID for UUID_TO_BIN",
                    ));
                }
                (
                    EvaluatedBytesOp::UuidVersionNative,
                    Some(EvaluatedSqlFailureKind::UuidVersionInvalid),
                ) => {
                    return ReadyValueBoundaryError::Frontend(EvalError::Unsupported(
                        "invalid UUID for UUID_VERSION",
                    ));
                }
                (
                    EvaluatedBytesOp::UuidTimestampNative,
                    Some(EvaluatedSqlFailureKind::UuidTimestampInvalid),
                ) => {
                    return ReadyValueBoundaryError::Frontend(EvalError::Unsupported(
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
                        return ReadyValueBoundaryError::Scope {
                            kind: ScopeFailureKind::Contract,
                            reason: "BIN_TO_UUID length receipt lacks its input payload",
                        };
                    };
                    return ReadyValueBoundaryError::Frontend(EvalError::WrongValueForType {
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
                        return ReadyValueBoundaryError::Scope {
                            kind: ScopeFailureKind::Contract,
                            reason: "binary arithmetic failure receipt lacks its native cause",
                        };
                    };
                    if operation == EvaluatedBytesOp::ModRealNative
                        && (cause.operation != BinaryArithmeticOperation::Modulo
                            || cause.kind != BinaryArithmeticErrorKind::FloatOverflow)
                    {
                        return ReadyValueBoundaryError::Scope {
                            kind: ScopeFailureKind::Contract,
                            reason: "native modulo failure receipt has an unexpected cause",
                        };
                    }
                    if operation == EvaluatedBytesOp::DivRealNative
                        && (cause.operation != BinaryArithmeticOperation::Divide
                            || cause.kind != BinaryArithmeticErrorKind::FloatOverflow)
                    {
                        return ReadyValueBoundaryError::Scope {
                            kind: ScopeFailureKind::Contract,
                            reason: "native division failure receipt has an unexpected cause",
                        };
                    }
                    if matches!(
                        operation,
                        EvaluatedBytesOp::IntDivIntSsNative
                            | EvaluatedBytesOp::IntDivIntUsNative
                            | EvaluatedBytesOp::IntDivIntSuNative
                            | EvaluatedBytesOp::IntDivIntUuNative
                    ) && (cause.operation != BinaryArithmeticOperation::IntDivide
                        || cause.kind != BinaryArithmeticErrorKind::IntOverflow)
                    {
                        return ReadyValueBoundaryError::Scope {
                            kind: ScopeFailureKind::Contract,
                            reason:
                                "native integer division failure receipt has an unexpected cause",
                        };
                    }
                    return ReadyValueBoundaryError::Frontend(match cause.kind {
                        BinaryArithmeticErrorKind::IntOverflow => EvalError::IntOverflow,
                        BinaryArithmeticErrorKind::FloatOverflow => EvalError::FloatOverflow,
                        BinaryArithmeticErrorKind::DecimalOverflow => EvalError::DecimalOverflow,
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
                        return ReadyValueBoundaryError::Scope {
                            kind: ScopeFailureKind::Contract,
                            reason: "binary arithmetic failure receipt lacks its legacy cause",
                        };
                    };
                    let expression = match cause.operation {
                        BinaryArithmeticOperation::Add => "ADD",
                        BinaryArithmeticOperation::Subtract => "SUBTRACT",
                        BinaryArithmeticOperation::Multiply => "MULTIPLY",
                        BinaryArithmeticOperation::Modulo => {
                            return ReadyValueBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "legacy modulo has no arithmetic SQL failure",
                            };
                        }
                        BinaryArithmeticOperation::Divide
                        | BinaryArithmeticOperation::IntDivide => {
                            return ReadyValueBoundaryError::Scope {
                                kind: ScopeFailureKind::Contract,
                                reason: "legacy division has no integer arithmetic SQL failure",
                            };
                        }
                    };
                    return ReadyValueBoundaryError::Frontend(EvalError::DataOutOfRange {
                        value: if cause.unsigned {
                            "BIGINT UNSIGNED"
                        } else {
                            "BIGINT"
                        },
                        expression: expression.to_owned(),
                    });
                }
                (
                    EvaluatedBytesOp::UnaryMinusIntNative | EvaluatedBytesOp::UnaryMinusUIntNative,
                    Some(EvaluatedSqlFailureKind::UnaryMinusNative),
                ) => {
                    let Some(cause) = report.native_unary_minus_error() else {
                        return ReadyValueBoundaryError::Scope {
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
                    return ReadyValueBoundaryError::Frontend(EvalError::DataOutOfRange {
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
                        return ReadyValueBoundaryError::Scope {
                            kind: ScopeFailureKind::Contract,
                            reason: "regexp failure receipt lacks its native cause",
                        };
                    };
                    return ReadyValueBoundaryError::Frontend(crate::regexp::native_regexp_error(
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
                        return ReadyValueBoundaryError::Scope {
                            kind: ScopeFailureKind::Contract,
                            reason: "vector failure receipt lacks its native cause",
                        };
                    };
                    return ReadyValueBoundaryError::Frontend(EvalError::Vector(cause.to_string()));
                }
                _ => {}
            }
        }
        ReadyValueBoundaryError::Kernel(ExpressionRuntimeFailure::from_local_eval(
            report.into_error(),
            Some(ExpressionRuntimeFailurePhase::Invoke),
        ))
    })
}

struct NativeComputedInt {
    value: Option<i64>,
    metadata: ValueMetadata,
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

fn result_kind_error() -> ReadyValueBoundaryError {
    ReadyValueBoundaryError::Scope {
        kind: ScopeFailureKind::Contract,
        reason: "closed Bytes result kind mismatch",
    }
}

pub(crate) fn native_time_result_contract_error() -> EvalError {
    result_kind_error().into_eval_error()
}

impl NativeComputedInt {
    fn into_datum(self) -> Result<Datum, ReadyValueBoundaryError> {
        from_scalar(
            ScalarValueRef::Int(self.value.as_ref()),
            EvalType::Int,
            &self.metadata,
        )
        .map_err(ReadyValueBoundaryError::Metadata)
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
            ReadyValueBoundaryError::Scope {
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
) -> Result<EvaluatedBytesResult, ReadyValueBoundaryError> {
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
            | EvaluatedBytesOp::TimestampDiffTextNative
            | EvaluatedBytesOp::TimestampDiffCoreNative
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
            | EvaluatedBytesOp::JsonSumCrc32SerdeNative
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
            | EvaluatedBytesOp::CoalesceEndNative
            | EvaluatedBytesOp::NullIfNative
            | EvaluatedBytesOp::CastRealUnsignedNative
            | EvaluatedBytesOp::BoundedStalenessHeadNative
            | EvaluatedBytesOp::BoundedStalenessFinishNative
            | EvaluatedBytesOp::ToBinaryNative
            | EvaluatedBytesOp::FromBinaryNative
            | EvaluatedBytesOp::ConvertUsingNative
            | EvaluatedBytesOp::StrToDateHeadNative
            | EvaluatedBytesOp::StrToDateFinishNative
            | EvaluatedBytesOp::StrToDateTypedFinishNative
            | EvaluatedBytesOp::ExtractSelectNative
            | EvaluatedBytesOp::ExtractDatetimeNative
            | EvaluatedBytesOp::ExtractDurationNative
            | EvaluatedBytesOp::ExtractMixedDurationNative
            | EvaluatedBytesOp::ExtractMixedFinishNative
            | EvaluatedBytesOp::ExtractCompositeNative
            | EvaluatedBytesOp::ExtremumHeadNative
            | EvaluatedBytesOp::ExtremumNumericNative
            | EvaluatedBytesOp::ExtremumTimeNative
            | EvaluatedBytesOp::ExtremumVectorNative
            | EvaluatedBytesOp::ExtremumStringNative
            | EvaluatedBytesOp::ExtremumTimeTextNative
            | EvaluatedBytesOp::ExtremumTimeContextNative
            | EvaluatedBytesOp::ExtremumFinishNative
            | EvaluatedBytesOp::IntervalEagerHeadNative
            | EvaluatedBytesOp::IntervalLazyHeadNative
            | EvaluatedBytesOp::IntervalStepNative
            | EvaluatedBytesOp::DateArithmeticHeadNative
            | EvaluatedBytesOp::DateArithmeticDurationHeadNative
            | EvaluatedBytesOp::DateArithmeticStepNative
            | EvaluatedBytesOp::DateArithmeticOverflowNative
            | EvaluatedBytesOp::LegacyDateArithmeticTextHeadNative
            | EvaluatedBytesOp::LegacyDateArithmeticTimeHeadNative
            | EvaluatedBytesOp::LegacyDateArithmeticDurationHeadNative
            | EvaluatedBytesOp::LegacyDateArithmeticStepNative
            | EvaluatedBytesOp::LegacyDateArithmeticParseNative
            | EvaluatedBytesOp::InTypedValuesNative
            | EvaluatedBytesOp::InLegacyIntHeadNative
            | EvaluatedBytesOp::InLegacyStringHeadNative
            | EvaluatedBytesOp::InLegacyStepNative
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
                    ReadyValueBoundaryError::Frontend(super::math_decimal_bridge_error(error))
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
                    return Err(ReadyValueBoundaryError::Scope {
                        kind: ScopeFailureKind::Contract,
                        reason: "decimal division disposition contradicts its computed value",
                    });
                }
            }
            let value = value
                .map(|value| tidb_datatype::Decimal::try_from_shared_math(&value, usize::MAX))
                .transpose()
                .map_err(|error| {
                    ReadyValueBoundaryError::Frontend(super::math_decimal_bridge_error(error))
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

fn evaluate_cached_args<T>(
    cache: &ReadyValueCache,
    prepare: impl FnOnce() -> Result<(EvaluatedBytesOp, EvaluatedArgs), EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult) -> Result<T, EvalError>,
) -> Result<T, ReadyValueBoundaryError> {
    let mut guard = CacheNativeGuard::new(cache);
    let result = (|| {
        let (operation, ready) = prepare().map_err(ReadyValueBoundaryError::Frontend)?;
        let computed = cache.run_args(operation, ready)?;
        pack(materialize_computed(operation, computed)?).map_err(ReadyValueBoundaryError::Frontend)
    })();
    guard.disarm();
    result
}

pub(crate) fn evaluate_prepared_args_in<T>(
    ctx: &dyn Columns,
    prepare: impl FnOnce() -> Result<(EvaluatedBytesOp, EvaluatedArgs), EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    route_prepared_args_in(ctx, prepare, |computed, _columns| pack(computed))
}

/// Lend the selected lane cache to a dependent stage after the first worker call
/// has finished and its result is owned. The existing guard and one-shot cache
/// remain alive through this callback; no worker borrow crosses it.
///
/// Bind directly rather than rediscovering a possibly different cache through
/// `with_columns`. Callers must use these columns for their dependent stage.
pub(crate) fn evaluate_prepared_args_scoped_in<T>(
    ctx: &dyn Columns,
    prepare: impl FnOnce() -> Result<(EvaluatedBytesOp, EvaluatedArgs), EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult, &dyn Columns) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    route_prepared_args_in(ctx, prepare, pack)
}

fn route_prepared_args_in<T>(
    ctx: &dyn Columns,
    prepare: impl FnOnce() -> Result<(EvaluatedBytesOp, EvaluatedArgs), EvalError>,
    pack: impl FnOnce(EvaluatedBytesResult, &dyn Columns) -> Result<T, EvalError>,
) -> Result<T, EvalError> {
    if let Some(cache) = ctx.ready_value_cache() {
        return evaluate_cached_args(cache, prepare, |computed| pack(computed, ctx))
            .map_err(ReadyValueBoundaryError::into_eval_error);
    }
    let result = (|| {
        // No lane capability: preserve frontend precedence, then use an affine
        // one-shot cache. It owns exactly the demanded workers and drops them
        // before returning; no session/shared pool is synthesized.
        let ready = prepare().map_err(ReadyValueBoundaryError::Frontend)?;
        let cache = ReadyValueCache::new();
        evaluate_cached_args(
            &cache,
            || Ok(ready),
            |computed| {
                let columns = ScopedReadyValueColumns {
                    native: ctx,
                    cache: &cache,
                };
                pack(computed, &columns)
            },
        )
    })();
    result.map_err(ReadyValueBoundaryError::into_eval_error)
}

/// One closed operation router, sharing the ready-value capabilities.
/// Neither frontend callback enters C4: coercion precedes admission, and native
/// packing follows the exclusive invocation. The lane cache guards both; the
/// no-capability route retains its original preparation-before-cache precedence.
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
        return Err(ReadyValueBoundaryError::Scope {
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
        return Err(ReadyValueBoundaryError::Scope {
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
                EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
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
                EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
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
                    EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
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
                    EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
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

/// Compare the actual full-width legacy integer pair through the caller's lane cache.
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
                        return Err(ReadyValueBoundaryError::Scope {
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
            return Err(ReadyValueBoundaryError::Scope {
                kind: ScopeFailureKind::Contract,
                reason: "integer division is not a legacy decimal arithmetic profile",
            }
            .into_eval_error());
        }
        BinaryArithmeticOperation::Divide => {
            return Err(ReadyValueBoundaryError::Scope {
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
            EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
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
            return Err(ReadyValueBoundaryError::Scope {
                kind: ScopeFailureKind::Contract,
                reason: "modulo is unsupported by the decimal fast contract",
            }
            .into_eval_error());
        }
        BinaryArithmeticOperation::Divide | BinaryArithmeticOperation::IntDivide => {
            return Err(ReadyValueBoundaryError::Scope {
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

/// Evaluate legacy LIKE through the caller's lane cache without native matching.
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
                    return Err(ReadyValueBoundaryError::Scope {
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
                    return Err(ReadyValueBoundaryError::Scope {
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
                return Err(ReadyValueBoundaryError::Scope {
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

/// Compatible single-Bytes entry; all shapes use the same context/cache driver.
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

/// Opaque lexical Columns binding created by [`ReadyValueCache::with_columns`].
///
/// It borrows the original native context and one executor-lane cache. Ordinary
/// methods forward unchanged; only `ready_value_cache` is overridden.
pub struct ScopedReadyValueColumns<'native, 'cache> {
    native: &'native dyn Columns,
    cache: &'cache ReadyValueCache,
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

impl Columns for ScopedReadyValueColumns<'_, '_> {
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        Some(self.cache)
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
#[path = "ready_value_tests.rs"]
mod tests;
