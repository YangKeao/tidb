// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native opaque ownership of a ready-value adapter failure, not a backend error.
//!
//! Pool, scope and result-bridge failures retain their actual origin and original
//! cause. They are not reconstructed as `LocalError`, SQL overflow or query OOM.
//! Frontend errors pass through unchanged and do not belong in this envelope.
//! Neither this carrier nor its fixed message accessor activates SQL dispatch.

use std::fmt;
use std::sync::Arc;

use tidb_datatype::tikv_compat::value::BridgeError;

use super::ready_value::{OwnerErrorKind, ReadyValueOwnerError};

/// Native adapter failure classes, independent of backend error codes or text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpressionAdapterFailureClass {
    /// The explicit pool policy is inconsistent.
    PoolPolicy,
    /// The pool refused a local resource demand.
    PoolResource,
    /// The independently owned execution is closed.
    PoolClosed,
    /// The pool has been poisoned.
    PoolPoisoned,
    /// A pool lifecycle or accounting contract was violated.
    PoolContract,
    /// The operation scope has been poisoned.
    ScopePoisoned,
    /// The scope was reentered while its runtime was borrowed.
    ScopeReentry,
    /// A scope ownership or borrow contract was violated.
    ScopeContract,
    /// A result could not cross the checked representation bridge.
    ResultContract,
}

impl ExpressionAdapterFailureClass {
    fn from_owner_kind(kind: OwnerErrorKind) -> Self {
        match kind {
            OwnerErrorKind::Policy => Self::PoolPolicy,
            OwnerErrorKind::Resource => Self::PoolResource,
            OwnerErrorKind::Closed => Self::PoolClosed,
            OwnerErrorKind::Poisoned => Self::PoolPoisoned,
            OwnerErrorKind::Contract => Self::PoolContract,
        }
    }

    const fn from_scope_kind(kind: ScopeFailureKind) -> Self {
        match kind {
            ScopeFailureKind::Poisoned => Self::ScopePoisoned,
            ScopeFailureKind::Reentry => Self::ScopeReentry,
            ScopeFailureKind::Contract => Self::ScopeContract,
        }
    }

    const fn client_message(self) -> &'static str {
        match self {
            Self::PoolPolicy => "Expression runtime pool policy failure",
            Self::PoolResource => "Expression runtime pool resource limit exceeded",
            Self::PoolClosed => "Expression runtime execution is closed",
            Self::PoolPoisoned => "Expression runtime pool is poisoned",
            Self::PoolContract => "Expression runtime pool contract failure",
            Self::ScopePoisoned => "Expression runtime scope is poisoned",
            Self::ScopeReentry => "Expression runtime scope reentry",
            Self::ScopeContract => "Expression runtime scope contract failure",
            Self::ResultContract => "Expression runtime result contract failure",
        }
    }
}

/// The native component that supplied the original adapter failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpressionAdapterFailureOrigin {
    /// Pool policy, resource or lifecycle management.
    Pool,
    /// The affine operation scope.
    Scope,
    /// Checked result-representation transport.
    Bridge,
}

/// Classification supplied by the scope at the actual failure site.
///
/// The accompanying reason is retained, not parsed to reconstruct this kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScopeFailureKind {
    Poisoned,
    Reentry,
    Contract,
}

// No Clone or Debug: the original typed causes stay behind the opaque handle.
// BridgeError may contain backend EvalType values; none is publicly exposed.
enum AdapterFailureCause {
    Owner(ReadyValueOwnerError),
    Scope {
        kind: ScopeFailureKind,
        reason: &'static str,
    },
    Bridge(BridgeError),
}

/// One immutable adapter-failure capture, owned by a native opaque handle.
///
/// Clone shares the capture; equality is capture identity, not equality of the
/// original reasons, classes or error values. Independently captured failures
/// remain unequal. Debug emits only native class and origin.
///
/// Construction is private to the adaptation boundary. There is no public
/// constructor, raw-cause accessor, `Display`, `Error`/`source` or downcast hook.
/// Capturing allocates an ordinary Arc on the error path; this is not a claim of
/// allocation-failure recovery or inclusion in the pool/worker byte ledger.
pub struct ExpressionAdapterFailure {
    cause: Arc<AdapterFailureCause>,
}

impl ExpressionAdapterFailure {
    /// Moves a native pool failure without cloning or reclassifying its text.
    #[must_use]
    pub(super) fn from_owner(cause: ReadyValueOwnerError) -> Self {
        Self {
            cause: Arc::new(AdapterFailureCause::Owner(cause)),
        }
    }

    /// Captures an explicitly classified scope failure with its original reason.
    #[must_use]
    pub(super) fn from_scope(kind: ScopeFailureKind, reason: &'static str) -> Self {
        Self {
            cause: Arc::new(AdapterFailureCause::Scope { kind, reason }),
        }
    }

    /// Moves the complete bridge error, including any private backend type data.
    #[must_use]
    pub(super) fn from_bridge(cause: BridgeError) -> Self {
        Self {
            cause: Arc::new(AdapterFailureCause::Bridge(cause)),
        }
    }

    /// Returns the native class derived only from the structured cause kind.
    #[must_use]
    pub fn class(&self) -> ExpressionAdapterFailureClass {
        match self.cause.as_ref() {
            AdapterFailureCause::Owner(cause) => {
                ExpressionAdapterFailureClass::from_owner_kind(cause.kind())
            }
            AdapterFailureCause::Scope { kind, .. } => {
                ExpressionAdapterFailureClass::from_scope_kind(*kind)
            }
            AdapterFailureCause::Bridge(_) => ExpressionAdapterFailureClass::ResultContract,
        }
    }

    /// Returns the captured native origin, never inferred from a reason string.
    #[must_use]
    pub fn origin(&self) -> ExpressionAdapterFailureOrigin {
        match self.cause.as_ref() {
            AdapterFailureCause::Owner(_) => ExpressionAdapterFailureOrigin::Pool,
            AdapterFailureCause::Scope { .. } => ExpressionAdapterFailureOrigin::Scope,
            AdapterFailureCause::Bridge(_) => ExpressionAdapterFailureOrigin::Bridge,
        }
    }

    /// Returns the approved fixed native text for the generic 1105/HY000 path.
    ///
    /// The terminal renderer owns SQL error construction and the existing Eval
    /// origin flag. Original reasons and backend type details are not wire text.
    #[must_use]
    pub fn client_message(&self) -> &'static str {
        self.class().client_message()
    }
}

impl Clone for ExpressionAdapterFailure {
    fn clone(&self) -> Self {
        Self {
            cause: Arc::clone(&self.cause),
        }
    }
}

impl PartialEq for ExpressionAdapterFailure {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cause, &other.cause)
    }
}

impl Eq for ExpressionAdapterFailure {}

impl fmt::Debug for ExpressionAdapterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpressionAdapterFailure")
            .field("class", &self.class())
            .field("origin", &self.origin())
            .finish()
    }
}

impl ReadyValueOwnerError {
    /// Retains this native lifecycle/configuration cause for evaluation diagnostics.
    ///
    /// This explicit conversion accepts only the opaque native owner error, not
    /// backend or bridge errors, and does not change generic error inference via
    /// a new From implementation. It never creates SQL arithmetic status. Its
    /// Arc allocation is outside the pool ledger, as with other adapter captures.
    #[must_use]
    pub fn into_eval_error(self) -> crate::EvalError {
        crate::EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_owner(self))
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::ptr;

    use tidb_datatype::{DatumKind, FieldTypeCode};
    use tidb_query_datatype::EvalType;

    use super::super::ready_value::ReadyValuePoolPolicy;
    use super::{
        AdapterFailureCause, BridgeError, ExpressionAdapterFailure,
        ExpressionAdapterFailureClass as Class, ExpressionAdapterFailureOrigin as Origin,
        OwnerErrorKind, ScopeFailureKind,
    };

    #[test]
    fn public_owner_error_conversion_retains_the_native_cause() {
        let cause =
            ReadyValuePoolPolicy::checked(0, 1, usize::MAX, 1, 1, 64, 16, usize::MAX).unwrap_err();
        let expected = cause.clone();
        let native = cause.into_eval_error();
        let crate::EvalError::ExpressionAdapterFailure(failure) = native else {
            panic!("native owner error must retain its adapter origin");
        };
        assert_eq!(failure.class(), Class::PoolPolicy);
        assert_eq!(failure.origin(), Origin::Pool);
        let AdapterFailureCause::Owner(original) = failure.cause.as_ref() else {
            panic!("native conversion changed the cause variant");
        };
        assert_eq!(original, &expected);
        assert_eq!(failure.clone(), failure);
        assert_eq!(
            failure.client_message(),
            "Expression runtime pool policy failure"
        );
    }

    #[test]
    fn all_owner_kinds_have_distinct_native_classes_and_fixed_messages() {
        // Classification-policy coverage, not a claim that the public API can
        // manufacture poisoned or contract-violating owner states.
        for (kind, class, message) in [
            (
                OwnerErrorKind::Policy,
                Class::PoolPolicy,
                "Expression runtime pool policy failure",
            ),
            (
                OwnerErrorKind::Resource,
                Class::PoolResource,
                "Expression runtime pool resource limit exceeded",
            ),
            (
                OwnerErrorKind::Closed,
                Class::PoolClosed,
                "Expression runtime execution is closed",
            ),
            (
                OwnerErrorKind::Poisoned,
                Class::PoolPoisoned,
                "Expression runtime pool is poisoned",
            ),
            (
                OwnerErrorKind::Contract,
                Class::PoolContract,
                "Expression runtime pool contract failure",
            ),
        ] {
            assert_eq!(Class::from_owner_kind(kind), class);
            assert_eq!(class.client_message(), message);
        }
    }

    #[test]
    fn real_policy_failure_retains_the_original_owner_capture() {
        // Real policy validation produces the private cause; no owner-error
        // constructor or test-only factory is exposed for this test.
        let cause =
            ReadyValuePoolPolicy::checked(0, 1, usize::MAX, 1, 1, 64, 16, usize::MAX).unwrap_err();
        let expected =
            ReadyValuePoolPolicy::checked(0, 1, usize::MAX, 1, 1, 64, 16, usize::MAX).unwrap_err();
        let failure = ExpressionAdapterFailure::from_owner(cause);
        let address = failure.cause.as_ref() as *const _;
        let cloned = failure.clone();
        assert_eq!(failure, cloned);
        drop(failure);
        assert!(ptr::eq(cloned.cause.as_ref(), address));
        let AdapterFailureCause::Owner(original) = cloned.cause.as_ref() else {
            panic!("owner cause changed origin");
        };
        assert_eq!(original, &expected);
        assert_eq!(original.kind(), OwnerErrorKind::Policy);
        assert_eq!(cloned.class(), Class::PoolPolicy);
        assert_eq!(cloned.origin(), Origin::Pool);
        assert_eq!(
            cloned.client_message(),
            "Expression runtime pool policy failure"
        );
        let independent = ExpressionAdapterFailure::from_owner(expected);
        assert_ne!(cloned, independent);
    }

    #[test]
    fn real_control_budget_failure_stays_native_pool_resource() {
        let cause = ReadyValuePoolPolicy::checked(0, 0, 0, 1, 1, 64, 16, usize::MAX).unwrap_err();
        assert_eq!(cause.kind(), OwnerErrorKind::Resource);
        let failure = ExpressionAdapterFailure::from_owner(cause);
        assert_eq!(failure.class(), Class::PoolResource);
        assert_eq!(failure.origin(), Origin::Pool);
        assert_eq!(
            failure.client_message(),
            "Expression runtime pool resource limit exceeded"
        );
        let AdapterFailureCause::Owner(original) = failure.cause.as_ref() else {
            panic!("pool failure was reclassified");
        };
        assert_eq!(original.kind(), OwnerErrorKind::Resource);
    }

    #[test]
    fn scope_kinds_preserve_reason_without_parsing_it() {
        const REASON: &str = "private identical reason: poisoned reentry 1690 TiKV";
        for (kind, class, message) in [
            (
                ScopeFailureKind::Poisoned,
                Class::ScopePoisoned,
                "Expression runtime scope is poisoned",
            ),
            (
                ScopeFailureKind::Reentry,
                Class::ScopeReentry,
                "Expression runtime scope reentry",
            ),
            (
                ScopeFailureKind::Contract,
                Class::ScopeContract,
                "Expression runtime scope contract failure",
            ),
        ] {
            let failure = ExpressionAdapterFailure::from_scope(kind, REASON);
            assert_eq!(failure.class(), class);
            assert_eq!(failure.origin(), Origin::Scope);
            assert_eq!(failure.client_message(), message);
            let AdapterFailureCause::Scope {
                kind: original_kind,
                reason,
            } = failure.cause.as_ref()
            else {
                panic!("scope cause changed origin");
            };
            assert_eq!(*original_kind, kind);
            assert_eq!(*reason, REASON);
            assert_eq!(reason.as_ptr(), REASON.as_ptr());
        }
    }

    #[test]
    fn all_bridge_shapes_keep_their_original_fields_behind_one_native_class() {
        let constructors: [fn() -> BridgeError; 9] = [
            || BridgeError::UnsupportedFieldType(FieldTypeCode::LongLong),
            || BridgeError::ArrayFieldType(FieldTypeCode::Json),
            || BridgeError::MetadataOutOfRange {
                field: "private metadata field",
                value: i128::MAX,
            },
            || BridgeError::InvalidElementEncoding(usize::MAX),
            || BridgeError::UnsupportedDatumKind(DatumKind::String),
            || BridgeError::UnsupportedEvalType(EvalType::Json),
            || BridgeError::EvalTypeMismatch {
                expected: EvalType::Int,
                actual: EvalType::Real,
            },
            || BridgeError::InvalidValueMetadata {
                kind: DatumKind::String,
                field: "private result metadata",
            },
            || BridgeError::NonRepresentableReal {
                bits: 0x7ff8_0000_0000_0042,
            },
        ];
        for constructor in constructors {
            let cause = constructor();
            let discriminant = mem::discriminant(&cause);
            let failure = ExpressionAdapterFailure::from_bridge(cause);
            let address = failure.cause.as_ref() as *const _;
            let cloned = failure.clone();
            drop(failure);
            assert!(ptr::eq(cloned.cause.as_ref(), address));
            let AdapterFailureCause::Bridge(original) = cloned.cause.as_ref() else {
                panic!("bridge cause changed origin");
            };
            assert_eq!(mem::discriminant(original), discriminant);
            assert_eq!(original, &constructor());
            assert_eq!(cloned.class(), Class::ResultContract);
            assert_eq!(cloned.origin(), Origin::Bridge);
            assert_eq!(
                cloned.client_message(),
                "Expression runtime result contract failure"
            );
        }
    }

    #[test]
    fn clone_equality_is_capture_identity_not_equal_cause_content() {
        let failure =
            ExpressionAdapterFailure::from_scope(ScopeFailureKind::Reentry, "same original reason");
        let cloned = failure.clone();
        let another_clone = cloned.clone();
        let independent =
            ExpressionAdapterFailure::from_scope(ScopeFailureKind::Reentry, "same original reason");
        assert_eq!(failure, cloned);
        assert_eq!(cloned, failure);
        assert_eq!(failure, another_clone);
        assert_eq!(failure.class(), independent.class());
        assert_eq!(failure.origin(), independent.origin());
        assert_eq!(failure.client_message(), independent.client_message());
        assert_ne!(failure, independent);
        assert!(ptr::eq(failure.cause.as_ref(), cloned.cause.as_ref()));

        let bridge = ExpressionAdapterFailure::from_bridge(BridgeError::InvalidElementEncoding(3));
        let independent_bridge =
            ExpressionAdapterFailure::from_bridge(BridgeError::InvalidElementEncoding(3));
        assert_eq!(bridge, bridge.clone());
        assert_ne!(bridge, independent_bridge);
    }

    #[test]
    fn native_eval_error_preserves_adapter_capture_and_required_traits() {
        fn native_traits<T: Clone + Eq + std::fmt::Debug + Send + Sync>() {}
        native_traits::<ExpressionAdapterFailure>();
        native_traits::<crate::EvalError>();

        let failure = ExpressionAdapterFailure::from_scope(
            ScopeFailureKind::Contract,
            "private original scope detail",
        );
        let native = crate::EvalError::ExpressionAdapterFailure(failure.clone());
        let cloned_native = native.clone();
        assert_eq!(native, cloned_native);
        drop(native);
        let crate::EvalError::ExpressionAdapterFailure(retained) = cloned_native else {
            panic!("native error changed variant");
        };
        assert_eq!(retained, failure);
        assert!(ptr::eq(retained.cause.as_ref(), failure.cause.as_ref()));
    }

    #[test]
    fn debug_reveals_only_native_class_and_origin_for_every_source() {
        let owner =
            ReadyValuePoolPolicy::checked(0, 1, usize::MAX, 1, 1, 64, 16, usize::MAX).unwrap_err();
        let failures = [
            ExpressionAdapterFailure::from_owner(owner),
            ExpressionAdapterFailure::from_scope(
                ScopeFailureKind::Contract,
                "private TiKV detail code=1690",
            ),
            ExpressionAdapterFailure::from_bridge(BridgeError::EvalTypeMismatch {
                expected: EvalType::Json,
                actual: EvalType::Real,
            }),
        ];
        for failure in failures {
            assert_eq!(
                format!("{failure:?}"),
                format!(
                    "ExpressionAdapterFailure {{ class: {:?}, origin: {:?} }}",
                    failure.class(),
                    failure.origin()
                )
            );
            let pretty = format!("{failure:#?}");
            for hidden in [
                "private",
                "TiKV",
                "1690",
                "ReadyValueOwnerError",
                "OwnerErrorKind",
                "EvalTypeMismatch",
                "Json",
                "Real",
                "reason",
                "cause",
                "ASCII",
            ] {
                assert!(!pretty.contains(hidden), "leaked {hidden} in {pretty}");
            }
        }
    }
}
