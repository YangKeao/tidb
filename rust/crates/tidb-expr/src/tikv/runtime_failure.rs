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

//! Opaque ownership of a C4 failure at the TiDB boundary.
//!
//! This module does not register an evaluator caller. The native `EvalError`
//! variant retains this payload while keeping its existing
//! `Debug + Clone + PartialEq + Eq` contract. The backend error is moved once,
//! never cloned, parsed, rendered into a replacement cause, or re-evaluated.
//!
//! Only an actual C4 `LocalError` belongs here. Frontend errors must move through
//! unchanged; pool, scope and bridge failures retain their distinct origins.
//! Existing failed-fold suppression and DEFAULT error remapping are not changed.

use std::fmt;
use std::sync::Arc;

use tidb_query_expr::local::LocalError;

/// Native categories for the six outer runtime-error discriminants.
///
/// These are not SQL conditions. In particular, a resource refusal is not an
/// arithmetic overflow or a statement-memory-limit error. An evaluation cause's
/// numeric code does not authorize choosing a native builtin diagnostic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpressionRuntimeFailureClass {
    /// An invalid specification or violated worker contract.
    InvalidSpecification,
    /// An invalid batch or violated result contract.
    InvalidBatch,
    /// A demanded input binding violated its contract.
    InputContract,
    /// A host service violated its contract.
    HostContract,
    /// The local runtime refused a resource demand.
    ResourceLimit,
    /// The backend returned its original structured evaluation or storage error.
    Evaluation,
}

impl ExpressionRuntimeFailureClass {
    // Fixed native messages approved for the existing generic SQL error path.
    // No backend reason text, custom code, charset or expression enters them.
    const fn client_message(self) -> &'static str {
        match self {
            Self::InvalidSpecification => "Expression runtime specification failure",
            Self::InvalidBatch => "Expression runtime batch contract failure",
            Self::InputContract => "Expression runtime input contract failure",
            Self::HostContract => "Expression runtime host contract failure",
            Self::ResourceLimit => "Expression runtime resource limit exceeded",
            Self::Evaluation => "Expression runtime evaluation failure",
        }
    }
}

/// The boundary operation known at the site that captured a failure.
///
/// This is not a kernel site, SQL source location or inferred execution origin.
/// A caller may supply a phase only while it still knows which operation failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpressionRuntimeFailurePhase {
    /// Preparing the closed runtime worker.
    Prepare,
    /// Invoking the runtime worker; not necessarily entering its kernel body.
    Invoke,
    /// Observing retained storage or checking the worker on publication/return.
    Observe,
}

// Deliberately no Clone, Debug or equality implementation for this payload.
// The complete backend cause, including any boxed inner error, is retained.
struct RuntimeFailurePayload {
    cause: LocalError,
    phase: Option<ExpressionRuntimeFailurePhase>,
}

/// A native-only handle to one immutable, privately owned runtime failure.
///
/// Cloning shares the same cause. Equality means that two handles own that same
/// failure, not that their classes, messages or backend error codes are equal.
/// Independently captured failures remain unequal even if their text is identical.
///
/// Debug exposes only native class and known phase. There is intentionally no
/// public constructor, backend accessor, `Display`, `Error`/`source`, downcast
/// hook, or blanket `From<LocalError>` conversion. The private boundary retains
/// the original cause without making it part of the native public error API.
pub struct ExpressionRuntimeFailure {
    payload: Arc<RuntimeFailurePayload>,
}

impl ExpressionRuntimeFailure {
    /// Moves an actual C4 cause into the native opaque payload.
    ///
    /// `None` explicitly means unattributed. The evaluated-value caller captures
    /// Prepare, Observe or Invoke at the actual failing API call, before placing
    /// this handle in its Kernel variant; Invoke does not prove kernel entry.
    /// There is no default phase and no phase inference from a code or message.
    ///
    /// This allocates one ordinary Arc on the error path. It does not promise
    /// allocation-failure recovery or inclusion in the worker/pool byte ledger.
    #[must_use]
    pub(super) fn from_local_eval(
        cause: LocalError,
        phase: Option<ExpressionRuntimeFailurePhase>,
    ) -> Self {
        Self {
            payload: Arc::new(RuntimeFailurePayload { cause, phase }),
        }
    }

    /// Classifies only the original outer error discriminant.
    #[must_use]
    pub fn class(&self) -> ExpressionRuntimeFailureClass {
        match &self.payload.cause {
            LocalError::InvalidSpec(_) => ExpressionRuntimeFailureClass::InvalidSpecification,
            LocalError::InvalidBatch(_) => ExpressionRuntimeFailureClass::InvalidBatch,
            LocalError::BindingContract(_) => ExpressionRuntimeFailureClass::InputContract,
            LocalError::HostContract(_) => ExpressionRuntimeFailureClass::HostContract,
            LocalError::ResourceLimit(_) => ExpressionRuntimeFailureClass::ResourceLimit,
            LocalError::Evaluation(_) => ExpressionRuntimeFailureClass::Evaluation,
        }
    }

    /// Returns the explicitly captured phase, or `None` for an unattributed cause.
    #[must_use]
    pub fn phase(&self) -> Option<ExpressionRuntimeFailurePhase> {
        self.payload.phase
    }

    /// Returns the fixed native class text, never a rendered backend cause.
    ///
    /// The executor's terminal renderer uses the existing `MysqlError::unknown`
    /// 1105/HY000 route and the Eval `from_evaluation` handling. This accessor
    /// does not itself build a SQL error or override native wrappers.
    #[must_use]
    pub fn client_message(&self) -> &'static str {
        self.class().client_message()
    }

    /// Borrows the original cause only inside the private adaptation boundary.
    pub(super) fn local_error(&self) -> &LocalError {
        &self.payload.cause
    }
}

impl Clone for ExpressionRuntimeFailure {
    fn clone(&self) -> Self {
        Self {
            payload: Arc::clone(&self.payload),
        }
    }
}

impl PartialEq for ExpressionRuntimeFailure {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.payload, &other.payload)
    }
}

impl Eq for ExpressionRuntimeFailure {}

impl fmt::Debug for ExpressionRuntimeFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpressionRuntimeFailure")
            .field("class", &self.class())
            .field("phase", &self.phase())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::ptr;

    use tidb_query_datatype::codec::Error as BackendCodecError;

    use super::{
        ExpressionRuntimeFailure, ExpressionRuntimeFailureClass as Class,
        ExpressionRuntimeFailurePhase as Phase, LocalError,
    };

    #[test]
    fn native_eval_error_keeps_owned_cause_identity_and_redaction() {
        fn native_traits<T: Clone + Eq + std::fmt::Debug + Send + Sync>() {}
        native_traits::<crate::EvalError>();
        let cause = LocalError::ResourceLimit("private backend reason 1690".to_owned());
        let failure = ExpressionRuntimeFailure::from_local_eval(cause, Some(Phase::Invoke));
        let original = failure.clone();
        let native = crate::EvalError::ExpressionRuntimeFailure(failure);
        let cloned = native.clone();
        assert_eq!(native, cloned);
        assert_ne!(
            native,
            crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit("private backend reason 1690".to_owned()),
                Some(Phase::Invoke),
            ))
        );
        drop(native);
        let crate::EvalError::ExpressionRuntimeFailure(retained) = cloned else {
            panic!("native error changed its variant");
        };
        assert!(ptr::eq(retained.local_error(), original.local_error()));
        assert_eq!(retained.phase(), Some(Phase::Invoke));
        assert_eq!(retained.class(), Class::ResourceLimit);
        let rendered_debug = format!("{:?}", crate::EvalError::ExpressionRuntimeFailure(retained));
        assert!(rendered_debug.contains("ResourceLimit"));
        assert!(!rendered_debug.contains("private backend reason"));
        assert!(!rendered_debug.contains("1690"));
        // This tests the real native envelope, not a SQL renderer or a public
        // producer; those boundaries have separate integration obligations.
    }

    #[test]
    fn six_outer_variants_keep_distinct_classes_and_fixed_messages() {
        let cases = [
            (
                LocalError::InvalidSpec("same private reason".to_owned()),
                Class::InvalidSpecification,
                "Expression runtime specification failure",
            ),
            (
                LocalError::InvalidBatch("same private reason".to_owned()),
                Class::InvalidBatch,
                "Expression runtime batch contract failure",
            ),
            (
                LocalError::BindingContract("same private reason".to_owned()),
                Class::InputContract,
                "Expression runtime input contract failure",
            ),
            (
                LocalError::HostContract("same private reason".to_owned()),
                Class::HostContract,
                "Expression runtime host contract failure",
            ),
            (
                LocalError::ResourceLimit("same private reason".to_owned()),
                Class::ResourceLimit,
                "Expression runtime resource limit exceeded",
            ),
            (
                LocalError::Evaluation(
                    BackendCodecError::Eval("same private reason".to_owned(), 1690).into(),
                ),
                Class::Evaluation,
                "Expression runtime evaluation failure",
            ),
        ];
        for (cause, class, message) in cases {
            let failure = ExpressionRuntimeFailure::from_local_eval(cause, None);
            assert_eq!(failure.class(), class);
            assert_eq!(failure.client_message(), message);
            assert_eq!(failure.phase(), None);
            assert_eq!(
                format!("{failure:?}"),
                format!("ExpressionRuntimeFailure {{ class: {class:?}, phase: None }}")
            );
        }
    }

    #[test]
    fn phase_is_explicit_and_none_remains_unattributed() {
        for phase in [
            None,
            Some(Phase::Prepare),
            Some(Phase::Invoke),
            Some(Phase::Observe),
        ] {
            let failure = ExpressionRuntimeFailure::from_local_eval(
                LocalError::ResourceLimit("not evidence of a kernel site".to_owned()),
                phase,
            );
            assert_eq!(failure.phase(), phase);
            assert_eq!(failure.clone().phase(), phase);
            assert_eq!(failure.class(), Class::ResourceLimit);
        }
    }

    #[test]
    fn clone_equality_is_cause_identity_not_equal_error_text() {
        // These are the derives needed by native EvalError, without changing it.
        #[derive(Clone, Debug, PartialEq, Eq)]
        struct NativePayloadShape(ExpressionRuntimeFailure);

        let failure = ExpressionRuntimeFailure::from_local_eval(
            LocalError::InvalidSpec("identical reason".to_owned()),
            Some(Phase::Prepare),
        );
        let cloned = failure.clone();
        let another_clone = cloned.clone();
        let independent = ExpressionRuntimeFailure::from_local_eval(
            LocalError::InvalidSpec("identical reason".to_owned()),
            Some(Phase::Prepare),
        );
        assert_eq!(failure, cloned);
        assert_eq!(cloned, failure);
        assert_eq!(cloned, another_clone);
        assert_eq!(failure, another_clone);
        assert!(ptr::eq(failure.local_error(), cloned.local_error()));
        assert_eq!(failure.class(), independent.class());
        assert_eq!(failure.phase(), independent.phase());
        assert_eq!(failure.client_message(), independent.client_message());
        assert_ne!(failure, independent);

        let native = NativePayloadShape(failure);
        let native_clone = native.clone();
        assert_eq!(native, native_clone);
        drop(native);
        drop(native_clone);
        drop(another_clone);
        assert!(matches!(
            cloned.local_error(),
            LocalError::InvalidSpec(reason) if reason == "identical reason"
        ));
    }

    #[test]
    fn string_causes_keep_their_original_allocation_and_discriminant() {
        let constructors: [fn(String) -> LocalError; 5] = [
            LocalError::InvalidSpec,
            LocalError::InvalidBatch,
            LocalError::BindingContract,
            LocalError::HostContract,
            LocalError::ResourceLimit,
        ];
        for constructor in constructors {
            let mut reason = String::with_capacity(128);
            reason.push_str("original backend-owned reason");
            let address = reason.as_ptr();
            let capacity = reason.capacity();
            let cause = constructor(reason);
            let discriminant = mem::discriminant(&cause);
            let failure = ExpressionRuntimeFailure::from_local_eval(cause, None);
            let cloned = failure.clone();
            assert!(ptr::eq(failure.local_error(), cloned.local_error()));
            drop(failure);
            assert_eq!(mem::discriminant(cloned.local_error()), discriminant);
            let reason = match cloned.local_error() {
                LocalError::InvalidSpec(reason)
                | LocalError::InvalidBatch(reason)
                | LocalError::BindingContract(reason)
                | LocalError::HostContract(reason)
                | LocalError::ResourceLimit(reason) => reason,
                LocalError::Evaluation(_) => panic!("string cause changed variant"),
            };
            assert_eq!(reason.as_ptr(), address);
            assert_eq!(reason.capacity(), capacity);
            assert_eq!(reason, "original backend-owned reason");
        }
    }

    #[test]
    fn evaluation_keeps_its_box_and_never_classifies_by_custom_code() {
        // The existing codec conversion constructs a real boxed
        // ErrorInner::Evaluate(EvaluateError::Custom), without a new dependency.
        for code in [1690, 8175, 1105, 9007, 10000, -1] {
            let cause = LocalError::Evaluation(
                BackendCodecError::Eval("private backend detail".to_owned(), code).into(),
            );
            let address = match &cause {
                LocalError::Evaluation(error) => error.0.as_ref() as *const _ as usize,
                _ => unreachable!(),
            };
            let failure = ExpressionRuntimeFailure::from_local_eval(cause, Some(Phase::Invoke));
            let cloned = failure.clone();
            assert!(ptr::eq(failure.local_error(), cloned.local_error()));
            drop(failure);
            let LocalError::Evaluation(error) = cloned.local_error() else {
                panic!("original evaluation cause was replaced");
            };
            assert_eq!(error.0.as_ref() as *const _ as usize, address);
            assert_eq!(cloned.class(), Class::Evaluation);
            assert_eq!(
                cloned.client_message(),
                "Expression runtime evaluation failure"
            );
        }
    }

    #[test]
    fn other_evaluation_payload_is_retained_without_text_classification() {
        // This existing codec conversion produces EvaluateError::Other. Its
        // overflow-looking text must not change the native outer class.
        let cause = LocalError::Evaluation(
            BackendCodecError::InvalidDataType("1690 BIGINT overflow".to_owned()).into(),
        );
        let address = match &cause {
            LocalError::Evaluation(error) => error.0.as_ref() as *const _ as usize,
            _ => unreachable!(),
        };
        let failure = ExpressionRuntimeFailure::from_local_eval(cause, None);
        let LocalError::Evaluation(error) = failure.local_error() else {
            panic!("original evaluation cause was replaced");
        };
        assert_eq!(error.0.as_ref() as *const _ as usize, address);
        assert_eq!(failure.class(), Class::Evaluation);
        assert_eq!(failure.phase(), None);
        assert_eq!(
            failure.client_message(),
            "Expression runtime evaluation failure"
        );
    }

    #[test]
    fn debug_contains_only_native_class_and_explicit_phase() {
        let secret = "TiKV LocalError raw private message code=1690";
        for phase in [None, Some(Phase::Observe)] {
            let cause =
                LocalError::Evaluation(BackendCodecError::Eval(secret.to_owned(), 1690).into());
            let failure = ExpressionRuntimeFailure::from_local_eval(cause, phase);
            assert_eq!(
                format!("{failure:?}"),
                format!("ExpressionRuntimeFailure {{ class: Evaluation, phase: {phase:?} }}")
            );
            let pretty = format!("{failure:#?}");
            for hidden in [
                secret,
                "TiKV",
                "LocalError",
                "Custom",
                "1690",
                "payload",
                "cause",
            ] {
                assert!(!pretty.contains(hidden), "leaked {hidden} in {pretty}");
            }
            assert_eq!(
                failure.client_message(),
                "Expression runtime evaluation failure"
            );
        }
    }
}
