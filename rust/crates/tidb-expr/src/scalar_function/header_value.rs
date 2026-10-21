// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original scalar outcomes that never demand an argument value.
use super::ScalarFunction;
use crate::{Columns, Datum, EvalError};

/// A closed original before-name phase, independent of argument declarations.
#[derive(Clone, Copy, Debug)]
pub(crate) enum HeaderKind {
    /// The truth predicate reports its original invalid parameter count.
    TruthArity,
    /// A searched CASE with no condition or ELSE is exhausted.
    EmptyCase,
    /// COALESCE with no argument is exhausted.
    EmptyCoalesce,
}
impl HeaderKind {
    /// Select only the real SQL scalar owner and its original zero-demand phase.
    pub(crate) fn for_function(function: &ScalarFunction) -> Option<Self> {
        if function.pb_signature().is_some() || function.pb_origin().is_some() {
            return None;
        }
        Some(
            match (function.func_name.lowercase(), function.args.len()) {
                (
                    "istrue" | "istrue_with_null" | "isnottrue" | "isfalse" | "isnotfalse"
                    | "isunknown" | "isnotunknown",
                    arity,
                ) if arity != 1 => Self::TruthArity,
                ("case", 0) => Self::EmptyCase,
                ("coalesce", 0) => Self::EmptyCoalesce,
                _ => return None,
            },
        )
    }
    /// Authenticate the original logical arity even though no value is fetched.
    pub(crate) fn accepts_arity(self, arity: usize) -> bool {
        match self {
            Self::TruthArity => arity != 1,
            Self::EmptyCase | Self::EmptyCoalesce => arity == 0,
        }
    }
    /// Run the same original empty value helper, without an expression callback.
    pub(crate) fn evaluate(self, context: &dyn Columns) -> Result<Datum, EvalError> {
        match self {
            Self::TruthArity => Err(truth_arity_error()),
            Self::EmptyCase => crate::tikv::eval_case_in(
                context,
                0,
                false,
                |_| Ok(()),
                |_, _, _| unreachable!("an empty CASE has no condition"),
                |_, _| unreachable!("an empty CASE has no selected value"),
            ),
            Self::EmptyCoalesce => {
                crate::tikv::eval_coalesce_in(context, 0, |_, _| -> Result<Datum, EvalError> {
                    unreachable!("an empty COALESCE has no value")
                })
            }
        }
    }
}
/// The existing scalar truth-predicate header error, shared without reading args.
pub(crate) fn truth_arity_error() -> EvalError {
    EvalError::WrongParameterCount("is true or false")
}
