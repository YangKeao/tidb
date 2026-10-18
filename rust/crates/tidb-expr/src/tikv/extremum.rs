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

use super::adapter_failure::{ExpressionAdapterFailure, ScopeFailureKind};
use super::{ExpressionRuntimeFailure, LocalError};
use crate::builtin_ext::{GlCmpStringMode, GlSignature};
use crate::{Columns, Datum, EvalError};
use std::cmp::Ordering;
use tidb_datatype::{Collation, EvalType, FieldType};
use tidb_query_expr as extrema_sdk;
#[path = "extremum_value.rs"]
mod value;
pub(crate) use value::eval_extremum_values_bounded;

fn extremum_signature_metadata(value: GlSignature) -> extrema_sdk::NativeExtremumSignature {
    use extrema_sdk::{NativeExtremumEvalType as Type, NativeExtremumStringMode as Mode};
    extrema_sdk::NativeExtremumSignature {
        arg_type: match value.arg_type {
            EvalType::Int => Type::Int,
            EvalType::Real => Type::Real,
            EvalType::Decimal => Type::Decimal,
            EvalType::String => Type::String,
            EvalType::Datetime => Type::Datetime,
            EvalType::Timestamp => Type::Timestamp,
            EvalType::Duration => Type::Duration,
            EvalType::Json => Type::Json,
            EvalType::VectorFloat32 => Type::VectorFloat32,
        },
        cmp_string_mode: match value.cmp_string_mode {
            GlCmpStringMode::Directly => Mode::Directly,
            GlCmpStringMode::AsDate => Mode::AsDate,
            GlCmpStringMode::AsDatetime => Mode::AsDatetime,
        },
        ret_date: value.ret_date,
    }
}

// Keep this existing coercion boundary: Real uses Rust Display, while Float32
// and the remaining kinds take Datum::to_bytes. This is not winner selection.
fn extremum_string_value(value: &Datum) -> Result<Vec<u8>, EvalError> {
    Ok(match value {
        Datum::String(value) => value.bytes().to_vec(),
        Datum::Bytes(value) => value.clone(),
        Datum::Int(value) => value.to_string().into_bytes(),
        Datum::UInt(value) => value.to_string().into_bytes(),
        Datum::Decimal(value) => value.to_string().into_bytes(),
        Datum::Real(value) => value.to_string().into_bytes(),
        Datum::Null => return Err(EvalError::Unsupported("NULL string operand")),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("range sentinel string operand"));
        }
        other => other
            .to_bytes()
            .map_err(|_| EvalError::Unsupported("datum string conversion"))?,
    })
}

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native extremum report",
    ))
}

pub(crate) fn eval_extremum_in(
    ctx: &dyn Columns,
    vals: &[Datum],
    want: Ordering,
    signature: Option<GlSignature>,
    arg_decimals: &[i64],
    all_constant: bool,
    collation: Collation,
) -> Result<Datum, EvalError> {
    eval_extremum_values_bounded(
        ctx,
        vals,
        want,
        signature,
        arg_decimals,
        all_constant,
        collation,
        super::control_value::default_budget(),
    )
}
