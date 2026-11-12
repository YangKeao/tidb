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
use crate::{Columns, Datum, EvalError};
use tidb_datatype::{Charset, Collation, FieldType};
use tidb_query_datatype::codec::collation::native_encoding::is_supported_encoding;
#[path = "charset_value.rs"]
mod value;
pub(crate) use value::{
    convert_using_bounded, from_binary_bounded, source_null, to_binary_bounded,
};

fn invalid_report() -> EvalError {
    EvalError::ExpressionAdapterFailure(ExpressionAdapterFailure::from_scope(
        ScopeFailureKind::Contract,
        "invalid native charset conversion report",
    ))
}

pub(crate) fn eval_to_binary_in(
    _ctx: &dyn Columns,
    value: &Datum,
    charset: &str,
) -> Result<Datum, EvalError> {
    to_binary_bounded(value, charset, super::identity_actual::default_budget())
}
pub(crate) fn eval_from_binary_in(
    _ctx: &dyn Columns,
    value: &Datum,
    charset: &str,
) -> Result<Datum, EvalError> {
    from_binary_bounded(value, charset, super::identity_actual::default_budget())
}
pub(crate) fn eval_convert_using_in(
    _ctx: &dyn Columns,
    value: &Datum,
    arg_type: &FieldType,
    target: &str,
) -> Result<Datum, EvalError> {
    convert_using_bounded(
        value,
        arg_type,
        target,
        super::identity_actual::default_budget(),
    )
}
/// Only actual source NULL uses this primitive; typed-reader None is empty bytes.
pub(crate) fn eval_charset_null_in(_ctx: &dyn Columns) -> Result<Datum, EvalError> {
    source_null()
}
