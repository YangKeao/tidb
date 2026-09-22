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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Remaining family modules for builtin scalar functions. The JSON family
//! exposes `dispatch(name, vals) -> Option<Result<Datum, EvalError>>`; `None`
//! ultimately reaches `crate::func::eval_func`'s `Unsupported` error.
//!
//! These files are seed material until their complete upstream Go packages
//! are transcreated. Every builtin must cite the Go function it was read from
//! in `pkg/expression/builtin_*.go`.

use crate::{Datum, EvalError};

pub(crate) mod json;

pub(crate) use json::{
    cast_as_json, cast_as_json_typed, cast_as_json_value_typed,
    dispatch_typed as json_dispatch_typed,
};
/// Tries each family in turn; `None` if no family implements `name`.
///
pub(crate) fn dispatch(
    name: &str,
    vals: &[Datum],
    _ctx: &dyn crate::Columns,
) -> Option<Result<Datum, EvalError>> {
    json::dispatch(name, vals)
}
