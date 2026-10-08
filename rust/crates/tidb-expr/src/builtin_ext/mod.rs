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

//! Family modules for builtin scalar functions. Each family exposes
//! `dispatch(name, vals) -> Option<Result<Datum, EvalError>>`; `None` falls
//! through to the next family and ultimately to `crate::func::eval_func`'s
//! `Unsupported` error.
//!
//! These files are seed material until their complete upstream Go packages
//! are transcreated. Every builtin must cite the Go function it was read from
//! in `pkg/expression/builtin_*.go`.
//!
//! The five approved TiDB-owned exceptions are deliberately absent from this
//! generic family chain. [`crate::host_compat`] exposes their only value-level
//! entry, so retaining them does not retain a second expression evaluator.

use crate::{Datum, EvalError};

pub(crate) mod cache;
pub(crate) mod compare2;
pub(crate) mod crypto;
pub(crate) mod info;
pub(crate) mod json;
pub(crate) mod json2;
pub(crate) mod misc;
pub(crate) mod regexp;
pub(crate) mod string2;
pub(crate) mod vec;

pub(crate) use cache::BuiltinFuncCache;
pub(crate) use compare2::{extremum_with_signature, interval_lazy, GlCmpStringMode, GlSignature};
pub(crate) use crypto::eval_aes_lazy;
pub(crate) use json::{
    cast_as_json, cast_as_json_typed, cast_as_json_value_typed,
    dispatch_typed_cached_in as json_dispatch_typed_cached_in,
    dispatch_typed_in as json_dispatch_typed_in, JsonPath, JsonSchemaCache,
};
#[cfg(test)]
pub(crate) use string2::find_in_set_lookup;
pub(crate) use string2::{
    build_find_in_set_lookup, find_in_set_lookup_in, find_in_set_with_collation_in, FindInSetLookup,
};

/// Tries each family in turn; `None` if no family implements `name`.
///
/// `ctx` carries statement coercion/warning policy and the evaluated-value
/// execution capability. Migrated families must retain that real capability
/// even for NULL or empty results; a separate historical cast context does not
/// replace the execution context.
pub(crate) fn dispatch(
    name: &str,
    vals: &[Datum],
    ctx: &dyn crate::Columns,
) -> Option<Result<Datum, EvalError>> {
    string2::dispatch(name, vals, ctx)
        .or_else(|| crypto::dispatch(name, vals, ctx))
        .or_else(|| info::dispatch(name, vals, ctx))
        .or_else(|| json::dispatch_in(name, vals, ctx))
        .or_else(|| json2::dispatch_in(name, vals, ctx))
        .or_else(|| regexp::dispatch_in(name, vals, ctx))
        .or_else(|| compare2::dispatch(name, vals, ctx))
        .or_else(|| misc::dispatch_in(name, vals, ctx))
        .or_else(|| vec::dispatch_in(name, vals, ctx))
}
