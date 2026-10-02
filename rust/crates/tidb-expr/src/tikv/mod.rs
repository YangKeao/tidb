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

//! Closed ready-argument value dispatch plus private signed-LongLong control/PLUS slices.
//! This alone is not complete migrated-family evidence. No rejection/error can replay native
//! evaluation. Only immutable lowering results may be shared; a worker compiles
//! and owns its own non-Sync official TiKV RPN program.

// Control/PLUS prototypes remain private; only the closed ready-argument hooks are active.
#![allow(dead_code)]

mod adapter_failure;
mod batch;
mod catalog;
mod context;
// The closed ready-argument families share this value boundary and one pool.
mod evaluated_ascii;
pub use adapter_failure::{
    ExpressionAdapterFailure, ExpressionAdapterFailureClass, ExpressionAdapterFailureOrigin,
};
pub(crate) use evaluated_ascii::{
    eval_arithmetic_decimal_fast_in, evaluate_args_in, evaluate_ascii_in, evaluate_bytes_in,
    evaluate_logical_in, evaluate_prepared_args_in, evaluate_regexp_in, EvaluatedBytesResult,
    RegexpFunction,
};
pub use evaluated_ascii::{
    eval_legacy_bytes_comparison_in, eval_legacy_decimal_arithmetic_in,
    eval_legacy_decimal_comparison_in, eval_legacy_decimal_division_in,
    eval_legacy_integer_arithmetic_in, eval_legacy_integer_comparison_in,
    eval_legacy_json_array_append_step_in, eval_legacy_json_member_of_in,
    eval_legacy_json_output_none_in, eval_legacy_json_replace_in, eval_legacy_like_in,
    eval_legacy_real_arithmetic_in, eval_legacy_real_comparison_in, eval_legacy_time_comparison_in,
    eval_regexp_legacy_ready_in, AsciiExecution, AsciiOwnerError, AsciiPoolOwner, AsciiPoolPolicy,
    AsciiScope, LegacyBinaryArgs, LegacyIntegerArithmetic, LegacyLikeArgs, RegexpLegacyInput,
    ScopedAsciiColumns,
};
pub(crate) use tidb_query_datatype::codec::mysql::json::{
    parse_native_json_document, NativeJsonError,
};
use tidb_query_expr::local::prepare_concat_args as prepare_concat_args_local;
use tidb_query_expr::local::prepare_find_in_set_keys as prepare_find_in_set_keys_local;
pub use tidb_query_expr::local::NativeDecimalDivisionDisposition;
#[cfg(test)]
pub(crate) use tidb_query_expr::local::{
    conv_valid_prefix_native, frame_compressed, go_atan, go_atan2, go_cos, go_sin, go_tan,
    go_zlib_deflate, inflate, trig_reduce,
};
pub(crate) use tidb_query_expr::local::{
    elt_selected_arg, field_int_equal, field_real_equal, legacy_substring_needs_len,
    make_set_selected, native_decimal_target_scale, ConcatKind, ConcatTerminal, EvaluatedArgs,
    EvaluatedBytesOp, FieldIntValue, FieldTerminal, JsonReportOutcome, NativeCollation,
    NativeSearchPolicy, OutputDisposition, PreparedConcatArgs, PreparedExportSetArgs,
    PreparedFieldArgs, PreparedFindInSetKeys, PreparedMakeSetArgs, ReadyBytesArg, ReadyConvBaseArg,
    ReadyDecimalArg, ReadyFieldIntArg, ReadyIeee754Arg, ReadyIntArg, ReadySubstringI128,
    UncompressOutcome,
};
use tidb_query_expr::local::{
    field_bytes_equal as field_bytes_equal_local,
    prepare_export_set_args as prepare_export_set_args_local,
    prepare_field_bytes_args as prepare_field_bytes_args_local,
    prepare_field_int_args as prepare_field_int_args_local,
    prepare_field_real_args as prepare_field_real_args_local,
    prepare_make_set_args as prepare_make_set_args_local,
};
pub use tidb_query_expr::{BinaryArithmeticOperation, ComparisonOp};
pub(crate) use tidb_query_expr::{NativeDecimalFastOutcome, NativeDecimalFastValue};

/// Builds only the opaque constant-list key owner, without a runtime scope.
/// The existing caller has no encoded-size SQL policy; retain the real backend
/// failure without labeling this pure preparation as a worker phase.
pub(crate) fn prepare_find_in_set_keys(
    list: Option<&[u8]>,
    key_policy: NativeCollation,
) -> Result<PreparedFindInSetKeys, crate::EvalError> {
    prepare_find_in_set_keys_local(list, key_policy, usize::MAX).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            error, None,
        ))
    })
}

/// Encodes only the actual demanded CONCAT prefix and its terminal record.
/// This pure builder neither joins a SQL result nor borrows a runtime scope.
pub(crate) fn prepare_concat_args(
    kind: ConcatKind,
    total_sql_arity: usize,
    prefix: Vec<Option<Vec<u8>>>,
    terminal: ConcatTerminal,
) -> Result<PreparedConcatArgs, crate::EvalError> {
    prepare_concat_args_local(kind, total_sql_arity, prefix, terminal, usize::MAX).map_err(
        |error| {
            crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                error, None,
            ))
        },
    )
}

/// Builds only CHAR's closed nullable-integer input, including an empty list.
/// Keep the actual preparation failure without inventing a worker phase.
pub(crate) fn prepare_char_args(values: &[Option<i64>]) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_char_args(values).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            error, None,
        ))
    })
}

/// Packs only the actual grouping id and validated mark sets with checked extent.
/// Preserve the shared preparation error without inventing a worker phase.
pub(crate) fn prepare_grouping_args(
    gid: u64,
    metadata: &tidb_query_expr::GroupingMetadata,
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_grouping_args(gid, metadata).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            error, None,
        ))
    })
}

/// Serialize already-coerced serde documents and the actual optional path.
/// This transports data only; SQL validation and worker evaluation remain separate.
pub(crate) fn prepare_json_serde_args(
    first: &serde_json::Value,
    second: Option<&serde_json::Value>,
    path: Option<&str>,
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_json_serde_args(first, second, path).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            error, None,
        ))
    })
}

/// Pack actual ARRAY arguments without constructing the computed JSON array.
pub(crate) fn prepare_json_array_args(
    values: &[serde_json::Value],
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_json_array_args(values).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            error, None,
        ))
    })
}

/// Keep OBJECT pair order and duplicate keys in the checked argument frame.
pub(crate) fn prepare_json_object_args(
    pairs: &[(String, serde_json::Value)],
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_json_object_args(pairs).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            error, None,
        ))
    })
}

/// Transport original parsed path legs/metadata without reparsing cached text.
pub(crate) fn prepare_json_paths_args(
    document: &serde_json::Value,
    paths: &[tidb_query_expr::NativeJsonPath],
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_json_paths_args(document, paths).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            error, None,
        ))
    })
}

/// Preserve the ordered actual path/value list; no mutation or zip truncation.
pub(crate) fn prepare_json_path_values_args(
    document: &serde_json::Value,
    paths: &[tidb_query_expr::NativeJsonPath],
    values: &[serde_json::Value],
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_json_path_values_args(document, paths, values).map_err(
        |error| {
            crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                error, None,
            ))
        },
    )
}

/// Transport one actual raw JSON document without selecting an identity recipe.
pub(crate) fn prepare_json_binary_args(
    document: &tidb_datatype::BinaryJSON,
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_json_raw_identity_args((document.type_code(), document.value()))
        .map_err(|error| {
            crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                error, None,
            ))
        })
}

/// Preserve each actual BinaryJSON type code and payload without serde conversion.
pub(crate) fn prepare_json_binary_pair_args(
    first: &tidb_datatype::BinaryJSON,
    second: &tidb_datatype::BinaryJSON,
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_json_binary_pair_args(
        first.type_code(),
        first.value(),
        second.type_code(),
        second.value(),
    )
    .map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            error, None,
        ))
    })
}

/// Transports an existing native coefficient and both scales without computing
/// a SQL answer or imposing a new native-side decimal precision policy.
pub(crate) fn prepare_math_decimal(
    value: &tidb_datatype::Decimal,
) -> Result<tidb_query_datatype::codec::mysql::Decimal, crate::EvalError> {
    value
        .try_to_shared_math(usize::MAX)
        .map_err(math_decimal_bridge_error)
}

// Used only by the input/output decimal representation bridges. The backend's
// narrow wrapper owns the actual NativeDecimalError, not its rendered message.
fn math_decimal_bridge_error(
    error: tidb_query_datatype::codec::mysql::decimal::NativeDecimalError,
) -> crate::EvalError {
    let cause = tidb_query_expr::local::native_decimal_bridge_error(error);
    crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
        cause, None,
    ))
}

// Only these purpose-specific FIELD/SET pure calls use this conversion. Keep
// the actual LocalError, without inventing a worker phase or a SQL diagnostic.
fn field_set_pure_error(error: LocalError) -> crate::EvalError {
    crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
        error, None,
    ))
}

pub(crate) fn field_bytes_equal(
    needle: &[u8],
    candidate: &[u8],
    collation: NativeCollation,
) -> Result<bool, crate::EvalError> {
    field_bytes_equal_local(needle, candidate, collation).map_err(field_set_pure_error)
}

pub(crate) fn prepare_field_bytes_args(
    total_sql_arity: usize,
    needle: ReadyBytesArg,
    prefix: Vec<Option<Vec<u8>>>,
    terminal: FieldTerminal,
    collation: NativeCollation,
) -> Result<PreparedFieldArgs, crate::EvalError> {
    prepare_field_bytes_args_local(
        total_sql_arity,
        needle,
        prefix,
        terminal,
        collation,
        usize::MAX,
    )
    .map_err(field_set_pure_error)
}

pub(crate) fn prepare_field_int_args(
    total_sql_arity: usize,
    needle: ReadyFieldIntArg,
    prefix: Vec<Option<FieldIntValue>>,
    terminal: FieldTerminal,
) -> Result<PreparedFieldArgs, crate::EvalError> {
    prepare_field_int_args_local(total_sql_arity, needle, prefix, terminal, usize::MAX)
        .map_err(field_set_pure_error)
}

pub(crate) fn prepare_field_real_args(
    total_sql_arity: usize,
    needle: ReadyIeee754Arg,
    prefix: Vec<Option<u64>>,
    terminal: FieldTerminal,
) -> Result<PreparedFieldArgs, crate::EvalError> {
    prepare_field_real_args_local(total_sql_arity, needle, prefix, terminal, usize::MAX)
        .map_err(field_set_pure_error)
}

pub(crate) fn prepare_make_set_args(
    mask: Option<u64>,
    total_sql_arity: usize,
    entries: Vec<ReadyBytesArg>,
) -> Result<PreparedMakeSetArgs, crate::EvalError> {
    prepare_make_set_args_local(mask, total_sql_arity, entries, usize::MAX)
        .map_err(field_set_pure_error)
}

pub(crate) fn prepare_export_set_args(
    bits: ReadyIntArg,
    on: ReadyBytesArg,
    off: ReadyBytesArg,
    separator: Option<ReadyBytesArg>,
    count: Option<ReadyIntArg>,
) -> Result<PreparedExportSetArgs, crate::EvalError> {
    prepare_export_set_args_local(bits, on, off, separator, count, usize::MAX)
        .map_err(field_set_pure_error)
}
mod lineage;
mod lower;
mod ordinary;
// Only native opaque types cross the crate boundary, never LocalError itself.
mod runtime_failure;
pub use runtime_failure::{
    ExpressionRuntimeFailure, ExpressionRuntimeFailureClass, ExpressionRuntimeFailurePhase,
};
#[cfg(test)]
mod tests;

// These other crate-private entrypoints remain outside general evaluation.
#[allow(unused_imports)]
pub(crate) use batch::PreparedIntControlSeed;
#[allow(unused_imports)]
pub(crate) use lineage::{
    lower_typed_control_lineage, ControlSourceLimits, LoweredControlLineage, NativeControlBatch,
    PreparedControlLineage,
};
#[allow(unused_imports)]
pub(crate) use lower::{lower_int_control_seed, LoweredSpec};
#[allow(unused_imports)]
pub(crate) use ordinary::{
    lower_pb_int_plus_row, lower_typed_int_plus_row, LoweredIntPlusRow, PreparedIntPlusRow,
};

use tidb_datatype::tikv_compat::value::BridgeError;
use tidb_query_expr::local::LocalError;

/// Do not erase TiKV's typed SQL/contract/resource errors or pretend that an
/// admission failure is a SQL NULL. General native diagnostic plumbing is not
/// part of this effect-free seed.
#[derive(Debug)]
pub(super) enum SeedError {
    Admission(&'static str),
    Bridge(BridgeError),
    Local(LocalError),
}

impl From<BridgeError> for SeedError {
    fn from(error: BridgeError) -> Self {
        Self::Bridge(error)
    }
}

impl From<LocalError> for SeedError {
    fn from(error: LocalError) -> Self {
        Self::Local(error)
    }
}

type SeedResult<T> = Result<T, SeedError>;
