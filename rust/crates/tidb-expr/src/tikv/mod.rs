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
mod bounded_staleness;
pub(crate) use bounded_staleness::eval_bounded_staleness_in;
mod case_control;
mod cast_arg_string;
pub(crate) use cast_arg_string::{eval_cast_arg_as_string, eval_cast_arg_as_string_type};
pub use cast_arg_string::{eval_legacy_cast_string_datum, eval_legacy_cast_string_integer};
#[cfg(test)]
mod cast_arg_string_tests;
mod cast_decimal;
#[cfg(test)]
mod closed_decimal_tests;
pub(crate) use cast_decimal::{eval_cast_decimal_in, report_cast_decimal_input_in};
pub use cast_decimal::{eval_legacy_cast_decimal_datum, eval_legacy_cast_decimal_integer};
mod cast_json;
pub use cast_json::eval_legacy_cast_json_datum;
mod cast_duration;
pub(crate) use cast_duration::{
    eval_cast_arg_as_duration_in, eval_cast_duration_in, eval_parse_computed_duration_in,
};
#[cfg(test)]
mod cast_duration_tests;
mod cast_float;
#[cfg(test)]
mod closed_float_tests;
pub(crate) use cast_float::{eval_cast_double_in, eval_cast_float_in, eval_cast_float_value};
pub use cast_float::{
    eval_legacy_cast_real_datum, eval_legacy_cast_real_integer, eval_legacy_numeric_prefix,
};
mod cast_integer;
pub(crate) use cast_integer::eval_cast_arg_as_int_in;
pub use cast_integer::{eval_legacy_cast_integer_datum, LegacyCastIntegerResult};
#[cfg(test)]
mod cast_arg_integer_tests;
#[cfg(test)]
mod decimal_context_tests;
#[cfg(test)]
mod decimal_datum_tests;
#[cfg(test)]
mod scalar_datum_tests;
#[cfg(test)]
mod signed_datum_tests;
#[cfg(test)]
pub(crate) use cast_integer::eval_cast_unsigned_value_in;
pub(crate) use cast_integer::{
    eval_cast_signed_in, eval_cast_signed_value_in, eval_cast_unsigned_in,
    eval_cast_unsigned_union_in, report_cast_integer_input_in,
};
mod cast_real_unsigned;
pub(crate) use cast_real_unsigned::eval_cast_real_unsigned_in;
mod cast_string;
pub(crate) use cast_string::{eval_cast_binary_in, eval_cast_char_in};
mod cast_vector;
pub(crate) use cast_vector::eval_cast_vector;
mod cast_time;
pub(crate) use cast_time::{eval_cast_arg_as_datetime_in, eval_cast_time_value_in};
#[cfg(test)]
mod cast_time_tests;
mod cast_year;
pub(crate) use cast_year::eval_cast_year_in;
#[cfg(test)]
mod cast_year_tests;
mod catalog;
pub(crate) use case_control::eval_case_in;
mod coalesce;
pub(crate) use coalesce::eval_coalesce_in;
mod context;
mod convert_charset;
pub(crate) use convert_charset::{
    eval_charset_null_in, eval_convert_using_in, eval_from_binary_in, eval_to_binary_in,
};
mod date_arithmetic;
pub(crate) use date_arithmetic::{
    eval_date_add_default_in, eval_date_add_duration_in, eval_date_add_in,
};
// The closed ready-argument families share this value boundary and one pool.
mod evaluated_ascii;
mod extract;
pub(crate) use extract::{eval_extract_composite_in, eval_extract_in, eval_extract_null_unit_in};
mod extremum;
pub(crate) use extremum::eval_extremum_in;
mod from_unixtime;
mod identity_value;
mod if_control;
mod if_null;
mod in_list;
pub(crate) use in_list::{
    eval_in_typed_values_in, in_control_datum, in_eq_observation, in_row_control_error,
};
pub use in_list::{eval_legacy_in_bytes_in, eval_legacy_in_int_in};
mod interval;
pub(crate) use interval::{eval_interval_in, eval_interval_lazy_in};
mod json_sum_crc32;
pub(crate) use json_sum_crc32::eval_json_sum_crc32_in;
mod legacy_date_arithmetic;
pub use legacy_date_arithmetic::{
    eval_legacy_date_arithmetic_duration_in, eval_legacy_date_arithmetic_text_in,
    eval_legacy_date_arithmetic_time_in, LegacyDateArithmeticChannel, LegacyDateArithmeticDateKind,
    LegacyDateArithmeticIntervalKind, LegacyDateArithmeticMetadata, LegacyDateArithmeticResult,
    LegacyDateArithmeticValue,
};
mod null_if;
pub use from_unixtime::eval_from_unixtime_legacy_scoped_in;
pub(crate) use if_control::eval_if_in;
pub(crate) use if_null::eval_if_null_in;
pub(crate) use null_if::eval_null_if_in;
mod str_to_date;
pub(crate) use str_to_date::eval_str_to_date_in;
mod timestamp_diff;
pub use timestamp_diff::{eval_legacy_timestamp_diff_in, LegacyTimestampDiffArgs};
pub(crate) use timestamp_diff::{eval_timestamp_diff_in, eval_timestamp_diff_null_in};
mod unix_timestamp;
pub use adapter_failure::{
    ExpressionAdapterFailure, ExpressionAdapterFailureClass, ExpressionAdapterFailureOrigin,
};
pub(crate) use evaluated_ascii::{
    eval_arithmetic_decimal_fast_in, eval_decimal_integer_division_in, evaluate_args_in,
    evaluate_ascii_in, evaluate_bytes_in, evaluate_logical_in, evaluate_prepared_args_in,
    evaluate_prepared_args_scoped_in, evaluate_regexp_in, native_time_result_contract_error,
    EvaluatedBytesResult, RegexpFunction,
};
pub use evaluated_ascii::{
    eval_legacy_bytes_comparison_in, eval_legacy_date_in, eval_legacy_decimal_arithmetic_in,
    eval_legacy_decimal_comparison_in, eval_legacy_decimal_division_in,
    eval_legacy_decimal_integer_division_in, eval_legacy_integer_arithmetic_in,
    eval_legacy_integer_comparison_in, eval_legacy_json_array_append_step_in,
    eval_legacy_json_member_of_in, eval_legacy_json_merge_patch_in,
    eval_legacy_json_output_none_in, eval_legacy_json_replace_in, eval_legacy_like_in,
    eval_legacy_microsecond_in, eval_legacy_real_arithmetic_in, eval_legacy_real_comparison_in,
    eval_legacy_time_comparison_in, eval_regexp_legacy_ready_in, AsciiExecution, AsciiOwnerError,
    AsciiPoolOwner, AsciiPoolPolicy, AsciiScope, LegacyBinaryArgs, LegacyIntegerArithmetic,
    LegacyLikeArgs, RegexpLegacyInput, ScopedAsciiColumns,
};
pub(crate) use tidb_query_datatype::codec::mysql::json::NativeJsonError;
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
pub(crate) use tidb_query_expr::{
    native_binary_json_datum, native_cast_as_json, native_cast_as_json_typed,
    native_cast_as_json_value_typed, native_cast_json_prepared_argument, native_json_argument,
    native_json_cast_admission, native_json_document_string, native_json_document_text_argument,
    native_json_sql_string, native_parse_json_document_argument,
    native_parse_json_document_argument_strict, native_parse_json_expression,
    NativeJsonCastAdmissionError, NativeJsonCoercionError, NativeJsonCoercionSource,
    NativeJsonStringArgument,
};
pub use tidb_query_expr::{BinaryArithmeticOperation, ComparisonOp};
pub(crate) use tidb_query_expr::{NativeDecimalFastOutcome, NativeDecimalFastValue};
pub use unix_timestamp::{unix_timestamp_dec_legacy_in, unix_timestamp_int_legacy_in};

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

/// Encode one actual native datum in the existing identity representation.
pub(crate) fn prepare_datum_identity_args(
    value: &tidb_datatype::Datum,
) -> Result<EvaluatedArgs, crate::EvalError> {
    identity_value::encode(value).map(EvaluatedArgs::Bytes)
}

/// Transfer the actual bytes and original unpadded collation metadata.
pub(crate) fn prepare_weight_string_args(
    bytes: Vec<u8>,
    tag: i64,
    new_mode: bool,
) -> Result<EvaluatedArgs, crate::EvalError> {
    if !(0..=15).contains(&tag) {
        return Err(crate::EvalError::ExpressionAdapterFailure(
            ExpressionAdapterFailure::from_scope(
                adapter_failure::ScopeFailureKind::Contract,
                "weight collation tag is outside its transport domain",
            ),
        ));
    }
    let mut metadata = Vec::new();
    metadata.try_reserve_exact(2).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            LocalError::ResourceLimit(format!("weight metadata allocation failed: {error}").into()),
            None,
        ))
    })?;
    metadata.extend_from_slice(&[tag as u8, u8::from(new_mode)]);
    Ok(EvaluatedArgs::Bytes2(Some(bytes), Some(metadata)))
}

/// Transfer actual padding inputs, including source getters that were not demanded.
pub(crate) fn prepare_weight_padded_args(
    bytes: Vec<u8>,
    rawlen: i64,
    budget: Option<u64>,
    tag: i64,
    new_mode: Option<bool>,
) -> Result<EvaluatedArgs, crate::EvalError> {
    if !(0..=15).contains(&tag) {
        return Err(crate::EvalError::ExpressionAdapterFailure(
            ExpressionAdapterFailure::from_scope(
                adapter_failure::ScopeFailureKind::Contract,
                "padded weight collation tag is outside its transport domain",
            ),
        ));
    }
    let mut metadata = Vec::new();
    metadata.try_reserve_exact(19).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            LocalError::ResourceLimit(
                format!("padded weight metadata allocation failed: {error}").into(),
            ),
            None,
        ))
    })?;
    metadata.extend_from_slice(&rawlen.to_le_bytes());
    metadata.push(u8::from(budget.is_some()));
    metadata.extend_from_slice(&budget.unwrap_or(0).to_le_bytes());
    metadata.push(tag as u8);
    metadata.push(new_mode.map_or(2, u8::from));
    Ok(EvaluatedArgs::Bytes2(Some(bytes), Some(metadata)))
}

/// Transport the actual temporal core and the original three DATE mode bits.
pub(crate) fn prepare_date_args(
    core: u64,
    modes: tidb_datatype::DateModes,
) -> Result<EvaluatedArgs, crate::EvalError> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(8).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            LocalError::ResourceLimit(format!("DATE core input allocation failed: {error}").into()),
            None,
        ))
    })?;
    bytes.extend_from_slice(&core.to_le_bytes());
    let flags = i64::from(modes.no_zero_date)
        | (i64::from(modes.no_zero_in_date) << 1)
        | (i64::from(modes.allow_invalid_dates) << 2);
    Ok(EvaluatedArgs::BytesInt(Some(bytes), Some(flags)))
}

/// Transport the actual UTC seconds, nanoseconds and offset without clock math.
/// Fractional precision is a separate operand; validation belongs to the recipe.
pub(crate) fn prepare_clock_args(
    clock: (i64, u32, i32),
    fsp: Option<u32>,
) -> Result<EvaluatedArgs, crate::EvalError> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(16).map_err(|error| {
        crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
            LocalError::ResourceLimit(format!("clock input allocation failed: {error}").into()),
            None,
        ))
    })?;
    bytes.extend_from_slice(&clock.0.to_le_bytes());
    bytes.extend_from_slice(&clock.1.to_le_bytes());
    bytes.extend_from_slice(&clock.2.to_le_bytes());
    Ok(match fsp {
        None => EvaluatedArgs::Bytes(Some(bytes)),
        Some(fsp) => EvaluatedArgs::BytesInt(Some(bytes), Some(i64::from(fsp))),
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

/// Preserve each actual SQL-NULL presence bit separately from a JSON null value.
pub(crate) fn prepare_json_merge_patch_args(
    values: &[Option<serde_json::Value>],
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_json_nullable_values_args(values).map_err(|error| {
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

/// Transport the actual JSON search operands and parsed path metadata only.
pub(crate) fn prepare_json_search_args(
    document: &serde_json::Value,
    paths: &[tidb_query_expr::NativeJsonPath],
    one: bool,
    pattern: &str,
    escape: char,
) -> Result<EvaluatedArgs, crate::EvalError> {
    tidb_query_expr::local::prepare_json_search_args(document, paths, one, pattern, escape).map_err(
        |error| {
            crate::EvalError::ExpressionRuntimeFailure(ExpressionRuntimeFailure::from_ascii_local(
                error, None,
            ))
        },
    )
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
