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

//! Closed context operations driven by TiKV's staged argument protocol.
//! This module owns no Expression, ScalarFunction or evaluation callback.
//! SQL algorithms and demanded-prefix rules stay in the shared core.

use super::*;
use tidb_query_datatype::codec::data_type::ScalarValueRef;
use tidb_query_expr::local::{
    ArgMode, ConcatDemand, ConcatDemandAction, ConcatKind, HostArgReply, HostArgRequest,
    HostCatalogKey, HostInvocation, HostSlot, HostStart, HostStep, HostTaskId, HostWindowArgReply,
    HostWindowInvocation, LocalHostServices,
};

#[path = "batch_arithmetic_window.rs"]
mod arithmetic_window;
#[path = "batch_bounded_staleness.rs"]
mod bounded_staleness_source;
#[path = "batch_byte_unary.rs"]
mod byte_unary;
#[path = "batch_calendar.rs"]
mod calendar_source;
#[path = "batch_charset.rs"]
mod charset_source;
#[path = "batch_clock.rs"]
mod clock_source;
#[path = "batch_crypto.rs"]
mod crypto;
#[path = "batch_date_arithmetic.rs"]
mod date_arithmetic_source;
#[path = "batch_decimal_fast_window.rs"]
mod decimal_fast_window;
#[path = "batch_duration_construction.rs"]
mod duration_construction_source;
#[path = "batch_epoch.rs"]
mod epoch_source;
#[path = "batch_extract.rs"]
mod extract_source;
#[path = "batch_grouping.rs"]
mod grouping;
#[path = "batch_identity.rs"]
mod identity;
#[path = "batch_json_sum_crc32.rs"]
mod json_sum_crc32_source;
#[path = "batch_misc.rs"]
mod misc;
#[path = "batch_str_to_date.rs"]
mod str_to_date_source;
#[path = "batch_timestamp_diff.rs"]
mod timestamp_diff_source;
#[path = "batch_tso.rs"]
mod tso_source;
#[path = "batch_typed_window.rs"]
mod typed_window;
#[path = "batch_vector.rs"]
mod vector;
#[path = "batch_wildcard.rs"]
mod wildcard;
pub(super) use charset_source::NativeCharsetMetadata;
#[path = "batch_extremum.rs"]
mod extremum_source;
pub(super) use extremum_source::NativeExtremumMetadata;
#[path = "batch_interval.rs"]
mod interval_source;
pub(super) use interval_source::{NativeIntervalArgument, NativeIntervalMetadata};
#[path = "batch_in.rs"]
mod in_source;
pub(super) use in_source::{NativeInArgument, NativeInMetadata};
#[path = "batch_numeric_parent.rs"]
mod numeric_parent;
pub(super) use numeric_parent::{CompiledNumericParent, NumericPlan, NumericPlanBody};
#[path = "batch_quotient.rs"]
mod quotient;
#[path = "batch_raw_values.rs"]
mod raw_values;
#[path = "batch_unary_window.rs"]
mod unary_window;
#[path = "batch_window_values.rs"]
mod window_values;

#[derive(Clone, Copy, Debug)]
pub(super) enum NativeValuePreparation {
    Literal,
    Computed,
    TypedReady,
}
#[derive(Clone, Copy, Debug)]
pub(super) enum WindowArithmeticSource {
    Literal,
    Column,
    Computed,
    IntegerReady,
    TypedReady,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct WindowArithmeticMetadata {
    pub domain: NativeEvalType,
    pub integer: bool,
    pub output_integer: bool,
    pub sources: [WindowArithmeticSource; 2],
}

struct WindowSourceFacts<'a> {
    field: &'a FieldType,
    owner: WindowArithmeticSource,
    node: usize,
    target: NativeEvalType,
}

#[derive(Clone, Copy)]
pub(super) enum NativeRawValueOperation {
    Bitwise {
        operation: tidb_query_expr::impl_op::NativeBitwiseOp,
        unsigned: [bool; 2],
    },
    LogicalXor,
    UnarySign {
        sign: tidb_query_expr::impl_op::NativeUnarySign,
        unsigned: bool,
        constant: bool,
    },
}
impl NativeRawValueOperation {
    fn sql_operation(self) -> SqlBatchHostOperation {
        match self {
            Self::Bitwise { operation, .. } => SqlBatchHostOperation::BitwiseNative(operation),
            Self::LogicalXor => SqlBatchHostOperation::XorNative,
            Self::UnarySign { sign, .. } => SqlBatchHostOperation::UnarySignNative(sign),
        }
    }
    fn arity(self) -> usize {
        match self {
            Self::Bitwise { operation, .. } => operation.arity(),
            Self::LogicalXor => 2,
            Self::UnarySign { .. } => 1,
        }
    }
}

#[path = "batch_eager_values.rs"]
mod eager_values_source;
#[path = "batch_prefix_values.rs"]
mod prefix_values_source;
#[path = "batch_weight_string.rs"]
mod weight_string_source;
pub(super) use weight_string_source::NativeWeightString;
#[path = "batch_regexp_values.rs"]
mod regexp_values_source;
pub(super) use regexp_values_source::NativeRegexpValues;
#[path = "batch_pb_eager_values.rs"]
mod pb_eager_values_source;
pub(super) use pb_eager_values_source::{NativePbEagerValues, PbCastKernel, PbValueKernel};
#[path = "batch_pb_binary_values.rs"]
mod pb_binary_values_source;
#[path = "batch_pb_lazy_values.rs"]
mod pb_lazy_values_source;
pub(super) use pb_lazy_values_source::NativePbLazy;
#[path = "batch_pb_boolean_values.rs"]
mod pb_boolean_values_source;
pub(super) use pb_binary_values_source::NativePbBinary;
pub(super) use pb_boolean_values_source::NativePbBoolean;

#[path = "batch_typed_context.rs"]
mod typed_context_source;
pub(super) use typed_context_source::{NativeTypedContext, NativeTypedDescriptor};

#[path = "batch_cached_list.rs"]
mod cached_list_source;
pub(super) use cached_list_source::NativeCachedList;

#[path = "batch_stateful.rs"]
mod stateful_source;
pub(super) use stateful_source::NativeStateful;
#[path = "batch_final_state.rs"]
mod final_state_source;
pub(super) use final_state_source::NativeFinalState;

#[path = "batch_json_schema.rs"]
mod json_schema_source;
pub(super) use json_schema_source::NativeJsonSchema;

pub(super) enum NativeHostMetadata {
    Ast {
        node: usize,
        arguments: Box<[usize]>,
        // Proven from the genuine final CHAR_FUNC argument, never a SQL type.
        raw_string: bool,
    },
    PbLazy(Box<NativePbLazy>),
    PbBoolean(Box<NativePbBoolean>),
    PbBinary(Box<NativePbBinary>),
    PbEagerValues(Box<NativePbEagerValues>),
    RegexpValues(Box<NativeRegexpValues>),
    WeightString(Box<NativeWeightString>),
    HeaderOutcome {
        kind: crate::scalar_function::header_value::HeaderKind,
        logical_arity: usize,
        property_value: Option<Datum>,
        result_type: Option<FieldType>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    JsonSchema(Box<NativeJsonSchema>),
    Stateful(Box<NativeStateful>),
    FinalState(Box<NativeFinalState>),
    CachedList(Box<NativeCachedList>),
    TypedContext(NativeTypedContext),
    ContextValue {
        descriptor: Box<crate::context_value::ContextValue>,
        argument: Option<usize>,
        diagnostic: Option<crate::scalar_function::dynamic_diagnostic::LiteralPolicy>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    EagerValues {
        arguments: Box<[usize]>,
        kernel: crate::scalar_function::eager_values::EagerValueKernel,
        probe: Option<Datum>,
        result_type: Option<FieldType>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Crypto {
        arguments: Box<[usize]>,
        operation: tidb_query_expr::NativeCryptoOperation,
        probe: Option<Datum>,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Compression {
        argument: usize,
        operation: tidb_query_expr::NativeCompressionOperation,
        probe: Option<Datum>,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    ByteUnary {
        argument: usize,
        operation: tidb_query_expr::NativeByteUnaryOperation,
        probe: Option<Datum>,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Misc {
        arguments: Box<[usize]>,
        operation: tidb_query_expr::NativeMiscOperation,
        probe: Option<Datum>,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Identity {
        arguments: Box<[usize]>,
        name_const: bool,
        probe: Option<Datum>,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    ConcatValues {
        arguments: Box<[usize]>,
        kind: ConcatKind,
        result_type: Option<FieldType>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Like {
        arguments: [usize; 3],
        escape_type: Option<FieldType>,
        invocation: tidb_query_expr::NativeLikeInvocation,
        result_type: Option<FieldType>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    In(NativeInMetadata),
    Interval(NativeIntervalMetadata),
    Extremum(NativeExtremumMetadata),
    Charset(NativeCharsetMetadata),
    TimestampDiff {
        arguments: [usize; 3],
        argument_types: [FieldType; 3],
        stop_on_null: bool,
        result_type: FieldType,
        output_node: usize,
        output_role: Option<NativeValueRole>,
    },
    Grouping {
        argument: usize,
        metadata: Option<std::sync::Arc<crate::grouping::GroupingMetadata>>,
        result_type: Option<FieldType>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Vector {
        arguments: Box<[usize]>,
        operation: tidb_query_expr::NativeVectorOperation,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    DurationConstruction {
        arguments: Box<[usize]>,
        argument_types: Box<[FieldType]>,
        operation: tidb_query_expr::NativeDurationConstructionOperation,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Calendar {
        arguments: Box<[usize]>,
        source_type: FieldType,
        operation: tidb_query_expr::NativeCalendarOperation,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Clock {
        argument: Option<usize>,
        operation: tidb_query_expr::NativeClockOperation,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Tso {
        argument_type: Option<FieldType>,
        argument: Option<usize>,
        operation: tidb_query_expr::NativeTsoOperation,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Epoch {
        arguments: Box<[usize]>,
        from: bool,
        result_type: FieldType,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Extract {
        arguments: Box<[usize]>,
        source_type: FieldType,
        unit: Option<Box<str>>,
        result_type: FieldType,
        output_node: usize,
        output_role: Option<NativeValueRole>,
    },
    DateArithmetic {
        arguments: [usize; 2],
        argument_types: [Option<FieldType>; 2],
        unit: Box<str>,
        sign: i64,
        result_fsp: Option<u32>,
        result_type: Option<FieldType>,
        output_node: usize,
        output_role: Option<NativeValueRole>,
        output: tidb_query_expr::local::SqlTemporalOutput,
    },
    BoundedStaleness {
        arguments: [usize; 2],
        argument_types: [FieldType; 2],
        result_type: FieldType,
        output_node: usize,
        output_role: Option<NativeValueRole>,
    },
    StrToDate {
        arguments: [usize; 2],
        result_type: FieldType,
        output_node: usize,
        output_role: Option<NativeValueRole>,
        output: tidb_query_expr::local::SqlTemporalOutput,
    },
    JsonSumCrc32 {
        argument_node: usize,
        result_type: FieldType,
        output_node: usize,
        output_role: Option<NativeValueRole>,
    },
    RawValues {
        operation: NativeRawValueOperation,
        // Actual typed batch consumer; Some(true) is its unsigned root.
        integer_batch: Option<bool>,
        name: Box<str>,
        result_type: Option<FieldType>,
        left_node: usize,
        right_node: Option<usize>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    UnaryWindow {
        operation: SqlBatchHostOperation,
        protobuf_owner: bool,
        result_type: FieldType,
        source_type: FieldType,
        source: WindowArithmeticSource,
        argument_node: usize,
        output_node: usize,
        output_role: NativeValueRole,
        overflow_expression: Option<Box<str>>,
    },
    DecimalFast {
        operation: SqlBatchHostOperation,
        argument_nodes: Box<[usize]>,
        output_node: usize,
        native_scope: bool,
    },
    Plain,
    Regexp(tidb_query_expr::NativeRegexpInvocation),
    CharCast {
        charset: Box<str>,
        unsigned: bool,
    },
    Selection {
        name: Box<str>,
        result_type: Option<FieldType>,
        argument_nodes: Box<[usize]>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Comparison {
        operation: SqlBatchHostOperation,
        name: Box<str>,
        result_type: Option<FieldType>,
        argument_nodes: [usize; 2],
        operands: [crate::ops::OperandFacts; 2],
        collation: tidb_datatype::Collation,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Arithmetic {
        batch: Option<WindowArithmeticMetadata>,
        operation: SqlBatchHostOperation,
        name: Box<str>,
        result_type: Option<FieldType>,
        argument_nodes: [usize; 2],
        source_types: [Option<FieldType>; 2],
        literal: [bool; 2],
        binary_literal: [bool; 2],
        mode: crate::scalar_function::NativeArithmeticInputMode,
        generic_mode: crate::scalar_function::NativeArithmeticInputMode,
        subjects: [Option<Box<str>>; 2],
        subject_format_failures: [Option<[u8; 8]>; 2],
        overflow_format_failure: Option<[u8; 8]>,
        overflow_expressions: [Option<Box<str>>; 2],
        intdiv: Option<Box<crate::scalar_function::NativeIntDivDiagnostics>>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Math {
        name: Box<str>,
        result_type: FieldType,
        argument_node: usize,
        output_node: usize,
        output_role: NativeValueRole,
        overflow_expression: Option<Box<str>>,
    },
    ValueCast {
        cast_type: tidb_ast::CastType,
        name: Box<str>,
        source_type: FieldType,
        input: NativeValuePreparation,
        result_type: FieldType,
        argument_node: usize,
        output_node: usize,
        output_role: NativeValueRole,
    },
    CharNative {
        charset: Box<str>,
        source_type: FieldType,
        argument_node: usize,
    },
}
// An absent declaration is neither an explicit signed nor unsigned operand.
fn arithmetic_unsigned(fields: &[Option<FieldType>; 2]) -> [bool; 2] {
    fields
        .each_ref()
        .map(|field| field.as_ref().is_some_and(FieldType::is_unsigned))
}
fn arithmetic_signed(fields: &[Option<FieldType>; 2]) -> [bool; 2] {
    fields
        .each_ref()
        .map(|field| field.as_ref().is_some_and(|field| !field.is_unsigned()))
}
fn arithmetic_typed_source(
    fields: &[Option<FieldType>; 2],
    index: usize,
) -> LocalResult<&FieldType> {
    fields
        .get(index)
        .and_then(Option::as_ref)
        .ok_or_else(|| binding("typed arithmetic source declaration missing"))
}
// Absence is actual SQL metadata; do not synthesize a coercion target.
fn coerce_optional_value_result(
    name: &str,
    result_type: Option<&FieldType>,
    value: Datum,
) -> std::result::Result<Datum, EvalError> {
    match result_type {
        Some(field) => crate::tikv::identity_actual::coerce_result(name, field, value),
        None => Ok(value),
    }
}

// The original generic integer batch consumer, independent of PB signature.
fn present_integer_batch_value(
    value: Datum,
    result_type: Option<&FieldType>,
    context: &dyn Columns,
) -> std::result::Result<Datum, EvalError> {
    (|| {
        (|| -> std::result::Result<Datum, EvalError> {
            let value = crate::expression::apply_enum_set_integer_view(value, result_type);
            let value = crate::scalar_function::normalize_numeric_row_value(
                value,
                result_type,
                NativeEvalType::Int,
                context,
            )?;
            let bits = crate::scalar_function::native_batch_integer_bits(value)?;
            Ok(crate::scalar_function::native_batch_integer_value(
                bits,
                result_type.is_some_and(FieldType::is_unsigned),
            ))
        })()
    })()
}

impl NativeHostMetadata {
    fn result_type(&self) -> Option<&FieldType> {
        match self {
            Self::EagerValues { result_type, .. } => result_type.as_ref(),
            Self::Selection { result_type, .. } => result_type.as_ref(),
            Self::HeaderOutcome { result_type, .. } => result_type.as_ref(),
            Self::Comparison { result_type, .. } => result_type.as_ref(),
            Self::RawValues { result_type, .. } => result_type.as_ref(),
            Self::JsonSchema(metadata) => metadata.result_type.as_ref(),
            Self::Stateful(metadata) => metadata.result_type.as_ref(),
            Self::FinalState(metadata) => metadata.result_type.as_ref(),
            Self::CachedList(metadata) => metadata.result_type.as_ref(),
            Self::WeightString(metadata) => metadata.result_type.as_ref(),
            Self::RegexpValues(metadata) => metadata.result_type.as_ref(),
            Self::PbEagerValues(metadata) => metadata.result_type.as_ref(),
            Self::PbBinary(metadata) => metadata.result_type.as_ref(),
            Self::PbLazy(metadata) => metadata.result_type.as_ref(),
            Self::PbBoolean(metadata) => metadata.result_type.as_ref(),
            Self::Grouping { result_type, .. } => result_type.as_ref(),
            Self::Like { result_type, .. } => result_type.as_ref(),
            Self::ConcatValues { result_type, .. } => result_type.as_ref(),
            Self::Arithmetic { result_type, .. } => result_type.as_ref(),
            Self::DateArithmetic { result_type, .. } => result_type.as_ref(),
            Self::In(metadata) => metadata.result_type.as_ref(),
            Self::Interval(metadata) => metadata.result_type.as_ref(),
            Self::Extremum(metadata) => metadata.result_type.as_ref(),
            Self::Charset(metadata) => Some(&metadata.result_type),
            Self::TimestampDiff { result_type, .. }
            | Self::Clock { result_type, .. }
            | Self::Calendar { result_type, .. }
            | Self::DurationConstruction { result_type, .. }
            | Self::Vector { result_type, .. }
            | Self::Crypto { result_type, .. }
            | Self::Compression { result_type, .. }
            | Self::ByteUnary { result_type, .. }
            | Self::Misc { result_type, .. }
            | Self::Identity { result_type, .. }
            | Self::Tso { result_type, .. }
            | Self::Epoch { result_type, .. }
            | Self::Extract { result_type, .. }
            | Self::BoundedStaleness { result_type, .. }
            | Self::StrToDate { result_type, .. }
            | Self::JsonSumCrc32 { result_type, .. }
            | Self::UnaryWindow { result_type, .. }
            | Self::Math { result_type, .. }
            | Self::ValueCast { result_type, .. } => Some(result_type),
            _ => None,
        }
    }
    fn regexp(&self) -> Option<&tidb_query_expr::NativeRegexpInvocation> {
        match self {
            Self::Regexp(invocation) => Some(invocation),
            _ => None,
        }
    }
}

#[path = "batch_compression.rs"]
mod compression_source;

enum SqlDemand {
    Ast(Box<ast_source::AstTask>),
    PbLazy(Box<pb_lazy_values_source::PbLazyTask>),
    PbBoolean(Box<pb_boolean_values_source::PbBooleanTask>),
    PbIntegerMod(Box<pb_binary_values_source::PbModTask>),
    PbEagerValues {
        tokens: Vec<[u8; native_values::HANDLE_BYTES]>,
    },
    RegexpValues(Box<regexp_values_source::RegexpTask>),
    WeightString(weight_string_source::WeightTask),
    PrefixValues {
        phase: crate::scalar_function::prefix_value::PrefixPhase,
    },
    ConcatValues {
        pending: Option<ConcatDemand>,
    },
    HeaderOutcome,
    Consumed,
    JsonSchema(Box<json_schema_source::SchemaTask>),
    Stateful(Box<stateful_source::StatefulTask>),
    FinalState(Box<final_state_source::FinalTask>),
    CachedList(Box<cached_list_source::CachedListTask>),
    TypedWindow(Box<typed_window::TypedWindowTask>),
    Numeric(Box<numeric_parent::NumericTask>),
    NumericWindow(Box<numeric_parent::window::NumericWindowTask>),
    TypedContext {
        consumed: bool,
    },
    Diagnostic(Box<diagnostic_source::DiagnosticTask>),
    ContextValue {
        consumed: bool,
    },
    UserVariableRead {
        name: String,
        consumed: bool,
    },
    UserVariableWrite {
        name: String,
        consumed: bool,
    },
    EagerValues {
        next: usize,
        tokens: Vec<[u8; native_values::HANDLE_BYTES]>,
    },
    Crypto {
        next: usize,
        tokens: [Option<[u8; native_values::HANDLE_BYTES]>; 2],
    },
    Compression {
        consumed: bool,
    },
    ByteUnary {
        consumed: bool,
    },
    Misc {
        next: usize,
        tokens: [Option<[u8; native_values::HANDLE_BYTES]>; 2],
    },
    Identity {
        next: usize,
    },
    Like {
        pending: Option<crate::tikv::wildcard_value::Prepared>,
    },
    Grouping {
        consumed: bool,
    },
    Vector {
        next: usize,
        tokens: [Option<[u8; native_values::HANDLE_BYTES]>; 2],
    },
    DurationConstruction {
        next: usize,
        tokens: [Option<[u8; native_values::HANDLE_BYTES]>; 3],
    },
    TimestampDiff {
        next: usize,
        tokens: [Option<[u8; native_values::HANDLE_BYTES]>; 3],
    },
    Calendar {
        first: Option<[u8; native_values::HANDLE_BYTES]>,
    },
    Clock,
    Tso,
    Epoch {
        first: Option<[u8; native_values::HANDLE_BYTES]>,
    },
    Extract {
        first: Option<[u8; native_values::HANDLE_BYTES]>,
    },
    DateArithmetic {
        first: Option<[u8; native_values::HANDLE_BYTES]>,
    },
    BoundedStaleness {
        first: Option<[u8; native_values::HANDLE_BYTES]>,
    },
    StrToDate {
        first: Option<[u8; native_values::HANDLE_BYTES]>,
    },
    JsonSumCrc32,
    Charset {
        source: Option<[u8; native_values::HANDLE_BYTES]>,
    },
    Extremum {
        next: usize,
        tokens: Vec<[u8; native_values::HANDLE_BYTES]>,
    },
    Interval {
        search: tidb_query_expr::NativeIntervalSearch,
    },
    In {
        next: usize,
        tokens: Vec<[u8; native_values::HANDLE_BYTES]>,
        membership: Option<tidb_query_expr::NativeInMembership>,
    },
    RawValues {
        left_token: Option<[u8; native_values::HANDLE_BYTES]>,
    },
    WindowUnary {
        rows: Vec<usize>,
    },
    WindowDecimalFast {
        next: usize,
        rows: Vec<usize>,
        left: Vec<Option<tidb_query_expr::NativeDecimalFastValue>>,
    },
    WindowArithmetic {
        next: usize,
        rows: Vec<usize>,
        left: Vec<Datum>,
    },
    Arithmetic {
        next: usize,
        typed: bool,
        left: Option<Datum>,
    },
    Math,
    Comparison {
        left_token: Option<[u8; native_values::HANDLE_BYTES]>,
        integer_probe: bool,
    },
    Truth,
    Logical {
        next: usize,
        left: Option<bool>,
    },
    Conditional {
        next: usize,
    },
    Nullable {
        next: usize,
    },
    ValueCast,
    CastCharNative {
        len: Option<u32>,
    },
    CastCharInt {
        len: Option<u32>,
    },
    Concat {
        kind: ConcatKind,
        demand: ConcatDemand,
    },
    RegexpLike {
        case_insensitive: bool,
        next: Option<usize>,
    },
}
struct NativeTask {
    slot: HostSlot,
    row: InputRow,
    arity: usize,
    demand: SqlDemand,
    values: Vec<Option<Vec<u8>>>,
}
impl NativeTask {
    fn next_argument(&self) -> LocalResult<Option<usize>> {
        match &self.demand {
            SqlDemand::Ast(task) => Ok(task.next),
            SqlDemand::WeightString(task) => Ok(task.next_argument()),
            SqlDemand::RegexpValues(task) => Ok(task.next_argument()),
            SqlDemand::PbIntegerMod(task) => Ok(Some(task.tokens.len())),
            SqlDemand::PbLazy(task) => Ok(task.phase.argument()),
            SqlDemand::PbBoolean(task) => Ok(Some(task.tokens.len())),
            SqlDemand::PbEagerValues { tokens } => Ok(Some(tokens.len())),
            SqlDemand::PrefixValues { phase } => Ok((*phase
                != crate::scalar_function::prefix_value::PrefixPhase::Consumed)
                .then_some(0)),
            SqlDemand::Consumed | SqlDemand::HeaderOutcome => Ok(None),
            SqlDemand::ConcatValues { pending } => match pending {
                Some(demand) => demand.next_argument(),
                None => Ok(None),
            },
            SqlDemand::JsonSchema(task) => Ok(Some(task.next_argument())),
            SqlDemand::Stateful(task) => Ok(Some(task.next_argument())),
            SqlDemand::FinalState(task) => Ok(Some(task.next)),
            SqlDemand::CachedList(task) => Ok(Some(task.next_argument())),
            SqlDemand::TypedWindow(_) => Ok(Some(0)),
            SqlDemand::Numeric(task) => Ok(Some(task.next_argument())),
            SqlDemand::NumericWindow(task) => Ok(Some(task.next_argument())),
            SqlDemand::Diagnostic(task) => task.next_argument(),
            SqlDemand::TypedContext { consumed } => {
                Ok((!*consumed && self.arity == 1).then_some(0))
            }
            SqlDemand::ContextValue { consumed } => {
                Ok((!*consumed && self.arity == 1).then_some(0))
            }
            SqlDemand::UserVariableRead { .. } => Ok(None),
            SqlDemand::UserVariableWrite { consumed, .. } => Ok((!*consumed).then_some(1)),
            SqlDemand::Like { pending } => Ok(pending.as_ref().map(|p| p.argument())),
            SqlDemand::EagerValues { next, .. } | SqlDemand::Crypto { next, .. } => Ok(Some(*next)),
            SqlDemand::Compression { consumed } | SqlDemand::ByteUnary { consumed } => {
                Ok(Some(usize::from(*consumed)))
            }
            SqlDemand::Misc { next, .. } => Ok(Some(*next)),
            SqlDemand::Identity { next } => Ok(Some(*next)),
            SqlDemand::Grouping { consumed } => Ok(Some(usize::from(*consumed))),
            SqlDemand::TimestampDiff { next, .. }
            | SqlDemand::DurationConstruction { next, .. }
            | SqlDemand::Vector { next, .. } => Ok(Some(*next)),
            SqlDemand::Calendar { first }
            | SqlDemand::Epoch { first }
            | SqlDemand::Extract { first }
            | SqlDemand::DateArithmetic { first }
            | SqlDemand::BoundedStaleness { first }
            | SqlDemand::StrToDate { first } => Ok(Some(usize::from(first.is_some()))),
            SqlDemand::Clock | SqlDemand::Tso | SqlDemand::JsonSumCrc32 => Ok(Some(0)),
            SqlDemand::Charset { source } => Ok(Some(usize::from(source.is_some()))),
            SqlDemand::Interval { search } => Ok(match search.step() {
                tidb_query_expr::NativeIntervalValueStep::Probe { index, .. } => Some(index),
                tidb_query_expr::NativeIntervalValueStep::Index(_) => None,
            }),
            SqlDemand::WindowUnary { .. } => Ok(Some(0)),
            SqlDemand::WindowDecimalFast { next, .. }
            | SqlDemand::WindowArithmetic { next, .. }
            | SqlDemand::Arithmetic { next, .. } => Ok(Some(*next)),
            SqlDemand::RawValues { left_token } | SqlDemand::Comparison { left_token, .. } => {
                Ok(Some(usize::from(left_token.is_some())))
            }
            SqlDemand::Extremum { next, .. }
            | SqlDemand::In { next, .. }
            | SqlDemand::Nullable { next }
            | SqlDemand::Conditional { next }
            | SqlDemand::Logical { next, .. } => Ok(Some(*next)),
            SqlDemand::Concat { demand, .. } => demand.next_argument(),
            SqlDemand::RegexpLike { next, .. } => Ok(*next),
            SqlDemand::CastCharInt { .. }
            | SqlDemand::CastCharNative { .. }
            | SqlDemand::ValueCast
            | SqlDemand::Truth
            | SqlDemand::Math => Ok(Some(0)),
        }
    }
    fn observe(&mut self, len: Option<usize>) -> LocalResult<ConcatDemandAction> {
        match &mut self.demand {
            SqlDemand::Ast(_) => Err(binding("AST demand cannot buffer wire data")),
            SqlDemand::Consumed => Err(binding("consumed task cannot buffer a reply")),
            SqlDemand::RegexpValues(_) => Err(binding("regexp values own their actual phases")),
            SqlDemand::PbIntegerMod(_) => Err(binding("PB MOD owns its actual raw prefix")),
            SqlDemand::PbLazy(_) => Err(binding("PB lazy owns its actual demand phases")),
            SqlDemand::PbBoolean(_) => Err(binding("PB Boolean owns its actual raw prefix")),
            SqlDemand::PbEagerValues { .. } => Err(binding("PB eager values own their raw prefix")),
            SqlDemand::WeightString(_) => Err(binding("weight owns its actual value phases")),
            SqlDemand::HeaderOutcome => Err(binding("header outcome cannot buffer a reply")),
            SqlDemand::PrefixValues { .. } => Err(binding("prefix outcome cannot buffer a reply")),
            SqlDemand::ConcatValues { .. } => Err(binding("raw CONCAT owns its actual prefix")),
            SqlDemand::JsonSchema(_) => Err(binding("schema host cannot buffer wire values")),
            SqlDemand::Stateful(_) => Err(binding("stateful host cannot buffer wire values")),
            SqlDemand::FinalState(_) => Err(binding("final state host cannot buffer wire values")),
            SqlDemand::CachedList(_) => Err(binding("cached list cannot buffer wire values")),
            SqlDemand::Diagnostic(_) => Err(binding("diagnostic demand cannot buffer wire values")),
            SqlDemand::TypedContext { .. } => {
                Err(binding("typed context cannot buffer a wire prefix"))
            }
            SqlDemand::ContextValue { .. } => {
                Err(binding("context values cannot buffer a wire prefix"))
            }
            SqlDemand::UserVariableRead { .. } | SqlDemand::UserVariableWrite { .. } => {
                Err(binding("user variable cannot buffer a wire prefix"))
            }
            SqlDemand::Like { .. } => Err(binding("LIKE demand cannot retain wire byte prefix")),
            SqlDemand::EagerValues { .. } => {
                Err(binding("eager values cannot buffer a wire prefix"))
            }
            SqlDemand::Crypto { .. } => Err(binding("crypto cannot buffer a wire prefix")),
            SqlDemand::Compression { .. } => {
                Err(binding("compression cannot buffer a wire prefix"))
            }
            SqlDemand::ByteUnary { .. } => Err(binding("byte unary cannot buffer a wire prefix")),
            SqlDemand::Misc { .. } => Err(binding("misc cannot buffer a wire prefix")),
            SqlDemand::Identity { .. } => Err(binding("identity cannot retain a wire prefix")),
            SqlDemand::Grouping { .. } => Err(binding("GROUPING demand cannot retain byte prefix")),
            SqlDemand::TimestampDiff { .. }
            | SqlDemand::DurationConstruction { .. }
            | SqlDemand::Vector { .. } => {
                Err(binding("timestamp diff demand cannot retain byte prefix"))
            }
            SqlDemand::Calendar { .. }
            | SqlDemand::Clock
            | SqlDemand::Tso
            | SqlDemand::Epoch { .. }
            | SqlDemand::Extract { .. }
            | SqlDemand::DateArithmetic { .. }
            | SqlDemand::BoundedStaleness { .. }
            | SqlDemand::StrToDate { .. } => Err(binding("date demand cannot retain byte prefix")),
            SqlDemand::JsonSumCrc32 => Err(binding("JSON checksum cannot retain byte prefix")),
            SqlDemand::Charset { .. } => Err(binding("charset demand cannot retain a byte prefix")),
            SqlDemand::Extremum { .. }
            | SqlDemand::Interval { .. }
            | SqlDemand::In { .. }
            | SqlDemand::WindowUnary { .. }
            | SqlDemand::WindowDecimalFast { .. }
            | SqlDemand::WindowArithmetic { .. }
            | SqlDemand::Nullable { .. }
            | SqlDemand::Conditional { .. }
            | SqlDemand::Logical { .. }
            | SqlDemand::Truth
            | SqlDemand::Comparison { .. }
            | SqlDemand::RawValues { .. }
            | SqlDemand::Arithmetic { .. }
            | SqlDemand::Numeric(_)
            | SqlDemand::NumericWindow(_)
            | SqlDemand::TypedWindow(_)
            | SqlDemand::Math => Err(LocalError::HostContract(
                "native nullable control does not buffer a prefix".into(),
            )),
            SqlDemand::Concat { demand, .. } => demand.observe(len),
            SqlDemand::CastCharInt { .. }
            | SqlDemand::CastCharNative { .. }
            | SqlDemand::ValueCast => Err(LocalError::HostContract(
                "character cast cannot buffer a prefix".into(),
            )),
            SqlDemand::RegexpLike { next, .. } => {
                *next = if len.is_some() && self.values.len() < self.arity {
                    Some(self.values.len())
                } else {
                    None
                };
                Ok(ConcatDemandAction::Continue)
            }
        }
    }
    #[cfg(test)]
    fn retained_bytes(&self) -> LocalResult<usize> {
        let bytes = self.values.iter().flatten().try_fold(
            array_bytes::<Option<Vec<u8>>>(self.values.capacity())?,
            |bytes, value| {
                bytes
                    .checked_add(value.capacity())
                    .ok_or_else(|| resource("SQL host prefix retained size overflow"))
            },
        )?;
        if let SqlDemand::JsonSchema(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("schema retained extent overflow"));
        }
        if let SqlDemand::FinalState(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("final state retained extent overflow"));
        }
        if let SqlDemand::Stateful(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("stateful retained extent overflow"));
        }
        if let SqlDemand::PbLazy(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("PB lazy task retained extent overflow"));
        }
        if let SqlDemand::PbBoolean(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("PB Boolean task retained extent overflow"));
        }
        if let SqlDemand::PbIntegerMod(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("PB MOD task retained extent overflow"));
        }
        if let SqlDemand::RegexpValues(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("regexp task retained extent overflow"));
        }
        if let SqlDemand::CachedList(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes())
                .ok_or_else(|| resource("cached list task extent overflow"));
        }
        if let SqlDemand::Ast(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("AST task extent overflow"));
        }
        if let SqlDemand::TypedWindow(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("typed forward retained extent overflow"));
        }
        if let SqlDemand::NumericWindow(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("numeric window retained extent overflow"));
        }
        if let SqlDemand::Numeric(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("numeric retained bytes overflow"));
        }
        if let SqlDemand::Diagnostic(task) = &self.demand {
            return bytes
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("diagnostic retained bytes overflow"));
        }
        if let SqlDemand::UserVariableRead { name, .. }
        | SqlDemand::UserVariableWrite { name, .. } = &self.demand
        {
            return bytes
                .checked_add(name.capacity())
                .ok_or_else(|| resource("user variable name extent overflow"));
        }
        if let SqlDemand::Like { pending } = &self.demand {
            return bytes
                .checked_add(pending.as_ref().map_or(0, |p| p.retained_bytes()))
                .ok_or_else(|| resource("LIKE retained prefix overflow"));
        }
        if let SqlDemand::PbEagerValues { tokens }
        | SqlDemand::EagerValues { tokens, .. }
        | SqlDemand::In { tokens, .. }
        | SqlDemand::Extremum { tokens, .. } = &self.demand
        {
            return bytes
                .checked_add(array_bytes::<[u8; native_values::HANDLE_BYTES]>(
                    tokens.capacity(),
                )?)
                .ok_or_else(|| resource("IN token storage overflow"));
        }
        if let SqlDemand::WindowUnary { rows } = &self.demand {
            return bytes
                .checked_add(array_bytes::<usize>(rows.capacity())?)
                .ok_or_else(|| resource("unary window retained bytes overflow"));
        }
        if let SqlDemand::WindowDecimalFast { rows, left, .. } = &self.demand {
            return bytes
                .checked_add(array_bytes::<usize>(rows.capacity())?)
                .and_then(|bytes| {
                    bytes.checked_add(
                        array_bytes::<Option<tidb_query_expr::NativeDecimalFastValue>>(
                            left.capacity(),
                        )
                        .ok()?,
                    )
                })
                .ok_or_else(|| resource("fast decimal retained size overflow"));
        }
        if let SqlDemand::WindowArithmetic { rows, left, .. } = &self.demand {
            return bytes
                .checked_add(array_bytes::<usize>(rows.capacity())?)
                .and_then(|bytes| {
                    left.iter().try_fold(
                        bytes.checked_add(array_bytes::<Datum>(left.capacity()).ok()?)?,
                        |bytes, value| {
                            bytes.checked_add(native_payload_bytes(value).ok()?.checked_mul(2)?)
                        },
                    )
                })
                .ok_or_else(|| resource("window arithmetic retained bytes overflow"));
        }
        if let SqlDemand::Arithmetic {
            left: Some(value), ..
        } = &self.demand
        {
            bytes
                .checked_add(
                    native_payload_bytes(value)?
                        .checked_mul(2)
                        .ok_or_else(|| resource("arithmetic retained value overflow"))?,
                )
                .ok_or_else(|| resource("arithmetic retained bytes overflow"))
        } else {
            Ok(bytes)
        }
    }
}
struct CapturedWarnings {
    rows: Vec<(u16, String)>,
    failure: Option<LocalError>,
}
impl CapturedWarnings {
    fn capture(&mut self, code: u16, message: &str) {
        if self.failure.is_some() {
            return;
        }
        if let Err(error) = self.try_capture(code, message) {
            self.failure = Some(error);
        }
    }
    fn try_capture(&mut self, code: u16, message: &str) -> LocalResult<()> {
        self.rows
            .try_reserve(1)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        let mut owned = String::new();
        owned
            .try_reserve_exact(message.len())
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        owned.push_str(message);
        self.rows.push((code, owned));
        Ok(())
    }
}

struct TaskSlot<'a> {
    ready_preparation: Option<crate::tikv::PreparationUnwindGuard<'a>>,
    generation: u64,
    task: Option<NativeTask>,
}

pub(super) struct NativeBatchHosts<'a> {
    ast_nodes: &'a [&'a tidb_ast::Expr],
    catalog: HostCatalog,
    context: &'a dyn Columns,
    metadata: &'a [NativeHostMetadata],
    constant_probes: &'a [Option<Datum>],
    diagnostic_slots: &'a [usize],
    diagnostic_parents: &'a [diagnostic_source::CompiledDiagnosticParent],
    numeric_parents: &'a [CompiledNumericParent],
    native_values: Option<&'a RefCell<NativeValueStore>>,
    slots: Vec<TaskSlot<'a>>,
    generation: u64,
    native_error: Option<EvalError>,
    capture_root_error_value: bool,
    root_error_value: Option<(NativeValueOrigin, Datum)>,
    constant_root_shortcut: bool,
    root_value: Option<Datum>,
    typed_values: Box<[Option<Datum>]>,
    input: Option<&'a Chunk>,
    input_is_bounded: bool,
}
impl<'a> NativeBatchHosts<'a> {
    pub(super) fn with_ast_nodes(mut self, nodes: &'a [&'a tidb_ast::Expr]) -> Self {
        self.ast_nodes = nodes;
        self
    }

    pub(super) fn with_input(mut self, input: &'a Chunk) -> Self {
        self.input = Some(input);
        self.input_is_bounded = true;
        self
    }

    // The genuine public native entry supplies selected cursor coordinates.
    // Its demanded cells keep native validation; other callers stay bounded.
    pub(super) fn with_coordinate_input(mut self, input: &'a Chunk) -> Self {
        self.input = Some(input);
        self.input_is_bounded = false;
        self
    }
    pub(super) fn new(
        catalog: HostCatalog,
        metadata: &'a [NativeHostMetadata],
        context: &'a dyn Columns,
    ) -> Self {
        Self {
            ast_nodes: &[],
            catalog,
            metadata,
            constant_probes: &[],
            diagnostic_slots: &[],
            diagnostic_parents: &[],
            numeric_parents: &[],
            native_values: None,
            context,
            slots: Vec::new(),
            generation: 0,
            native_error: None,
            capture_root_error_value: false,
            root_error_value: None,
            constant_root_shortcut: false,
            root_value: None,
            typed_values: Box::new([]),
            input: None,
            input_is_bounded: true,
        }
    }
    pub(super) fn with_constant_root_shortcut(mut self, enabled: bool) -> Self {
        self.constant_root_shortcut = enabled;
        self
    }
    pub(super) fn with_native_values(
        mut self,
        values: Option<&'a RefCell<NativeValueStore>>,
    ) -> Self {
        self.native_values = values;
        self
    }
    pub(super) fn with_constant_probes(mut self, probes: &'a [Option<Datum>]) -> Self {
        self.constant_probes = probes;
        self
    }
    pub(super) fn take_native_error(&mut self) -> Option<EvalError> {
        // The ordinary consumer deliberately drops the accompanying Datum.
        self.root_error_value = None;
        self.native_error.take()
    }
    pub(super) fn with_root_error_values(mut self, enabled: bool) -> Self {
        self.capture_root_error_value = enabled;
        self
    }
    pub(super) fn take_native_failure_with_value(
        &mut self,
        expected: NativeValueOrigin,
    ) -> LocalResult<Option<(EvalError, Option<Datum>)>> {
        if !self.capture_root_error_value
            || self
                .root_error_value
                .as_ref()
                .is_some_and(|(origin, _)| *origin != expected)
        {
            return Err(binding("root failure value belongs to another invocation"));
        }
        Ok(self
            .native_error
            .take()
            .map(|error| (error, self.root_error_value.take().map(|(_, value)| value))))
    }
    fn retain_root_error_value(&mut self, id: &HostTaskId, value: Datum) -> LocalResult<()> {
        if !self.capture_root_error_value {
            return Ok(());
        }
        let task = self.task(id)?;
        let Some(NativeHostMetadata::ContextValue {
            output_node: 0,
            output_role: NativeValueRole::Output,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            // A parent Constant returns its own cached Datum when its
            // child fails, rather than leaking that child's error Datum.
            return Ok(());
        };
        if self.root_error_value.is_some() {
            return Err(binding("root failure value is already retained"));
        }
        let origin = NativeValueOrigin {
            node: 0,
            row: task.row,
        };

        self.root_error_value = Some((origin, value));
        Ok(())
    }
    fn install_task(
        &mut self,
        host_slot: HostSlot,
        row: InputRow,
        arity: usize,
        demand: SqlDemand,
    ) -> LocalResult<HostStart> {
        let generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| resource("SQL host generation space exhausted"))?;
        let slot = match self.slots.iter().position(|slot| slot.task.is_none()) {
            Some(slot) => slot,
            None => {
                self.slots
                    .try_reserve_exact(1)
                    .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
                self.slots.push(TaskSlot {
                    ready_preparation: None,
                    generation: 0,
                    task: None,
                });

                self.slots.len() - 1
            }
        };
        let first_argument = match &demand {
            SqlDemand::UserVariableWrite { .. } => 1,
            SqlDemand::WeightString(task) => task.next_argument().unwrap_or(0),
            _ => 0,
        };
        let task = NativeTask {
            slot: host_slot,
            row,
            arity,
            demand,
            values: Vec::new(),
        };

        self.slots[slot] = TaskSlot {
            ready_preparation: None,
            generation,
            task: Some(task),
        };
        self.generation = generation;
        Ok(HostStart::Pending {
            task: HostTaskId { slot, generation },
            request: HostArgRequest {
                index: first_argument,
                mode: ArgMode::Fresh,
            },
        })
    }
    #[cfg(test)]
    fn retained_bytes(&self) -> LocalResult<usize> {
        let typed = self.typed_values.iter().try_fold(
            array_bytes::<Option<Datum>>(self.typed_values.len())?,
            |n, value| {
                n.checked_add(
                    value
                        .as_ref()
                        .map(native_payload_bytes)
                        .transpose()?
                        .unwrap_or(0),
                )
                .ok_or_else(|| resource("typed cache retained size overflow"))
            },
        )?;
        let failed = self
            .root_error_value
            .as_ref()
            .map(|(_, value)| {
                native_payload_bytes(value)?
                    .checked_mul(3)
                    .ok_or_else(|| resource("root failure value retained extent overflow"))
            })
            .transpose()?
            .unwrap_or(0);
        self.slots
            .iter()
            .filter_map(|slot| slot.task.as_ref())
            .try_fold(
                array_bytes::<TaskSlot>(self.slots.capacity())?
                    .checked_add(typed)
                    .ok_or_else(|| resource("typed cache and task layout overflow"))?
                    .checked_add(failed)
                    .ok_or_else(|| resource("root failure value and tasks extent overflow"))?
                    .checked_add(
                        self.root_value
                            .as_ref()
                            .map(native_payload_bytes)
                            .transpose()?
                            .unwrap_or(0),
                    )
                    .ok_or_else(|| resource("context broadcast retained extent overflow"))?,
                |bytes, task| {
                    bytes
                        .checked_add(task.retained_bytes()?)
                        .ok_or_else(|| resource("SQL host retained size overflow"))
                },
            )
    }

    fn task(&self, id: &HostTaskId) -> LocalResult<&NativeTask> {
        self.slots
            .get(id.slot)
            .filter(|slot| slot.generation == id.generation && id.generation != 0)
            .and_then(|slot| slot.task.as_ref())
            .ok_or_else(|| LocalError::HostContract("SQL host task is stale or absent".into()))
    }
    fn finish(&mut self, id: &HostTaskId) -> LocalResult<VectorValue> {
        self.task(id)?;
        let task = self.slots[id.slot]
            .task
            .take()
            .expect("validated live task");
        if let SqlDemand::RegexpLike {
            case_insensitive,
            next,
        } = &task.demand
        {
            if next.is_some() {
                return Err(LocalError::HostContract(
                    "regexp task is not terminal".into(),
                ));
            }
            return self.finish_regexp(&task, *case_insensitive, id);
        }
        let SqlDemand::Concat { kind, demand } = &task.demand else {
            unreachable!()
        };
        let terminal = demand.finish_terminal()?;

        // Bound the overlap while the encoder consumes the owned prefix.

        let encode_cap = usize::MAX;
        let prepared = tidb_query_expr::local::prepare_concat_args(
            *kind,
            task.arity,
            task.values,
            terminal,
            encode_cap,
        )?;
        let output_cap = usize::MAX;
        let value = prepared.execute(output_cap)?;
        let len = value.as_ref().map_or(0, Vec::len);

        let mut output = ChunkedVecBytes::try_with_capacities(1, len)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(value.as_deref());

        Ok(VectorValue::Bytes(output))
    }
    fn finish_value_cast(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<VectorValue> {
        let task = self.task(id)?;
        let Some(NativeHostMetadata::ValueCast {
            cast_type,
            name,
            source_type,
            input,
            result_type,
            argument_node,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("native CAST lost its immutable descriptor"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("native CAST has no value store"))?;
        let origin = NativeValueOrigin {
            node: *argument_node,
            row,
        };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;

        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        // The actual statement context owns diagnostics, just as for the
        // native kernel. A later resource failure is terminal, never replayed.
        // No warning buffer or expression callback is retained by this host.
        let result = if matches!(cast_type, tidb_ast::CastType::Json) {
            let target = source_type.eval_type();
            let value = match input {
                NativeValuePreparation::Literal => {
                    crate::constant::typed_constant_value(source_type, self.context, target, value)
                }
                NativeValuePreparation::Computed => {
                    crate::scalar_function::normalize_numeric_row_value(
                        value,
                        Some(source_type),
                        target,
                        self.context,
                    )
                }
                NativeValuePreparation::TypedReady => Ok(match (target, value) {
                    (NativeEvalType::Int, Datum::UInt(value)) => Datum::Int(value as i64),
                    (NativeEvalType::Real, Datum::Float32(value)) => Datum::Real(value),
                    (_, value) => value,
                }),
            };
            value.and_then(|value| {
                crate::builtin_ext::json::cast_json_prepared(
                    &value,
                    Some(source_type),
                    Some(result_type),
                )
            })
        } else if value.is_null() {
            Ok(Datum::Null)
        } else {
            crate::cast::eval_cast(cast_type, value, Some(source_type), self.context)
        }
        .and_then(|value| {
            crate::scalar_function::coerce_function_result(name, Some(result_type), value)
        });
        let result = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row)
    }
    fn finish_header_outcome(&mut self, id: &HostTaskId) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let Some(NativeHostMetadata::HeaderOutcome {
            kind,
            property_value,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("header outcome descriptor missing"));
        };

        let (kind, value, node, role, row) = (
            *kind,
            property_value.clone(),
            *output_node,
            *output_role,
            task.row,
        );
        self.slots[id.slot]
            .task
            .as_mut()
            .expect("live header task")
            .demand = SqlDemand::Consumed;
        let result = kind.evaluate(self.context, value).and_then(|value| {
            coerce_optional_value_result(kind.name(), result_type.as_ref(), value)
        });
        let result = match result {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, result, node, role, row)
            .map(HostStep::Ready)
    }
    fn finish_native_value(
        &mut self,
        id: &HostTaskId,
        result: Datum,
        output_node: usize,
        output_role: NativeValueRole,
        row: InputRow,
    ) -> LocalResult<VectorValue> {
        // ScalarFunction.Eval's return normalization has already run. Its
        // enclosing Expression.Eval applies this immutable result view next.
        // Host slots provide O(1) declaration access without a tree callback,
        // a per-row metadata scan or an extra source-schema allocation.
        let metadata = self
            .metadata
            .get(self.task(id)?.slot.index())
            .ok_or_else(|| binding("native SQL result lacks its metadata"))?;
        let ast = matches!(metadata, NativeHostMetadata::Ast { .. });
        let result = if ast || !output_role.expression_view() {
            // A genuine ScalarFunction caller has already received its own
            // result-family coercion, without an enclosing Expression view.
            result
        } else if matches!(
            metadata,
            NativeHostMetadata::ContextValue { .. } | NativeHostMetadata::TypedContext(_)
        ) {
            // Context leaves already applied their consumer's expression view.
            // A root Constant broadcast deliberately publishes the raw Datum.
            result
        } else if matches!(
            metadata,
            NativeHostMetadata::EagerValues { .. }
                | NativeHostMetadata::JsonSchema(_)
                | NativeHostMetadata::Stateful(_)
                | NativeHostMetadata::FinalState(_)
                | NativeHostMetadata::CachedList(_)
                | NativeHostMetadata::WeightString(_)
                | NativeHostMetadata::RegexpValues(_)
                | NativeHostMetadata::PbEagerValues(_)
                | NativeHostMetadata::PbBinary(_)
                | NativeHostMetadata::PbLazy(_)
                | NativeHostMetadata::PbBoolean(_)
                | NativeHostMetadata::Selection { .. }
                | NativeHostMetadata::HeaderOutcome { .. }
                | NativeHostMetadata::Comparison { .. }
                | NativeHostMetadata::RawValues { .. }
                | NativeHostMetadata::Grouping { .. }
                | NativeHostMetadata::Like { .. }
                | NativeHostMetadata::ConcatValues { .. }
                | NativeHostMetadata::Arithmetic { batch: None, .. }
                | NativeHostMetadata::In(_)
                | NativeHostMetadata::Interval(_)
                | NativeHostMetadata::Extremum(_)
                | NativeHostMetadata::DateArithmetic {
                    output: tidb_query_expr::local::SqlTemporalOutput::Value,
                    ..
                }
        ) {
            crate::expression::apply_enum_set_integer_view(result, metadata.result_type())
        } else {
            let field = metadata
                .result_type()
                .ok_or_else(|| binding("native SQL result lacks its declaration"))?;
            crate::expression::apply_enum_set_integer_view(result, Some(field))
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("native result has no value store"))?;

        let token = store.borrow_mut().insert_owned(
            result,
            output_role,
            NativeValueOrigin {
                node: output_node,
                row,
            },
        )?;
        let mut output = ChunkedVecBytes::try_with_capacities(1, token.len())
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(Some(&token));

        self.slots[id.slot].task = None;
        self.slots[id.slot].ready_preparation = None;
        Ok(VectorValue::Bytes(output))
    }
    fn resume_nullable(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Nullable { next } = task.demand else {
            return Err(binding("nullable resume has another task kind"));
        };
        let Some(NativeHostMetadata::Selection { argument_nodes, .. }) =
            self.metadata.get(task.slot.index())
        else {
            return Err(binding("nullable selection lost its source metadata"));
        };
        let node = *argument_nodes
            .get(next)
            .ok_or_else(|| binding("nullable argument source missing"))?;
        let row = task.row;
        let arity = task.arity;
        let origin = NativeValueOrigin { node, row };
        let store = self
            .native_values
            .ok_or_else(|| binding("nullable selection has no value store"))?;
        let is_null = {
            let store = store.borrow();
            let value = store.get(token, NativeValueRole::CharInput, origin)?;
            value.is_null()
        };
        if matches!(
            tidb_query_expr::native_if_null_choose_first((!is_null).then_some(())),
            tidb_query_expr::NativeIfNullChoice::NeedSecond
        ) && next + 1 < arity
        {
            let SqlDemand::Nullable { next: cursor } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated task")
                .demand
            else {
                unreachable!()
            };
            *cursor = next + 1;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: next + 1,
                mode: ArgMode::Fresh,
            }));
        }
        self.finish_selection_argument(id, token, next)
            .map(HostStep::Ready)
    }
    fn finish_selection_argument(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
        index: usize,
    ) -> LocalResult<VectorValue> {
        let task = self.task(id)?;
        let Some(NativeHostMetadata::Selection {
            name,
            result_type,
            argument_nodes,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("selected argument lost its descriptor"));
        };
        let row = task.row;
        let node = *argument_nodes
            .get(index)
            .ok_or_else(|| binding("selected producer ordinal missing"))?;
        let origin = NativeValueOrigin { node, row };
        let store = self
            .native_values
            .ok_or_else(|| binding("selection has no native store"))?;
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;

        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        let value =
            match crate::scalar_function::coerce_function_result(name, result_type.as_ref(), value)
            {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };
        self.finish_native_value(id, value, *output_node, *output_role, row)
    }
    fn resume_arithmetic(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        use crate::scalar_function::NativeArithmeticInputMode as Mode;
        let task = self.task(id)?;
        let SqlDemand::Arithmetic { next, typed, .. } = &task.demand else {
            return Err(binding("arithmetic resume task differs"));
        };
        let (index, typed, row, slot) = (*next, *typed, task.row, task.slot);
        let Some(NativeHostMetadata::Arithmetic {
            operation,
            name: _,
            result_type: _,
            argument_nodes,
            source_types,
            literal,
            binary_literal,
            mode: _,
            generic_mode,
            subjects,
            subject_format_failures,
            overflow_format_failure: _,
            overflow_expressions: _,
            intdiv: _,
            output_node: _,
            output_role: _,
            batch: None,
        }) = self.metadata.get(slot.index())
        else {
            return Err(binding("arithmetic source descriptor missing"));
        };
        let op = match operation {
            SqlBatchHostOperation::AddNative => tidb_ast::BinaryOp::Plus,
            SqlBatchHostOperation::SubtractNative => tidb_ast::BinaryOp::Minus,
            SqlBatchHostOperation::MultiplyNative => tidb_ast::BinaryOp::Mul,
            SqlBatchHostOperation::ModuloNative => tidb_ast::BinaryOp::Mod,
            SqlBatchHostOperation::DivideNative => tidb_ast::BinaryOp::Div,
            SqlBatchHostOperation::IntegerDivideNative => tidb_ast::BinaryOp::IntDiv,
            _ => return Err(binding("arithmetic operation differs")),
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("arithmetic native value store missing"))?;
        let origin = NativeValueOrigin {
            node: argument_nodes[index],
            row,
        };
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;

        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        let mode = if typed {
            Mode::TypedInteger
        } else {
            *generic_mode
        };
        let value = if mode == Mode::Raw {
            value
        } else {
            match crate::scalar_function::native_numeric_operand(
                arithmetic_typed_source(source_types, index)?,
                literal[index],
                binary_literal[index],
                value,
                mode,
                self.context,
                subjects[index].as_deref(),
                subject_format_failures[index],
            ) {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            }
        };
        let prepared = if index == 0 {
            let stop = value.is_null()
                && (typed
                    || matches!(mode,Mode::Numeric(domain) if !(domain==NativeEvalType::Real && op==tidb_ast::BinaryOp::Plus)
                        && !(op==tidb_ast::BinaryOp::Mod && domain!=NativeEvalType::Decimal)));
            if stop {
                if quotient::is_quotient(op) {
                    return self.finish_arithmetic_row_value(
                        id,
                        crate::ops::eval_binary_arithmetic_null_in(self.context),
                    );
                }
                crate::ops::arithmetic::PreparedArithmetic::Null
            } else {
                let SqlDemand::Arithmetic { next, left, .. } = &mut self.slots[id.slot]
                    .task
                    .as_mut()
                    .expect("validated arithmetic task")
                    .demand
                else {
                    unreachable!()
                };
                *left = Some(value);
                *next = 1;
                return Ok(HostStep::NeedArg(HostArgRequest {
                    index: 1,
                    mode: ArgMode::Fresh,
                }));
            }
        } else {
            let SqlDemand::Arithmetic { left, .. } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated arithmetic task")
                .demand
            else {
                unreachable!()
            };
            let left = left
                .take()
                .ok_or_else(|| binding("arithmetic left value missing"))?;
            let unsigned = arithmetic_unsigned(source_types);
            if quotient::is_quotient(op) {
                let available = usize::MAX;
                let result =
                    self.compute_quotient(&self.metadata[slot.index()], left, value, available)?;
                return self.finish_arithmetic_row_value(id, result);
            }
            if typed {
                match crate::ops::prepare_native_typed_arithmetic(
                    op,
                    left,
                    value,
                    unsigned,
                    self.context,
                ) {
                    Ok(Some(prepared)) => prepared,
                    Ok(None) => {
                        // Retain the original failed fast probe's fresh second
                        // demands inside the same shared driver.
                        let SqlDemand::Arithmetic { next, typed, .. } = &mut self.slots[id.slot]
                            .task
                            .as_mut()
                            .expect("validated arithmetic task")
                            .demand
                        else {
                            unreachable!()
                        };
                        *typed = false;
                        *next = 0;
                        return Ok(HostStep::NeedArg(HostArgRequest {
                            index: 0,
                            mode: ArgMode::Fresh,
                        }));
                    }
                    Err(error) => return self.native_failure(error),
                }
            } else {
                let (left, right) = crate::binary_literal::cast_signed_literal_operands(
                    op,
                    left,
                    value,
                    arithmetic_signed(source_types),
                );
                let _ = self.context.div_precision_increment();
                match crate::ops::arithmetic::prepare(op, left, right, unsigned, self.context) {
                    Ok(prepared) => prepared,
                    Err(error) => return self.native_failure(error),
                }
            }
        };
        let available = usize::MAX;
        self.finish_arithmetic_row_value(id, prepared.compute(available))
    }

    fn finish_math(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let Some(NativeHostMetadata::Math {
            name,
            result_type,
            argument_node,
            output_node,
            output_role,
            overflow_expression,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("math descriptor missing"));
        };
        let row = task.row;
        let slot = task.slot;
        let origin = NativeValueOrigin {
            node: *argument_node,
            row,
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("math native value store missing"))?;
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;

        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        // A context warning can unwind. Keep the authenticated task record,
        // but consume the value demand before crossing that callback boundary.
        self.slots[id.slot]
            .task
            .as_mut()
            .expect("authenticated math task")
            .demand = SqlDemand::Consumed;
        let available = usize::MAX;
        use tidb_query_expr::impl_math::{
            native_abs_decimal_value, native_abs_int_value, native_abs_real_value,
            native_abs_uint_value,
        };
        let value = match value {
            Datum::Null => native_abs_int_value(None)
                .expect("NULL cannot overflow")
                .map_or(Datum::Null, Datum::Int),
            Datum::Int(value) => match native_abs_int_value(Some(value)) {
                Ok(value) => value.map_or(Datum::Null, Datum::Int),
                Err(_) => {
                    if self.diagnostic_parent(slot).is_some() {
                        return self.begin_math_diagnostic(id, EvalError::IntOverflow);
                    }
                    let error = if let Some(expression) = overflow_expression {
                        EvalError::DataOutOfRange {
                            value: "BIGINT",
                            expression: expression.to_string(),
                        }
                    } else {
                        EvalError::IntOverflow
                    };
                    return self.native_failure(error);
                }
            },
            Datum::UInt(value) => Datum::UInt(native_abs_uint_value(value)),
            Datum::Real(value) => {
                native_abs_real_value(Some(value)).map_or(Datum::Null, Datum::Real)
            }
            Datum::Decimal(value) => {
                let shared = match crate::tikv::prepare_math_decimal(&value) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                let budget = u64::try_from(available)
                    .map_err(|_| resource("math kernel allowance overflow"))?
                    as i64;
                let computed =
                    native_abs_decimal_value(&shared, &budget).map_err(LocalError::Evaluation)?;
                let value = match crate::tikv::restore_math_decimal(&computed, available) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                Datum::Decimal(value)
            }
            value => {
                let ready = crate::math_fn::numeric_arg(&value, self.context);
                store
                    .borrow()
                    .get(token, NativeValueRole::CharInput, origin)?;
                let ready = match ready {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                native_abs_real_value(ready).map_or(Datum::Null, Datum::Real)
            }
        };
        let value =
            match crate::scalar_function::coerce_function_result(name, Some(result_type), value) {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };
        self.finish_native_value(id, value, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }

    fn resume_comparison(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Comparison {
            left_token,
            integer_probe,
        } = task.demand
        else {
            return Err(binding("comparison resume has another task kind"));
        };
        let Some(NativeHostMetadata::Comparison {
            operation,
            name,
            argument_nodes,
            operands,
            collation,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("comparison descriptor missing"));
        };
        let row = task.row;
        let store = self
            .native_values
            .ok_or_else(|| binding("comparison native value store missing"))?;
        if left_token.is_none() {
            // Validate/copy only the capability. Original comparisons demand
            // both raw operands before any value coercion, including NULL.
            store.borrow().get(
                token,
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: argument_nodes[0],
                    row,
                },
            )?;
            let left: [u8; native_values::HANDLE_BYTES] = token
                .try_into()
                .map_err(|_| binding("comparison token width differs"))?;
            let SqlDemand::Comparison { left_token, .. } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated comparison task")
                .demand
            else {
                unreachable!()
            };
            *left_token = Some(left);
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        let left_token = left_token.expect("left was demanded");
        let left_origin = NativeValueOrigin {
            node: argument_nodes[0],
            row,
        };
        let right_origin = NativeValueOrigin {
            node: argument_nodes[1],
            row,
        };
        {
            let store = store.borrow();
            store.get(&left_token, NativeValueRole::CharInput, left_origin)?;
            store.get(token, NativeValueRole::CharInput, right_origin)?;
        }

        let (left, right) = {
            let store = store.borrow();
            (
                store.clone_for_kernel(&left_token, NativeValueRole::CharInput, left_origin)?,
                store.clone_for_kernel(token, NativeValueRole::CharInput, right_origin)?,
            )
        };
        if integer_probe {
            // ScalarFunction.Eval first reads both raw integer-declared
            // arguments. A runtime-domain refusal then re-demands both in its
            // ordinary ladder; reusing the first values would skip effects.
            let op = crate::scalar_function::binary_op_for_name(name)
                .ok_or_else(|| binding("integer comparison operator name missing"))?;
            match crate::ops::integer_binary_typed(
                op,
                left,
                right,
                operands.map(|operand| !operand.unsigned),
                self.context,
            ) {
                Ok(Some(result)) => return self.finish_comparison(id, result).map(HostStep::Ready),
                Ok(None) => {
                    let SqlDemand::Comparison {
                        left_token,
                        integer_probe,
                    } = &mut self.slots[id.slot]
                        .task
                        .as_mut()
                        .expect("validated comparison task")
                        .demand
                    else {
                        unreachable!()
                    };
                    *left_token = None;
                    *integer_probe = false;
                    return Ok(HostStep::NeedArg(HostArgRequest {
                        index: 0,
                        mode: ArgMode::Fresh,
                    }));
                }
                Err(error) => return self.native_failure(error),
            }
        }
        // The plain conversion may publish diagnostics through the real context;
        // neither source handle nor store borrow crosses that boundary.
        // NULLIF is a value-only builtin, not the binary operator entry.
        // Its original arguments never take the signed-literal normalization.
        let (left, right) = if *operation == SqlBatchHostOperation::NullIfNative {
            (left, right)
        } else {
            let binary = crate::scalar_function::binary_op_for_name(name)
                .ok_or_else(|| binding("comparison operator name missing"))?;
            crate::binary_literal::cast_signed_literal_operands(
                binary,
                left,
                right,
                operands.map(|operand| operand.eval_type.is_some() && !operand.unsigned),
            )
        };
        let available = usize::MAX;
        let result = match *operation {
            SqlBatchHostOperation::CompareNative(op) => {
                let prepared = match crate::ops::prepare_native_comparison_after_probe(
                    op,
                    left,
                    right,
                    *collation,
                    *operands,
                    self.context,
                ) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                prepared.compute(available)?.map_or(Datum::Null, Datum::Int)
            }
            SqlBatchHostOperation::NullSafeEqNative => {
                let prepared = match crate::ops::prepare_native_null_safe_comparison(
                    left,
                    right,
                    *collation,
                    *operands,
                    self.context,
                ) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                prepared.compute(available)?.map_or(Datum::Null, Datum::Int)
            }
            SqlBatchHostOperation::NullIfNative => {
                // The original builtin compares the already fetched values as
                // literals, independently of the compiled child declarations.
                let prepared = match crate::ops::prepare_literal_comparison(
                    tidb_query_expr::ComparisonOp::Eq,
                    left,
                    right,
                    self.context,
                ) {
                    Ok(value) => value,
                    Err(error) => return self.native_failure(error),
                };
                let comparison = prepared.compute(available)?;
                if !tidb_query_expr::if_head_native_args_valid(comparison) {
                    return Err(binding(
                        "NULLIF comparison is not a normalized nullable boolean",
                    ));
                }
                match tidb_query_expr::native_if_choose_branch(comparison.map(|value| value == 1)) {
                    tidb_query_expr::NativeIfBranch::Then => Datum::Null,
                    tidb_query_expr::NativeIfBranch::Else => {
                        // Both original capabilities and the larger two-input
                        // clone allowance were checked before comparison. Its
                        // temporary owners are gone before this selected copy.
                        store.borrow().clone_for_kernel(
                            &left_token,
                            NativeValueRole::CharInput,
                            left_origin,
                        )?
                    }
                }
            }
            _ => return Err(binding("comparison descriptor has another operation")),
        };
        self.finish_comparison(id, result).map(HostStep::Ready)
    }

    fn finish_comparison(&mut self, id: &HostTaskId, result: Datum) -> LocalResult<VectorValue> {
        let task = self.task(id)?;
        let row = task.row;
        let Some(NativeHostMetadata::Comparison {
            name,
            result_type,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("comparison finish descriptor missing"));
        };
        let result = match crate::scalar_function::coerce_function_result(
            name,
            result_type.as_ref(),
            result,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row)
    }

    fn finish_truth(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<VectorValue> {
        let task = self.task(id)?;
        let Some(NativeHostMetadata::Selection {
            name,
            result_type,
            argument_nodes,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("native truth conversion lost its descriptor"));
        };
        let row = task.row;
        let node = *argument_nodes
            .first()
            .ok_or_else(|| binding("native truth producer missing"))?;
        let origin = NativeValueOrigin { node, row };
        let store = self
            .native_values
            .ok_or_else(|| binding("native truth conversion has no value store"))?;
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;

        // IS NULL observes raw nullness; UNKNOWN aliases preserve the scalar
        // caller's truth validation. NOT has its own range-sentinel diagnostic.
        // These plain conversions do not emit truncation warnings.
        let truth = {
            let store = store.borrow();
            let value = store.get(token, NativeValueRole::CharInput, origin)?;
            if name.as_ref() == "isnull" {
                Ok((!value.is_null()).then_some(false))
            } else if name.as_ref() == "not" && value.is_range_sentinel() {
                Err(EvalError::Unsupported("range sentinel expression operand"))
            } else {
                crate::truthy_of(value)
            }
        };
        let truth = match truth {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let function = match name.as_ref() {
            "not" => crate::BooleanFunction::UnaryNot,
            "isnull" | "isunknown" => crate::BooleanFunction::IsNull,
            "istrue" => crate::BooleanFunction::IsTrue,
            "istrue_with_null" => crate::BooleanFunction::IsTrueWithNull,
            "isfalse" => crate::BooleanFunction::IsFalse,
            "isnottrue" => crate::BooleanFunction::IsNotTrue,
            "isnotfalse" => crate::BooleanFunction::IsNotFalse,
            "isnotunknown" => crate::BooleanFunction::IsNotNull,
            _ => return Err(binding("unknown native boolean predicate")),
        };
        let result = crate::tikv::boolean_value(function, truth)?;
        let result = result.map_or(Datum::Null, Datum::Int);
        let result = match crate::scalar_function::coerce_function_result(
            name,
            result_type.as_ref(),
            result,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row)
    }
    fn resume_logical(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Logical { next, left } = task.demand else {
            return Err(binding("logical resume has another task kind"));
        };
        let Some(NativeHostMetadata::Selection {
            name,
            result_type,
            argument_nodes,
            output_node,
            output_role,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("logical operation lost its descriptor"));
        };
        let function = if matches!(name.as_ref(), "or" | "logicor") {
            crate::LogicalFunction::Or
        } else {
            crate::LogicalFunction::And
        };
        let row = task.row;
        let node = *argument_nodes
            .get(next)
            .ok_or_else(|| binding("logical argument source missing"))?;
        let origin = NativeValueOrigin { node, row };
        let store = self
            .native_values
            .ok_or_else(|| binding("logical operation has no value store"))?;
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;

        // The conversion may publish a statement warning or error. The store
        // borrow is released before entering that actual context boundary.
        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        let truth = match crate::scalar_function::logic_truthy(&value, self.context) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        if next == 0 && crate::scalar_function::logical_needs_right(function, truth) {
            let SqlDemand::Logical { next, left } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated task")
                .demand
            else {
                unreachable!()
            };
            *next = 1;
            *left = truth;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index: 1,
                mode: ArgMode::Fresh,
            }));
        }
        let (left, right) = if next == 0 {
            // Only a validated absorbing left reaches this branch. This is an
            // irrelevant kernel representative, not a demanded right value.
            (truth, Some(false))
        } else {
            (left, truth)
        };
        if function == crate::LogicalFunction::Xor {
            return Err(binding("XOR is outside this logical source contract"));
        }
        let result = crate::tikv::logical_value(function, left, right)?;
        let result = result.map_or(Datum::Null, Datum::Int);
        let result = match crate::scalar_function::coerce_function_result(
            name,
            result_type.as_ref(),
            result,
        ) {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        self.finish_native_value(id, result, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
    fn resume_conditional(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Conditional { next } = task.demand else {
            return Err(binding("conditional resume has another task kind"));
        };
        let Some(NativeHostMetadata::Selection {
            name,
            argument_nodes,
            output_node,
            output_role,
            ..
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("conditional selection lost its descriptor"));
        };
        let arity = task.arity;
        let condition = (name.as_ref() == "if" && next == 0)
            || (name.as_ref() == "case" && next % 2 == 0 && next + 1 < arity);
        if !condition {
            return self
                .finish_selection_argument(id, token, next)
                .map(HostStep::Ready);
        }
        let row = task.row;
        let node = *argument_nodes
            .get(next)
            .ok_or_else(|| binding("conditional producer ordinal missing"))?;
        let origin = NativeValueOrigin { node, row };
        let store = self
            .native_values
            .ok_or_else(|| binding("conditional has no native store"))?;
        store
            .borrow()
            .get(token, NativeValueRole::CharInput, origin)?;

        let truth = crate::truthy_of(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?);
        let truth = match truth {
            Ok(value) => value,
            Err(error) => return self.native_failure(error),
        };
        let selected = tidb_query_expr::native_if_choose_branch(truth)
            == tidb_query_expr::NativeIfBranch::Then;
        let requested = if name.as_ref() == "if" {
            Some(if selected { 1 } else { 2 })
        } else if selected {
            Some(next + 1)
        } else {
            (next + 2 < arity).then_some(next + 2)
        };
        if let Some(index) = requested {
            let SqlDemand::Conditional { next: cursor } = &mut self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated task")
                .demand
            else {
                unreachable!()
            };
            *cursor = index;
            return Ok(HostStep::NeedArg(HostArgRequest {
                index,
                mode: ArgMode::Fresh,
            }));
        }
        // CASE without ELSE owns this NULL, not the final predicate's identity.
        self.finish_native_value(id, Datum::Null, *output_node, *output_role, row)
            .map(HostStep::Ready)
    }
    fn finish_native_cast(
        &mut self,
        id: &HostTaskId,
        token: &[u8],
        len: Option<u32>,
    ) -> LocalResult<VectorValue> {
        let task = self.task(id)?;
        let Some(NativeHostMetadata::CharNative {
            charset,
            source_type,
            argument_node,
        }) = self.metadata.get(task.slot.index())
        else {
            return Err(binding("native character cast lost its source metadata"));
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("native character cast has no value store"))?;
        let origin = NativeValueOrigin {
            node: *argument_node,
            row: task.row,
        };
        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        let mut warnings = CapturedWarnings {
            rows: Vec::new(),
            failure: None,
        };
        let result = if value.is_null() {
            Ok(Datum::Null)
        } else {
            crate::cast::validate_cast_value_admission(
                &value,
                tidb_query_expr::NativeCastAdmissionTarget::String,
            )
            .and_then(|()| {
                crate::tikv::eval_cast_char_reported_in(
                    self.context,
                    &value,
                    Some(source_type),
                    len,
                    Some(charset),
                    |code, message| warnings.capture(code, message),
                )
            })
        };
        if let Some(error) = warnings.failure.take() {
            return Err(error);
        }
        let result = match result {
            Ok(value) => value,
            Err(error) => {
                for (code, message) in &warnings.rows {
                    self.context.append_warning(*code, message);
                }
                return self.native_failure(error);
            }
        };
        let bytes = match result {
            Datum::Null => None,
            Datum::String(value) => Some(value.into_bytes()),
            Datum::Bytes(value) => Some(value),
            _ => return Err(binding("native CHAR kernel returned an unexpected kind")),
        };
        let length = bytes.as_ref().map_or(0, Vec::len);

        let mut output = ChunkedVecBytes::try_with_capacities(1, length)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(bytes.as_deref());

        for (code, message) in &warnings.rows {
            self.context.append_warning(*code, message);
        }
        self.slots[id.slot].task = None;
        self.slots[id.slot].ready_preparation = None;
        Ok(VectorValue::Bytes(output))
    }
    fn finish_int_cast(
        &mut self,
        id: &HostTaskId,
        scalar: ScalarValueRef<'_>,
        len: Option<u32>,
    ) -> LocalResult<VectorValue> {
        use tidb_query_datatype::codec::native_sql_string::{
            native_sql_string, NativeSqlStringInput,
        };
        use tidb_query_expr::{
            NativeCastStringError, NativeCastStringInput, NativeCastStringResult,
            NativeCastStringTarget,
        };
        let slot = self.task(id)?.slot;
        // Integer display is at most 20 bytes. Bound both temporary strings,
        // the output byte vector and its offsets before emitting any warning.

        let Some(NativeHostMetadata::CharCast { charset, unsigned }) =
            self.metadata.get(slot.index())
        else {
            return Err(LocalError::HostContract(
                "character cast has no target metadata".into(),
            ));
        };
        let ScalarValueRef::Int(value) = scalar else {
            unreachable!("checked Int")
        };
        let value = if let Some(value) = value {
            let (input, display) = if *unsigned {
                (
                    NativeCastStringInput::UInt(*value as u64),
                    NativeSqlStringInput::UInt(*value as u64),
                )
            } else {
                (
                    NativeCastStringInput::Int(*value),
                    NativeSqlStringInput::Int(*value),
                )
            };
            let result = tidb_query_expr::native_cast_string(
                input,
                NativeCastStringTarget::Char {
                    len,
                    charset: Some(charset),
                },
                None,
                || self.context.connection_charset_info().0,
                || native_sql_string(display),
                || self.context.max_allowed_packet(),
                |name| self.context.handle_allowed_packet_overflowed(name),
                |code, message| self.context.append_warning(code, message),
            );
            match result {
                Ok(NativeCastStringResult::Text(value)) => Some(value.into_bytes()),
                Ok(NativeCastStringResult::Bytes(value)) => Some(value),
                Ok(NativeCastStringResult::Null) => None,
                Err(NativeCastStringError::Child(error)) => return self.native_failure(error),
                Err(NativeCastStringError::InvalidUtf8StringCoercion) => {
                    return self
                        .native_failure(EvalError::Unsupported("invalid UTF-8 string coercion"))
                }
            }
        } else {
            None
        };
        let mut output =
            ChunkedVecBytes::try_with_capacities(1, value.as_ref().map_or(0, Vec::len))
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(value.as_deref());

        self.slots[id.slot].task = None;
        self.slots[id.slot].ready_preparation = None;
        Ok(VectorValue::Bytes(output))
    }
    fn native_failure<T>(&mut self, error: EvalError) -> LocalResult<T> {
        self.native_error = Some(error);
        Err(LocalError::HostContract(
            "native SQL context operation failed".into(),
        ))
    }
    fn check_regexp_scope(&mut self, id: &HostTaskId) -> LocalResult<()> {
        if let Some(guard) = &self.slots[id.slot].ready_preparation {
            if let Err(error) = guard.cache().original_scope_status() {
                return self.native_failure(error);
            }
        }
        Ok(())
    }
    fn finish_regexp(
        &mut self,
        task: &NativeTask,
        case_insensitive: bool,
        id: &HostTaskId,
    ) -> LocalResult<VectorValue> {
        let result = if task.values.iter().any(Option::is_none) {
            self.check_regexp_scope(id)?;
            None
        } else {
            if task.values.len() != task.arity {
                return Err(LocalError::HostContract(
                    "regexp argument prefix is incomplete".into(),
                ));
            }
            let flags = task.values.get(2).and_then(Option::as_deref).unwrap_or(b"");
            let flags_len = flags
                .len()
                .checked_add(usize::from(case_insensitive))
                .ok_or_else(|| resource("regexp flags size overflow"))?;

            let mut prepared_flags = Vec::new();
            prepared_flags
                .try_reserve_exact(flags_len)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            if case_insensitive {
                prepared_flags.push(b'i');
            }
            prepared_flags.extend_from_slice(flags);

            // Every demanded argument has already passed its original UTF-8 gate.
            let text = std::str::from_utf8(task.values[0].as_deref().expect("nonnull text"))
                .expect("checked UTF-8");
            let pattern = std::str::from_utf8(task.values[1].as_deref().expect("nonnull pattern"))
                .expect("checked UTF-8");
            let flags = std::str::from_utf8(&prepared_flags).expect("checked UTF-8 flags");
            let invocation = self
                .metadata
                .get(task.slot.index())
                .and_then(NativeHostMetadata::regexp)
                .ok_or_else(|| {
                    LocalError::HostContract("regexp slot has no native cache handles".into())
                })?;
            // Context lookup and compilation occur only after all non-NULL arguments.
            let context_id = self.context.context_id();
            self.check_regexp_scope(id)?;
            let prepared = invocation
                .with_context_id(context_id)
                .prepare_like(pattern, flags);

            match prepared.execute(text) {
                Ok(value) => Some(i64::from(value)),
                Err(error) => {
                    return self.native_failure(crate::regexp::native_regexp_error(&error))
                }
            }
        };

        Ok(VectorValue::from_scalar(&ScalarValue::Int(result), 1))
    }
}
impl NativeBatchHosts<'_> {
    fn start_sql(
        &mut self,
        _ctx: &mut EvalContext,
        invocation: HostInvocation<'_>,
    ) -> LocalResult<HostStart> {
        if self.capture_root_error_value && self.native_error.is_some() {
            return Err(binding("previous scalar failure has not been consumed"));
        }
        let signature = self.catalog.signature(invocation.slot)?;
        if signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
        {
            return Err(LocalError::HostContract(
                "SQL host invocation differs from its catalog signature".into(),
            ));
        }
        if self.catalog.sql_batch_operation(invocation.slot)? == SqlBatchHostOperation::AstNative {
            return self.start_ast(invocation);
        }
        if !self.constant_probes.is_empty() {
            if self.constant_probes.len() != self.metadata.len() {
                return Err(binding("source prefix descriptor count differs"));
            }
            let probe = self
                .constant_probes
                .get(invocation.slot.index())
                .ok_or_else(|| binding("source prefix slot absent"))?;
            if let Err(error) = crate::tikv::identity_actual::probe(probe.as_ref()) {
                return self.native_failure(error);
            }
        }
        if self.numeric_parent(invocation.slot).is_some() {
            return self.start_numeric_parent(invocation);
        }
        let mut ready_preparation = None;
        let demand = match self.catalog.sql_batch_operation(invocation.slot)? {
            SqlBatchHostOperation::LegacyNative => {
                return Err(binding(
                    "legacy source hosts require their actual source provider",
                ));
            }
            SqlBatchHostOperation::AstNative => unreachable!("routed above"),
            SqlBatchHostOperation::EagerValuesNative => {
                if let Some(NativeHostMetadata::HeaderOutcome {
                    kind,
                    logical_arity,
                    property_value,
                    output_role,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || !invocation.arg_types.is_empty()
                        || !kind.accepts_arity(*logical_arity)
                        || !kind.accepts_snapshot(property_value.as_ref())
                        || !output_role.sql_value()
                    {
                        return Err(binding("header outcome declaration differs"));
                    }
                    SqlDemand::HeaderOutcome
                } else if let Some(NativeHostMetadata::PbLazy(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || meta.arguments.len() != meta.kernel.data_arity(meta.logical_arity)
                        || meta.arguments.len() != invocation.arg_types.len()
                        || !meta.output_role.sql_value()
                    {
                        return Err(binding("PB lazy declaration differs"));
                    }
                    SqlDemand::PbLazy(Box::new(pb_lazy_values_source::PbLazyTask::new(
                        meta.kernel.initial(meta.logical_arity),
                    )))
                } else if let Some(NativeHostMetadata::PbBoolean(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || meta.arguments.len() != meta.kernel.data_arity(meta.logical_arity)
                        || meta.arguments.len() != invocation.arg_types.len()
                        || !meta.output_role.sql_value()
                    {
                        return Err(binding("PB Boolean declaration differs"));
                    }
                    SqlDemand::PbBoolean(Box::new(pb_boolean_values_source::PbBooleanTask::new()))
                } else if let Some(NativeHostMetadata::PbBinary(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if meta.kernel.domain().is_some()
                        || self.native_values.is_none()
                        || meta.arguments.len() != meta.logical_arity.min(2)
                        || meta.arguments.len() != invocation.arg_types.len()
                        || !meta.output_role.sql_value()
                    {
                        return Err(binding("PB raw MOD declaration differs"));
                    }
                    SqlDemand::PbIntegerMod(Box::new(pb_binary_values_source::PbModTask::new()))
                } else if let Some(NativeHostMetadata::PbEagerValues(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || meta.arguments.len() != invocation.arg_types.len()
                        || !meta.output_role.sql_value()
                        || (meta.integer_batch_result
                            && (meta.output_role != NativeValueRole::Output
                                || !meta
                                    .result_type
                                    .as_ref()
                                    .is_some_and(|field| field.eval_type() == NativeEvalType::Int)))
                    {
                        return Err(binding("PB eager declaration differs"));
                    }
                    SqlDemand::PbEagerValues { tokens: Vec::new() }
                } else if let Some(NativeHostMetadata::RegexpValues(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if (meta.operator && meta.protobuf_result)
                        || (meta.integer_batch_result
                            && (!meta.protobuf_result
                                || meta.output_role != NativeValueRole::Output
                                || !meta
                                    .result_type
                                    .as_ref()
                                    .is_some_and(|field| field.eval_type() == NativeEvalType::Int)))
                        || self.native_values.is_none()
                        || meta.arguments.len() != invocation.arg_types.len()
                        || meta.source_types.len() != meta.arguments.len()
                        || if meta.operator {
                            meta.arguments.len() != 2
                        } else {
                            !matches!(meta.arguments.len(), 2 | 3)
                        }
                        || !meta.output_role.sql_value()
                    {
                        return Err(binding("regexp value declaration differs"));
                    }
                    ready_preparation = self
                        .context
                        .ready_value_cache()
                        .map(|cache| cache.preparation_unwind_guard());
                    SqlDemand::RegexpValues(Box::new(regexp_values_source::RegexpTask::First))
                } else if let Some(NativeHostMetadata::WeightString(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || meta.logical_arity == 0
                        || meta.arguments.len() != invocation.arg_types.len()
                        || meta.arguments.len()
                            != crate::scalar_function::weight_value::data_arity(
                                meta.logical_arity,
                                meta.source_type.as_ref(),
                            )
                        || !meta.output_role.sql_value()
                    {
                        return Err(binding("weight declaration differs"));
                    }
                    SqlDemand::WeightString(if meta.logical_arity == 3 {
                        weight_string_source::WeightTask::Kind
                    } else if meta.arguments.is_empty() {
                        weight_string_source::WeightTask::Numeric
                    } else {
                        weight_string_source::WeightTask::Value(None)
                    })
                } else if let Some(NativeHostMetadata::JsonSchema(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || meta.arguments.len() != invocation.arg_types.len()
                    {
                        return Err(binding("schema declaration differs"));
                    }
                    SqlDemand::JsonSchema(Box::new(json_schema_source::SchemaTask::Schema))
                } else if let Some(NativeHostMetadata::FinalState(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || meta.arguments.len() != invocation.arg_types.len()
                    {
                        return Err(binding("final state declaration differs"));
                    }
                    if meta.name.starts_with("aes_") {
                        ready_preparation = self
                            .context
                            .ready_value_cache()
                            .map(|cache| cache.preparation_unwind_guard());
                    }
                    SqlDemand::FinalState(Box::new(final_state_source::FinalTask::start(
                        meta,
                        self.context,
                    )))
                } else if let Some(NativeHostMetadata::Stateful(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || meta.arguments.len() != invocation.arg_types.len()
                    {
                        return Err(binding("stateful declaration differs"));
                    }
                    SqlDemand::Stateful(Box::new(stateful_source::StatefulTask::First))
                } else if let Some(NativeHostMetadata::CachedList(meta)) =
                    self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || invocation.arg_types.len() != 2
                        || !meta.output_role.sql_value()
                    {
                        return Err(binding("cached list declaration differs"));
                    }
                    SqlDemand::CachedList(Box::new(cached_list_source::CachedListTask::Needle))
                } else if matches!(
                    self.metadata.get(invocation.slot.index()),
                    Some(NativeHostMetadata::TypedContext(_))
                ) {
                    self.validate_typed_context(&invocation)?;
                    SqlDemand::TypedContext { consumed: false }
                } else if let Some(NativeHostMetadata::Math { name, .. }) =
                    self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || name.as_ref() != "abs"
                        || self.diagnostic_parent(invocation.slot).is_none()
                    {
                        return Err(binding("eager math transport lacks its diagnostic parent"));
                    }
                    SqlDemand::Math
                } else if let Some(NativeHostMetadata::ContextValue {
                    descriptor,
                    argument,
                    diagnostic,
                    output_role,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                {
                    if self.native_values.is_none()
                        || usize::from(descriptor.demands_child()) != invocation.arg_types.len()
                        || argument.is_some() != descriptor.demands_child()
                        || (diagnostic.is_some() && *output_role != NativeValueRole::CharInput)
                        || (diagnostic.is_some()
                            && !matches!(
                                &descriptor.read,
                                crate::context_value::ContextRead::Parameter(_)
                                    | crate::context_value::ContextRead::Deferred
                            ))
                    {
                        return Err(binding("context descriptor arity differs"));
                    }
                    SqlDemand::ContextValue { consumed: false }
                } else {
                    let Some(NativeHostMetadata::EagerValues {
                        arguments,
                        kernel,
                        probe,
                        ..
                    }) = self.metadata.get(invocation.slot.index())
                    else {
                        return Err(binding("eager values descriptor absent"));
                    };
                    if self.native_values.is_none()
                        || arguments.len().checked_add(
                            self.diagnostic_parent(invocation.slot)
                                .map_or(0, |p| p.arguments.len()),
                        ) != Some(invocation.arg_types.len())
                    {
                        return Err(binding("eager values descriptor differs"));
                    }
                    let name = match crate::tikv::identity_actual::probe_name(probe.as_ref()) {
                        Ok(name) => name,
                        Err(e) => return self.native_failure(e),
                    };
                    if let Some(phase) = kernel.prefix_phase() {
                        SqlDemand::PrefixValues { phase }
                    } else {
                        match (kernel.user_variable(), name) {
                            (
                                Some(
                                    crate::scalar_function::user_variable::UserVariableAction::Get(
                                        _,
                                    ),
                                ),
                                Some(name),
                            ) => SqlDemand::UserVariableRead {
                                name,
                                consumed: false,
                            },
                            (
                                Some(
                                    crate::scalar_function::user_variable::UserVariableAction::Set,
                                ),
                                Some(name),
                            ) => SqlDemand::UserVariableWrite {
                                name,
                                consumed: false,
                            },
                            _ => SqlDemand::EagerValues {
                                next: 0,
                                tokens: Vec::new(),
                            },
                        }
                    }
                }
            }

            SqlBatchHostOperation::TimestampDiffNative {
                typed,
                stop_on_null,
            } => {
                let Some(NativeHostMetadata::TimestampDiff {
                    stop_on_null: declared,
                    output_role,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("timestamp diff descriptor absent"));
                };
                if self.native_values.is_none()
                    || invocation.arg_types.len() != 3
                    || typed != output_role.is_none()
                    || stop_on_null != *declared
                {
                    return Err(binding("timestamp diff descriptor differs"));
                }
                SqlDemand::TimestampDiff {
                    next: 0,
                    tokens: [None; 3],
                }
            }
            SqlBatchHostOperation::LikeNative { ilike } => {
                let Some(NativeHostMetadata::Like {
                    invocation: value, ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("LIKE descriptor absent"));
                };
                if self.native_values.is_none()
                    || invocation.arg_types.len() != 3
                    || value.kind()
                        != if ilike {
                            tidb_query_expr::NativeLikeKind::Ilike
                        } else {
                            tidb_query_expr::NativeLikeKind::Like
                        }
                {
                    return Err(binding("LIKE descriptor differs"));
                }
                SqlDemand::Like {
                    pending: Some(crate::tikv::wildcard_value::Prepared::Text),
                }
            }
            SqlBatchHostOperation::CryptoNative(operation) => {
                let Some(NativeHostMetadata::Crypto {
                    operation: declared,
                    arguments,
                    probe,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("crypto descriptor absent"));
                };
                if self.native_values.is_none()
                    || operation != *declared
                    || arguments.len() != operation.arity()
                    || invocation.arg_types.len() != arguments.len()
                {
                    return Err(binding("crypto descriptor differs"));
                }
                if let Err(e) = crate::tikv::identity_actual::probe(probe.as_ref()) {
                    return self.native_failure(e);
                }
                SqlDemand::Crypto {
                    next: 0,
                    tokens: [None; 2],
                }
            }
            SqlBatchHostOperation::CompressionNative(operation) => {
                let Some(NativeHostMetadata::Compression {
                    operation: declared,
                    probe,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("compression descriptor absent"));
                };
                if self.native_values.is_none()
                    || operation != *declared
                    || invocation.arg_types.len() != 1
                {
                    return Err(binding("compression descriptor differs"));
                }
                if let Err(e) = crate::tikv::identity_actual::probe(probe.as_ref()) {
                    return self.native_failure(e);
                }
                SqlDemand::Compression { consumed: false }
            }
            SqlBatchHostOperation::ByteUnaryNative(operation) => {
                let Some(NativeHostMetadata::ByteUnary {
                    operation: declared,
                    probe,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("byte unary descriptor absent"));
                };
                if self.native_values.is_none()
                    || operation != *declared
                    || invocation.arg_types.len() != 1
                {
                    return Err(binding("byte unary descriptor differs"));
                }
                if let Err(e) = crate::tikv::identity_actual::probe(probe.as_ref()) {
                    return self.native_failure(e);
                }
                SqlDemand::ByteUnary { consumed: false }
            }
            SqlBatchHostOperation::MiscNative { operation, arity } => {
                let Some(NativeHostMetadata::Misc {
                    arguments,
                    operation: declared,
                    probe,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("misc descriptor absent"));
                };
                if self.native_values.is_none()
                    || operation != *declared
                    || usize::from(arity) != arguments.len()
                    || invocation.arg_types.len() != arguments.len()
                    || !operation.accepts_arity(arguments.len())
                {
                    return Err(binding("misc descriptor differs"));
                }
                if let Err(e) = crate::tikv::identity_actual::probe(probe.as_ref()) {
                    return self.native_failure(e);
                }
                SqlDemand::Misc {
                    next: 0,
                    tokens: [None; 2],
                }
            }
            SqlBatchHostOperation::ValueIdentityNative { name_const } => {
                let Some(NativeHostMetadata::Identity {
                    arguments,
                    name_const: declared,
                    probe,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("identity descriptor absent"));
                };
                let arity = 1 + usize::from(name_const);
                if self.native_values.is_none()
                    || *declared != name_const
                    || arguments.len() != arity
                    || invocation.arg_types.len() != arity
                {
                    return Err(binding("identity descriptor differs"));
                }
                // Original signature prefix, before ANY child demand. No store
                // borrow or expression evaluation is retained in this metadata.
                if let Err(error) = crate::tikv::identity_actual::probe(probe.as_ref()) {
                    return self.native_failure(error);
                }
                SqlDemand::Identity { next: 0 }
            }
            SqlBatchHostOperation::GroupingNative => {
                if self.native_values.is_none()
                    || invocation.arg_types.len() != 1
                    || !matches!(
                        self.metadata.get(invocation.slot.index()),
                        Some(NativeHostMetadata::Grouping { .. })
                    )
                {
                    return Err(binding("GROUPING descriptor differs"));
                }
                SqlDemand::Grouping { consumed: false }
            }
            SqlBatchHostOperation::VectorNative(op) => {
                let Some(NativeHostMetadata::Vector {
                    arguments,
                    operation,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("vector descriptor absent"));
                };
                if self.native_values.is_none()
                    || op != *operation
                    || arguments.len() != op.arity()
                    || invocation.arg_types.len() != op.arity()
                {
                    return Err(binding("vector descriptor differs"));
                }
                SqlDemand::Vector {
                    next: 0,
                    tokens: [None; 2],
                }
            }
            SqlBatchHostOperation::DurationConstructionNative(op) => {
                let Some(NativeHostMetadata::DurationConstruction {
                    arguments,
                    argument_types,
                    operation,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("duration descriptor absent"));
                };
                if self.native_values.is_none()
                    || op != *operation
                    || arguments.len() != op.arity()
                    || argument_types.len() != op.arity()
                    || invocation.arg_types.len() != op.arity()
                {
                    return Err(binding("duration descriptor differs"));
                }
                SqlDemand::DurationConstruction {
                    next: 0,
                    tokens: [None; 3],
                }
            }
            SqlBatchHostOperation::CalendarNative { operation, arity } => {
                let Some(NativeHostMetadata::Calendar {
                    arguments,
                    operation: declared,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("calendar descriptor absent"));
                };
                if self.native_values.is_none()
                    || operation != *declared
                    || !operation.accepts_arity(usize::from(arity))
                    || arguments.len() != usize::from(arity)
                    || invocation.arg_types.len() != arguments.len()
                {
                    return Err(binding("calendar descriptor differs"));
                }
                SqlDemand::Calendar { first: None }
            }
            SqlBatchHostOperation::ClockNative { operation, arity } => {
                let Some(NativeHostMetadata::Clock {
                    argument,
                    operation: declared,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("clock descriptor missing"));
                };
                if self.native_values.is_none()
                    || operation != *declared
                    || !operation.accepts_arity(usize::from(arity))
                    || invocation.arg_types.len() != usize::from(arity)
                    || argument.is_some() != (arity != 0)
                {
                    return Err(binding("clock descriptor differs"));
                }
                SqlDemand::Clock
            }
            SqlBatchHostOperation::TsoNative(op) => {
                let Some(NativeHostMetadata::Tso {
                    argument,
                    argument_type,
                    operation,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("TSO descriptor missing"));
                };
                if self.native_values.is_none()
                    || op != *operation
                    || argument.is_some() != (op.arity() != 0)
                    || argument_type.is_some() != argument.is_some()
                    || invocation.arg_types.len() != op.arity()
                {
                    return Err(binding("TSO descriptor differs"));
                }
                SqlDemand::Tso
            }
            SqlBatchHostOperation::EpochNative { from, arity } => {
                let Some(NativeHostMetadata::Epoch {
                    arguments,
                    from: declared,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("epoch descriptor missing"));
                };
                if self.native_values.is_none()
                    || *declared != from
                    || arguments.len() != usize::from(arity)
                    || invocation.arg_types.len() != arguments.len()
                {
                    return Err(binding("epoch descriptor differs"));
                }
                SqlDemand::Epoch { first: None }
            }
            SqlBatchHostOperation::ExtractNative { typed, composite } => {
                let Some(NativeHostMetadata::Extract {
                    arguments,
                    unit,
                    output_role,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("EXTRACT descriptor absent"));
                };
                if self.native_values.is_none()
                    || arguments.len() != if composite { 1 } else { 2 }
                    || invocation.arg_types.len() != arguments.len()
                    || unit.is_some() != composite
                    || typed == output_role.is_some()
                {
                    return Err(binding("EXTRACT descriptor differs"));
                }
                SqlDemand::Extract { first: None }
            }
            SqlBatchHostOperation::DateArithmeticNative { output } => {
                let Some(NativeHostMetadata::DateArithmetic {
                    output: declared,
                    output_role,
                    argument_types,
                    result_type,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("date arithmetic descriptor absent"));
                };
                if self.native_values.is_none()
                    || invocation.arg_types.len() != 2
                    || output != *declared
                    || (output != tidb_query_expr::local::SqlTemporalOutput::Value
                        && (result_type.is_none() || argument_types.iter().any(Option::is_none)))
                    || (output == tidb_query_expr::local::SqlTemporalOutput::Value)
                        != output_role.is_some()
                {
                    return Err(binding("date arithmetic descriptor differs"));
                }
                SqlDemand::DateArithmetic { first: None }
            }
            SqlBatchHostOperation::BoundedStalenessNative { typed } => {
                let Some(NativeHostMetadata::BoundedStaleness { output_role, .. }) =
                    self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("bounded descriptor absent"));
                };
                if self.native_values.is_none()
                    || invocation.arg_types.len() != 2
                    || typed != output_role.is_none()
                {
                    return Err(binding("bounded source descriptor differs"));
                }
                SqlDemand::BoundedStaleness { first: None }
            }
            SqlBatchHostOperation::StrToDateNative { output } => {
                let Some(NativeHostMetadata::StrToDate {
                    output: declared,
                    output_role,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("date descriptor absent"));
                };
                if self.native_values.is_none()
                    || invocation.arg_types.len() != 2
                    || output != *declared
                    || (output == tidb_query_expr::local::SqlTemporalOutput::Value)
                        != output_role.is_some()
                {
                    return Err(binding("date source descriptor differs"));
                }
                SqlDemand::StrToDate { first: None }
            }
            SqlBatchHostOperation::JsonSumCrc32Native { typed } => {
                let Some(NativeHostMetadata::JsonSumCrc32 { output_role, .. }) =
                    self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("JSON checksum descriptor absent"));
                };
                if self.native_values.is_none()
                    || invocation.arg_types.len() != 1
                    || typed != output_role.is_none()
                {
                    return Err(binding("JSON checksum source descriptor differs"));
                }
                SqlDemand::JsonSumCrc32
            }
            SqlBatchHostOperation::IntervalNative => self.start_interval(&invocation)?,
            SqlBatchHostOperation::ExtremumNative => self.start_extremum(&invocation)?,
            SqlBatchHostOperation::CharsetNative(_) => self.start_charset(&invocation)?,
            SqlBatchHostOperation::InNative => {
                self.validate_in_start(&invocation)?;
                SqlDemand::In {
                    next: 0,
                    tokens: Vec::new(),
                    membership: None,
                }
            }
            SqlBatchHostOperation::Concat | SqlBatchHostOperation::ConcatWs
                if matches!(
                    self.metadata.get(invocation.slot.index()),
                    Some(NativeHostMetadata::ConcatValues { .. })
                ) =>
            {
                let Some(NativeHostMetadata::ConcatValues {
                    arguments, kind, ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    unreachable!()
                };
                let expected = if self.catalog.sql_batch_operation(invocation.slot)?
                    == SqlBatchHostOperation::Concat
                {
                    ConcatKind::Concat
                } else {
                    ConcatKind::ConcatWs
                };
                if self.native_values.is_none()
                    || *kind != expected
                    || arguments.len() != invocation.arg_types.len()
                {
                    return Err(binding("CONCAT value descriptor differs"));
                }
                ready_preparation = self
                    .context
                    .ready_value_cache()
                    .map(|cache| cache.preparation_unwind_guard());
                SqlDemand::ConcatValues {
                    pending: Some(ConcatDemand::new(*kind, arguments.len())?),
                }
            }
            SqlBatchHostOperation::Concat => SqlDemand::Concat {
                kind: ConcatKind::Concat,
                demand: ConcatDemand::new(ConcatKind::Concat, invocation.arg_types.len())?,
            },
            SqlBatchHostOperation::ConcatWs => SqlDemand::Concat {
                kind: ConcatKind::ConcatWs,
                demand: ConcatDemand::new(ConcatKind::ConcatWs, invocation.arg_types.len())?,
            },
            SqlBatchHostOperation::AddWindowNative
            | SqlBatchHostOperation::SubtractWindowNative
            | SqlBatchHostOperation::MultiplyWindowNative
            | SqlBatchHostOperation::ModuloWindowNative
            | SqlBatchHostOperation::DivideWindowNative
            | SqlBatchHostOperation::IntegerDivideWindowNative
            | SqlBatchHostOperation::AbsIntWindowNative
            | SqlBatchHostOperation::CastDecimalWindowNative
            | SqlBatchHostOperation::CastJsonWindowNative
            | SqlBatchHostOperation::EagerValuesWindowNative => {
                return Err(binding(
                    "window operation requires its explicit window callbacks",
                ));
            }
            operation @ (SqlBatchHostOperation::AddNative
            | SqlBatchHostOperation::SubtractNative
            | SqlBatchHostOperation::MultiplyNative
            | SqlBatchHostOperation::ModuloNative
            | SqlBatchHostOperation::DivideNative
            | SqlBatchHostOperation::IntegerDivideNative) => {
                let Some(NativeHostMetadata::Arithmetic {
                    operation: descriptor_operation,
                    batch: None,
                    name,
                    mode,
                    generic_mode,
                    source_types,
                    intdiv,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("arithmetic descriptor missing"));
                };
                if self.native_values.is_none()
                    || invocation.arg_types.len() != 2
                    || operation != *descriptor_operation
                    || native_arithmetic_operation(name) != Some(operation)
                    || (name.as_ref() == "intdiv") != intdiv.is_some()
                    || ((*mode != crate::scalar_function::NativeArithmeticInputMode::Raw
                        || *generic_mode != crate::scalar_function::NativeArithmeticInputMode::Raw)
                        && source_types.iter().any(Option::is_none))
                {
                    return Err(binding("arithmetic catalog descriptor differs"));
                }
                SqlDemand::Arithmetic {
                    next: 0,
                    typed: *mode == crate::scalar_function::NativeArithmeticInputMode::TypedInteger,
                    left: None,
                }
            }
            operation @ (SqlBatchHostOperation::BitwiseNative(_)
            | SqlBatchHostOperation::XorNative
            | SqlBatchHostOperation::UnarySignNative(_)) => {
                let Some(NativeHostMetadata::RawValues {
                    operation: declared,
                    name,
                    right_node,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("raw value source descriptor missing"));
                };
                let named = native_unary_sign_operation(name)
                    .or_else(|| native_bitwise_operation(name))
                    .or_else(|| {
                        (name.as_ref() == "xor").then_some(SqlBatchHostOperation::XorNative)
                    });
                if self.native_values.is_none()
                    || declared.sql_operation() != operation
                    || named != Some(operation)
                    || invocation.arg_types.len() != declared.arity()
                    || right_node.is_some() != (declared.arity() == 2)
                {
                    return Err(binding("raw value descriptor differs"));
                }
                SqlDemand::RawValues { left_token: None }
            }
            SqlBatchHostOperation::AbsNative => {
                let Some(NativeHostMetadata::Math { name, .. }) =
                    self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("math source descriptor missing"));
                };
                if self.native_values.is_none()
                    || name.as_ref() != "abs"
                    || invocation.arg_types.len() != 1
                {
                    return Err(binding("math descriptor differs"));
                }
                SqlDemand::Math
            }
            operation @ (SqlBatchHostOperation::CompareNative(_)
            | SqlBatchHostOperation::NullSafeEqNative
            | SqlBatchHostOperation::NullIfNative) => {
                let Some(NativeHostMetadata::Comparison {
                    operation: descriptor_operation,
                    name,
                    operands,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("comparison source descriptor missing"));
                };
                if self.native_values.is_none()
                    || *descriptor_operation != operation
                    || native_comparison_operation(name) != Some(operation)
                    || operands.iter().any(|operand| {
                        operand.eval_type.is_none() && (operand.unsigned || operand.duration_column)
                    })
                    || invocation.arg_types.len() != 2
                {
                    return Err(binding("comparison descriptor differs"));
                }
                SqlDemand::Comparison {
                    left_token: None,
                    integer_probe: matches!(operation, SqlBatchHostOperation::CompareNative(_))
                        && operands
                            .iter()
                            .all(|operand| operand.eval_type == Some(NativeEvalType::Int)),
                }
            }
            operation @ (SqlBatchHostOperation::IsTrueWithNullNative
            | SqlBatchHostOperation::UnaryNotNative
            | SqlBatchHostOperation::IsNullNative
            | SqlBatchHostOperation::IsTrueNative
            | SqlBatchHostOperation::IsFalseNative
            | SqlBatchHostOperation::IsNotTrueNative
            | SqlBatchHostOperation::IsNotFalseNative
            | SqlBatchHostOperation::IsUnknownNative
            | SqlBatchHostOperation::IsNotUnknownNative) => {
                let Some(NativeHostMetadata::Selection {
                    name,
                    argument_nodes,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(LocalError::HostContract(
                        "truth wrapper has no descriptor".into(),
                    ));
                };
                if self.native_values.is_none()
                    || native_boolean_operation(name) != Some(operation)
                    || argument_nodes.len() != 1
                    || invocation.arg_types.len() != 1
                {
                    return Err(LocalError::HostContract(
                        "truth wrapper descriptor differs".into(),
                    ));
                }
                SqlDemand::Truth
            }
            operation @ (SqlBatchHostOperation::AndNative | SqlBatchHostOperation::OrNative) => {
                let Some(NativeHostMetadata::Selection {
                    name,
                    argument_nodes,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(LocalError::HostContract(
                        "logical operation has no descriptor".into(),
                    ));
                };
                let expected = match operation {
                    SqlBatchHostOperation::AndNative => matches!(name.as_ref(), "and" | "logicand"),
                    _ => matches!(name.as_ref(), "or" | "logicor"),
                };
                if self.native_values.is_none()
                    || !expected
                    || argument_nodes.len() != 2
                    || invocation.arg_types.len() != 2
                {
                    return Err(LocalError::HostContract(
                        "logical descriptor differs".into(),
                    ));
                }
                SqlDemand::Logical {
                    next: 0,
                    left: None,
                }
            }
            operation @ (SqlBatchHostOperation::IfNative | SqlBatchHostOperation::CaseNative) => {
                let Some(NativeHostMetadata::Selection {
                    name,
                    argument_nodes,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(LocalError::HostContract(
                        "conditional has no descriptor".into(),
                    ));
                };
                let expected = if operation == SqlBatchHostOperation::IfNative {
                    "if"
                } else {
                    "case"
                };
                if self.native_values.is_none()
                    || name.as_ref() != expected
                    || argument_nodes.len() != invocation.arg_types.len()
                {
                    return Err(LocalError::HostContract(
                        "conditional descriptor differs".into(),
                    ));
                }
                SqlDemand::Conditional { next: 0 }
            }
            operation @ (SqlBatchHostOperation::CoalesceNative
            | SqlBatchHostOperation::IfNullNative) => {
                let Some(NativeHostMetadata::Selection {
                    name,
                    argument_nodes,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(LocalError::HostContract(
                        "native nullable selection has no descriptor".into(),
                    ));
                };
                let expected = if operation == SqlBatchHostOperation::IfNullNative {
                    "ifnull"
                } else {
                    "coalesce"
                };
                if self.native_values.is_none()
                    || name.as_ref() != expected
                    || argument_nodes.len() != invocation.arg_types.len()
                {
                    return Err(LocalError::HostContract(
                        "native nullable selection descriptor differs".into(),
                    ));
                }
                SqlDemand::Nullable { next: 0 }
            }
            SqlBatchHostOperation::CastNative => {
                if self.native_values.is_none()
                    || !matches!(
                        self.metadata.get(invocation.slot.index()),
                        Some(NativeHostMetadata::ValueCast { .. })
                    )
                {
                    return Err(LocalError::HostContract(
                        "native CAST lacks value/metadata capability".into(),
                    ));
                }
                if let Some(NativeHostMetadata::ValueCast {
                    cast_type: tidb_ast::CastType::Json,
                    source_type,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                {
                    if let Err(error) =
                        crate::builtin_ext::json::validate_json_cast_source(Some(source_type))
                    {
                        return self.native_failure(error);
                    }
                }
                SqlDemand::ValueCast
            }
            SqlBatchHostOperation::CastCharNative { len } => {
                if self.native_values.is_none()
                    || !matches!(
                        self.metadata.get(invocation.slot.index()),
                        Some(NativeHostMetadata::CharNative { .. })
                    )
                {
                    return Err(LocalError::HostContract(
                        "native character cast lacks value/metadata capability".into(),
                    ));
                }
                SqlDemand::CastCharNative { len }
            }
            SqlBatchHostOperation::CastCharInt { len } => {
                if !matches!(
                    self.metadata.get(invocation.slot.index()),
                    Some(NativeHostMetadata::CharCast { .. })
                ) {
                    return Err(LocalError::HostContract(
                        "character cast has no target metadata".into(),
                    ));
                }
                SqlDemand::CastCharInt { len }
            }
            SqlBatchHostOperation::RegexpLike { case_insensitive } => {
                if self
                    .metadata
                    .get(invocation.slot.index())
                    .and_then(NativeHostMetadata::regexp)
                    .is_none()
                {
                    return Err(LocalError::HostContract(
                        "regexp slot has no native cache handles".into(),
                    ));
                }
                ready_preparation = self
                    .context
                    .ready_value_cache()
                    .map(|cache| cache.preparation_unwind_guard());
                SqlDemand::RegexpLike {
                    case_insensitive,
                    next: Some(0),
                }
            }
        };
        let immediate = matches!(&demand,SqlDemand::PbLazy(state) if state.phase.argument().is_none())
            || matches!(demand, SqlDemand::HeaderOutcome)
            || matches!(demand, SqlDemand::PbIntegerMod(_)) && invocation.arg_types.is_empty()
            || matches!(demand, SqlDemand::PbBoolean(_)) && invocation.arg_types.is_empty()
            || matches!(demand, SqlDemand::PbEagerValues { .. }) && invocation.arg_types.is_empty()
            || matches!(
                demand,
                SqlDemand::WeightString(weight_string_source::WeightTask::Numeric)
            )
            || matches!(&demand,SqlDemand::FinalState(task) if task.immediate)
            || matches!(demand, SqlDemand::JsonSchema(_))
                && matches!(self.metadata.get(invocation.slot.index()),Some(NativeHostMetadata::JsonSchema(meta)) if meta.arguments.len()!=2)
            || matches!(demand, SqlDemand::Stateful(_))
                && matches!(self.metadata.get(invocation.slot.index()),Some(NativeHostMetadata::Stateful(meta)) if meta.descriptor.argument_types.len()!=meta.descriptor.operation.arity() || invocation.arg_types.is_empty())
            || matches!(demand, SqlDemand::UserVariableRead { .. })
            || (matches!(
                demand,
                SqlDemand::EagerValues { .. }
                    | SqlDemand::ContextValue { .. }
                    | SqlDemand::TypedContext { .. }
                    | SqlDemand::Clock
                    | SqlDemand::Tso
                    | SqlDemand::Epoch { .. }
            ) && invocation.arg_types.is_empty());
        let start = self.install_task(
            invocation.slot,
            invocation.row,
            invocation.arg_types.len(),
            demand,
        )?;
        if let HostStart::Pending { task, .. } = &start {
            self.slots[task.slot].ready_preparation = ready_preparation;
        }
        if immediate {
            let HostStart::Pending { task, .. } = start else {
                unreachable!()
            };
            // No task handle has crossed the host boundary yet. A context
            // callback can unwind before start returns, so this scope owns
            // cancellation until a ready result is returned to the driver.
            struct ImmediateGuard<'call, 'context> {
                provider: &'call mut NativeBatchHosts<'context>,
                task: HostTaskId,
            }
            impl Drop for ImmediateGuard<'_, '_> {
                fn drop(&mut self) {
                    self.provider.cancel(&self.task);
                }
            }
            let guard = ImmediateGuard {
                provider: self,
                task,
            };
            let result = if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::HeaderOutcome
            ) {
                guard.provider.finish_header_outcome(&guard.task)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::PbLazy(_)
            ) {
                guard.provider.resume_pb_lazy(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::PbBoolean(_)
            ) {
                guard.provider.resume_pb_boolean(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::PbIntegerMod(_)
            ) {
                guard.provider.resume_pb_integer_mod(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::PbEagerValues { .. }
            ) {
                guard.provider.resume_pb_eager_values(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::WeightString(_)
            ) {
                guard.provider.resume_weight_string(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::JsonSchema(_)
            ) {
                guard.provider.resume_json_schema(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::FinalState(_)
            ) {
                guard.provider.resume_final_state(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::Stateful(_)
            ) {
                guard.provider.resume_stateful(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::TypedContext { .. }
            ) {
                guard.provider.finish_typed_context(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::ContextValue { .. }
            ) {
                guard.provider.finish_context_value(&guard.task, None)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::EagerValues { .. }
            ) {
                guard.provider.finish_eager_values(&guard.task)
            } else if matches!(
                guard.provider.task(&guard.task)?.demand,
                SqlDemand::UserVariableRead { .. }
            ) {
                guard.provider.finish_user_variable(&guard.task, None)
            } else if matches!(guard.provider.task(&guard.task)?.demand, SqlDemand::Clock) {
                guard.provider.resume_clock(&guard.task, None)
            } else if matches!(guard.provider.task(&guard.task)?.demand, SqlDemand::Tso) {
                guard.provider.resume_tso(&guard.task, None)
            } else {
                guard.provider.resume_epoch(&guard.task, None)
            };
            return match result {
                Ok(HostStep::Ready(value)) => Ok(HostStart::Ready(value)),
                Ok(HostStep::NeedArg(_) | HostStep::NeedArgRow { .. }) => {
                    Err(binding("niladic host unexpectedly demanded an argument"))
                }
                Err(error) => Err(error),
            };
        }
        Ok(start)
    }
    fn resume_sql(
        &mut self,
        _ctx: &mut EvalContext,
        id: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
        self.validate_row_reply(id, reply)?;
        macro_rules! native_token {
            ($token:ident, $message:literal) => {
                let ScalarValueRef::Bytes(Some($token)) = reply.values.get_scalar_ref(0) else {
                    return Err(binding($message));
                };
            };
        }
        if matches!(self.task(id)?.demand, SqlDemand::Ast(_)) {
            native_token!(token, "AST source cannot be wire NULL");
            return self.resume_ast(id, Some(token));
        }
        let task = self.task(id)?;
        if matches!(task.demand, SqlDemand::JsonSchema(_)) {
            native_token!(token, "schema source cannot be wire NULL");
            return self.resume_json_schema(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::FinalState(_)) {
            native_token!(token, "final state source cannot be wire NULL");
            return self.resume_final_state(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::Stateful(_)) {
            native_token!(token, "stateful source cannot be wire NULL");
            return self.resume_stateful(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::ConcatValues { .. }) {
            native_token!(token, "CONCAT value cannot be wire NULL");
            return self.resume_concat_value(id, token);
        }
        if matches!(task.demand, SqlDemand::CachedList(_)) {
            native_token!(token, "cached list source cannot be wire NULL");
            return self.resume_cached_list(id, token);
        }
        if matches!(task.demand, SqlDemand::TypedWindow(_)) {
            return Err(binding("typed forward requires its window reply"));
        }
        if matches!(task.demand, SqlDemand::NumericWindow(_)) {
            return Err(binding("numeric window requires a window reply"));
        }
        if matches!(task.demand, SqlDemand::Numeric(_)) {
            return self.resume_numeric_parent(id, reply);
        }
        if matches!(task.demand, SqlDemand::TimestampDiff { .. }) {
            native_token!(token, "timestamp diff source cannot be wire NULL");
            return self.resume_timestamp_diff(id, token);
        }
        if matches!(task.demand, SqlDemand::Like { .. }) {
            native_token!(token, "LIKE source cannot be wire NULL");
            return self.resume_wildcard(id, token);
        }
        if matches!(task.demand, SqlDemand::Crypto { .. }) {
            native_token!(token, "crypto source cannot be wire NULL");
            return self.resume_crypto(id, token);
        }
        if matches!(task.demand, SqlDemand::TypedContext { .. }) {
            native_token!(token, "typed context operand cannot be wire NULL");
            return self.finish_typed_context(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::ContextValue { .. }) {
            native_token!(token, "context operand cannot be wire NULL");
            return self.finish_context_value(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::UserVariableWrite { .. }) {
            native_token!(token, "user variable operand cannot be wire NULL");
            return self.finish_user_variable(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::PbEagerValues { .. }) {
            native_token!(token, "PB eager reply is not a capability");
            return self.resume_pb_eager_values(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::PbLazy(_)) {
            native_token!(token, "PB lazy reply is not a capability");
            return self.resume_pb_lazy(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::PbBoolean(_)) {
            native_token!(token, "PB Boolean reply is not a capability");
            return self.resume_pb_boolean(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::PbIntegerMod(_)) {
            native_token!(token, "PB MOD reply is not a capability");
            return self.resume_pb_integer_mod(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::RegexpValues(_)) {
            native_token!(token, "regexp value reply is not a capability");
            return self.resume_regexp_values(id, token);
        }
        if matches!(task.demand, SqlDemand::WeightString(_)) {
            native_token!(token, "weight reply is not a value capability");
            return self.resume_weight_string(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::PrefixValues { .. }) {
            native_token!(token, "prefix reply is not a value capability");
            return self.resume_prefix_value(id, token);
        }
        if matches!(task.demand, SqlDemand::EagerValues { .. }) {
            native_token!(token, "eager argument cannot be wire NULL");
            return self.resume_eager_values(id, token);
        }
        if matches!(task.demand, SqlDemand::Compression { .. }) {
            native_token!(token, "compression source cannot be wire NULL");
            return self.resume_compression(id, token);
        }
        if matches!(task.demand, SqlDemand::ByteUnary { .. }) {
            native_token!(token, "byte unary source cannot be wire NULL");
            return self.resume_byte_unary(id, token);
        }
        if matches!(task.demand, SqlDemand::Misc { .. }) {
            native_token!(token, "misc source cannot be wire NULL");
            return self.resume_misc(id, token);
        }
        if matches!(task.demand, SqlDemand::Identity { .. }) {
            native_token!(token, "identity source cannot be wire NULL");
            return self.resume_identity(id, token);
        }
        if matches!(task.demand, SqlDemand::Grouping { .. }) {
            native_token!(token, "GROUPING source cannot be wire NULL");
            return self.resume_grouping(id, token);
        }
        if matches!(task.demand, SqlDemand::Vector { .. }) {
            native_token!(token, "vector source cannot be wire NULL");
            return self.resume_vector(id, token);
        }
        if matches!(task.demand, SqlDemand::DurationConstruction { .. }) {
            native_token!(token, "duration source cannot be wire NULL");
            return self.resume_duration_construction(id, token);
        }
        if matches!(task.demand, SqlDemand::Calendar { .. }) {
            native_token!(token, "calendar source cannot be wire NULL");
            return self.resume_calendar(id, token);
        }
        if matches!(task.demand, SqlDemand::Clock) {
            native_token!(token, "clock source cannot be wire NULL");
            return self.resume_clock(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::Tso) {
            native_token!(token, "TSO source cannot be wire NULL");
            return self.resume_tso(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::Epoch { .. }) {
            native_token!(token, "epoch source cannot be wire NULL");
            return self.resume_epoch(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::Extract { .. }) {
            native_token!(token, "EXTRACT source cannot be wire NULL");
            return self.resume_extract(id, token);
        }
        if matches!(task.demand, SqlDemand::DateArithmetic { .. }) {
            native_token!(token, "date arithmetic source cannot be wire NULL");
            return self.resume_date_arithmetic(id, token);
        }
        if matches!(task.demand, SqlDemand::BoundedStaleness { .. }) {
            native_token!(token, "bounded source cannot be wire NULL");
            return self.resume_bounded_staleness(id, token);
        }
        if matches!(task.demand, SqlDemand::StrToDate { .. }) {
            native_token!(token, "date source cannot be wire NULL");
            return self.resume_str_to_date(id, token);
        }
        if matches!(task.demand, SqlDemand::JsonSumCrc32) {
            native_token!(token, "JSON checksum source cannot be wire NULL");
            return self.resume_json_sum_crc32(id, token);
        }
        if matches!(task.demand, SqlDemand::Charset { .. }) {
            native_token!(token, "charset source cannot be wire NULL");
            return self.resume_charset(id, token);
        }
        if matches!(task.demand, SqlDemand::Extremum { .. }) {
            native_token!(token, "extremum source cannot be wire NULL");
            return self.resume_extremum(id, token);
        }
        if matches!(task.demand, SqlDemand::Interval { .. }) {
            native_token!(token, "INTERVAL source cannot be wire NULL");
            return self.resume_interval(id, token);
        }
        if matches!(task.demand, SqlDemand::In { .. }) {
            native_token!(token, "native IN argument cannot be wire NULL");
            return self.resume_in(id, token);
        }
        if matches!(task.demand, SqlDemand::Arithmetic { .. }) {
            native_token!(token, "arithmetic argument cannot be wire NULL");
            return self.resume_arithmetic(id, token);
        }
        if matches!(task.demand, SqlDemand::RawValues { .. }) {
            native_token!(token, "raw value argument cannot be wire NULL");
            return self.resume_raw_values(id, token);
        }
        if matches!(task.demand, SqlDemand::Math) {
            native_token!(token, "math argument cannot be wire NULL");
            return self.finish_math(id, token);
        }
        if matches!(task.demand, SqlDemand::Comparison { .. }) {
            native_token!(token, "native comparison argument cannot be wire NULL");
            return self.resume_comparison(id, token);
        }
        if matches!(task.demand, SqlDemand::Truth) {
            native_token!(token, "native truth argument cannot be wire NULL");
            return self.finish_truth(id, token).map(HostStep::Ready);
        }
        if matches!(task.demand, SqlDemand::Logical { .. }) {
            native_token!(token, "native logical argument cannot be wire NULL");
            return self.resume_logical(id, token);
        }
        if matches!(task.demand, SqlDemand::Conditional { .. }) {
            native_token!(token, "native predicate/branch cannot be wire NULL");
            return self.resume_conditional(id, token);
        }
        if matches!(task.demand, SqlDemand::Nullable { .. }) {
            native_token!(token, "native nullable argument cannot be wire NULL");
            return self.resume_nullable(id, token);
        }
        if matches!(task.demand, SqlDemand::ValueCast) {
            native_token!(token, "native CAST argument handle cannot be wire NULL");
            return self.finish_value_cast(id, token).map(HostStep::Ready);
        }
        if let SqlDemand::CastCharNative { len } = &task.demand {
            native_token!(token, "native value handle cannot be wire NULL");
            return self
                .finish_native_cast(id, token, *len)
                .map(HostStep::Ready);
        }
        if let SqlDemand::CastCharInt { len } = &task.demand {
            return self
                .finish_int_cast(id, reply.values.get_scalar_ref(0), *len)
                .map(HostStep::Ready);
        }
        let ScalarValueRef::Bytes(bytes) = reply.values.get_scalar_ref(0) else {
            unreachable!("checked Bytes")
        };
        if matches!(&task.demand, SqlDemand::RegexpLike { .. })
            && bytes.is_some_and(|bytes| std::str::from_utf8(bytes).is_err())
        {
            return self
                .native_failure(EvalError::Unsupported("invalid UTF-8 REGEXP_LIKE argument"));
        }

        let old_capacity = task.values.capacity();
        let target = if task.values.len() == old_capacity {
            old_capacity.saturating_mul(2).max(4).min(task.arity)
        } else {
            old_capacity
        };

        let mut owned = None;
        if let Some(bytes) = bytes {
            let mut value = Vec::new();
            value
                .try_reserve_exact(bytes.len())
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            value.extend_from_slice(bytes);
            owned = Some(value);
        }
        let action = {
            let task = self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated live task");
            if target > old_capacity {
                task.values
                    .try_reserve_exact(target - task.values.len())
                    .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            }
            task.values.push(owned);
            task.observe(bytes.map(<[u8]>::len))?
        };

        if action == ConcatDemandAction::ReadPacketLimit {
            let limit = self.context.max_allowed_packet();
            let task = self.slots[id.slot]
                .task
                .as_mut()
                .expect("validated live task");
            let SqlDemand::Concat { kind, demand } = &mut task.demand else {
                unreachable!()
            };
            if demand.accept_packet_limit(limit)? {
                let name = match kind {
                    ConcatKind::Concat => "concat",
                    ConcatKind::ConcatWs => "concat_ws",
                };
                if let Err(error) = self.context.handle_allowed_packet_overflowed(name) {
                    // The caller owns this exact native error. No numeric/text
                    // classification or replay is needed at the frame boundary.
                    self.native_error = Some(error);
                    return Err(LocalError::HostContract(
                        "native SQL context operation failed".into(),
                    ));
                }
            }
        }
        match self.task(id)?.next_argument()? {
            Some(index) => Ok(HostStep::NeedArg(HostArgRequest {
                index,
                mode: ArgMode::Fresh,
            })),
            None => self.finish(id).map(HostStep::Ready),
        }
    }
    fn validate_row_reply(&self, id: &HostTaskId, reply: HostArgReply<'_>) -> LocalResult<()> {
        let task = self.task(id)?;
        if task.next_argument()? != Some(reply.index)
            || self
                .catalog
                .signature(task.slot)?
                .arg_types
                .get(reply.index)
                != Some(reply.field_type)
            || reply.values.len() != 1
            || reply.values.eval_type()
                != match &task.demand {
                    SqlDemand::CastCharInt { .. } => TikvEvalType::Int,
                    _ => TikvEvalType::Bytes,
                }
        {
            return Err(LocalError::HostContract(
                "SQL host reply differs from its demanded argument".into(),
            ));
        }
        Ok(())
    }
}

impl LocalHostServices for NativeBatchHosts<'_> {
    fn catalog_key(&self) -> &HostCatalogKey {
        self.catalog.key()
    }
    fn start(
        &mut self,
        ctx: &mut EvalContext,
        invocation: HostInvocation<'_>,
    ) -> LocalResult<HostStart> {
        self.diagnostic_start(ctx, invocation)
    }
    fn start_window(
        &mut self,
        _ctx: &mut EvalContext,
        invocation: HostWindowInvocation<'_>,
    ) -> LocalResult<HostStart> {
        if matches!(
            self.metadata.get(invocation.slot.index()),
            Some(NativeHostMetadata::TypedContext(_))
        ) {
            return self.start_typed_window(invocation);
        }
        if self.numeric_parent(invocation.slot).is_some() {
            return self.start_numeric_window(invocation);
        }
        if matches!(
            self.metadata.get(invocation.slot.index()),
            Some(NativeHostMetadata::UnaryWindow { .. })
        ) {
            self.start_unary_window(invocation)
        } else if matches!(
            self.metadata.get(invocation.slot.index()),
            Some(NativeHostMetadata::DecimalFast { .. })
        ) {
            self.start_decimal_fast_window(invocation)
        } else {
            self.start_arithmetic_window(invocation)
        }
    }
    fn resume_window(
        &mut self,
        _ctx: &mut EvalContext,
        id: &HostTaskId,
        reply: HostWindowArgReply<'_>,
    ) -> LocalResult<HostStep> {
        if matches!(self.task(id)?.demand, SqlDemand::TypedWindow(_)) {
            return self.resume_typed_window(id, reply);
        }
        if matches!(self.task(id)?.demand, SqlDemand::NumericWindow(_)) {
            return self.resume_numeric_window(id, reply);
        }
        if matches!(self.task(id)?.demand, SqlDemand::Diagnostic(_)) {
            self.resume_window_math_diagnostic(id, reply)
        } else if matches!(self.task(id)?.demand, SqlDemand::WindowUnary { .. }) {
            self.resume_unary_window(id, reply)
        } else if matches!(self.task(id)?.demand, SqlDemand::WindowDecimalFast { .. }) {
            self.resume_decimal_fast_window(id, reply)
        } else {
            self.resume_arithmetic_window(id, reply)
        }
    }
    fn resume(
        &mut self,
        ctx: &mut EvalContext,
        id: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
        self.diagnostic_resume(ctx, id, reply)
    }
    fn cancel(&mut self, id: &HostTaskId) {
        if let Some(slot) = self.slots.get_mut(id.slot) {
            if slot.generation == id.generation {
                slot.task = None;
                slot.ready_preparation = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn prefix_heap_denial_and_stale_cancel_keep_task_generations_separate() {
        let field: tipb::FieldType = tidb_query_datatype::FieldTypeTp::VarString.into();
        let args = vec![field.clone(); 2];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::Concat,
            HostSignature {
                arg_types: args.clone().into_boxed_slice(),
                return_type: field.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let mut provider = NativeBatchHosts::new(catalog, &[], &crate::NoColumns);
        let mut ctx = EvalContext::default();
        let invocation = || HostInvocation {
            slot,
            row: InputRow {
                occurrence: 0,
                input_row: 0,
            },
            arg_types: &args,
            return_type: &field,
        };
        let HostStart::Pending { task: first, .. } =
            provider.start(&mut ctx, invocation()).unwrap()
        else {
            panic!("expected argument request")
        };
        let value = VectorValue::from_scalar(&ScalarValue::Bytes(Some(vec![b'x'; 700])), 1);
        assert!(matches!(
            provider.resume(
                &mut ctx,
                &first,
                HostArgReply {
                    index: 0,
                    field_type: &field,
                    values: &value
                }
            ),
            Ok(HostStep::NeedArg(_))
        ));
        provider.cancel(&first);
        provider.cancel(&first);
        assert!(provider.task(&first).is_err());
        let HostStart::Pending { task: second, .. } =
            provider.start(&mut ctx, invocation()).unwrap()
        else {
            panic!("expected new argument request")
        };
        assert_eq!(first.slot, second.slot);
        assert_ne!(first.generation, second.generation);
        provider.cancel(&first);
        assert!(provider.task(&second).is_ok());
        provider.cancel(&second);
        assert!(provider.retained_bytes().is_ok());
        assert!(provider.take_native_error().is_none());
    }

    #[test]
    fn integer_char_cast_resource_denial_precedes_native_warning() {
        struct Warnings(std::cell::Cell<usize>);
        impl Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn append_warning(&self, _: u16, _: &str) {
                self.0.set(self.0.get() + 1);
            }
        }
        let argument: tipb::FieldType = tidb_query_datatype::FieldTypeTp::LongLong.into();
        let output: tipb::FieldType = tidb_query_datatype::FieldTypeTp::VarString.into();
        let args = [argument.clone()];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::CastCharInt { len: Some(1) },
            HostSignature {
                arg_types: args.clone().into(),
                return_type: output.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::CharCast {
            charset: "utf8mb4".into(),
            unsigned: false,
        }];
        let context = Warnings(std::cell::Cell::new(0));
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &context);
        let mut ctx = EvalContext::default();
        let HostStart::Pending { task, .. } = provider
            .start(
                &mut ctx,
                HostInvocation {
                    slot,
                    row: InputRow {
                        occurrence: 0,
                        input_row: 0,
                    },
                    arg_types: &args,
                    return_type: &output,
                },
            )
            .unwrap()
        else {
            panic!("expected argument request")
        };
        let value = VectorValue::from_scalar(&ScalarValue::Int(Some(12345)), 1);
        let HostStep::Ready(result) = provider
            .resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &argument,
                    values: &value,
                },
            )
            .unwrap()
        else {
            panic!("expected converted bytes");
        };
        let expected_context = Warnings(std::cell::Cell::new(0));
        let expected = crate::tikv::eval_cast_char_in(
            &expected_context,
            &Datum::Int(12345),
            Some(&FieldType::new(FieldTypeCode::LongLong)),
            Some(1),
            Some("utf8mb4"),
        )
        .unwrap();
        assert!(
            matches!(result.get_scalar_ref(0), ScalarValueRef::Bytes(Some(bytes)) if bytes == expected.go_bytes())
        );
        assert_eq!(context.0.get(), expected_context.0.get());
        assert!(context.0.get() > 0);
        assert!(provider.take_native_error().is_none());
        provider.cancel(&task);
        assert!(provider.task(&task).is_err());
    }

    #[test]
    fn native_value_cast_warning_budget_and_forged_provenance_are_terminal() {
        struct Warnings(std::cell::Cell<usize>);
        impl Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn append_warning(&self, _: u16, _: &str) {
                self.0.set(self.0.get() + 1);
            }
        }
        let argument: tipb::FieldType = tidb_query_datatype::FieldTypeTp::Blob.into();
        let output: tipb::FieldType = tidb_query_datatype::FieldTypeTp::VarString.into();
        let args = [argument.clone()];
        let catalog = HostCatalog::for_sql_batch(vec![(
            SqlBatchHostOperation::CastCharNative { len: Some(1) },
            HostSignature {
                arg_types: args.clone().into(),
                return_type: output.clone(),
            },
        )])
        .unwrap();
        let slot = catalog.slot(0).unwrap();
        let metadata = [NativeHostMetadata::CharNative {
            charset: "gbk".into(),
            source_type: string_ty_for_test(),
            argument_node: 3,
        }];
        let values = RefCell::new(NativeValueStore::new().unwrap());
        values.borrow_mut().begin_window().unwrap();
        let row = InputRow {
            occurrence: 0,
            input_row: 0,
        };
        let token = values
            .borrow_mut()
            .insert_clone(
                &Datum::Bytes(vec![0xff; 100]),
                NativeValueRole::CharInput,
                NativeValueOrigin { node: 3, row },
            )
            .unwrap();
        let wrong = values
            .borrow_mut()
            .insert_clone(
                &Datum::Int(1),
                NativeValueRole::CharInput,
                NativeValueOrigin {
                    node: 3,
                    row: InputRow {
                        occurrence: 1,
                        input_row: 0,
                    },
                },
            )
            .unwrap();
        let context = Warnings(std::cell::Cell::new(0));
        let mut provider =
            NativeBatchHosts::new(catalog, &metadata, &context).with_native_values(Some(&values));
        let mut ctx = EvalContext::default();
        let invocation = || HostInvocation {
            slot,
            row,
            arg_types: &args,
            return_type: &output,
        };
        let HostStart::Pending { task, .. } = provider.start(&mut ctx, invocation()).unwrap()
        else {
            panic!("request")
        };
        let input = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token.to_vec())), 1);
        let HostStep::Ready(result) = provider
            .resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &argument,
                    values: &input,
                },
            )
            .unwrap()
        else {
            panic!("expected converted bytes");
        };
        let expected_context = Warnings(std::cell::Cell::new(0));
        let expected = crate::tikv::eval_cast_char_in(
            &expected_context,
            &Datum::Bytes(vec![0xff; 100]),
            Some(&string_ty_for_test()),
            Some(1),
            Some("gbk"),
        )
        .unwrap();
        assert!(
            matches!(result.get_scalar_ref(0), ScalarValueRef::Bytes(Some(bytes)) if bytes == expected.go_bytes())
        );
        let warning_count = context.0.get();
        assert_eq!(warning_count, expected_context.0.get());
        assert!(warning_count > 0);
        assert!(provider.take_native_error().is_none());
        provider.cancel(&task);
        let HostStart::Pending { task, .. } = provider.start(&mut ctx, invocation()).unwrap()
        else {
            panic!("request")
        };
        let input = VectorValue::from_scalar(&ScalarValue::Bytes(Some(wrong.to_vec())), 1);
        assert!(matches!(
            provider.resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &argument,
                    values: &input
                }
            ),
            Err(LocalError::BindingContract(_))
        ));
        assert_eq!(context.0.get(), warning_count);
        assert!(provider.take_native_error().is_none());
        provider.cancel(&task);
    }
    fn string_ty_for_test() -> FieldType {
        FieldType::new(FieldTypeCode::VarString).with_collation(tidb_datatype::Collation::Binary)
    }
    #[test]
    fn native_cast_hosts_reject_wrong_provenance_and_input_policy_before_context_work() {
        for generic in [false, true] {
            let operation = if generic {
                SqlBatchHostOperation::CastNative
            } else {
                SqlBatchHostOperation::CastCharNative { len: None }
            };
            let argument = projected(&native_bytes_type(), true).unwrap();
            let args = vec![argument.clone()];
            let output = argument.clone();
            let catalog = HostCatalog::for_sql_batch(vec![(
                operation,
                HostSignature {
                    arg_types: args.clone().into_boxed_slice(),
                    return_type: output.clone(),
                },
            )])
            .unwrap();
            let slot = catalog.slot(0).unwrap();
            let metadata = [if generic {
                NativeHostMetadata::ValueCast {
                    cast_type: tidb_ast::CastType::Signed,
                    name: "cast_signed".into(),
                    source_type: native_bytes_type(),
                    input: NativeValuePreparation::Computed,
                    result_type: FieldType::new(FieldTypeCode::LongLong),
                    argument_node: 3,
                    output_node: 2,
                    output_role: NativeValueRole::Output,
                }
            } else {
                NativeHostMetadata::CharNative {
                    charset: "BINARY".into(),
                    source_type: native_bytes_type(),
                    argument_node: 3,
                }
            }];
            let store = RefCell::new(NativeValueStore::new().unwrap());
            store.borrow_mut().begin_window().unwrap();
            let row = InputRow {
                occurrence: 0,
                input_row: 0,
            };
            let origin = NativeValueOrigin { node: 3, row };
            let large = store
                .borrow_mut()
                .insert_clone(
                    &Datum::Bytes([vec![b'0'; 16383], vec![b'1']].concat()),
                    NativeValueRole::CharInput,
                    origin,
                )
                .unwrap();
            let wrong = store
                .borrow_mut()
                .insert_clone(&Datum::Int(1), NativeValueRole::Output, origin)
                .unwrap();
            let context = crate::NoColumns;
            let mut provider = NativeBatchHosts::new(catalog, &metadata, &context)
                .with_native_values(Some(&store));
            let mut ctx = EvalContext::default();
            for (token, resource_failure) in [(large, true), (wrong, false)] {
                let HostStart::Pending { task, .. } = provider
                    .start(
                        &mut ctx,
                        HostInvocation {
                            slot,
                            row,
                            arg_types: &args,
                            return_type: &output,
                        },
                    )
                    .unwrap()
                else {
                    panic!("request")
                };
                let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token.to_vec())), 1);
                let result = provider.resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &argument,
                        values: &values,
                    },
                );
                if resource_failure {
                    let HostStep::Ready(result) = result.unwrap() else {
                        panic!("expected admitted value");
                    };
                    let ScalarValueRef::Bytes(Some(bytes)) = result.get_scalar_ref(0) else {
                        panic!("expected value carrier");
                    };
                    if generic {
                        assert_eq!(
                            store
                                .borrow()
                                .get(
                                    bytes,
                                    NativeValueRole::Output,
                                    NativeValueOrigin { node: 2, row }
                                )
                                .unwrap(),
                            &Datum::Int(1)
                        );
                    } else {
                        assert_eq!(bytes, [vec![b'0'; 16383], vec![b'1']].concat().as_slice());
                    }
                } else {
                    assert!(matches!(result, Err(LocalError::BindingContract(_))));
                }
                assert!(provider.take_native_error().is_none());
                provider.cancel(&task);
            }
        }
    }
    #[test]
    fn native_nullable_selected_input_keeps_resource_and_provenance_failures_terminal() {
        for result_type in [Some(native_bytes_type()), None] {
            let argument = projected(&native_bytes_type(), true).unwrap();
            let args = vec![argument.clone(), argument.clone()];
            let output = argument.clone();
            let catalog = HostCatalog::for_sql_batch(vec![(
                SqlBatchHostOperation::IfNullNative,
                HostSignature {
                    arg_types: args.clone().into_boxed_slice(),
                    return_type: output.clone(),
                },
            )])
            .unwrap();
            let slot = catalog.slot(0).unwrap();
            let metadata = [NativeHostMetadata::Selection {
                name: "ifnull".into(),
                result_type: result_type.clone(),
                argument_nodes: vec![3, 8].into_boxed_slice(),
                output_node: 2,
                output_role: NativeValueRole::Output,
            }];
            let store = RefCell::new(NativeValueStore::new().unwrap());
            store.borrow_mut().begin_window().unwrap();
            let row = InputRow {
                occurrence: 0,
                input_row: 0,
            };
            let origin = NativeValueOrigin { node: 3, row };
            let large = store
                .borrow_mut()
                .insert_clone(
                    &Datum::Bytes(vec![b'x'; 16384]),
                    NativeValueRole::CharInput,
                    origin,
                )
                .unwrap();
            let wrong = store
                .borrow_mut()
                .insert_clone(&Datum::Null, NativeValueRole::Output, origin)
                .unwrap();
            let mut provider = NativeBatchHosts::new(catalog, &metadata, &crate::NoColumns)
                .with_native_values(Some(&store));
            let mut ctx = EvalContext::default();
            for (token, resource_failure) in [(large, true), (wrong, false)] {
                let HostStart::Pending { task, .. } = provider
                    .start(
                        &mut ctx,
                        HostInvocation {
                            slot,
                            row,
                            arg_types: &args,
                            return_type: &output,
                        },
                    )
                    .unwrap()
                else {
                    panic!("request")
                };
                let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token.to_vec())), 1);
                let result = provider.resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &argument,
                        values: &values,
                    },
                );
                if resource_failure {
                    let HostStep::Ready(result) = result.unwrap() else {
                        panic!("expected selected value");
                    };
                    let ScalarValueRef::Bytes(Some(token)) = result.get_scalar_ref(0) else {
                        panic!("expected native value handle");
                    };
                    let expected = match &result_type {
                        Some(field) => crate::scalar_function::coerce_function_result(
                            "ifnull",
                            Some(field),
                            Datum::Bytes(vec![b'x'; 16384]),
                        )
                        .unwrap(),
                        None => Datum::Bytes(vec![b'x'; 16384]),
                    };
                    assert_eq!(
                        store
                            .borrow()
                            .get(
                                token,
                                NativeValueRole::Output,
                                NativeValueOrigin { node: 2, row }
                            )
                            .unwrap(),
                        &expected
                    );
                } else {
                    assert!(matches!(result, Err(LocalError::BindingContract(_))));
                }
                assert!(provider.take_native_error().is_none());
                provider.cancel(&task);
            }
        }
    }
}

#[path = "batch_context_value.rs"]
mod context_value_source;

#[path = "batch_diagnostic.rs"]
mod diagnostic_source;

pub(super) use diagnostic_source::CompiledDiagnosticParent;

#[path = "batch_concat_value.rs"]
mod concat_value_source;

#[path = "batch_ast_host.rs"]
mod ast_host;
