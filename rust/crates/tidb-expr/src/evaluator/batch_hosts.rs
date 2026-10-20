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
#[path = "batch_quotient.rs"]
mod quotient;
#[path = "batch_raw_values.rs"]
mod raw_values;
#[path = "batch_unary_window.rs"]
mod unary_window;
#[path = "batch_window_values.rs"]
mod window_values;

#[derive(Clone, Copy, Debug)]
pub(super) enum WindowArithmeticSource {
    Literal,
    Column,
    Computed,
    IntegerReady,
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
struct WindowSourceValues {
    values: Vec<Datum>,
    retained_bytes: usize,
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

pub(super) enum NativeHostMetadata {
    EagerValues {
        arguments: Box<[usize]>,
        kernel: crate::scalar_function::eager_values::EagerValueKernel,
        probe: Option<Datum>,
        result_type: FieldType,
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
    Like {
        arguments: [usize; 3],
        escape_type: FieldType,
        invocation: tidb_query_expr::NativeLikeInvocation,
        result_type: FieldType,
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
        result_type: FieldType,
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
        argument_types: [FieldType; 2],
        unit: Box<str>,
        sign: i64,
        result_fsp: Option<u32>,
        result_type: FieldType,
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
        name: Box<str>,
        result_type: FieldType,
        left_node: usize,
        right_node: Option<usize>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    UnaryWindow {
        operation: SqlBatchHostOperation,
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
    },
    Plain,
    Regexp(tidb_query_expr::NativeRegexpInvocation),
    CharCast {
        charset: Box<str>,
        unsigned: bool,
    },
    Selection {
        name: Box<str>,
        result_type: FieldType,
        argument_nodes: Box<[usize]>,
        output_node: usize,
        output_role: NativeValueRole,
    },
    Comparison {
        operation: SqlBatchHostOperation,
        name: Box<str>,
        result_type: FieldType,
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
        result_type: FieldType,
        argument_nodes: [usize; 2],
        source_types: [FieldType; 2],
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
        input_is_literal: bool,
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
impl NativeHostMetadata {
    fn result_type(&self) -> Option<&FieldType> {
        match self {
            Self::In(metadata) => Some(&metadata.result_type),
            Self::Interval(metadata) => Some(&metadata.result_type),
            Self::Extremum(metadata) => Some(&metadata.result_type),
            Self::Charset(metadata) => Some(&metadata.result_type),
            Self::TimestampDiff { result_type, .. }
            | Self::Clock { result_type, .. }
            | Self::Calendar { result_type, .. }
            | Self::DurationConstruction { result_type, .. }
            | Self::Vector { result_type, .. }
            | Self::Crypto { result_type, .. }
            | Self::EagerValues { result_type, .. }
            | Self::Compression { result_type, .. }
            | Self::ByteUnary { result_type, .. }
            | Self::Misc { result_type, .. }
            | Self::Identity { result_type, .. }
            | Self::Grouping { result_type, .. }
            | Self::Like { result_type, .. }
            | Self::Tso { result_type, .. }
            | Self::Epoch { result_type, .. }
            | Self::Extract { result_type, .. }
            | Self::DateArithmetic { result_type, .. }
            | Self::BoundedStaleness { result_type, .. }
            | Self::StrToDate { result_type, .. }
            | Self::JsonSumCrc32 { result_type, .. }
            | Self::RawValues { result_type, .. }
            | Self::UnaryWindow { result_type, .. }
            | Self::Selection { result_type, .. }
            | Self::Comparison { result_type, .. }
            | Self::Arithmetic { result_type, .. }
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
        left_bytes: usize,
    },
    Arithmetic {
        next: usize,
        typed: bool,
        left: Option<Datum>,
    },
    Math,
    Comparison {
        left_token: Option<[u8; native_values::HANDLE_BYTES]>,
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
            SqlDemand::RawValues { left_token } | SqlDemand::Comparison { left_token } => {
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
    fn retained_bytes(&self) -> LocalResult<usize> {
        let bytes = self.values.iter().flatten().try_fold(
            array_bytes::<Option<Vec<u8>>>(self.values.capacity())?,
            |bytes, value| {
                bytes
                    .checked_add(value.capacity())
                    .ok_or_else(|| resource("SQL host prefix retained size overflow"))
            },
        )?;
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
        if let SqlDemand::EagerValues { tokens, .. }
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
        if let SqlDemand::WindowArithmetic {
            rows, left_bytes, ..
        } = &self.demand
        {
            return bytes
                .checked_add(array_bytes::<usize>(rows.capacity())?)
                .and_then(|bytes| bytes.checked_add(*left_bytes))
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
    base_bytes: usize,
    max_bytes: usize,
    failure: Option<LocalError>,
}
impl CapturedWarnings {
    fn retained_bytes(&self) -> LocalResult<usize> {
        self.rows.iter().try_fold(
            array_bytes::<(u16, String)>(self.rows.capacity())?,
            |bytes, (_, message)| {
                bytes
                    .checked_add(message.capacity())
                    .ok_or_else(|| resource("native warning retained size overflow"))
            },
        )
    }
    fn check(&self, extra: usize) -> LocalResult<()> {
        let bytes = self
            .base_bytes
            .checked_add(self.retained_bytes()?)
            .and_then(|bytes| bytes.checked_add(extra))
            .ok_or_else(|| resource("native warning capture size overflow"))?;
        if bytes > self.max_bytes {
            Err(resource("native warning capture policy exceeded"))
        } else {
            Ok(())
        }
    }
    fn capture(&mut self, code: u16, message: &str) {
        if self.failure.is_some() {
            return;
        }
        if let Err(error) = self.try_capture(code, message) {
            self.failure = Some(error);
        }
    }
    fn try_capture(&mut self, code: u16, message: &str) -> LocalResult<()> {
        let growth = if self.rows.len() == self.rows.capacity() {
            size_of::<(u16, String)>()
        } else {
            0
        };
        self.check(
            growth
                .checked_add(message.len())
                .ok_or_else(|| resource("native warning growth overflow"))?,
        )?;
        self.rows
            .try_reserve_exact(1)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        self.check(message.len())?;
        let mut owned = String::new();
        owned
            .try_reserve_exact(message.len())
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        owned.push_str(message);
        self.check(owned.capacity())?;
        self.rows.push((code, owned));
        Ok(())
    }
}

struct TaskSlot {
    generation: u64,
    task: Option<NativeTask>,
}

pub(super) struct NativeBatchHosts<'a> {
    catalog: HostCatalog,
    context: &'a dyn Columns,
    metadata: &'a [NativeHostMetadata],
    constant_probes: &'a [Option<Datum>],
    native_values: Option<&'a RefCell<NativeValueStore>>,
    slots: Vec<TaskSlot>,
    generation: u64,
    max_retained_bytes: usize,
    native_error: Option<EvalError>,
}
impl<'a> NativeBatchHosts<'a> {
    pub(super) fn new(
        catalog: HostCatalog,
        metadata: &'a [NativeHostMetadata],
        context: &'a dyn Columns,
        max_retained_bytes: usize,
    ) -> Self {
        Self {
            catalog,
            metadata,
            constant_probes: &[],
            native_values: None,
            context,
            slots: Vec::new(),
            generation: 0,
            max_retained_bytes,
            native_error: None,
        }
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
        self.native_error.take()
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
                self.check_bytes(
                    self.retained_bytes()?
                        .checked_add(std::mem::size_of::<TaskSlot>())
                        .ok_or_else(|| resource("SQL host slot size overflow"))?,
                )?;
                self.slots
                    .try_reserve_exact(1)
                    .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
                self.slots.push(TaskSlot {
                    generation: 0,
                    task: None,
                });
                self.check_bytes(self.retained_bytes()?)?;
                self.slots.len() - 1
            }
        };
        let first_argument = if matches!(demand, SqlDemand::UserVariableWrite { .. }) {
            1
        } else {
            0
        };
        let task = NativeTask {
            slot: host_slot,
            row,
            arity,
            demand,
            values: Vec::new(),
        };
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(task.retained_bytes()?)
                .ok_or_else(|| resource("SQL host task size overflow"))?,
        )?;
        self.slots[slot] = TaskSlot {
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
    fn retained_bytes(&self) -> LocalResult<usize> {
        self.slots
            .iter()
            .filter_map(|slot| slot.task.as_ref())
            .try_fold(
                array_bytes::<TaskSlot>(self.slots.capacity())?,
                |bytes, task| {
                    bytes
                        .checked_add(task.retained_bytes()?)
                        .ok_or_else(|| resource("SQL host retained size overflow"))
                },
            )
    }
    fn check_bytes(&self, bytes: usize) -> LocalResult<()> {
        if bytes > self.max_retained_bytes {
            Err(resource("SQL host retained byte cap exceeded"))
        } else {
            Ok(())
        }
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
            return self.finish_regexp(&task, *case_insensitive);
        }
        let SqlDemand::Concat { kind, demand } = &task.demand else {
            unreachable!()
        };
        let terminal = demand.finish_terminal()?;
        let other = self.retained_bytes()?;
        // Bound the overlap while the encoder consumes the owned prefix.
        let task_bytes = task.retained_bytes()?;
        let encode_cap = self
            .max_retained_bytes
            .checked_sub(other)
            .and_then(|bytes| bytes.checked_sub(task_bytes))
            .ok_or_else(|| resource("SQL host encoding overlap exceeds retained cap"))?;
        let prepared = tidb_query_expr::local::prepare_concat_args(
            *kind,
            task.arity,
            task.values,
            terminal,
            encode_cap,
        )?;
        let output_cap = self
            .max_retained_bytes
            .checked_sub(other)
            .and_then(|bytes| bytes.checked_sub(prepared.retained_encoded_bytes()))
            .ok_or_else(|| resource("SQL host encoded transport exceeds retained cap"))?;
        let value = prepared.execute(output_cap)?;
        let len = value.as_ref().map_or(0, Vec::len);
        let external = value.as_ref().map_or(0, Vec::capacity);
        self.check_bytes(
            other
                .checked_add(external)
                .and_then(|bytes| bytes.checked_add(len))
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<usize>().saturating_mul(4)))
                .ok_or_else(|| resource("SQL host result overlap size overflow"))?,
        )?;
        let mut output = ChunkedVecBytes::try_with_capacities(1, len)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(value.as_deref());
        self.check_bytes(
            other
                .checked_add(external)
                .and_then(|bytes| bytes.checked_add(output.retained_heap_bytes()?))
                .ok_or_else(|| resource("SQL host result retained size overflow"))?,
        )?;
        Ok(VectorValue::Bytes(output))
    }
    fn finish_value_cast(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<VectorValue> {
        let task = self.task(id)?;
        let Some(NativeHostMetadata::ValueCast {
            cast_type,
            name,
            source_type,
            input_is_literal,
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
        let payload = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?;
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("native CAST input allowance overflow"))?,
            )
            .ok_or_else(|| resource("native CAST input size overflow"))?;
        self.check_bytes(base)?;
        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        // The actual statement context owns diagnostics, just as for the
        // native kernel. A later resource failure is terminal, never replayed.
        // No warning buffer or expression callback is retained by this host.
        let result = if matches!(cast_type, tidb_ast::CastType::Json) {
            let target = source_type.eval_type();
            let value = if *input_is_literal {
                crate::constant::typed_constant_value(source_type, self.context, target, value)
            } else {
                crate::scalar_function::normalize_numeric_row_value(
                    value,
                    Some(source_type),
                    target,
                    self.context,
                )
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
        self.finish_native_value(id, result, *output_node, *output_role, row, base)
    }
    fn finish_native_value(
        &mut self,
        id: &HostTaskId,
        result: Datum,
        output_node: usize,
        output_role: NativeValueRole,
        row: InputRow,
        base: usize,
    ) -> LocalResult<VectorValue> {
        // ScalarFunction.Eval's return normalization has already run. Its
        // enclosing Expression.Eval applies this immutable result view next.
        // Host slots provide O(1) declaration access without a tree callback,
        // a per-row metadata scan or an extra source-schema allocation.
        let field = self
            .metadata
            .get(self.task(id)?.slot.index())
            .and_then(NativeHostMetadata::result_type)
            .ok_or_else(|| binding("native SQL result lacks its declaration"))?;
        let result = crate::expression::apply_enum_set_integer_view(result, Some(field));
        let store = self
            .native_values
            .ok_or_else(|| binding("native result has no value store"))?;
        let live = base
            .checked_add(
                native_payload_bytes(&result)?
                    .checked_mul(2)
                    .ok_or_else(|| resource("native CAST result allowance overflow"))?,
            )
            .ok_or_else(|| resource("native CAST live size overflow"))?;
        self.check_bytes(
            live.checked_add(128)
                .ok_or_else(|| resource("native CAST handle overlap overflow"))?,
        )?;
        let token = store.borrow_mut().insert_clone(
            &result,
            output_role,
            NativeValueOrigin {
                node: output_node,
                row,
            },
        )?;
        let mut output = ChunkedVecBytes::try_with_capacities(1, token.len())
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(Some(&token));
        self.check_bytes(
            live.checked_add(token.capacity())
                .and_then(|bytes| {
                    output
                        .retained_heap_bytes()
                        .and_then(|more| bytes.checked_add(more))
                })
                .ok_or_else(|| resource("native CAST result vector size overflow"))?,
        )?;
        self.slots[id.slot].task = None;
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
        let payload = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?;
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("selected native payload allowance overflow"))?,
            )
            .ok_or_else(|| resource("selected native input size overflow"))?;
        self.check_bytes(base)?;
        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        let value =
            match crate::scalar_function::coerce_function_result(name, Some(result_type), value) {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };
        self.finish_native_value(id, value, *output_node, *output_role, row, base)
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
            overflow_expressions,
            intdiv,
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
        let payload = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?;
        let diagnostic_copies = subjects
            .iter()
            .chain(overflow_expressions.iter())
            .chain(
                intdiv
                    .as_deref()
                    .map_or(&[][..], |facts| facts.arguments.as_slice())
                    .iter(),
            )
            .flatten()
            .try_fold(0usize, |total, text| {
                total
                    .checked_add(text.len())
                    .ok_or_else(|| resource("arithmetic diagnostics overflow"))
            })?
            .checked_mul(2)
            .ok_or_else(|| resource("arithmetic diagnostics copy overflow"))?;
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("arithmetic argument copy overflow"))?,
            )
            .and_then(|bytes| bytes.checked_add(diagnostic_copies))
            .ok_or_else(|| resource("arithmetic argument allowance overflow"))?;
        self.check_bytes(base)?;
        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        let mode = if typed {
            Mode::TypedInteger
        } else {
            *generic_mode
        };
        let value = match crate::scalar_function::native_numeric_operand(
            &source_types[index],
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
                        base,
                    );
                }
                crate::ops::arithmetic::PreparedArithmetic::Null
            } else {
                self.check_bytes(
                    base.checked_add(
                        native_payload_bytes(&value)?
                            .checked_mul(2)
                            .ok_or_else(|| resource("arithmetic converted value overflow"))?,
                    )
                    .ok_or_else(|| resource("arithmetic left allowance overflow"))?,
                )?;
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
            let unsigned = source_types.each_ref().map(FieldType::is_unsigned);
            if quotient::is_quotient(op) {
                let available = self
                    .max_retained_bytes
                    .checked_sub(base)
                    .ok_or_else(|| resource("quotient allowance exhausted"))?;
                let result =
                    self.compute_quotient(&self.metadata[slot.index()], left, value, available)?;
                return self.finish_arithmetic_row_value(id, result, base);
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
                    unsigned.map(|value| !value),
                );
                let _ = self.context.div_precision_increment();
                match crate::ops::arithmetic::prepare(op, left, right, unsigned, self.context) {
                    Ok(prepared) => prepared,
                    Err(error) => return self.native_failure(error),
                }
            }
        };
        let available = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("arithmetic kernel allowance exhausted"))?;
        self.finish_arithmetic_row_value(id, prepared.compute(available), base)
    }

    fn finish_math(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<VectorValue> {
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
        let origin = NativeValueOrigin {
            node: *argument_node,
            row,
        };
        let store = self
            .native_values
            .ok_or_else(|| binding("math native value store missing"))?;
        let payload = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?;
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("math copy allowance overflow"))?,
            )
            .ok_or_else(|| resource("math value size overflow"))?;
        self.check_bytes(base)?;
        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        let available = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("math value allowance exhausted"))?;
        use tidb_query_expr::impl_math::{
            native_abs_decimal_value, native_abs_int_value, native_abs_real_value,
            native_abs_uint_value,
        };
        let value =
            match value {
                Datum::Null => native_abs_int_value(None)
                    .expect("NULL cannot overflow")
                    .map_or(Datum::Null, Datum::Int),
                Datum::Int(value) => match native_abs_int_value(Some(value)) {
                    Ok(value) => value.map_or(Datum::Null, Datum::Int),
                    Err(_) => {
                        let error =
                            if let Some(expression) = overflow_expression {
                                self.check_bytes(
                                    base.checked_add(expression.len().checked_mul(2).ok_or_else(
                                        || resource("math diagnostic copy overflow"),
                                    )?)
                                    .ok_or_else(|| resource("math diagnostic size overflow"))?,
                                )?;
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
                    let computed = native_abs_decimal_value(&shared, &budget)
                        .map_err(LocalError::Evaluation)?;
                    let value = match crate::tikv::restore_math_decimal(&computed, available) {
                        Ok(value) => value,
                        Err(error) => return self.native_failure(error),
                    };
                    Datum::Decimal(value)
                }
                value => {
                    let ready = match crate::math_fn::numeric_arg(&value, self.context) {
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
        self.finish_native_value(id, value, *output_node, *output_role, row, base)
    }

    fn resume_comparison(&mut self, id: &HostTaskId, token: &[u8]) -> LocalResult<HostStep> {
        let task = self.task(id)?;
        let SqlDemand::Comparison { left_token } = task.demand else {
            return Err(binding("comparison resume has another task kind"));
        };
        let Some(NativeHostMetadata::Comparison {
            operation,
            name,
            result_type,
            argument_nodes,
            operands,
            collation,
            output_node,
            output_role,
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
            let SqlDemand::Comparison { left_token } = &mut self.slots[id.slot]
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
        let payload = {
            let store = store.borrow();
            native_payload_bytes(store.get(
                &left_token,
                NativeValueRole::CharInput,
                left_origin,
            )?)?
            .checked_add(native_payload_bytes(store.get(
                token,
                NativeValueRole::CharInput,
                right_origin,
            )?)?)
            .ok_or_else(|| resource("comparison payload overflow"))?
        };
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("comparison copy allowance overflow"))?,
            )
            .ok_or_else(|| resource("comparison host size overflow"))?;
        self.check_bytes(base)?;
        let (left, right) = {
            let store = store.borrow();
            (
                store.clone_for_kernel(&left_token, NativeValueRole::CharInput, left_origin)?,
                store.clone_for_kernel(token, NativeValueRole::CharInput, right_origin)?,
            )
        };
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
                operands.map(|operand| !operand.unsigned),
            )
        };
        let available = self
            .max_retained_bytes
            .checked_sub(base)
            .ok_or_else(|| resource("comparison value allowance exhausted"))?;
        let result = match *operation {
            SqlBatchHostOperation::CompareNative(op) => {
                let prepared = match crate::ops::prepare_native_comparison(
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
        let result =
            match crate::scalar_function::coerce_function_result(name, Some(result_type), result) {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };
        self.finish_native_value(id, result, *output_node, *output_role, row, base)
            .map(HostStep::Ready)
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
        let payload = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?;
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("native truth allowance overflow"))?,
            )
            .ok_or_else(|| resource("native truth size overflow"))?;
        self.check_bytes(base)?;
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
        let result =
            match crate::scalar_function::coerce_function_result(name, Some(result_type), result) {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };
        self.finish_native_value(id, result, *output_node, *output_role, row, base)
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
        let payload = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?;
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("logical input allowance overflow"))?,
            )
            .ok_or_else(|| resource("logical input size overflow"))?;
        self.check_bytes(base)?;
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
        let result =
            match crate::scalar_function::coerce_function_result(name, Some(result_type), result) {
                Ok(value) => value,
                Err(error) => return self.native_failure(error),
            };
        self.finish_native_value(id, result, *output_node, *output_role, row, base)
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
        let payload = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?;
        let base = self
            .retained_bytes()?
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| resource("native predicate allowance overflow"))?,
            )
            .ok_or_else(|| resource("native predicate size overflow"))?;
        self.check_bytes(base)?;
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
        self.finish_native_value(id, Datum::Null, *output_node, *output_role, row, base)
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
        let input_charge = native_payload_bytes(store.borrow().get(
            token,
            NativeValueRole::CharInput,
            origin,
        )?)?
        .checked_mul(2)
        .ok_or_else(|| resource("native cast input allowance overflow"))?;
        let base_bytes = self
            .retained_bytes()?
            .checked_add(input_charge)
            .ok_or_else(|| resource("native cast live input size overflow"))?;
        self.check_bytes(base_bytes)?;
        let value = store
            .borrow()
            .clone_for_kernel(token, NativeValueRole::CharInput, origin)?;
        let mut warnings = CapturedWarnings {
            rows: Vec::new(),
            base_bytes,
            max_bytes: self.max_retained_bytes,
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
        let capacity = bytes.as_ref().map_or(0, Vec::capacity);
        warnings.check(
            capacity
                .checked_add(length)
                .and_then(|bytes| bytes.checked_add(size_of::<usize>().saturating_mul(4)))
                .ok_or_else(|| resource("native CHAR output overlap overflow"))?,
        )?;
        let mut output = ChunkedVecBytes::try_with_capacities(1, length)
            .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
        output.push_ref(bytes.as_deref());
        warnings.check(
            output
                .retained_heap_bytes()
                .and_then(|size| size.checked_add(capacity))
                .ok_or_else(|| resource("native CHAR result size overflow"))?,
        )?;
        for (code, message) in &warnings.rows {
            self.context.append_warning(*code, message);
        }
        self.slots[id.slot].task = None;
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
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(256)
                .ok_or_else(|| resource("integer character cast retained size overflow"))?,
        )?;
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
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(
                    output
                        .retained_heap_bytes()
                        .ok_or_else(|| resource("character cast output size overflow"))?,
                )
                .and_then(|size| size.checked_add(value.as_ref().map_or(0, Vec::capacity)))
                .ok_or_else(|| resource("integer character cast result size overflow"))?,
        )?;
        self.slots[id.slot].task = None;
        Ok(VectorValue::Bytes(output))
    }
    fn native_failure<T>(&mut self, error: EvalError) -> LocalResult<T> {
        self.native_error = Some(error);
        Err(LocalError::HostContract(
            "native SQL context operation failed".into(),
        ))
    }
    fn finish_regexp(
        &mut self,
        task: &NativeTask,
        case_insensitive: bool,
    ) -> LocalResult<VectorValue> {
        let other = self.retained_bytes()?;
        let result = if task.values.iter().any(Option::is_none) {
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
            let prefix_bytes = other
                .checked_add(task.retained_bytes()?)
                .and_then(|size| size.checked_add(flags_len))
                .ok_or_else(|| resource("regexp prefix retained size overflow"))?;
            self.check_bytes(prefix_bytes)?;
            let mut prepared_flags = Vec::new();
            prepared_flags
                .try_reserve_exact(flags_len)
                .map_err(|error| LocalError::ResourceLimit(error.to_string()))?;
            if case_insensitive {
                prepared_flags.push(b'i');
            }
            prepared_flags.extend_from_slice(flags);
            let prefix_bytes = prefix_bytes
                .checked_add(prepared_flags.capacity().saturating_sub(flags_len))
                .ok_or_else(|| resource("regexp flags capacity overflow"))?;
            self.check_bytes(prefix_bytes)?;
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
            let prepared = invocation
                .with_context_id(self.context.context_id())
                .prepare_like(pattern, flags);
            self.check_bytes(
                prefix_bytes
                    .checked_add(
                        prepared
                            .known_cache_bytes()
                            .ok_or_else(|| resource("regexp cache retained size overflow"))?,
                    )
                    .ok_or_else(|| resource("regexp retained size overflow"))?,
            )?;
            match prepared.execute(text) {
                Ok(value) => Some(i64::from(value)),
                Err(error) => {
                    return self.native_failure(crate::regexp::native_regexp_error(&error))
                }
            }
        };
        self.check_bytes(
            other
                .checked_add(task.retained_bytes()?)
                .and_then(|size| size.checked_add(std::mem::size_of::<Option<i64>>()))
                .ok_or_else(|| resource("regexp output size overflow"))?,
        )?;
        Ok(VectorValue::from_scalar(&ScalarValue::Int(result), 1))
    }
}
impl LocalHostServices for NativeBatchHosts<'_> {
    fn catalog_key(&self) -> &HostCatalogKey {
        self.catalog.key()
    }
    fn start(
        &mut self,
        _ctx: &mut EvalContext,
        invocation: HostInvocation<'_>,
    ) -> LocalResult<HostStart> {
        let signature = self.catalog.signature(invocation.slot)?;
        if signature.arg_types.as_ref() != invocation.arg_types
            || &signature.return_type != invocation.return_type
        {
            return Err(LocalError::HostContract(
                "SQL host invocation differs from its catalog signature".into(),
            ));
        }
        if !self.constant_probes.is_empty() {
            if self.constant_probes.len() != self.metadata.len() {
                return Err(binding("source prefix descriptor count differs"));
            }
            let probe = self
                .constant_probes
                .get(invocation.slot.index())
                .ok_or_else(|| binding("source prefix slot absent"))?;
            let maximum = self
                .max_retained_bytes
                .checked_sub(self.retained_bytes()?)
                .ok_or_else(|| resource("source prefix allowance exhausted"))?;
            if let Err(error) = crate::tikv::identity_actual::probe(probe.as_ref(), maximum) {
                return self.native_failure(error);
            }
        }
        let demand = match self.catalog.sql_batch_operation(invocation.slot)? {
            SqlBatchHostOperation::EagerValuesNative => {
                let Some(NativeHostMetadata::EagerValues {
                    arguments,
                    kernel,
                    probe,
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("eager values descriptor absent"));
                };
                if self.native_values.is_none() || arguments.len() != invocation.arg_types.len() {
                    return Err(binding("eager values descriptor differs"));
                }
                let maximum = self
                    .max_retained_bytes
                    .checked_sub(self.retained_bytes()?)
                    .ok_or_else(|| resource("eager probe allowance exhausted"))?;
                let name = match crate::tikv::identity_actual::probe_name(probe.as_ref(), maximum) {
                    Ok(name) => name,
                    Err(e) => return self.native_failure(e),
                };
                match (kernel.user_variable(), name) {
                    (
                        Some(crate::scalar_function::user_variable::UserVariableAction::Get(_)),
                        Some(name),
                    ) => SqlDemand::UserVariableRead {
                        name,
                        consumed: false,
                    },
                    (
                        Some(crate::scalar_function::user_variable::UserVariableAction::Set),
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
                let maximum = self
                    .max_retained_bytes
                    .checked_sub(self.retained_bytes()?)
                    .ok_or_else(|| resource("crypto probe allowance exhausted"))?;
                if let Err(e) = crate::tikv::identity_actual::probe(probe.as_ref(), maximum) {
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
                let maximum = self
                    .max_retained_bytes
                    .checked_sub(self.retained_bytes()?)
                    .ok_or_else(|| resource("compression probe allowance exhausted"))?;
                if let Err(e) = crate::tikv::identity_actual::probe(probe.as_ref(), maximum) {
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
                let maximum = self
                    .max_retained_bytes
                    .checked_sub(self.retained_bytes()?)
                    .ok_or_else(|| resource("byte unary probe allowance exhausted"))?;
                if let Err(e) = crate::tikv::identity_actual::probe(probe.as_ref(), maximum) {
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
                let maximum = self
                    .max_retained_bytes
                    .checked_sub(self.retained_bytes()?)
                    .ok_or_else(|| resource("misc probe allowance exhausted"))?;
                if let Err(e) = crate::tikv::identity_actual::probe(probe.as_ref(), maximum) {
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
                let maximum = self
                    .max_retained_bytes
                    .checked_sub(self.retained_bytes()?)
                    .ok_or_else(|| resource("identity probe allowance exhausted"))?;
                // Original signature prefix, before ANY child demand. No store
                // borrow or expression evaluation is retained in this metadata.
                if let Err(error) = crate::tikv::identity_actual::probe(probe.as_ref(), maximum) {
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
                    ..
                }) = self.metadata.get(invocation.slot.index())
                else {
                    return Err(binding("date arithmetic descriptor absent"));
                };
                if self.native_values.is_none()
                    || invocation.arg_types.len() != 2
                    || output != *declared
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
            | SqlBatchHostOperation::CastJsonWindowNative => {
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
                    || operands.iter().any(|operand| operand.eval_type.is_none())
                    || invocation.arg_types.len() != 2
                {
                    return Err(binding("comparison descriptor differs"));
                }
                SqlDemand::Comparison { left_token: None }
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
                SqlDemand::RegexpLike {
                    case_insensitive,
                    next: Some(0),
                }
            }
        };
        let immediate = matches!(demand, SqlDemand::UserVariableRead { .. })
            || (matches!(
                demand,
                SqlDemand::EagerValues { .. }
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
                Ok(HostStep::NeedArg(_)) => {
                    Err(binding("niladic host unexpectedly demanded an argument"))
                }
                Err(error) => Err(error),
            };
        }
        Ok(start)
    }
    fn start_window(
        &mut self,
        _ctx: &mut EvalContext,
        invocation: HostWindowInvocation<'_>,
    ) -> LocalResult<HostStart> {
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
        if matches!(self.task(id)?.demand, SqlDemand::WindowUnary { .. }) {
            self.resume_unary_window(id, reply)
        } else if matches!(self.task(id)?.demand, SqlDemand::WindowDecimalFast { .. }) {
            self.resume_decimal_fast_window(id, reply)
        } else {
            self.resume_arithmetic_window(id, reply)
        }
    }
    fn resume(
        &mut self,
        _ctx: &mut EvalContext,
        id: &HostTaskId,
        reply: HostArgReply<'_>,
    ) -> LocalResult<HostStep> {
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
        if matches!(task.demand, SqlDemand::TimestampDiff { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("timestamp diff source cannot be wire NULL"));
            };
            return self.resume_timestamp_diff(id, token);
        }
        if matches!(task.demand, SqlDemand::Like { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("LIKE source cannot be wire NULL"));
            };
            return self.resume_wildcard(id, token);
        }
        if matches!(task.demand, SqlDemand::Crypto { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("crypto source cannot be wire NULL"));
            };
            return self.resume_crypto(id, token);
        }
        if matches!(task.demand, SqlDemand::UserVariableWrite { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("user variable operand cannot be wire NULL"));
            };
            return self.finish_user_variable(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::EagerValues { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("eager argument cannot be wire NULL"));
            };
            return self.resume_eager_values(id, token);
        }
        if matches!(task.demand, SqlDemand::Compression { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("compression source cannot be wire NULL"));
            };
            return self.resume_compression(id, token);
        }
        if matches!(task.demand, SqlDemand::ByteUnary { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("byte unary source cannot be wire NULL"));
            };
            return self.resume_byte_unary(id, token);
        }
        if matches!(task.demand, SqlDemand::Misc { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("misc source cannot be wire NULL"));
            };
            return self.resume_misc(id, token);
        }
        if matches!(task.demand, SqlDemand::Identity { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("identity source cannot be wire NULL"));
            };
            return self.resume_identity(id, token);
        }
        if matches!(task.demand, SqlDemand::Grouping { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("GROUPING source cannot be wire NULL"));
            };
            return self.resume_grouping(id, token);
        }
        if matches!(task.demand, SqlDemand::Vector { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("vector source cannot be wire NULL"));
            };
            return self.resume_vector(id, token);
        }
        if matches!(task.demand, SqlDemand::DurationConstruction { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("duration source cannot be wire NULL"));
            };
            return self.resume_duration_construction(id, token);
        }
        if matches!(task.demand, SqlDemand::Calendar { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("calendar source cannot be wire NULL"));
            };
            return self.resume_calendar(id, token);
        }
        if matches!(task.demand, SqlDemand::Clock) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("clock source cannot be wire NULL"));
            };
            return self.resume_clock(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::Tso) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("TSO source cannot be wire NULL"));
            };
            return self.resume_tso(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::Epoch { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("epoch source cannot be wire NULL"));
            };
            return self.resume_epoch(id, Some(token));
        }
        if matches!(task.demand, SqlDemand::Extract { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("EXTRACT source cannot be wire NULL"));
            };
            return self.resume_extract(id, token);
        }
        if matches!(task.demand, SqlDemand::DateArithmetic { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("date arithmetic source cannot be wire NULL"));
            };
            return self.resume_date_arithmetic(id, token);
        }
        if matches!(task.demand, SqlDemand::BoundedStaleness { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("bounded source cannot be wire NULL"));
            };
            return self.resume_bounded_staleness(id, token);
        }
        if matches!(task.demand, SqlDemand::StrToDate { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("date source cannot be wire NULL"));
            };
            return self.resume_str_to_date(id, token);
        }
        if matches!(task.demand, SqlDemand::JsonSumCrc32) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("JSON checksum source cannot be wire NULL"));
            };
            return self.resume_json_sum_crc32(id, token);
        }
        if matches!(task.demand, SqlDemand::Charset { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("charset source cannot be wire NULL"));
            };
            return self.resume_charset(id, token);
        }
        if matches!(task.demand, SqlDemand::Extremum { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("extremum source cannot be wire NULL"));
            };
            return self.resume_extremum(id, token);
        }
        if matches!(task.demand, SqlDemand::Interval { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("INTERVAL source cannot be wire NULL"));
            };
            return self.resume_interval(id, token);
        }
        if matches!(task.demand, SqlDemand::In { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("native IN argument cannot be wire NULL"));
            };
            return self.resume_in(id, token);
        }
        if matches!(task.demand, SqlDemand::Arithmetic { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("arithmetic argument cannot be wire NULL"));
            };
            return self.resume_arithmetic(id, token);
        }
        if matches!(task.demand, SqlDemand::RawValues { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("raw value argument cannot be wire NULL"));
            };
            return self.resume_raw_values(id, token);
        }
        if matches!(task.demand, SqlDemand::Math) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("math argument cannot be wire NULL"));
            };
            return self.finish_math(id, token).map(HostStep::Ready);
        }
        if matches!(task.demand, SqlDemand::Comparison { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("native comparison argument cannot be wire NULL"));
            };
            return self.resume_comparison(id, token);
        }
        if matches!(task.demand, SqlDemand::Truth) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("native truth argument cannot be wire NULL"));
            };
            return self.finish_truth(id, token).map(HostStep::Ready);
        }
        if matches!(task.demand, SqlDemand::Logical { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("native logical argument cannot be wire NULL"));
            };
            return self.resume_logical(id, token);
        }
        if matches!(task.demand, SqlDemand::Conditional { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("native predicate/branch cannot be wire NULL"));
            };
            return self.resume_conditional(id, token);
        }
        if matches!(task.demand, SqlDemand::Nullable { .. }) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("native nullable argument cannot be wire NULL"));
            };
            return self.resume_nullable(id, token);
        }
        if matches!(task.demand, SqlDemand::ValueCast) {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("native CAST argument handle cannot be wire NULL"));
            };
            return self.finish_value_cast(id, token).map(HostStep::Ready);
        }
        if let SqlDemand::CastCharNative { len } = &task.demand {
            let ScalarValueRef::Bytes(Some(token)) = reply.values.get_scalar_ref(0) else {
                return Err(binding("native value handle cannot be wire NULL"));
            };
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
        let len = bytes.map_or(0, <[u8]>::len);
        let old_capacity = task.values.capacity();
        let target = if task.values.len() == old_capacity {
            old_capacity.saturating_mul(2).max(4).min(task.arity)
        } else {
            old_capacity
        };
        let growth = array_bytes::<Option<Vec<u8>>>(target.saturating_sub(old_capacity))?;
        self.check_bytes(
            self.retained_bytes()?
                .checked_add(growth)
                .and_then(|size| size.checked_add(len))
                .ok_or_else(|| resource("SQL host prefix growth size overflow"))?,
        )?;
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
        self.check_bytes(self.retained_bytes()?)?;
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
    fn cancel(&mut self, id: &HostTaskId) {
        if let Some(slot) = self.slots.get_mut(id.slot) {
            if slot.generation == id.generation {
                slot.task = None;
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
        let mut provider = NativeBatchHosts::new(catalog, &[], &crate::NoColumns, 512);
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
            Err(LocalError::ResourceLimit(_))
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
        assert!(provider.retained_bytes().unwrap() <= 512);
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
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 256);
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
        assert!(matches!(
            provider.resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &argument,
                    values: &value,
                }
            ),
            Err(LocalError::ResourceLimit(_))
        ));
        assert_eq!(context.0.get(), 0);
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
        let values = RefCell::new(NativeValueStore::new(1024 * 1024).unwrap());
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
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 512)
            .with_native_values(Some(&values));
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
        let input = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
        let failure = provider
            .resume(
                &mut ctx,
                &task,
                HostArgReply {
                    index: 0,
                    field_type: &argument,
                    values: &input,
                },
            )
            .unwrap_err();
        assert!(
            matches!(failure, LocalError::ResourceLimit(reason) if reason == "native warning capture policy exceeded")
        );
        assert_eq!(context.0.get(), 0);
        assert!(provider.take_native_error().is_none());
        provider.cancel(&task);
        let HostStart::Pending { task, .. } = provider.start(&mut ctx, invocation()).unwrap()
        else {
            panic!("request")
        };
        let input = VectorValue::from_scalar(&ScalarValue::Bytes(Some(wrong)), 1);
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
        assert_eq!(context.0.get(), 0);
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
                    input_is_literal: false,
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
            let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
            store.borrow_mut().begin_window().unwrap();
            let row = InputRow {
                occurrence: 0,
                input_row: 0,
            };
            let origin = NativeValueOrigin { node: 3, row };
            let large = store
                .borrow_mut()
                .insert_clone(
                    &Datum::Bytes(vec![b'1'; 16384]),
                    NativeValueRole::CharInput,
                    origin,
                )
                .unwrap();
            let wrong = store
                .borrow_mut()
                .insert_clone(&Datum::Int(1), NativeValueRole::Output, origin)
                .unwrap();
            let context = crate::NoColumns;
            let mut provider = NativeBatchHosts::new(catalog, &metadata, &context, 512)
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
                let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
                let failure = provider
                    .resume(
                        &mut ctx,
                        &task,
                        HostArgReply {
                            index: 0,
                            field_type: &argument,
                            values: &values,
                        },
                    )
                    .unwrap_err();
                if resource_failure {
                    assert!(matches!(failure, LocalError::ResourceLimit(_)));
                } else {
                    assert!(matches!(failure, LocalError::BindingContract(_)));
                }
                assert!(provider.take_native_error().is_none());
                provider.cancel(&task);
            }
        }
    }
    #[test]
    fn native_nullable_selected_input_keeps_resource_and_provenance_failures_terminal() {
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
            result_type: native_bytes_type(),
            argument_nodes: vec![3, 8].into_boxed_slice(),
            output_node: 2,
            output_role: NativeValueRole::Output,
        }];
        let store = RefCell::new(NativeValueStore::new(1 << 20).unwrap());
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
        let mut provider = NativeBatchHosts::new(catalog, &metadata, &crate::NoColumns, 512)
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
            let values = VectorValue::from_scalar(&ScalarValue::Bytes(Some(token)), 1);
            let error = provider
                .resume(
                    &mut ctx,
                    &task,
                    HostArgReply {
                        index: 0,
                        field_type: &argument,
                        values: &values,
                    },
                )
                .unwrap_err();
            if resource_failure {
                assert!(matches!(error, LocalError::ResourceLimit(_)));
            } else {
                assert!(matches!(error, LocalError::BindingContract(_)));
            }
            assert!(provider.take_native_error().is_none());
            provider.cancel(&task);
        }
    }
}
