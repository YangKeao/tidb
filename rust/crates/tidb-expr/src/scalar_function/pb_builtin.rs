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

//! Go `getSignatureByPB`: select an implementation from the wire enum.
//! SQL function names are diagnostic metadata, never an overload selector here.

use super::logic_truthy;
#[cfg(test)]
use super::{eval_numeric_operand_row, eval_numeric_row, ScalarFunction};
use crate::context::{Columns, EvalError};
#[cfg(test)]
use crate::expression::ConstLevel;
use tidb_ast::BinaryOp;
#[cfg(test)]
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, EvalType, FieldType, UNSPECIFIED_LENGTH};
use tidb_proto::tipb::ScalarFuncSig;

/// Present PB integer carriers through the actual return declaration before
/// result conversion. Signedness remains observable in binary JSON even 0/1.
pub(crate) fn interpret_integer_result(value: Datum, result_type: Option<&FieldType>) -> Datum {
    match (value, result_type) {
        (Datum::UInt(value), Some(field)) if !field.is_unsigned() => Datum::Int(value as i64),
        (Datum::Int(value), Some(field)) if field.is_unsigned() => Datum::UInt(value as u64),
        (value, _) => value,
    }
}

type ValuesKernel = fn(&[Datum], &dyn Columns) -> Result<Datum, EvalError>;

#[derive(Clone, Copy, Debug)]
enum Kernel {
    Binary(BinaryOp, EvalType),
    Logic(BinaryOp),
    IntegerMod { unsigned: [bool; 2] },
    IsNull,
    Truth { negate: bool },
    Case,
    If,
    IfNull,
    Cast { source: EvalType, target: EvalType },
    String { operation: StringOp, binary: bool },
    Round,
    FromUnixTime,
    Regexp,
    Json,
    Values(ValuesKernel),
}

#[derive(Clone, Copy, Debug)]
enum StringOp {
    Length,
    Upper,
    Lower,
    Substring,
}

/// The selected implementation survives cloning independently of FuncName.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PbBuiltin {
    signature: ScalarFuncSig,
    kernel: Kernel,
}

/// Actual selected PB value implementation and its original result precision.
/// No expression tree, display-name dispatch or evaluation callback is retained.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PbEagerValues {
    builtin: PbBuiltin,
    result_decimal: Option<i64>,
}
impl PbEagerValues {
    pub(crate) fn signature(self) -> ScalarFuncSig {
        self.builtin.signature()
    }
    pub(crate) fn null_result(
        self,
        observed: &Datum,
        arity: usize,
        null_index: usize,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        self.builtin
            .eager_null_value(observed, arity, null_index, ctx)
    }
    pub(crate) fn eval_values(
        self,
        values: &mut [Datum],
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        self.builtin
            .eager_value_result(self.result_decimal, values, ctx)
    }
}

/// Actual selected PB cast owner; declarations remain the caller's real facts.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PbCastValues {
    builtin: PbBuiltin,
}
impl PbCastValues {
    pub(crate) fn signature(self) -> ScalarFuncSig {
        self.builtin.signature()
    }
    pub(crate) fn source(self) -> EvalType {
        let Kernel::Cast { source, .. } = self.builtin.kernel else {
            unreachable!("selected cast")
        };
        source
    }
    pub(crate) fn arity_error() -> EvalError {
        EvalError::Unsupported("protobuf cast arity")
    }
    pub(crate) fn eval_value(
        self,
        value: Datum,
        source: Option<&FieldType>,
        result: Option<&FieldType>,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        let Kernel::Cast { target, .. } = self.builtin.kernel else {
            unreachable!("selected cast")
        };
        if value.is_null() {
            return Ok(Datum::Null);
        }
        if target == EvalType::Json {
            return crate::builtin_ext::json::cast_json_prepared(&value, source, result);
        }
        let field = result.ok_or(EvalError::Unsupported("protobuf cast result type"))?;
        use tidb_ast::CastType;
        let len = u32::try_from(field.flen()).ok();
        let fsp = u32::try_from(field.decimal()).ok();
        let cast = match target {
            EvalType::Int if field.is_unsigned() => CastType::Unsigned,
            EvalType::Int => CastType::Signed,
            EvalType::Real => CastType::Double,
            EvalType::Decimal => CastType::Decimal {
                flen: len.unwrap_or(0),
                scale: if field.decimal() == UNSPECIFIED_LENGTH {
                    crate::cast::UNSPECIFIED_CAST_SCALE
                } else {
                    fsp.unwrap_or(0)
                },
            },
            EvalType::String if field.is_binary_string() => CastType::Binary {
                len: if field.code() == tidb_datatype::FieldTypeCode::String {
                    len
                } else {
                    None
                },
            },
            EvalType::String => CastType::Char {
                len,
                charset: Some(field.charset_name().to_owned()),
            },
            EvalType::Datetime | EvalType::Timestamp
                if field.code() == tidb_datatype::FieldTypeCode::Date =>
            {
                CastType::Date
            }
            EvalType::Datetime | EvalType::Timestamp => CastType::DateTime { fsp },
            EvalType::Duration => CastType::Time { fsp },
            EvalType::VectorFloat32 => CastType::Vector { dimensions: len },
            _ => return Err(EvalError::Unsupported("protobuf cast target")),
        };
        crate::cast::eval_cast(&cast, value, source, ctx)
    }
}

/// Actual selected lazy PB owner; children and SQL declarations stay with callers.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PbLazyValues {
    builtin: PbBuiltin,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PbLazyPhase {
    Condition(usize),
    Value(usize),
    NullableFirst,
    Exhausted,
    Missing,
}
impl PbLazyPhase {
    pub(crate) fn argument(self) -> Option<usize> {
        match self {
            Self::Condition(i) | Self::Value(i) => Some(i),
            Self::NullableFirst => Some(0),
            Self::Exhausted | Self::Missing => None,
        }
    }
}
impl PbLazyValues {
    pub(crate) fn signature(self) -> ScalarFuncSig {
        self.builtin.signature
    }
    pub(crate) fn data_arity(self, logical: usize) -> usize {
        match self.builtin.kernel {
            Kernel::Case => logical,
            Kernel::If => logical.min(3),
            Kernel::IfNull => logical.min(2),
            _ => unreachable!("actual lazy owner"),
        }
    }
    fn case_phase(logical: usize, demand: crate::tikv::CaseDemand) -> PbLazyPhase {
        match demand {
            crate::tikv::CaseDemand::Condition(pair) => PbLazyPhase::Condition(pair * 2),
            crate::tikv::CaseDemand::Value(pair) => PbLazyPhase::Value(if pair == logical / 2 {
                pair * 2
            } else {
                pair * 2 + 1
            }),
            crate::tikv::CaseDemand::Exhausted => PbLazyPhase::Exhausted,
        }
    }
    pub(crate) fn initial(self, logical: usize) -> PbLazyPhase {
        match self.builtin.kernel {
            Kernel::Case => Self::case_phase(
                logical,
                crate::tikv::case_start(logical / 2, logical % 2 != 0),
            ),
            Kernel::If if logical != 0 => PbLazyPhase::Condition(0),
            Kernel::IfNull if logical != 0 => PbLazyPhase::NullableFirst,
            Kernel::If | Kernel::IfNull => PbLazyPhase::Missing,
            _ => unreachable!("actual lazy owner"),
        }
    }
    pub(crate) fn truth(self, value: &Datum, ctx: &dyn Columns) -> Result<Option<bool>, EvalError> {
        debug_assert!(matches!(self.builtin.kernel, Kernel::Case | Kernel::If));
        logic_truthy(value, ctx)
    }
    pub(crate) fn after_truth(
        self,
        logical: usize,
        index: usize,
        truth: Option<bool>,
    ) -> PbLazyPhase {
        match self.builtin.kernel {
            Kernel::Case => Self::case_phase(
                logical,
                crate::tikv::case_after_truth(logical / 2, logical % 2 != 0, index / 2, truth),
            ),
            Kernel::If => {
                let selected = if tidb_query_expr::native_if_choose_branch(truth)
                    == tidb_query_expr::NativeIfBranch::Then
                {
                    1
                } else {
                    2
                };
                if selected < logical {
                    PbLazyPhase::Value(selected)
                } else {
                    PbLazyPhase::Missing
                }
            }
            _ => unreachable!("actual conditional owner"),
        }
    }
    pub(crate) fn needs_second(self, is_null: bool) -> bool {
        debug_assert!(matches!(self.builtin.kernel, Kernel::IfNull));
        matches!(
            tidb_query_expr::native_if_null_choose_first((!is_null).then_some(())),
            tidb_query_expr::NativeIfNullChoice::NeedSecond
        )
    }
    pub(crate) fn selected_value(self, value: Datum) -> Result<Datum, EvalError> {
        crate::tikv::selected_control_value(value)
    }
    pub(crate) fn exhausted_value(self) -> Result<Datum, EvalError> {
        debug_assert!(matches!(self.builtin.kernel, Kernel::Case));
        crate::tikv::exhausted_case_value()
    }
}

/// Actual selected PB Boolean implementation, with no expression children.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PbBooleanValues {
    builtin: PbBuiltin,
}
impl PbBooleanValues {
    pub(crate) fn signature(self) -> ScalarFuncSig {
        self.builtin.signature
    }
    pub(crate) fn observes_null_only(self) -> bool {
        matches!(self.builtin.kernel, Kernel::IsNull)
    }
    pub(crate) fn data_arity(self, logical: usize) -> usize {
        logical.min(if self.logical_function().is_some() {
            2
        } else {
            1
        })
    }
    pub(crate) fn logical_function(self) -> Option<crate::LogicalFunction> {
        match self.builtin.kernel {
            Kernel::Logic(BinaryOp::LogicAnd) => Some(crate::LogicalFunction::And),
            Kernel::Logic(BinaryOp::LogicOr) => Some(crate::LogicalFunction::Or),
            _ => None,
        }
    }
    pub(crate) fn nullness(self, is_null: bool) -> Option<bool> {
        debug_assert!(self.observes_null_only());
        if is_null {
            None
        } else {
            Some(false)
        }
    }
    pub(crate) fn truth(self, value: &Datum, ctx: &dyn Columns) -> Result<Option<bool>, EvalError> {
        if self.observes_null_only() {
            Ok(self.nullness(value.is_null()))
        } else {
            logic_truthy(value, ctx)
        }
    }
    pub(crate) fn needs_right(self, left: Option<bool>) -> bool {
        self.logical_function()
            .is_some_and(|function| super::logical_needs_right(function, left))
    }
    pub(crate) fn unary_value(
        self,
        ready: Option<bool>,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        let function = match self.builtin.kernel {
            Kernel::IsNull => crate::BooleanFunction::IsNull,
            Kernel::Truth { negate: true } => crate::BooleanFunction::UnaryNot,
            Kernel::Truth { negate: false } => crate::BooleanFunction::IsTrueWithNull,
            _ => unreachable!("actual unary Boolean owner"),
        };
        crate::eval_boolean_ready_in(function, ready, ctx)
    }
    pub(crate) fn logical_value(
        self,
        arguments: crate::LogicalArgs,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        crate::eval_logical_ready_in(
            self.logical_function().expect("actual logical owner"),
            arguments,
            ctx,
        )
    }
}

/// Actual selected binary implementation, without expression children.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PbBinaryValues {
    builtin: PbBuiltin,
}
impl PbBinaryValues {
    pub(crate) fn arity_error() -> EvalError {
        EvalError::Unsupported("protobuf binary builtin arity")
    }
    pub(crate) fn missing_argument_error() -> EvalError {
        EvalError::Unsupported("missing protobuf builtin argument")
    }
    pub(crate) fn signature(self) -> ScalarFuncSig {
        self.builtin.signature
    }
    pub(crate) fn operation(self) -> BinaryOp {
        match self.builtin.kernel {
            Kernel::Binary(op, _) => op,
            Kernel::IntegerMod { .. } => BinaryOp::Mod,
            _ => unreachable!("actual binary owner"),
        }
    }
    pub(crate) fn domain(self) -> Option<EvalType> {
        match self.builtin.kernel {
            Kernel::Binary(_, domain) => Some(domain),
            Kernel::IntegerMod { .. } => None,
            _ => unreachable!("actual binary owner"),
        }
    }
    pub(crate) fn stops_on_left_null(self) -> bool {
        self.operation() != BinaryOp::NullEq
            && !(self.operation() == BinaryOp::Mod && self.domain() != Some(EvalType::Decimal))
    }
    pub(crate) fn left_null_result(
        self,
        observed: Datum,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        debug_assert!(observed.is_null() && self.domain().is_some() && self.stops_on_left_null());
        let op = self.operation();
        if matches!(
            op,
            BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Mul | BinaryOp::Mod | BinaryOp::Div
        ) {
            crate::ops::eval_binary_arithmetic_null_in(ctx)
        } else if matches!(
            op,
            BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
        ) {
            // Strict PB comparisons stop before RHS demand. NULL-safe
            // equality instead needs both arguments and bypasses this arm.
            crate::ops::eval_comparison_null_in(op, ctx)
        } else {
            Ok(observed)
        }
    }
    pub(crate) fn integer_mod_operand(
        self,
        index: usize,
        value: Datum,
    ) -> Result<Datum, EvalError> {
        let Kernel::IntegerMod { unsigned } = self.builtin.kernel else {
            unreachable!("actual integer MOD owner")
        };
        Ok(match crate::arg_eval_type::eval_int(&value)? {
            None => Datum::Null,
            Some(value) if unsigned[index] => Datum::UInt(value as u64),
            Some(value) => Datum::Int(value),
        })
    }
    pub(crate) fn integer_mod_values(
        self,
        left: Datum,
        right: Datum,
        precision: u32,
        collation: tidb_datatype::Collation,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        debug_assert!(self.domain().is_none());
        crate::ops::eval_binary_full(
            BinaryOp::Mod,
            left,
            right,
            precision,
            collation,
            crate::ops::Operands::LITERALS,
            ctx,
        )
    }
    pub(crate) fn eval_values(
        self,
        left: Datum,
        right: Datum,
        signed: [bool; 2],
        collation: tidb_datatype::Collation,
        operands: crate::ops::Operands<'_>,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        debug_assert!(self.domain().is_some());
        let (left, right) = crate::binary_literal::cast_signed_literal_operands(
            self.operation(),
            left,
            right,
            signed,
        );
        super::eval_binary_value_body(self.operation(), left, right, collation, operands, ctx)
    }
}

impl PbBuiltin {
    pub(crate) fn cast_values(self) -> Option<PbCastValues> {
        matches!(self.kernel, Kernel::Cast { .. }).then_some(PbCastValues { builtin: self })
    }

    pub(crate) fn lazy_values(self) -> Option<PbLazyValues> {
        matches!(self.kernel, Kernel::Case | Kernel::If | Kernel::IfNull)
            .then_some(PbLazyValues { builtin: self })
    }
    pub(crate) fn boolean_values(self) -> Option<PbBooleanValues> {
        matches!(
            self.kernel,
            Kernel::IsNull | Kernel::Truth { .. } | Kernel::Logic(..)
        )
        .then_some(PbBooleanValues { builtin: self })
    }
    pub(crate) fn binary_values(self) -> Option<PbBinaryValues> {
        matches!(self.kernel, Kernel::Binary(..) | Kernel::IntegerMod { .. })
            .then_some(PbBinaryValues { builtin: self })
    }
    pub(crate) fn json_signature(self) -> Option<ScalarFuncSig> {
        matches!(self.kernel, Kernel::Json).then_some(self.signature)
    }
    pub(crate) fn eager_values(self, result_decimal: Option<i64>) -> Option<PbEagerValues> {
        matches!(
            self.kernel,
            Kernel::Values(_) | Kernel::String { .. } | Kernel::Round | Kernel::FromUnixTime
        )
        .then_some(PbEagerValues {
            builtin: self,
            result_decimal,
        })
    }
    pub(crate) fn signature(self) -> ScalarFuncSig {
        self.signature
    }

    pub(crate) fn new(signature: ScalarFuncSig) -> Option<Self> {
        use ScalarFuncSig::*;
        let kernel = match signature {
            PlusInt => Kernel::Binary(BinaryOp::Plus, EvalType::Int),
            PlusDecimal => Kernel::Binary(BinaryOp::Plus, EvalType::Decimal),
            MinusDecimal => Kernel::Binary(BinaryOp::Minus, EvalType::Decimal),
            MultiplyDecimal => Kernel::Binary(BinaryOp::Mul, EvalType::Decimal),
            DivideReal => Kernel::Binary(BinaryOp::Div, EvalType::Real),
            DivideDecimal => Kernel::Binary(BinaryOp::Div, EvalType::Decimal),
            ModReal => Kernel::Binary(BinaryOp::Mod, EvalType::Real),
            ModDecimal => Kernel::Binary(BinaryOp::Mod, EvalType::Decimal),
            ModIntSignedSigned => Kernel::IntegerMod {
                unsigned: [false, false],
            },
            ModIntSignedUnsigned => Kernel::IntegerMod {
                unsigned: [false, true],
            },
            ModIntUnsignedSigned => Kernel::IntegerMod {
                unsigned: [true, false],
            },
            ModIntUnsignedUnsigned => Kernel::IntegerMod {
                unsigned: [true, true],
            },
            BitAndSig => Kernel::Binary(BinaryOp::BitAnd, EvalType::Int),
            BitOrSig => Kernel::Binary(BinaryOp::BitOr, EvalType::Int),
            BitXorSig => Kernel::Binary(BinaryOp::BitXor, EvalType::Int),
            LeftShift => Kernel::Binary(BinaryOp::LeftShift, EvalType::Int),
            RightShift => Kernel::Binary(BinaryOp::RightShift, EvalType::Int),
            EqInt => Kernel::Binary(BinaryOp::Eq, EvalType::Int),
            GtInt => Kernel::Binary(BinaryOp::Gt, EvalType::Int),
            LtInt => Kernel::Binary(BinaryOp::Lt, EvalType::Int),
            LeInt => Kernel::Binary(BinaryOp::Le, EvalType::Int),
            GeInt => Kernel::Binary(BinaryOp::Ge, EvalType::Int),
            NeInt => Kernel::Binary(BinaryOp::Ne, EvalType::Int),
            NullEqInt => Kernel::Binary(BinaryOp::NullEq, EvalType::Int),
            LtReal => Kernel::Binary(BinaryOp::Lt, EvalType::Real),
            LeReal => Kernel::Binary(BinaryOp::Le, EvalType::Real),
            GtReal => Kernel::Binary(BinaryOp::Gt, EvalType::Real),
            GeReal => Kernel::Binary(BinaryOp::Ge, EvalType::Real),
            EqReal => Kernel::Binary(BinaryOp::Eq, EvalType::Real),
            NeReal => Kernel::Binary(BinaryOp::Ne, EvalType::Real),
            NullEqReal => Kernel::Binary(BinaryOp::NullEq, EvalType::Real),
            LtDecimal => Kernel::Binary(BinaryOp::Lt, EvalType::Decimal),
            LeDecimal => Kernel::Binary(BinaryOp::Le, EvalType::Decimal),
            GtDecimal => Kernel::Binary(BinaryOp::Gt, EvalType::Decimal),
            GeDecimal => Kernel::Binary(BinaryOp::Ge, EvalType::Decimal),
            EqDecimal => Kernel::Binary(BinaryOp::Eq, EvalType::Decimal),
            NeDecimal => Kernel::Binary(BinaryOp::Ne, EvalType::Decimal),
            NullEqDecimal => Kernel::Binary(BinaryOp::NullEq, EvalType::Decimal),
            LogicalAnd => Kernel::Logic(BinaryOp::LogicAnd),
            LogicalOr => Kernel::Logic(BinaryOp::LogicOr),
            IntIsNull | RealIsNull | DecimalIsNull | StringIsNull | TimeIsNull | DurationIsNull
            | VectorFloat32IsNull => Kernel::IsNull,
            UnaryNotInt => Kernel::Truth { negate: true },
            IntIsTrueWithNull => Kernel::Truth { negate: false },
            CaseWhenInt | CaseWhenReal | CaseWhenDecimal | CaseWhenTime | CaseWhenDuration
            | CaseWhenJson => Kernel::Case,
            IfInt | IfReal | IfDecimal | IfTime | IfDuration | IfJson => Kernel::If,
            IfNullInt | IfNullReal | IfNullDecimal | IfNullString | IfNullTime | IfNullDuration
            | IfNullJson => Kernel::IfNull,
            CharLength => Kernel::String {
                operation: StringOp::Length,
                binary: true,
            },
            CharLengthUtf8 => Kernel::String {
                operation: StringOp::Length,
                binary: false,
            },
            Upper => Kernel::String {
                operation: StringOp::Upper,
                binary: true,
            },
            UpperUtf8 => Kernel::String {
                operation: StringOp::Upper,
                binary: false,
            },
            Lower => Kernel::String {
                operation: StringOp::Lower,
                binary: true,
            },
            LowerUtf8 => Kernel::String {
                operation: StringOp::Lower,
                binary: false,
            },
            Substring2Args | Substring3Args => Kernel::String {
                operation: StringOp::Substring,
                binary: true,
            },
            Substring2ArgsUtf8 | Substring3ArgsUtf8 => Kernel::String {
                operation: StringOp::Substring,
                binary: false,
            },
            Acos => Kernel::Values(crate::math_fn::acos),
            Asin => Kernel::Values(crate::math_fn::asin),
            Atan1Arg => Kernel::Values(crate::math_fn::atan),
            Atan2Args => Kernel::Values(crate::math_fn::atan2),
            Cos => Kernel::Values(crate::math_fn::cos),
            Cot => Kernel::Values(crate::math_fn::cot),
            Sin => Kernel::Values(crate::math_fn::sin),
            Pow => Kernel::Values(crate::math_fn::pow),
            Pi => Kernel::Values(crate::math_fn::pi),
            Conv => Kernel::Values(crate::math_fn::conv_in),
            RoundInt | RoundReal | RoundDec => Kernel::Round,
            Date => Kernel::Values(crate::time_fn::date),
            DateDiff => Kernel::Values(crate::time_fn::calendar::date_diff_in),
            DateFormatSig => Kernel::Values(|values, ctx| {
                let [date, format] = values else {
                    return Err(EvalError::WrongParameterCount("date_format"));
                };
                crate::time_fn::calendar::date_format_in(date, format, ctx)
            }),
            Hour => Kernel::Values(crate::time_fn::calendar::hour_in),
            Minute => Kernel::Values(crate::time_fn::calendar::minute_in),
            Second => Kernel::Values(crate::time_fn::calendar::second_in),
            MicroSecond => Kernel::Values(crate::time_fn::microsecond_in),
            Month => Kernel::Values(crate::time_fn::month_in),
            WeekWithoutMode => Kernel::Values(|values, ctx| {
                crate::time_fn::week_in(values, ctx.default_week_format(), ctx)
            }),
            TimestampDiff => Kernel::Values(crate::time_fn::calendar::timestamp_diff_in),
            UnixTimestampInt | UnixTimestampDec => {
                Kernel::Values(crate::time_fn::session_tz::unix_timestamp)
            }
            FromUnixTime1Arg | FromUnixTime2Arg => Kernel::FromUnixTime,
            RegexpLikeSig => Kernel::Regexp,
            JsonMemberOfSig | JsonReplaceSig | JsonArrayAppendSig | JsonMergePatchSig => {
                Kernel::Json
            }
            _ => {
                let (source, target) = cast_types(signature)?;
                Kernel::Cast { source, target }
            }
        };
        Some(Self { signature, kernel })
    }

    #[cfg(test)]
    pub(super) fn eval(
        self,
        function: &ScalarFunction,
        ctx: &dyn Columns,
        row: Row<'_>,
    ) -> Result<Datum, EvalError> {
        let args = &function.args;
        let argument = |index: usize| {
            args.get(index)
                .ok_or_else(PbBinaryValues::missing_argument_error)?
                .eval(ctx, row)
        };
        match self.kernel {
            Kernel::Case => {
                let (pairs, remainder) = args.as_chunks::<2>();
                crate::tikv::eval_case_in(
                    ctx,
                    pairs.len(),
                    !remainder.is_empty(),
                    |_| Ok(()),
                    |index, _, selected| {
                        logic_truthy(&pairs[index][0].eval(selected, row)?, selected)
                    },
                    |index, selected| {
                        let value = if index == pairs.len() {
                            remainder.first().ok_or(EvalError::Unsupported(
                                "missing protobuf builtin argument",
                            ))?
                        } else {
                            &pairs[index][1]
                        };
                        value.eval(selected, row)
                    },
                )
            }
            Kernel::If => crate::tikv::eval_if_in(
                ctx,
                |original_ctx| {
                    let value = args
                        .first()
                        .ok_or(EvalError::Unsupported("missing protobuf builtin argument"))?
                        .eval(original_ctx, row)?;
                    logic_truthy(&value, original_ctx)
                },
                |scoped_ctx| {
                    args.get(1)
                        .ok_or(EvalError::Unsupported("missing protobuf builtin argument"))?
                        .eval(scoped_ctx, row)
                },
                |scoped_ctx| {
                    args.get(2)
                        .ok_or(EvalError::Unsupported("missing protobuf builtin argument"))?
                        .eval(scoped_ctx, row)
                },
            ),
            Kernel::IfNull => crate::tikv::eval_if_null_in(
                ctx,
                |original_ctx| {
                    args.first()
                        .ok_or(EvalError::Unsupported("missing protobuf builtin argument"))?
                        .eval(original_ctx, row)
                },
                |scoped_ctx| {
                    args.get(1)
                        .ok_or(EvalError::Unsupported("missing protobuf builtin argument"))?
                        .eval(scoped_ctx, row)
                },
            ),
            Kernel::IsNull | Kernel::Truth { .. } => {
                let builtin = self.boolean_values().expect("actual unary Boolean owner");
                let ready = builtin.truth(&argument(0)?, ctx)?;
                builtin.unary_value(ready, ctx)
            }
            Kernel::Logic(_) => {
                let builtin = self.boolean_values().expect("actual logical owner");
                let left = builtin.truth(&argument(0)?, ctx)?;
                if !builtin.needs_right(left) {
                    return builtin
                        .logical_value(crate::LogicalArgs::UndemandedRight { left }, ctx);
                }
                let right = builtin.truth(&argument(1)?, ctx)?;
                builtin.logical_value(crate::LogicalArgs::Both(left, right), ctx)
            }
            Kernel::IntegerMod { .. } => {
                let builtin = self.binary_values().expect("actual integer MOD owner");
                let operand = |index| builtin.integer_mod_operand(index, argument(index)?);
                let left = operand(0)?;
                let right = operand(1)?;
                builtin.integer_mod_values(
                    left,
                    right,
                    ctx.div_precision_increment(),
                    function.derived_collation(),
                    ctx,
                )
            }
            Kernel::Binary(op, domain) => {
                if args.len() != 2 {
                    return Err(PbBinaryValues::arity_error());
                }
                let left = eval_numeric_operand_row(&args[0], ctx, row, domain)?;
                if left.is_null()
                    && op != BinaryOp::NullEq
                    && !(op == BinaryOp::Mod && domain != EvalType::Decimal)
                {
                    return self
                        .binary_values()
                        .expect("actual selected binary owner")
                        .left_null_result(left, ctx);
                }
                let right = eval_numeric_operand_row(&args[1], ctx, row, domain)?;
                function.eval_binary_values(op, left, right, ctx)
            }
            Kernel::Cast { source, .. } => {
                if args.len() != 1 {
                    return Err(PbCastValues::arity_error());
                }
                let value = eval_numeric_row(&args[0], ctx, row, source)?;
                self.cast_values()
                    .expect("actual selected cast owner")
                    .eval_value(
                        value,
                        args[0].static_type(),
                        function.get_static_type(),
                        ctx,
                    )
            }
            Kernel::Regexp => function.eval_regexp_like(ctx, row),
            kernel => {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    let value = arg.eval(ctx, row)?;
                    if value.is_null() && !matches!(kernel, Kernel::Json) {
                        return self.eager_null_value(&value, args.len(), values.len(), ctx);
                    }
                    values.push(value);
                }
                match kernel {
                    Kernel::Json => {
                        let types = args
                            .iter()
                            .map(|arg| arg.static_type().cloned())
                            .collect::<Vec<_>>();
                        let cache_paths = args.get(1..).is_some_and(|arguments| {
                            !arguments.is_empty()
                                && arguments
                                    .iter()
                                    .step_by(2)
                                    .all(|arg| arg.const_level() >= ConstLevel::ONLY_IN_CONTEXT)
                        });
                        crate::builtin_ext::json::eval_pb(
                            self.signature,
                            &values,
                            &types,
                            ctx,
                            cache_paths.then_some(&function.json_modify_path_cache),
                        )
                    }
                    _ => self
                        .eager_values(function.get_static_type().map(FieldType::decimal))
                        .expect("non-JSON eager PB owner")
                        .eval_values(&mut values, ctx),
                }
            }
        }
    }
    fn eager_null_value(
        self,
        observed: &Datum,
        arity: usize,
        null_index: usize,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        debug_assert!(observed.is_null() && null_index < arity);
        let value = observed.clone();
        let kernel = self.kernel;
        if let Kernel::String {
            operation: StringOp::Length,
            binary,
        } = kernel
        {
            // Preserve the existing child-demand order, but let
            // the migrated nullable signature actually enter C4.
            return eval_pb_char_length(&value, binary, ctx);
        }
        if matches!(
            self.signature,
            ScalarFuncSig::UnixTimestampInt | ScalarFuncSig::UnixTimestampDec
        ) {
            // Preserve the observed NULL boundary, including an
            // uncoerced prefix and unread extra children.
            return crate::time_fn::session_tz::unix_timestamp(std::slice::from_ref(&value), ctx);
        }
        if matches!(
            self.signature,
            ScalarFuncSig::FromUnixTime1Arg | ScalarFuncSig::FromUnixTime2Arg
        ) {
            // Only the observed NULL is prepared. Prefix values
            // remain uncoerced and suffix children remain unread.
            return crate::time_fn::session_tz::from_unixtime(std::slice::from_ref(&value), ctx);
        }
        if self.signature == ScalarFuncSig::MicroSecond {
            // Preserve the observed NULL boundary even for an
            // otherwise invalid arity or an uncoerced prefix.
            return crate::time_fn::microsecond_in(std::slice::from_ref(&value), ctx);
        }
        if self.signature == ScalarFuncSig::Date {
            // Only the observed NULL is handed off: do not coerce
            // the prefix, demand suffixes, or inspect their arity.
            return crate::time_fn::date(std::slice::from_ref(&value), ctx);
        }
        if self.signature == ScalarFuncSig::Month {
            // Only the observed NULL enters MONTH's typed core;
            // earlier values stay uncoerced and later children unread.
            return crate::time_fn::month_in(std::slice::from_ref(&value), ctx);
        }
        if self.signature == ScalarFuncSig::DateDiff {
            // Only this observed NULL is demanded; the prefix stays
            // uncoerced and even extra suffix children stay unread.
            return crate::tikv::evaluate_args_in(
                crate::tikv::EvaluatedBytesOp::DateDiffNullNative,
                ctx,
                || Ok(crate::tikv::EvaluatedArgs::NullWitness(None)),
                crate::tikv::EvaluatedBytesResult::into_int_datum,
            );
        }
        if self.signature == ScalarFuncSig::WeekWithoutMode {
            // Preserve the PB NULL prefix without coercing earlier
            // values, evaluating suffixes, or reading the default mode.
            return crate::tikv::evaluate_args_in(
                crate::tikv::EvaluatedBytesOp::WeekNullNative,
                ctx,
                || Ok(crate::tikv::EvaluatedArgs::NullWitness(None)),
                crate::tikv::EvaluatedBytesResult::into_int_datum,
            );
        }
        if self.signature == ScalarFuncSig::DateFormatSig {
            // Only this observed NULL is demanded, without coercing
            // the prefix, reading suffixes, or checking non-NULL arity.
            return crate::eval_date_format_null_in(ctx);
        }
        // HMS likewise demands only the observed NULL, even
        // when earlier values or later children are present.
        match self.signature {
            ScalarFuncSig::Hour => {
                return crate::time_fn::calendar::hour_in(std::slice::from_ref(&value), ctx);
            }
            ScalarFuncSig::Minute => {
                return crate::time_fn::calendar::minute_in(std::slice::from_ref(&value), ctx);
            }
            ScalarFuncSig::Second => {
                return crate::time_fn::calendar::second_in(std::slice::from_ref(&value), ctx);
            }
            _ => {}
        }
        match kernel {
            kernel
                if matches!(kernel, Kernel::Round)
                    || matches!(
                        self.signature,
                        ScalarFuncSig::Conv
                            | ScalarFuncSig::Atan1Arg
                            | ScalarFuncSig::Atan2Args
                            | ScalarFuncSig::Cos
                            | ScalarFuncSig::Cot
                            | ScalarFuncSig::Sin
                    ) =>
            {
                // The observed SQL NULL is the only demand
                // witness. Earlier values remain uncoerced and
                // later children, including extra ones, stay
                // unevaluated exactly as at the old boundary.
                return crate::tikv::evaluate_args_in(
                    crate::tikv::EvaluatedBytesOp::MathNullWitnessNative,
                    ctx,
                    || Ok(crate::tikv::EvaluatedArgs::NullWitness(None)),
                    crate::tikv::EvaluatedBytesResult::into_int_datum,
                );
            }
            Kernel::String {
                operation: StringOp::Upper,
                binary,
            } => {
                return crate::string_fn::case_convert_signature_in(
                    std::slice::from_ref(&value),
                    true,
                    binary,
                    ctx,
                );
            }
            Kernel::String {
                operation: StringOp::Lower,
                binary,
            } => {
                return crate::string_fn::case_convert_signature_in(
                    std::slice::from_ref(&value),
                    false,
                    binary,
                    ctx,
                );
            }
            Kernel::String {
                operation: StringOp::Substring,
                binary,
            } if matches!(arity, 2 | 3) => {
                use crate::tikv::{ReadyBytesArg, ReadyIntArg};
                // Earlier children were evaluated, not coerced.
                // Only this NULL is known at the ready boundary.
                let null_index = null_index;
                let bytes = if null_index == 0 {
                    ReadyBytesArg::Value(None)
                } else {
                    ReadyBytesArg::Undemanded
                };
                let pos = if null_index == 1 {
                    ReadyIntArg::Value(None)
                } else {
                    ReadyIntArg::Undemanded
                };
                // Actual arity, not the wire signature, has
                // always selected the two-/three-argument form.
                let len = (arity == 3).then(|| {
                    if null_index == 2 {
                        ReadyIntArg::Value(None)
                    } else {
                        ReadyIntArg::Undemanded
                    }
                });
                return crate::string_fn::substring_ready_in(bytes, pos, len, binary, ctx);
            }
            _ => {}
        }
        // Keep this exact NULL child-demand boundary, but do
        // not bypass the nullable migrated math call.
        match self.signature {
            ScalarFuncSig::TimestampDiff => {
                return crate::tikv::eval_timestamp_diff_null_in(ctx);
            }
            ScalarFuncSig::Asin => return crate::math_fn::asin(std::slice::from_ref(&value), ctx),
            ScalarFuncSig::Acos => return crate::math_fn::acos(std::slice::from_ref(&value), ctx),
            ScalarFuncSig::Pow if arity == 2 => {
                use crate::tikv::ReadyIeee754Arg::{Undemanded, Value};
                // A right NULL suppresses numeric coercion of
                // the already-evaluated left child as well.
                let (left, right) = if null_index == 0 {
                    (Value(None), Undemanded)
                } else {
                    (Undemanded, Value(None))
                };
                return crate::math_fn::pow_ready_in(left, right, ctx);
            }
            _ => {}
        }
        return Ok(Datum::Null);
    }
    fn eager_value_result(
        self,
        result_decimal: Option<i64>,
        values: &mut [Datum],
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        match self.kernel {
            Kernel::Values(eval) => eval(&values, ctx),
            Kernel::Round => crate::math_fn::round_or_truncate_with_result_decimal(
                &values,
                true,
                result_decimal,
                ctx,
            ),
            Kernel::String { operation, binary } => {
                let Some(value) = values.first_mut() else {
                    return Err(EvalError::Unsupported("protobuf string builtin arity"));
                };
                if !value.is_null() {
                    let bytes = crate::arg_eval_type::eval_string(value)?
                        .ok_or(EvalError::Unsupported("protobuf string argument"))?;
                    *value = if binary {
                        Datum::Bytes(bytes)
                    } else {
                        Datum::new_string(bytes)
                    };
                }
                match operation {
                    StringOp::Length => eval_pb_char_length(&values[0], binary, ctx),
                    StringOp::Upper => {
                        crate::string_fn::case_convert_signature_in(&values, true, binary, ctx)
                    }
                    StringOp::Lower => {
                        crate::string_fn::case_convert_signature_in(&values, false, binary, ctx)
                    }
                    StringOp::Substring => crate::string_fn::substring(&values, ctx),
                }
            }
            Kernel::FromUnixTime => {
                let result = crate::time_fn::session_tz::from_unixtime(&values, ctx)?;
                if self.signature == ScalarFuncSig::FromUnixTime1Arg {
                    crate::cast::parse_computed_time(
                        &result,
                        ctx,
                        tidb_datatype::TimeType::DateTime,
                        result_decimal,
                    )
                } else {
                    Ok(result)
                }
            }
            _ => unreachable!("non-eager PB owner has no value body"),
        }
    }
}

fn eval_pb_char_length(value: &Datum, binary: bool, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    // The wire signature is authoritative, not the runtime datum's charset.
    let collation = if binary {
        tidb_datatype::Collation::Binary
    } else {
        tidb_datatype::Collation::DEFAULT
    };
    crate::BuildContext::default()
        .build_string_length(
            crate::StringLengthFunction::CharLength,
            FieldType::new(tidb_datatype::FieldTypeCode::VarString).with_collation(collation),
        )
        .eval_in(value, ctx)
}

fn cast_types(sig: ScalarFuncSig) -> Option<(EvalType, EvalType)> {
    use ScalarFuncSig::*;
    Some(match sig {
        CastDecimalAsDuration => (EvalType::Decimal, EvalType::Duration),
        CastDecimalAsInt => (EvalType::Decimal, EvalType::Int),
        CastDecimalAsJson => (EvalType::Decimal, EvalType::Json),
        CastDecimalAsReal => (EvalType::Decimal, EvalType::Real),
        CastDecimalAsString => (EvalType::Decimal, EvalType::String),
        CastDecimalAsTime => (EvalType::Decimal, EvalType::Datetime),
        CastDurationAsDecimal => (EvalType::Duration, EvalType::Decimal),
        CastDurationAsInt => (EvalType::Duration, EvalType::Int),
        CastDurationAsJson => (EvalType::Duration, EvalType::Json),
        CastDurationAsReal => (EvalType::Duration, EvalType::Real),
        CastDurationAsString => (EvalType::Duration, EvalType::String),
        CastDurationAsTime => (EvalType::Duration, EvalType::Datetime),
        CastIntAsDecimal => (EvalType::Int, EvalType::Decimal),
        CastIntAsDuration => (EvalType::Int, EvalType::Duration),
        CastIntAsJson => (EvalType::Int, EvalType::Json),
        CastIntAsReal => (EvalType::Int, EvalType::Real),
        CastIntAsString => (EvalType::Int, EvalType::String),
        CastIntAsTime => (EvalType::Int, EvalType::Datetime),
        CastJsonAsDecimal => (EvalType::Json, EvalType::Decimal),
        CastJsonAsDuration => (EvalType::Json, EvalType::Duration),
        CastJsonAsInt => (EvalType::Json, EvalType::Int),
        CastJsonAsReal => (EvalType::Json, EvalType::Real),
        CastJsonAsString => (EvalType::Json, EvalType::String),
        CastJsonAsTime => (EvalType::Json, EvalType::Datetime),
        CastRealAsDecimal => (EvalType::Real, EvalType::Decimal),
        CastRealAsDuration => (EvalType::Real, EvalType::Duration),
        CastRealAsInt => (EvalType::Real, EvalType::Int),
        CastRealAsJson => (EvalType::Real, EvalType::Json),
        CastRealAsString => (EvalType::Real, EvalType::String),
        CastRealAsTime => (EvalType::Real, EvalType::Datetime),
        CastStringAsDecimal => (EvalType::String, EvalType::Decimal),
        CastStringAsDuration => (EvalType::String, EvalType::Duration),
        CastStringAsInt => (EvalType::String, EvalType::Int),
        CastStringAsJson => (EvalType::String, EvalType::Json),
        CastStringAsReal => (EvalType::String, EvalType::Real),
        CastStringAsTime => (EvalType::String, EvalType::Datetime),
        CastTimeAsDecimal => (EvalType::Datetime, EvalType::Decimal),
        CastTimeAsDuration => (EvalType::Datetime, EvalType::Duration),
        CastTimeAsInt => (EvalType::Datetime, EvalType::Int),
        CastTimeAsJson => (EvalType::Datetime, EvalType::Json),
        CastTimeAsReal => (EvalType::Datetime, EvalType::Real),
        CastTimeAsString => (EvalType::Datetime, EvalType::String),
        CastTimeAsTime => (EvalType::Datetime, EvalType::Datetime),
        _ => return None,
    })
}
