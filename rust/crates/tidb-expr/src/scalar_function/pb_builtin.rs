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

use super::{
    cast_json_argument_value, eval_numeric_operand_row, eval_numeric_row, logic_truthy,
    ScalarFunction,
};
use crate::context::{Columns, EvalError};
use crate::expression::ConstLevel;
use tidb_ast::BinaryOp;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, EvalType, FieldType, UNSPECIFIED_LENGTH};
use tidb_proto::tipb::ScalarFuncSig;

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

impl PbBuiltin {
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
            EqInt => Kernel::Binary(BinaryOp::Eq, EvalType::Int),
            GtInt => Kernel::Binary(BinaryOp::Gt, EvalType::Int),
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
            TimestampDiff => {
                Kernel::Values(|values, _| crate::time_fn::calendar::timestamp_diff(values))
            }
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

    pub(super) fn eval(
        self,
        function: &ScalarFunction,
        ctx: &dyn Columns,
        row: Row<'_>,
    ) -> Result<Datum, EvalError> {
        let args = &function.args;
        let argument = |index: usize| {
            args.get(index)
                .ok_or(EvalError::Unsupported("missing protobuf builtin argument"))?
                .eval(ctx, row)
        };
        match self.kernel {
            Kernel::Case => {
                let (pairs, remainder) = args.as_chunks::<2>();
                for pair in pairs {
                    if logic_truthy(&pair[0].eval(ctx, row)?, ctx)? == Some(true) {
                        return pair[1].eval(ctx, row);
                    }
                }
                remainder
                    .first()
                    .map_or(Ok(Datum::Null), |value| value.eval(ctx, row))
            }
            Kernel::If => {
                let branch = if logic_truthy(&argument(0)?, ctx)? == Some(true) {
                    1
                } else {
                    2
                };
                argument(branch)
            }
            Kernel::IfNull => {
                let value = argument(0)?;
                if value.is_null() {
                    argument(1)
                } else {
                    Ok(value)
                }
            }
            Kernel::IsNull => {
                let ready = if argument(0)?.is_null() {
                    None
                } else {
                    Some(false)
                };
                crate::eval_boolean_ready_in(crate::BooleanFunction::IsNull, ready, ctx)
            }
            Kernel::Truth { negate } => {
                let ready = logic_truthy(&argument(0)?, ctx)?;
                let function = if negate {
                    crate::BooleanFunction::UnaryNot
                } else {
                    crate::BooleanFunction::IsTrueWithNull
                };
                crate::eval_boolean_ready_in(function, ready, ctx)
            }
            Kernel::Logic(op) => {
                let left = logic_truthy(&argument(0)?, ctx)?;
                let function = match op {
                    BinaryOp::LogicAnd => crate::LogicalFunction::And,
                    BinaryOp::LogicOr => crate::LogicalFunction::Or,
                    _ => unreachable!("protobuf logic admits only AND/OR"),
                };
                if (op == BinaryOp::LogicAnd && left == Some(false))
                    || (op == BinaryOp::LogicOr && left == Some(true))
                {
                    return crate::eval_logical_ready_in(
                        function,
                        crate::LogicalArgs::UndemandedRight { left },
                        ctx,
                    );
                }
                let right = logic_truthy(&argument(1)?, ctx)?;
                crate::eval_logical_ready_in(function, crate::LogicalArgs::Both(left, right), ctx)
            }
            Kernel::IntegerMod { unsigned } => {
                // The four Go MOD signatures bake signedness into the builtin,
                // rather than reselecting it from each row or the SQL name.
                let operand = |index: usize| -> Result<Datum, EvalError> {
                    let value = argument(index)?;
                    Ok(match crate::arg_eval_type::eval_int(&value)? {
                        None => Datum::Null,
                        Some(value) if unsigned[index] => Datum::UInt(value as u64),
                        Some(value) => Datum::Int(value),
                    })
                };
                let left = operand(0)?;
                let right = operand(1)?;
                crate::ops::eval_binary_full(
                    BinaryOp::Mod,
                    left,
                    right,
                    ctx.div_precision_increment(),
                    function.derived_collation(),
                    crate::ops::Operands::LITERALS,
                    ctx,
                )
            }
            Kernel::Binary(op, domain) => {
                if args.len() != 2 {
                    return Err(EvalError::Unsupported("protobuf binary builtin arity"));
                }
                let left = eval_numeric_operand_row(&args[0], ctx, row, domain)?;
                if left.is_null() && !(op == BinaryOp::Mod && domain != EvalType::Decimal) {
                    return if matches!(
                        op,
                        BinaryOp::Plus
                            | BinaryOp::Minus
                            | BinaryOp::Mul
                            | BinaryOp::Mod
                            | BinaryOp::Div
                    ) {
                        crate::ops::eval_binary_arithmetic_null_in(ctx)
                    } else if matches!(op, BinaryOp::Eq | BinaryOp::Gt) {
                        // These are the only comparison signatures admitted
                        // above. Preserve PB's left-NULL stop before RHS demand.
                        crate::ops::eval_comparison_null_in(op, ctx)
                    } else {
                        Ok(Datum::Null)
                    };
                }
                let right = eval_numeric_operand_row(&args[1], ctx, row, domain)?;
                function.eval_binary_values(op, left, right, ctx)
            }
            Kernel::Cast { source, target } => {
                if args.len() != 1 {
                    return Err(EvalError::Unsupported("protobuf cast arity"));
                }
                let value = eval_numeric_row(&args[0], ctx, row, source)?;
                if value.is_null() {
                    return Ok(Datum::Null);
                }
                if target == EvalType::Json {
                    return cast_json_argument_value(function, value);
                }
                let field = function
                    .get_static_type()
                    .ok_or(EvalError::Unsupported("protobuf cast result type"))?;
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
                crate::cast::eval_cast(&cast, value, args[0].static_type(), ctx)
            }
            Kernel::Regexp => function.eval_regexp_like(ctx, row),
            kernel => {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    let value = arg.eval(ctx, row)?;
                    if value.is_null() && !matches!(kernel, Kernel::Json) {
                        if let Kernel::String {
                            operation: StringOp::Length,
                            binary,
                        } = kernel
                        {
                            // Preserve the existing child-demand order, but let
                            // the migrated nullable signature actually enter C4.
                            return eval_pb_char_length(&value, binary, ctx);
                        }
                        if self.signature == ScalarFuncSig::MicroSecond {
                            // Preserve the observed NULL boundary even for an
                            // otherwise invalid arity or an uncoerced prefix.
                            return crate::time_fn::microsecond_in(
                                std::slice::from_ref(&value),
                                ctx,
                            );
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
                                return crate::time_fn::calendar::hour_in(
                                    std::slice::from_ref(&value),
                                    ctx,
                                );
                            }
                            ScalarFuncSig::Minute => {
                                return crate::time_fn::calendar::minute_in(
                                    std::slice::from_ref(&value),
                                    ctx,
                                );
                            }
                            ScalarFuncSig::Second => {
                                return crate::time_fn::calendar::second_in(
                                    std::slice::from_ref(&value),
                                    ctx,
                                );
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
                            } if matches!(args.len(), 2 | 3) => {
                                use crate::tikv::{ReadyBytesArg, ReadyIntArg};
                                // Earlier children were evaluated, not coerced.
                                // Only this NULL is known at the ready boundary.
                                let null_index = values.len();
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
                                let len = (args.len() == 3).then(|| {
                                    if null_index == 2 {
                                        ReadyIntArg::Value(None)
                                    } else {
                                        ReadyIntArg::Undemanded
                                    }
                                });
                                return crate::string_fn::substring_ready_in(
                                    bytes, pos, len, binary, ctx,
                                );
                            }
                            _ => {}
                        }
                        // Keep this exact NULL child-demand boundary, but do
                        // not bypass the nullable migrated math call.
                        match self.signature {
                            ScalarFuncSig::Asin => {
                                return crate::math_fn::asin(std::slice::from_ref(&value), ctx)
                            }
                            ScalarFuncSig::Acos => {
                                return crate::math_fn::acos(std::slice::from_ref(&value), ctx)
                            }
                            ScalarFuncSig::Pow if args.len() == 2 => {
                                use crate::tikv::ReadyIeee754Arg::{Undemanded, Value};
                                // A right NULL suppresses numeric coercion of
                                // the already-evaluated left child as well.
                                let (left, right) = if values.is_empty() {
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
                    values.push(value);
                }
                match kernel {
                    Kernel::Values(eval) => eval(&values, ctx),
                    Kernel::Round => crate::math_fn::round_or_truncate_with_result_decimal(
                        &values,
                        true,
                        function.ret_type.as_ref().map(FieldType::decimal),
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
                            StringOp::Upper => crate::string_fn::case_convert_signature_in(
                                &values, true, binary, ctx,
                            ),
                            StringOp::Lower => crate::string_fn::case_convert_signature_in(
                                &values, false, binary, ctx,
                            ),
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
                                function.get_static_type().map(FieldType::decimal),
                            )
                        } else {
                            Ok(result)
                        }
                    }
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
                    _ => unreachable!("lazy kernels were handled before evaluating arguments"),
                }
            }
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

#[cfg(test)]
mod json_path_worker_tests {
    use super::*;
    use crate::expression::{Column, Constant, Expression};
    use crate::NoColumns;
    use tidb_datatype::{BinaryJSON, FieldTypeCode, FieldTypeFlags};

    #[test]
    fn protobuf_microsecond_keeps_context_and_observed_null_demand() {
        use std::cell::RefCell;
        struct Demand(RefCell<Vec<usize>>);
        impl Columns for Demand {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn get_param_value(&self, index: usize) -> Result<Datum, EvalError> {
                self.0.borrow_mut().push(index);
                if index == 1 {
                    return Err(EvalError::Unsupported("protobuf microsecond child"));
                }
                Ok(Datum::new_string("10:10:10.123456"))
            }
            fn append_warning(&self, _: u16, _: &str) {
                panic!("MICROSECOND suppresses duration parse errors")
            }
        }
        let int_type = FieldType::new(FieldTypeCode::LongLong);
        let text_type = FieldType::new(FieldTypeCode::VarString);
        let literal = |value| Expression::Constant(Constant::new(value, text_type.clone()));
        let child = |index| {
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("getparam"),
                text_type.clone(),
                vec![Expression::Constant(Constant::new(
                    Datum::Int(index),
                    int_type.clone(),
                ))],
            ))
        };
        let selected = |args| {
            ScalarFunction::from_pb(
                PbBuiltin::new(ScalarFuncSig::MicroSecond).unwrap(),
                int_type.clone(),
                args,
            )
        };
        let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        for slots in [1, 0] {
            let owner = crate::AsciiPoolOwner::new(
                crate::AsciiPoolPolicy::checked(
                    slots,
                    slots,
                    16 * 1024 * 1024,
                    4 * 1024 * 1024,
                    4 * 1024 * 1024,
                    64,
                    8,
                    4 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap();
            let execution = owner.begin_execution().unwrap();
            let ctx = Demand(RefCell::new(Vec::new()));
            for (args, reads, expected) in [
                (vec![child(0)], vec![0], Datum::Int(123456)),
                (vec![literal(Datum::new_string("bad"))], vec![], Datum::Null),
                (vec![literal(Datum::Null), child(1)], vec![], Datum::Null),
                (
                    vec![child(0), literal(Datum::Null), child(1)],
                    vec![0],
                    Datum::Null,
                ),
                (
                    vec![
                        literal(Datum::new_bytes(vec![0xff])),
                        literal(Datum::Null),
                        child(1),
                    ],
                    vec![],
                    Datum::Null,
                ),
            ] {
                let function = selected(args);
                let result = execution
                    .scope()
                    .with_columns(&ctx, |columns| function.eval(columns, empty.to_row()));
                if slots == 1 {
                    assert_eq!(result.unwrap(), expected);
                } else {
                    let error = result.expect_err("MicroSecond must retain its caller's scope");
                    let EvalError::ExpressionAdapterFailure(failure) = error else {
                        panic!("{error:?}")
                    };
                    assert_eq!(
                        failure.class(),
                        crate::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        crate::ExpressionAdapterFailureOrigin::Pool
                    );
                }
                assert_eq!(ctx.0.take(), reads);
            }
            for args in [vec![], vec![child(0), child(2)]] {
                let reads = if args.is_empty() { vec![] } else { vec![0, 2] };
                assert!(matches!(
                    execution
                        .scope()
                        .with_columns(&ctx, |columns| selected(args).eval(columns, empty.to_row())),
                    Err(EvalError::Unsupported("bad function arity"))
                ));
                assert_eq!(ctx.0.take(), reads);
            }
            assert!(matches!(
                execution
                    .scope()
                    .with_columns(&ctx, |columns| selected(vec![
                        child(1),
                        literal(Datum::Null)
                    ])
                    .eval(columns, empty.to_row())),
                Err(EvalError::Unsupported("protobuf microsecond child"))
            ));
            assert_eq!(ctx.0.take(), [1]);
        }
    }

    #[test]
    fn protobuf_date_keeps_hidden_clock_and_observed_null_demand() {
        use std::cell::Cell;
        use tidb_datatype::{DateModes, Time, TimeType};

        struct Modes(Cell<usize>);
        impl Columns for Modes {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn date_modes(&self) -> DateModes {
                self.0.set(self.0.get() + 1);
                DateModes::default()
            }
        }
        let date_type = FieldType::new(FieldTypeCode::Date).with_decimal(0);
        let datetime_type = FieldType::new(FieldTypeCode::Datetime).with_decimal(6);
        let text_type = FieldType::new(FieldTypeCode::VarString);
        let literal = |value: Datum, field: &FieldType| {
            Expression::Constant(Constant::new(value, field.clone()))
        };
        let selected = |args| {
            ScalarFunction::from_pb(
                PbBuiltin::new(ScalarFuncSig::Date).unwrap(),
                date_type.clone(),
                args,
            )
        };
        let pool = |slots| {
            crate::AsciiPoolOwner::new(
                crate::AsciiPoolPolicy::checked(
                    slots,
                    slots,
                    16 * 1024 * 1024,
                    4 * 1024 * 1024,
                    4 * 1024 * 1024,
                    64,
                    8,
                    4 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap()
        };
        let resource_error = |result: Result<Datum, EvalError>| {
            let error =
                result.expect_err("DATE's actual root must use the supplied zero-slot scope");
            let EvalError::ExpressionAdapterFailure(failure) = error else {
                panic!("DATE lost its infrastructure cause: {error:?}")
            };
            assert_eq!(
                failure.class(),
                crate::ExpressionAdapterFailureClass::PoolResource
            );
            assert_eq!(
                failure.origin(),
                crate::ExpressionAdapterFailureOrigin::Pool
            );
        };
        let bad_child = || {
            let mut cast_type = FieldType::new(FieldTypeCode::Json);
            cast_type.add_flags(FieldTypeFlags::PARSE_TO_JSON);
            Expression::ScalarFunction(ScalarFunction::from_pb(
                PbBuiltin::new(ScalarFuncSig::CastStringAsJson).unwrap(),
                cast_type,
                vec![literal(Datum::new_string("{"), &text_type)],
            ))
        };
        let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        for (owner, available) in [(pool(1), true), (pool(0), false)] {
            let execution = owner.begin_execution().unwrap();
            let ctx = Modes(Cell::new(0));
            for kind in [TimeType::Date, TimeType::DateTime] {
                // This is a real typed value, not a synthetic PB clock function.
                // DATE-kind storage can retain clock fields hidden by Display.
                let time = Time::from_date_checked(2024, 5, 6, 7, 8, 9, 987_654, kind, 6).unwrap();
                let field = if kind == TimeType::Date {
                    &date_type
                } else {
                    &datetime_type
                };
                let function = selected(vec![literal(Datum::Time(time), field)]);
                assert_eq!(function.pb_signature(), Some(ScalarFuncSig::Date));
                ctx.0.set(0);
                let result = execution
                    .scope()
                    .with_columns(&ctx, |columns| function.eval(columns, empty.to_row()));
                if available {
                    let Datum::Time(date) = result.unwrap() else {
                        panic!("DATE must return typed Time")
                    };
                    assert_eq!(date.kind(), TimeType::Date);
                    assert_eq!(date.fsp(), 0);
                    assert_eq!(date.to_string(), "2024-05-06");
                    let core = date.core_time();
                    assert_eq!(
                        (
                            core.hour(),
                            core.minute(),
                            core.second(),
                            core.microsecond()
                        ),
                        (0, 0, 0, 0)
                    );
                } else {
                    resource_error(result);
                }
                assert_eq!(ctx.0.get(), 1);
            }
            for with_prefix in [false, true] {
                let mut args = Vec::new();
                if with_prefix {
                    // The old early-NULL boundary never coerced this prefix or
                    // checked non-NULL DATE arity after observing its NULL child.
                    args.push(literal(Datum::new_string("uncoerced prefix"), &text_type));
                }
                args.push(literal(Datum::Null, &datetime_type));
                args.push(bad_child());
                let function = selected(args);
                ctx.0.set(0);
                let result = execution
                    .scope()
                    .with_columns(&ctx, |columns| function.eval(columns, empty.to_row()));
                if available {
                    assert_eq!(result.unwrap(), Datum::Null);
                } else {
                    resource_error(result);
                }
                assert_eq!(ctx.0.get(), 0, "observed NULL must not read DATE modes");
            }
            // In the other order, the original failing child remains primary;
            // a later NULL must not turn a child SQL error into a worker result.
            let function = selected(vec![bad_child(), literal(Datum::Null, &datetime_type)]);
            assert!(matches!(
                execution
                    .scope()
                    .with_columns(&ctx, |columns| function.eval(columns, empty.to_row())),
                Err(EvalError::Json(crate::JsonError::InvalidText))
            ));
        }
    }

    #[test]
    fn protobuf_json_merge_patch_keeps_three_arg_demand_and_worker_scope() {
        let json = |text: &str| Datum::Json(BinaryJSON::parse(text).unwrap());
        let json_type = FieldType::new(FieldTypeCode::Json);
        let text_type = FieldType::new(FieldTypeCode::VarString);
        let literal = |value: Datum, field: &FieldType| {
            Expression::Constant(Constant::new(value, field.clone()))
        };
        let selected = |args| {
            ScalarFunction::from_pb(
                PbBuiltin::new(ScalarFuncSig::JsonMergePatchSig).unwrap(),
                json_type.clone(),
                args,
            )
        };
        let pool = |slots| {
            crate::AsciiPoolOwner::new(
                crate::AsciiPoolPolicy::checked(
                    slots,
                    slots,
                    16 * 1024 * 1024,
                    4 * 1024 * 1024,
                    4 * 1024 * 1024,
                    64,
                    8,
                    4 * 1024 * 1024,
                )
                .unwrap(),
            )
            .unwrap()
        };
        let blocked_owner = pool(0);
        let blocked_execution = blocked_owner.begin_execution().unwrap();
        let owner = pool(1);
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        let function = selected(
            (0..3)
                .map(|index| {
                    let mut column = Column::new(index + 1, json_type.clone());
                    column.index = index;
                    Expression::Column(column)
                })
                .collect(),
        );
        assert_eq!(
            function.pb_signature(),
            Some(ScalarFuncSig::JsonMergePatchSig)
        );
        let cases = [
            (
                [
                    json(r#"{"a":1,"keep":true}"#),
                    json(r#"{"a":null}"#),
                    json(r#"{"b":2}"#),
                ],
                json(r#"{"b":2,"keep":true}"#),
            ),
            ([Datum::Null, json("{}"), json(r#"{"b":2}"#)], Datum::Null),
            ([Datum::Null, json("{}"), json("7")], json("7")),
            (
                [json(r#"{"a":1}"#), json(r#"{"a":2}"#), Datum::Null],
                Datum::Null,
            ),
            (
                [json("null"), json("{}"), json(r#"{"b":2}"#)],
                json(r#"{"b":2}"#),
            ),
        ];
        for (values, expected) in &cases {
            let row = tidb_chunk::mutrow::MutRow::from_datums(values);
            scope.with_columns(&NoColumns, |columns| {
                assert_eq!(function.eval(columns, row.to_row()).unwrap(), *expected);
            });
            // Plain JSON/NULL columns do not execute child workers: a zero-slot
            // failure therefore proves the selected PATCH root owns its result.
            blocked_execution
                .scope()
                .with_columns(&NoColumns, |columns| {
                    let error = function
                        .eval(columns, row.to_row())
                        .expect_err("PB PATCH, including SQL NULL, needs its worker");
                    let EvalError::ExpressionAdapterFailure(failure) = error else {
                        panic!("PB PATCH lost its infrastructure cause: {error:?}")
                    };
                    assert_eq!(
                        failure.class(),
                        crate::ExpressionAdapterFailureClass::PoolResource
                    );
                    assert_eq!(
                        failure.origin(),
                        crate::ExpressionAdapterFailureOrigin::Pool
                    );
                });
        }
        // Private pool accounting is covered by the SDK tests; this PB test
        // observes successful evaluation again after releasing its first scope.
        drop(scope);
        let row = tidb_chunk::mutrow::MutRow::from_datums(&cases[0].0);
        execution.scope().with_columns(&NoColumns, |columns| {
            assert_eq!(function.eval(columns, row.to_row()).unwrap(), cases[0].1);
        });

        let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        // PATCH prepares every document, even after SQL NULL. The real UTF-8
        // error precedes worker acquisition rather than becoming computed NULL.
        let bad_text = selected(vec![
            literal(Datum::Null, &json_type),
            literal(json("{}"), &json_type),
            literal(Datum::Bytes(vec![0xff]), &text_type),
        ]);
        blocked_execution
            .scope()
            .with_columns(&NoColumns, |columns| {
                assert!(matches!(
                    bad_text.eval(columns, empty.to_row()),
                    Err(EvalError::Unsupported("invalid UTF-8 string datum"))
                ));
            });
        // Kernel::Json must also demand a failing child after NULL, even when
        // the final scalar would replace every prior document during merging.
        let mut cast_type = json_type.clone();
        cast_type.add_flags(FieldTypeFlags::PARSE_TO_JSON);
        let bad_cast = Expression::ScalarFunction(ScalarFunction::from_pb(
            PbBuiltin::new(ScalarFuncSig::CastStringAsJson).unwrap(),
            cast_type,
            vec![literal(Datum::new_string("{"), &text_type)],
        ));
        let bad_child = selected(vec![
            literal(Datum::Null, &json_type),
            bad_cast,
            literal(json("7"), &json_type),
        ]);
        blocked_execution
            .scope()
            .with_columns(&NoColumns, |columns| {
                assert!(matches!(
                    bad_child.eval(columns, empty.to_row()),
                    Err(EvalError::Json(crate::JsonError::InvalidText))
                ));
            });
    }

    #[test]
    fn protobuf_json_replace_append_keep_five_arg_cast_demand_and_worker_scope() {
        let json = |text: &str| Datum::Json(BinaryJSON::parse(text).unwrap());
        let json_type = FieldType::new(FieldTypeCode::Json);
        let text_type = FieldType::new(FieldTypeCode::VarString);
        let literal = |value: Datum, field: &FieldType| {
            Expression::Constant(Constant::new(value, field.clone()))
        };
        let selected = |sig, args| {
            ScalarFunction::from_pb(PbBuiltin::new(sig).unwrap(), json_type.clone(), args)
        };
        let owner = crate::AsciiPoolOwner::new(
            crate::AsciiPoolPolicy::checked(
                0,
                0,
                16 * 1024 * 1024,
                4 * 1024 * 1024,
                4 * 1024 * 1024,
                64,
                8,
                4 * 1024 * 1024,
            )
            .unwrap(),
        )
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        // These are the two EXISTING PB signatures, each with the admitted
        // five-argument shape. Other JSON signatures are deliberately absent.
        for (sig, document, expected, null_value) in [
            (
                ScalarFuncSig::JsonReplaceSig,
                r#"{"a":1}"#,
                r#"{"a":9}"#,
                r#"{"a":null}"#,
            ),
            (
                ScalarFuncSig::JsonArrayAppendSig,
                r#"{"a":[1]}"#,
                r#"{"a":[1,9]}"#,
                r#"{"a":[1,null]}"#,
            ),
        ] {
            for cached_paths in [false, true] {
                for (doc, path, value, want) in [
                    (
                        json(document),
                        Datum::new_string("$.a"),
                        json("9"),
                        json(expected),
                    ),
                    (
                        json(r#"{"z":0}"#),
                        Datum::new_string("$.a"),
                        json("9"),
                        json(r#"{"z":0}"#),
                    ),
                    (
                        Datum::Null,
                        Datum::new_string("$.a"),
                        json("9"),
                        Datum::Null,
                    ),
                    (json(document), Datum::Null, json("9"), Datum::Null),
                    (
                        json(document),
                        Datum::new_string("$.a"),
                        Datum::Null,
                        json(null_value),
                    ),
                ] {
                    let values = [doc, path, value, Datum::new_string("$.missing"), json("2")];
                    let fields = [&json_type, &text_type, &json_type, &text_type, &json_type];
                    let args = fields
                        .iter()
                        .enumerate()
                        .map(|(index, field)| {
                            if cached_paths && (index == 1 || index == 3) {
                                literal(values[index].clone(), field)
                            } else {
                                let mut column = Column::new(index as i64 + 1, (*field).clone());
                                column.index = index as i64;
                                Expression::Column(column)
                            }
                        })
                        .collect();
                    let function = selected(sig, args);
                    assert_eq!(function.pb_signature(), Some(sig));
                    let row = tidb_chunk::mutrow::MutRow::from_datums(&values);
                    assert_eq!(
                        function.eval(&NoColumns, row.to_row()).unwrap(),
                        want,
                        "{sig:?}, cached={cached_paths}"
                    );
                    // Reuse the same selected node after warming its path cache;
                    // document/path NULL and no-op results still need the root worker.
                    execution.scope().with_columns(&NoColumns, |columns| {
                        let error = function
                            .eval(columns, row.to_row())
                            .expect_err("selected JSON PB requires worker");
                        let EvalError::ExpressionAdapterFailure(failure) = error else {
                            panic!("PB JSON lost infrastructure cause: {error:?}")
                        };
                        assert_eq!(
                            failure.class(),
                            crate::ExpressionAdapterFailureClass::PoolResource
                        );
                        assert_eq!(
                            failure.origin(),
                            crate::ExpressionAdapterFailureOrigin::Pool
                        );
                    });
                }
            }
            for (parse_document, replace_want, append_want) in [
                (false, r#"{"a":"1"}"#, r#"{"a":["1"]}"#),
                (true, r#"{"a":1}"#, r#"{"a":[1]}"#),
            ] {
                let mut cast_type = json_type.clone();
                if parse_document {
                    cast_type.add_flags(FieldTypeFlags::PARSE_TO_JSON);
                }
                let cast = Expression::ScalarFunction(ScalarFunction::from_pb(
                    PbBuiltin::new(ScalarFuncSig::CastStringAsJson).unwrap(),
                    cast_type,
                    vec![literal(Datum::new_string("1"), &text_type)],
                ));
                let (doc, want) = if sig == ScalarFuncSig::JsonReplaceSig {
                    (r#"{"a":0}"#, replace_want)
                } else {
                    (r#"{"a":[]}"#, append_want)
                };
                let function = selected(
                    sig,
                    vec![
                        literal(json(doc), &json_type),
                        literal(Datum::new_string("$.a"), &text_type),
                        cast,
                        literal(Datum::new_string("$.missing"), &text_type),
                        literal(json("2"), &json_type),
                    ],
                );
                let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
                assert_eq!(
                    function.eval(&NoColumns, empty.to_row()).unwrap(),
                    json(want)
                );
            }
            // Kernel::Json eagerly evaluates every child even after document
            // and path NULL. The real cast error must win before the value kernel.
            let mut cast_type = json_type.clone();
            cast_type.add_flags(FieldTypeFlags::PARSE_TO_JSON);
            let bad_cast = Expression::ScalarFunction(ScalarFunction::from_pb(
                PbBuiltin::new(ScalarFuncSig::CastStringAsJson).unwrap(),
                cast_type,
                vec![literal(Datum::new_string("{"), &text_type)],
            ));
            let function = selected(
                sig,
                vec![
                    literal(Datum::Null, &json_type),
                    literal(Datum::Null, &text_type),
                    bad_cast,
                    literal(Datum::new_string("$.missing"), &text_type),
                    literal(json("2"), &json_type),
                ],
            );
            let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
            assert!(matches!(
                function.eval(&NoColumns, empty.to_row()),
                Err(EvalError::Json(crate::JsonError::InvalidText))
            ));
        }
    }
}
