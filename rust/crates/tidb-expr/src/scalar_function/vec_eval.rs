// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `Expression.VecEval*`: typed argument columns and signature kernels.

use super::{Kernel, ScalarFunction};
use crate::expression::Expression;
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column, row::Row};
use tidb_datatype::{Datum, Decimal, EvalType, FieldType, FieldTypeCode};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `ScalarFunction.VecEval*`: whether this signature has a migrated kernel.
pub(crate) fn has_kernel(function: &ScalarFunction) -> bool {
    if supports_vec(function) {
        return true;
    }
    if function
        .get_static_type()
        .is_some_and(|f| f.eval_type() == EvalType::VectorFloat32)
        || function.args.iter().any(|a| {
            a.static_type()
                .is_some_and(|f| f.eval_type() == EvalType::VectorFloat32)
        })
    {
        return false;
    }
    matches!(
        function.pb_builtin.kernel,
        Kernel::RpnOnly(..)
            | Kernel::Binary(..)
            | Kernel::IntegerMultiply { .. }
            | Kernel::IntegerMod { .. }
            | Kernel::Compare(..)
            | Kernel::In(_)
            | Kernel::Case(_)
            | Kernel::Coalesce(_)
            | Kernel::If(_)
            | Kernel::IfNull(_)
            | Kernel::Cast { .. }
            | Kernel::Logic(_)
            | Kernel::Unary(_)
            | Kernel::IsNull(_)
            | Kernel::Truth { .. }
    ) || supports_math(function.pb_code())
        || supports_string(function)
        || supports_other(function)
        || supports_miscellaneous(function)
        || supports_time(function)
        || supports_encryption(function)
        || supports_like(function)
        || supports_info(function)
        || supports_json(function)
        || supports_compare(function.pb_code())
}

// Vector eligibility is part of the Go expression contract. Execution uses
// the prepared RPN program and these declarations describe supported APIs.
fn supports_vec(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        VecDimsSig
            | VecL1DistanceSig
            | VecL2DistanceSig
            | VecNegativeInnerProductSig
            | VecCosineDistanceSig
            | VecL2NormSig
            | VecAsTextSig
    ) || function.func_name.lowercase() == "vec_from_text"
}

fn supports_compare(sig: ScalarFuncSig) -> bool {
    use ScalarFuncSig::*;
    matches!(
        sig,
        GreatestInt
            | GreatestReal
            | GreatestDecimal
            | GreatestString
            | GreatestTime
            | GreatestDate
            | GreatestDuration
            | GreatestCmpStringAsTime
            | GreatestCmpStringAsDate
            | LeastInt
            | LeastReal
            | LeastDecimal
            | LeastString
            | LeastTime
            | LeastDate
            | LeastDuration
            | LeastCmpStringAsTime
            | LeastCmpStringAsDate
            | IntervalInt
            | IntervalReal
    )
}

fn supports_math(sig: ScalarFuncSig) -> bool {
    use ScalarFuncSig::*;
    matches!(
        sig,
        AbsInt
            | AbsUInt
            | AbsReal
            | AbsDecimal
            | Sqrt
            | Exp
            | Log1Arg
            | Log2Args
            | Log2
            | Log10
            | Radians
            | Degrees
            | Sign
            | CeilIntToInt
            | CeilIntToDec
            | CeilDecToInt
            | CeilDecToDec
            | CeilReal
            | FloorIntToInt
            | FloorIntToDec
            | FloorDecToInt
            | FloorDecToDec
            | FloorReal
            | Acos
            | Asin
            | Atan1Arg
            | Atan2Args
            | Cos
            | Cot
            | Sin
            | Tan
            | Pow
            | Pi
            | RoundInt
            | RoundReal
            | RoundDec
            | RoundWithFracInt
            | RoundWithFracReal
            | RoundWithFracDec
            | TruncateInt
            | TruncateUint
            | TruncateReal
            | TruncateDecimal
            | Rand
            | RandWithSeedFirstGen
    )
}

fn supports_string(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        CharLength
            | CharLengthUtf8
            | Lower
            | LowerUtf8
            | Upper
            | UpperUtf8
            | Substring2Args
            | Substring3Args
            | Substring2ArgsUtf8
            | Substring3ArgsUtf8
            | ToBinary
            | FromBinary
            | Reverse
            | ReverseUtf8
            | Strcmp
            | Elt
            | FieldInt
            | FieldReal
            | FieldString
            | HexStrArg
            | HexIntArg
            | Right
            | RightUtf8
            | Space
            | Concat
            | ConcatWs
            | Bin
            | SubstringIndex
            | ToBase64
            | FromBase64
            | Quote
            | Ord
            | Repeat
            | FindInSet
            | Left
            | LeftUtf8
            | Lpad
            | LpadUtf8
            | Rpad
            | RpadUtf8
            | Locate2Args
            | Locate2ArgsUtf8
            | Locate3Args
            | Locate3ArgsUtf8
            | Instr
            | InstrUtf8
            | OctInt
            | OctString
            | ExportSet3Arg
            | ExportSet4Arg
            | ExportSet5Arg
            | MakeSet
            | Format
            | FormatWithLocale
            | Insert
            | InsertUtf8
            | Trim1Arg
            | Trim2Args
            | Trim3Args
            | Convert
            | Char
    ) || function.func_name.lowercase() == "translate"
}

fn supports_json(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        JsonTypeSig
            | JsonUnquoteSig
            | JsonLengthSig
            | JsonObjectSig
            | JsonArraySig
            | JsonMergeSig
            | JsonSetSig
            | JsonInsertSig
            | JsonRemoveSig
            | JsonContainsSig
            | JsonMemberOfSig
            | JsonReplaceSig
            | JsonArrayAppendSig
            | JsonMergePatchSig
            | JsonExtractSig
            | JsonMergePreserveSig
            | JsonArrayInsertSig
            | JsonContainsPathSig
            | JsonDepthSig
            | JsonKeysSig
            | JsonKeys2ArgsSig
            | JsonPrettySig
            | JsonQuoteSig
            | JsonSearchSig
            | JsonStorageFreeSig
            | JsonStorageSizeSig
    ) || matches!(
        function.func_name.lowercase(),
        "json_overlaps" | "json_sum_crc32"
    )
}

pub(super) fn supports_info(function: &ScalarFunction) -> bool {
    if let crate::builtin::SigFields::Benchmark { const_loop_count } = function.sig {
        return const_loop_count > 0;
    }
    matches!(
        function.func_name.lowercase(),
        "database"
            | "schema"
            | "connection_id"
            | "tidb_version"
            | "row_count"
            | "current_user"
            | "current_resource_group"
            | "current_role"
            | "user"
            | "session_user"
            | "system_user"
            | "tidb_is_ddl_owner"
            | "found_rows"
            | "last_insert_id"
            | "version"
            | "tidb_decode_key"
    )
}

fn supports_like(function: &ScalarFunction) -> bool {
    matches!(
        function.pb_code(),
        ScalarFuncSig::LikeSig | ScalarFuncSig::IlikeSig
    )
}

fn supports_encryption(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        AesEncrypt
            | AesEncryptIv
            | AesDecrypt
            | AesDecryptIv
            | Compress
            | Uncompress
            | UncompressedLength
            | Encode
            | Decode
    ) || matches!(
        function.func_name.lowercase(),
        "password" | "random_bytes" | "sm3" | "validate_password_strength"
    )
}

fn supports_time(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    if matches!(
        function.sig,
        crate::builtin::SigFields::TimeLiteral { .. }
            | crate::builtin::SigFields::DurationLiteral { .. }
    ) {
        return true;
    }
    matches!(
        function.pb_builtin.kernel,
        super::Kernel::DateArithmetic { .. }
    ) || matches!(
        function.pb_code(),
        Month
            | Year
            | Date
            | FromUnixTime1Arg
            | FromUnixTime2Arg
            | ExtractDatetime
            | ExtractDuration
            | DayName
            | WeekDay
            | TimeFormat
            | UtcTimeWithArg
            | UtcTimeWithoutArg
            | UnixTimestampCurrent
            | YearWeekWithoutMode
            | YearWeekWithMode
            | PeriodDiff
            | PeriodAdd
            | NowWithArg
            | NowWithoutArg
            | GetFormat
            | LastDay
            | StrToDateDate
            | StrToDateDatetime
            | StrToDateDuration
            | FromDays
            | MicroSecond
            | Quarter
            | WeekWithMode
            | WeekWithoutMode
            | ToSeconds
            | Minute
            | Second
            | MakeDate
            | WeekOfYear
            | UtcTimestampWithArg
            | UtcTimestampWithoutArg
            | TimeToSec
            | UtcDate
            | UnixTimestampDec
            | UnixTimestampInt
            | TimestampAdd
            | ToDays
            | DateFormatSig
            | Hour
            | SecToTime
            | DateDiff
            | CurrentDate
            | MakeTime
            | DayOfYear
            | TimestampDiff
            | CurrentTime0Arg
            | CurrentTime1Arg
            | Time
            | MonthName
            | SysDateWithFsp
            | SysDateWithoutFsp
            | DayOfWeek
            | ConvertTz
            | Timestamp1Arg
            | Timestamp2Args
            | DayOfMonth
            | DurationDurationTimeDiff
            | DurationStringTimeDiff
            | StringDurationTimeDiff
            | StringStringTimeDiff
            | StringTimeTimeDiff
            | TimeStringTimeDiff
            | TimeTimeTimeDiff
            | NullTimeDiff
    ) || crate::time_fn::add_sub::kinds_of_signature(function.pb_code()).is_some()
        || matches!(
            function.func_name.lowercase(),
            "tidb_bounded_staleness"
                | "tidb_parse_tso"
                | "period_add"
                | "period_diff"
                | "curdate"
                | "current_date"
                | "utc_date"
        )
}

fn supports_miscellaneous(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        InetAton
            | InetNtoa
            | Inet6Aton
            | Inet6Ntoa
            | IsIPv4
            | IsIPv6
            | IsIPv4Compat
            | IsIPv4Mapped
            | IsUuid
            | Uuid
            | UuiDv4
            | UuiDv7
            | UuidVersion
            | UuidTimestamp
            | VitessHash
            | DecimalAnyValue
            | DurationAnyValue
            | IntAnyValue
            | JsonAnyValue
            | RealAnyValue
            | StringAnyValue
            | TimeAnyValue
    ) || matches!(
        function.func_name.lowercase(),
        "name_const" | "uuid_v4" | "uuid_v7" | "uuid_to_bin" | "bin_to_uuid" | "sleep"
    )
}

fn supports_other(function: &ScalarFunction) -> bool {
    match function.func_name.lowercase() {
        "bit_count" | "getparam" | "row" => true,
        "getvar" | "setvar" => function.get_static_type().is_some_and(|field| {
            matches!(
                field.eval_type(),
                EvalType::Int | EvalType::Real | EvalType::Decimal | EvalType::String
            )
        }),
        _ => false,
    }
}

/// Go `evalOneVec`: evaluate directly into the projection's owned result column.
pub(crate) fn eval_one(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &Chunk,
    output: &mut Chunk,
    index: usize,
) -> Result<bool, EvalError> {
    if !crate::chunk_executor::vectorized(expression) {
        return Ok(false);
    }
    let field = expression.static_type().expect("vector expression type");
    eval_into(
        expression,
        field.eval_type(),
        ctx,
        input,
        &mut output.column_mut(index),
    )?;
    if matches!(
        field.code(),
        FieldTypeCode::Bit | FieldTypeCode::Float | FieldTypeCode::Enum | FieldTypeCode::Set
    ) {
        let source = std::mem::take(&mut *output.column_mut(index));
        let mut destination = Column::new_column(field, input.num_rows());
        match field.code() {
            FieldTypeCode::Float => {
                destination.resize_float32(input.num_rows(), false);
                destination.merge_nulls(&[&source]);
                destination.with_float32s_mut(|values| {
                    for (i, value) in values.iter_mut().enumerate() {
                        *value = source.get_float64(i) as f32;
                    }
                });
            }
            FieldTypeCode::Bit => {
                destination.reserve_bytes(input.num_rows());
                let width = u8::try_from((field.flen() + 7) >> 3)
                    .ok()
                    .and_then(|width| tidb_datatype::BinaryLiteralWidth::try_from(width).ok());
                for i in 0..input.num_rows() {
                    if source.is_null(i) {
                        destination.append_null();
                    } else {
                        destination.append_bytes(
                            tidb_datatype::BinaryLiteral::from_uint(source.get_uint64(i), width)
                                .as_bytes(),
                        );
                    }
                }
            }
            FieldTypeCode::Enum | FieldTypeCode::Set => {
                let elements = field.elems_snapshot();
                let collation = tidb_datatype::get_collator(field.collation_name());
                if field.code() == FieldTypeCode::Enum {
                    destination.reserve_enum(input.num_rows());
                } else {
                    destination.reserve_set(input.num_rows());
                }
                for i in 0..input.num_rows() {
                    if source.is_null(i) {
                        destination.append_null();
                        continue;
                    }
                    let bytes = source.get_bytes(i);
                    if field.code() == FieldTypeCode::Enum {
                        destination.append_enum(
                            &tidb_datatype::parse_enum_name(
                                &elements,
                                bytes.as_ref(),
                                collation.clone(),
                            )
                            .unwrap_or_else(|error| error.returned_value()),
                        );
                    } else {
                        destination.append_set(
                            &tidb_datatype::parse_set_name(
                                &elements,
                                bytes.as_ref(),
                                collation.clone(),
                            )
                            .unwrap_or_default(),
                        );
                    }
                }
            }
            _ => unreachable!(),
        }
        *output.column_mut(index) = destination;
    }
    Ok(true)
}

/// Go `Expression.VecEval*`, including `Constant` broadcast and `Column.CopyReconstruct`.
pub(crate) fn eval_into(
    expression: &Expression,
    target: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    match expression {
        Expression::Constant(constant) => {
            if let Some(deferred) = constant.deferred_expr.as_deref() {
                return eval_into(deferred, target, ctx, input, result);
            }
            if input.num_rows() == 0 {
                result.reset_for_eval_type(target);
                return Ok(());
            }
            let variable = matches!(
                target,
                EvalType::String | EvalType::Json | EvalType::VectorFloat32
            );
            if variable {
                crate::chunk_executor::allocate(target, input.num_rows(), result);
            }
            let value = constant.eval_typed_as_on_row(ctx, Row::empty(), target)?;
            if !variable {
                crate::chunk_executor::allocate(target, input.num_rows(), result);
            }
            for i in 0..input.num_rows() {
                put(result, i, target, &value)?;
            }
            return Ok(());
        }
        Expression::CorrelatedColumn(column) => {
            if input.num_rows() == 0 {
                result.reset_for_eval_type(target);
                return Ok(());
            }
            let variable = matches!(
                target,
                EvalType::String | EvalType::Json | EvalType::VectorFloat32
            );
            if variable {
                crate::chunk_executor::allocate(target, input.num_rows(), result);
            }
            let value = column.eval_typed(ctx, target)?;
            if !variable {
                crate::chunk_executor::allocate(target, input.num_rows(), result);
            }
            for i in 0..input.num_rows() {
                put(result, i, target, &value)?;
            }
            return Ok(());
        }
        Expression::Column(column) => {
            let field = column.get_static_type().expect("typed column");
            let index = usize::try_from(column.index)
                .ok()
                .filter(|&i| i < input.num_cols())
                .ok_or(EvalError::Unsupported(
                    "column index is outside the input row",
                ))?;
            let time = |ty| matches!(ty, EvalType::Datetime | EvalType::Timestamp);
            if !field.is_hybrid()
                && field.code() != FieldTypeCode::Float
                && (field.eval_type() == target || (time(field.eval_type()) && time(target)))
            {
                *result = input
                    .column(index)
                    .copy_reconstruct(input.sel(), Some(std::mem::take(result)));
                return Ok(());
            }
        }
        Expression::ScalarFunction(function) if has_kernel(function) => {
            if function.get_static_type().is_some_and(|field| field.eval_type() == target) {
                let mut row = 0;
                if crate::rpn::PreparedExpression::try_eval_typed_batch(expression, ctx, input, |value| {
                    if row == 0 {
                        crate::chunk_executor::allocate(target, input.num_rows(), result);
                    }
                    put(result, row, target, &value)?;
                    row += 1;
                    Ok(())
                })? {
                    if row == 0 {
                        crate::chunk_executor::allocate(target, 0, result);
                    }
                    return Ok(());
                }
            }
        }
        Expression::ScalarSubQuery(_) => return Err(crate::scalar_subquery::vec_eval_error()),
        _ => {}
    }
    // Go `Column.VecEval*`: hybrid columns and FLOAT widening read typed
    // cells; ordinary columns have already returned through CopyReconstruct.
    if !matches!(expression, Expression::Column(_))
        && !matches!(expression, Expression::ScalarFunction(function) if has_kernel(function))
    {
        return Err(EvalError::Unsupported(
            "signature has no vectorized implementation",
        ));
    }
    if target == EvalType::Int
        && expression
            .static_type()
            .is_some_and(|field| field.is_hybrid())
    {
        result.resize_int64(0, false);
        for row in 0..input.num_rows() {
            let value = super::eval_numeric_row(expression, ctx, input.get_row(row), target)?;
            match crate::arg_eval_type::eval_int(&value)? {
                Some(value) => result.append_int64(value),
                None => result.append_null(),
            }
        }
        return Ok(());
    }
    crate::chunk_executor::allocate(target, input.num_rows(), result);
    for i in 0..input.num_rows() {
        let value = super::eval_numeric_row(expression, ctx, input.get_row(i), target)?;
        put(result, i, target, &value)?;
    }
    Ok(())
}

pub(crate) fn value(
    column: &Column,
    index: usize,
    target: EvalType,
    field: Option<&FieldType>,
) -> Datum {
    if column.is_null(index) {
        return Datum::Null;
    }
    match target {
        EvalType::Int if field.is_some_and(FieldType::is_unsigned) => {
            Datum::UInt(column.get_uint64(index))
        }
        EvalType::Int => Datum::Int(column.get_int64(index)),
        EvalType::Real => Datum::Real(column.get_float64(index)),
        EvalType::Decimal => {
            Datum::Decimal(Decimal::from_my_decimal(&column.get_my_decimal(index)))
        }
        EvalType::Datetime | EvalType::Timestamp => Datum::Time(column.get_time(index)),
        EvalType::Duration => {
            Datum::Duration(column.get_duration(index, field.map_or(0, FieldType::decimal)))
        }
        EvalType::String if field.is_some_and(FieldType::is_binary_string) => {
            Datum::Bytes(column.get_bytes(index).as_ref().to_vec())
        }
        EvalType::String => Datum::new_string_datum(column.get_bytes(index).as_ref().to_vec()),
        EvalType::Json => Datum::Json(column.get_json(index)),
        EvalType::VectorFloat32 => Datum::VectorFloat32(column.get_vector_float32(index)),
    }
}

/// Go typed result writes: fixed cells are updated in place, variable cells appended.
pub(crate) fn put(
    column: &mut Column,
    index: usize,
    target: EvalType,
    value: &Datum,
) -> Result<(), EvalError> {
    if matches!(
        target,
        EvalType::String | EvalType::Json | EvalType::VectorFloat32
    ) {
        match value {
            Datum::Null => column.append_null(),
            value if target == EvalType::String => column
                .append_bytes(&crate::arg_eval_type::eval_string(value)?.expect("non-NULL string")),
            Datum::Json(value) => column.append_json(value),
            Datum::VectorFloat32(value) => column.append_vector_float32(value),
            _ => {
                return Err(EvalError::Unsupported(
                    "vector result has wrong variable type",
                ))
            }
        }
        return Ok(());
    }
    column.set_null(index, value.is_null());
    match value {
        Datum::Null => {}
        Datum::Int(value) => {
            column.with_cell_bytes_mut(index, |cell| cell.copy_from_slice(&value.to_ne_bytes()))
        }
        Datum::UInt(value) => {
            column.with_cell_bytes_mut(index, |cell| cell.copy_from_slice(&value.to_ne_bytes()))
        }
        Datum::Real(value) | Datum::Float32(value) => {
            column.with_cell_bytes_mut(index, |cell| cell.copy_from_slice(&value.to_ne_bytes()))
        }
        Datum::Decimal(value) => column.with_cell_bytes_mut(index, |cell| {
            cell.copy_from_slice(&value.to_chunk_my_decimal_lossy().to_raw_bytes())
        }),
        Datum::Time(value) => column.with_cell_bytes_mut(index, |cell| {
            cell.copy_from_slice(&value.go_raw().to_ne_bytes())
        }),
        Datum::Duration(value) => column.with_cell_bytes_mut(index, |cell| {
            cell.copy_from_slice(&value.nanoseconds().to_ne_bytes())
        }),
        _ => return Err(EvalError::Unsupported("vector result has wrong fixed type")),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
