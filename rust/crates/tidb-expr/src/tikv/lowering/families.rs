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

//! Local-only mappings to TiKV's existing temporal, JSON and vector kernels.
//! These are shape-gated candidates, not distributed-pushdown policy. The TiKV
//! builder still validates every signature, argument domain and metadata item.

use tidb_datatype::{EvalType, FieldType, FieldTypeCode};
use tidb_proto::tipb::{Expr as PbExpr, ExprType};

use crate::scalar_function::ScalarFunction;

use super::{cast_args, node};

/// A shape refusal from these families must not fall through to the distributed
/// catalog: its narrower policy does not imply the same local context guarantees.
pub(super) fn owns(name: &str) -> bool {
    name.starts_with("date_add_")
        || name.starts_with("date_sub_")
        || name.starts_with("json_")
        || name.starts_with("vec_")
        || matches!(
            name,
            "date"
                | "date_format"
                | "day"
                | "dayofmonth"
                | "dayofweek"
                | "dayofyear"
                | "weekday"
                | "weekofyear"
                | "week"
                | "yearweek"
                | "month"
                | "monthname"
                | "dayname"
                | "year"
                | "quarter"
                | "to_days"
                | "to_seconds"
                | "datediff"
                | "last_day"
                | "from_days"
                | "makedate"
                | "maketime"
                | "period_add"
                | "period_diff"
                | "hour"
                | "minute"
                | "second"
                | "microsecond"
                | "time_to_sec"
                | "unix_timestamp"
                | "from_unixtime"
                | "str_to_date"
                | "timestampdiff"
                | "timediff"
                | "addtime"
                | "subtime"
        )
}

pub(super) fn lower(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    let name = function.func_name.lowercase();
    let ty = function.ret_type.as_ref()?;
    let types = function
        .args
        .iter()
        .map(|arg| arg.static_type().map(|ty| domain(ty.eval_type())))
        .collect::<Option<Vec<_>>>()?;
    if types.len() != children.len() {
        return None;
    }
    if name.starts_with("date_add_") || name.starts_with("date_sub_") {
        return date_arithmetic(name, children, &types, ty);
    }
    if name.starts_with("json_") {
        return json(name, children, &types, ty);
    }
    if name.starts_with("vec_") {
        return vector(name, children, &types, ty);
    }
    temporal(function, children, &types, ty)
}

fn domain(eval: EvalType) -> EvalType {
    match eval {
        EvalType::Timestamp => EvalType::Datetime,
        other => other,
    }
}

// A duration-to-datetime cast fills in today's date. The facade does not carry
// TiDB's statement clock, so even an otherwise ordinary temporal function must
// not smuggle that context-dependent cast into its argument tree.
fn temporal_args(
    children: Vec<PbExpr>,
    types: &[EvalType],
    targets: &[EvalType],
) -> Option<Vec<PbExpr>> {
    if types.len() != targets.len()
        || types
            .iter()
            .zip(targets)
            .any(|(from, to)| *from == EvalType::Duration && *to == EvalType::Datetime)
    {
        return None;
    }
    cast_args(children, targets)
}

fn temporal(
    function: &ScalarFunction,
    children: Vec<PbExpr>,
    types: &[EvalType],
    ty: &FieldType,
) -> Option<PbExpr> {
    use EvalType::{Datetime as T, Decimal as D, Duration as H, Int as I, Real as R, String as S};
    let name = function.func_name.lowercase();
    let (sig, targets): (&str, &[EvalType]) = match name {
        "date" => ("Date", &[T]),
        "date_format" => ("DateFormatSig", &[T, S]),
        "day" | "dayofmonth" => ("DayOfMonth", &[T]),
        "dayofweek" => ("DayOfWeek", &[T]),
        "dayofyear" => ("DayOfYear", &[T]),
        "weekday" => ("WeekDay", &[T]),
        "weekofyear" => ("WeekOfYear", &[T]),
        "week" if children.len() == 2 => ("WeekWithMode", &[T, I]),
        // Unlike WEEK, Go YEARWEEK's missing mode is always zero, not the
        // session default_week_format (builtin_time.go yearWeekFunctionClass).
        "yearweek" if children.len() == 1 => ("YearWeekWithoutMode", &[T]),
        "yearweek" if children.len() == 2 => ("YearWeekWithMode", &[T, I]),
        "month" => ("Month", &[T]),
        "monthname" => ("MonthName", &[T]),
        "dayname" => ("DayName", &[T]),
        "year" => ("Year", &[T]),
        "quarter" => ("Quarter", &[T]),
        "to_days" => ("ToDays", &[T]),
        "to_seconds" => ("ToSeconds", &[T]),
        "datediff" => ("DateDiff", &[T, T]),
        "last_day" => ("LastDay", &[T]),
        "from_days" => ("FromDays", &[I]),
        "makedate" => ("MakeDate", &[I, I]),
        "maketime" => ("MakeTime", &[I, I, R]),
        "period_add" => ("PeriodAdd", &[I, I]),
        "period_diff" => ("PeriodDiff", &[I, I]),
        // These read Duration, not Datetime. Preserve Go's WrapWithCastAsDuration.
        "hour" => ("Hour", &[H]),
        "minute" => ("Minute", &[H]),
        "second" => ("Second", &[H]),
        "microsecond" => ("MicroSecond", &[H]),
        "time_to_sec" => ("TimeToSec", &[H]),
        "unix_timestamp" if children.len() == 1 => {
            let sig = match ty.eval_type() {
                I => "UnixTimestampInt",
                D => "UnixTimestampDec",
                _ => return None,
            };
            (sig, &[T])
        }
        "from_unixtime" if matches!(types, [I | D]) => ("FromUnixTime1Arg", &[D]),
        "from_unixtime" if matches!(types, [I | D, S]) => ("FromUnixTime2Arg", &[D, S]),
        // String/real FROM_UNIXTIME inputs need Go's precision-adjusted decimal
        // cast; a generic cast can truncate fractional seconds, so stay native.
        "str_to_date" => {
            let sig = match ty.code() {
                FieldTypeCode::Date => "StrToDateDate",
                FieldTypeCode::Datetime => "StrToDateDatetime",
                FieldTypeCode::Duration => "StrToDateDuration",
                _ => return None,
            };
            (sig, &[S, S])
        }
        "timestampdiff" if children.len() == 3 => {
            let mut children = temporal_args(children, types, &[S, T, T])?;
            let first = children.first_mut()?;
            let unit = literal_unit(first)?;
            if !matches!(
                unit.as_str(),
                "MICROSECOND"
                    | "SECOND"
                    | "MINUTE"
                    | "HOUR"
                    | "DAY"
                    | "WEEK"
                    | "MONTH"
                    | "QUARTER"
                    | "YEAR"
            ) {
                return None;
            }
            first.val = Some(unit.into_bytes());
            return node("TimestampDiff", children, ty);
        }
        "timediff" => return time_diff(children, types, ty),
        "addtime" | "subtime" => return add_sub_time(function, children, types, ty),
        // Current clocks, mode-less WEEK, unsupported enum-only functions and
        // current-date duration conversions intentionally remain native.
        _ => return None,
    };
    node(sig, temporal_args(children, types, targets)?, ty)
}

fn time_diff(children: Vec<PbExpr>, types: &[EvalType], ty: &FieldType) -> Option<PbExpr> {
    use EvalType::{Duration as H, String as S};
    let sig = match types {
        [H, H] => "DurationDurationTimeDiff",
        [H, S] => "DurationStringTimeDiff",
        [S, H] => "StringDurationTimeDiff",
        [S, S] => "StringStringTimeDiff",
        // TimeTimeTimeDiff/TimeStringTimeDiff/StringTimeTimeDiff are enum-only
        // in this TiKV RPN dispatcher. NullTimeDiff takes zero children in TiKV,
        // unlike Go's two-argument lazy-null builtin; don't drop their effects.
        _ => return None,
    };
    node(sig, children, ty)
}

fn add_sub_time(
    function: &ScalarFunction,
    children: Vec<PbExpr>,
    types: &[EvalType],
    ty: &FieldType,
) -> Option<PbExpr> {
    use EvalType::{Datetime as T, Duration as H, String as S};
    let subtract = function.func_name.lowercase() == "subtime";
    let first_code = function.args.first()?.static_type()?.code();
    let sig = match (subtract, first_code, types) {
        (false, FieldTypeCode::Date, [T, S]) => "AddDateAndString",
        (_, FieldTypeCode::Date, _) => return None,
        (false, _, [T, H]) => "AddDatetimeAndDuration",
        (false, _, [T, S]) => "AddDatetimeAndString",
        (false, _, [H, H]) => "AddDurationAndDuration",
        (false, _, [H, S]) => "AddDurationAndString",
        (false, _, [S, H]) => "AddStringAndDuration",
        (true, _, [T, H]) => "SubDatetimeAndDuration",
        (true, _, [T, S]) => "SubDatetimeAndString",
        (true, _, [H, H]) => "SubDurationAndDuration",
        (true, _, [H, S]) => "SubDurationAndString",
        (true, _, [S, H]) => "SubStringAndDuration",
        // Date+Duration and String+String families are not RPN-dispatched.
        // AddTime*Null is lazy in Go and must not eagerly visit either child.
        _ => return None,
    };
    node(sig, children, ty)
}

// Go addSubDateFunctionClass crosses six date domains and four interval
// domains. Duration->Datetime's eight signatures are deliberately not here:
// those kernels obtain today's date outside the TiDB statement context.
const ADD_DATE: [[&str; 4]; 6] = [
    [
        "AddDateStringString",
        "AddDateStringInt",
        "AddDateStringReal",
        "AddDateStringDecimal",
    ],
    [
        "AddDateIntString",
        "AddDateIntInt",
        "AddDateIntReal",
        "AddDateIntDecimal",
    ],
    [
        "AddDateRealString",
        "AddDateRealInt",
        "AddDateRealReal",
        "AddDateRealDecimal",
    ],
    [
        "AddDateDecimalString",
        "AddDateDecimalInt",
        "AddDateDecimalReal",
        "AddDateDecimalDecimal",
    ],
    [
        "AddDateDatetimeString",
        "AddDateDatetimeInt",
        "AddDateDatetimeReal",
        "AddDateDatetimeDecimal",
    ],
    [
        "AddDateDurationString",
        "AddDateDurationInt",
        "AddDateDurationReal",
        "AddDateDurationDecimal",
    ],
];
const SUB_DATE: [[&str; 4]; 6] = [
    [
        "SubDateStringString",
        "SubDateStringInt",
        "SubDateStringReal",
        "SubDateStringDecimal",
    ],
    [
        "SubDateIntString",
        "SubDateIntInt",
        "SubDateIntReal",
        "SubDateIntDecimal",
    ],
    [
        "SubDateRealString",
        "SubDateRealInt",
        "SubDateRealReal",
        "SubDateRealDecimal",
    ],
    [
        "SubDateDecimalString",
        "SubDateDecimalInt",
        "SubDateDecimalReal",
        "SubDateDecimalDecimal",
    ],
    [
        "SubDateDatetimeString",
        "SubDateDatetimeInt",
        "SubDateDatetimeReal",
        "SubDateDatetimeDecimal",
    ],
    [
        "SubDateDurationString",
        "SubDateDurationInt",
        "SubDateDurationReal",
        "SubDateDurationDecimal",
    ],
];

fn date_arithmetic(
    name: &str,
    mut children: Vec<PbExpr>,
    types: &[EvalType],
    ty: &FieldType,
) -> Option<PbExpr> {
    use EvalType::{
        Datetime as T, Decimal as D, Duration as H, Int as I, Json as J, Real as R, String as S,
    };
    let [first, second] = types else {
        return None;
    };
    let (subtract, unit) = if let Some(unit) = name.strip_prefix("date_sub_") {
        (true, unit)
    } else {
        (false, name.strip_prefix("date_add_")?)
    };
    let unit = unit.to_ascii_uppercase();
    if !matches!(
        unit.as_str(),
        "MICROSECOND"
            | "SECOND"
            | "MINUTE"
            | "HOUR"
            | "DAY"
            | "WEEK"
            | "MONTH"
            | "QUARTER"
            | "YEAR"
            | "SECOND_MICROSECOND"
            | "MINUTE_MICROSECOND"
            | "MINUTE_SECOND"
            | "HOUR_MICROSECOND"
            | "HOUR_SECOND"
            | "HOUR_MINUTE"
            | "DAY_MICROSECOND"
            | "DAY_SECOND"
            | "DAY_MINUTE"
            | "DAY_HOUR"
            | "YEAR_MONTH"
    ) {
        return None;
    }
    if *first == H {
        if domain(ty.eval_type()) != H
            || matches!(
                unit.as_str(),
                "DAY"
                    | "WEEK"
                    | "MONTH"
                    | "QUARTER"
                    | "YEAR"
                    | "YEAR_MONTH"
                    | "DAY_HOUR"
                    | "DAY_MINUTE"
                    | "DAY_SECOND"
            )
        {
            return None;
        }
    }
    let first = if *first == J { S } else { *first };
    let second = match second {
        J => S,
        S | I | R | D => *second,
        _ => return None,
    };
    let row = match first {
        S => 0,
        I => 1,
        R => 2,
        D => 3,
        T => 4,
        H => 5,
        _ => return None,
    };
    let col = match second {
        S => 0,
        I => 1,
        R => 2,
        D => 3,
        _ => return None,
    };
    children = cast_args(children, &[first, second])?;
    let mut unit_type = FieldType::new(FieldTypeCode::VarString);
    unit_type.set_flen(unit.len() as i64);
    children.push(PbExpr {
        tp: Some(ExprType::String as i32),
        val: Some(unit.into_bytes()),
        field_type: Some(crate::pushdown_catalog::field_type_to_pb(&unit_type)?),
        ..PbExpr::default()
    });
    node(
        if subtract {
            SUB_DATE[row][col]
        } else {
            ADD_DATE[row][col]
        },
        children,
        ty,
    )
}

fn literal_unit(expr: &PbExpr) -> Option<String> {
    if !matches!(expr.tp, Some(value) if value == ExprType::String as i32 || value == ExprType::Bytes as i32)
    {
        return None;
    }
    Some(
        std::str::from_utf8(expr.val.as_deref()?)
            .ok()?
            .to_ascii_uppercase(),
    )
}

fn definitely_nonnull(expr: &PbExpr) -> bool {
    if expr.tp == Some(ExprType::Null as i32) {
        return false;
    }
    if matches!(expr.tp, Some(value) if value != ExprType::ScalarFunc as i32 && value != ExprType::ColumnRef as i32)
    {
        return true;
    }
    expr.field_type
        .as_ref()
        .is_some_and(|ft| ft.flag.unwrap_or_default() & 1 != 0)
}

fn json(name: &str, children: Vec<PbExpr>, types: &[EvalType], ty: &FieldType) -> Option<PbExpr> {
    use EvalType::{Json as J, String as S};
    let sig = match name {
        "json_depth" if types == [J] => "JsonDepthSig",
        "json_type" if types == [J] => "JsonTypeSig",
        "json_valid" if types.len() == 1 => match types[0] {
            J => "JsonValidJsonSig",
            S => "JsonValidStringSig",
            _ => "JsonValidOthersSig",
        },
        "json_unquote" if matches!(types, [J | S]) => {
            return node("JsonUnquoteSig", cast_args(children, &[S])?, ty);
        }
        // The existing quote kernel emits non-JSON escapes for some control
        // bytes. Only immutable already-string literals without controls are
        // admitted until that upstream kernel discrepancy is resolved.
        "json_quote" if types == [S] && children.first().is_some_and(|child| {
            matches!(child.tp, Some(tp) if tp == ExprType::String as i32 || tp == ExprType::Bytes as i32)
                && child.val.as_deref().is_some_and(|bytes| std::str::from_utf8(bytes).is_ok() && !bytes.iter().any(|byte| *byte < 0x20))
        }) => "JsonQuoteSig",
        "json_array" if types.iter().all(|ty| *ty == J) => "JsonArraySig",
        "json_object" if types.len() % 2 == 0
            && types.chunks_exact(2).all(|pair| pair == [S, J])
            && children.iter().step_by(2).all(definitely_nonnull) => "JsonObjectSig",
        "json_extract" | "json_remove" if types.len() >= 2 && types[0] == J && types[1..].iter().all(|ty| *ty == S) => {
            if name == "json_extract" { "JsonExtractSig" } else { "JsonRemoveSig" }
        }
        "json_keys" if types == [J] => "JsonKeysSig",
        "json_keys" if types == [J, S] => "JsonKeys2ArgsSig",
        "json_length" if types == [J] || types == [J, S] => "JsonLengthSig",
        "json_contains" if types == [J, J] || types == [J, J, S] => "JsonContainsSig",
        "json_memberof" | "json_member_of" if types == [J, J] => "JsonMemberOfSig",
        "json_set" | "json_insert" | "json_replace" | "json_array_append"
            if types.len() >= 3 && types.len() % 2 == 1 && types[0] == J
                && types[1..].chunks_exact(2).all(|pair| pair == [S, J]) => {
            // Unlike Go's jsonModify, TiKV's common modifier replaces a SQL
            // NULL base with JSON null. ARRAY_APPEND has its own NULL guard.
            if name != "json_array_append" && !definitely_nonnull(&children[0]) {
                return None;
            }
            match name {
                "json_set" => "JsonSetSig",
                "json_insert" => "JsonInsertSig",
                "json_replace" => "JsonReplaceSig",
                _ => "JsonArrayAppendSig",
            }
        }
        // JSON_MERGE itself adds a deprecation warning in TiDB, absent from
        // this kernel. The non-deprecated synonym can reuse its semantics.
        "json_merge_preserve"
            if types.len() >= 2 && types.iter().all(|ty| *ty == J)
                && children.iter().all(definitely_nonnull) => "JsonMergeSig",
        "json_merge_patch" if types.len() >= 2 && types.iter().all(|ty| *ty == J) => "JsonMergePatchSig",
        // Constructors/modifiers intentionally require already-JSON values:
        // Go clears ParseToJSONFlag on value operands but parses documents.
        // An unqualified coerce(..., Json) cannot represent both meanings.
        _ => return None,
    };
    if ty.eval_type() == S
        && matches!(
            sig,
            "JsonArraySig"
                | "JsonObjectSig"
                | "JsonExtractSig"
                | "JsonRemoveSig"
                | "JsonKeysSig"
                | "JsonKeys2ArgsSig"
                | "JsonSetSig"
                | "JsonInsertSig"
                | "JsonReplaceSig"
                | "JsonArrayAppendSig"
                | "JsonMergeSig"
                | "JsonMergePatchSig"
        )
    {
        // The Rust SQL rewriter currently exposes these JSON-valued builtins
        // as canonical JSON text. The native kernels still return Json, not
        // Bytes: give the inner node its actual carrier and explicitly format
        // it at the boundary. Keep every original SQL result field on the
        // outer cast; diagnostic/NULL/input-domain checks above still apply.
        // JsonQuoteSig/JsonTypeSig already return Bytes and must not be wrapped.
        let json_type = FieldType::new(FieldTypeCode::Json)
            .with_flags(ty.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL)
            .with_decimal(0);
        let value = node(sig, children, &json_type)?;
        return node("CastJsonAsString", vec![value], ty);
    }
    node(sig, children, ty)
}

fn vector(name: &str, children: Vec<PbExpr>, types: &[EvalType], ty: &FieldType) -> Option<PbExpr> {
    use EvalType::VectorFloat32 as V;
    let sig = match (name, types) {
        ("vec_as_text", [V]) => "VecAsTextSig",
        ("vec_dims", [V]) => "VecDimsSig",
        ("vec_l2_norm", [V]) => "VecL2NormSig",
        ("vec_l1_distance", [V, V]) => "VecL1DistanceSig",
        ("vec_l2_distance", [V, V]) => "VecL2DistanceSig",
        ("vec_negative_inner_product", [V, V]) => "VecNegativeInnerProductSig",
        ("vec_cosine_distance", [V, V]) => "VecCosineDistanceSig",
        // VecFromTextSig/CastStringAsVectorFloat32 are not dispatched by RPN.
        _ => return None,
    };
    node(sig, children, ty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;

    fn call(
        name: &str,
        codes: &[FieldTypeCode],
        result: FieldTypeCode,
    ) -> (ScalarFunction, Vec<PbExpr>) {
        let args = codes
            .iter()
            .enumerate()
            .map(|(index, code)| {
                crate::expression::Expression::Column(crate::column::Column::new(
                    index as i64,
                    FieldType::new(*code),
                ))
            })
            .collect();
        let children = codes
            .iter()
            .map(|code| PbExpr {
                tp: Some(ExprType::ColumnRef as i32),
                field_type: crate::pushdown_catalog::field_type_to_pb(&FieldType::new(*code)),
                ..PbExpr::default()
            })
            .collect();
        (
            ScalarFunction::new(CiString::new(name), FieldType::new(result), args),
            children,
        )
    }

    fn signature(name: &str) -> Option<i32> {
        tidb_query_expr::standalone::scalar_function_signature(name)
    }

    #[test]
    fn composite_date_unit_and_nested_field_types_are_preserved() {
        let (function, mut children) = call(
            "date_add_year_month",
            &[FieldTypeCode::Datetime, FieldTypeCode::LongLong],
            FieldTypeCode::Datetime,
        );
        children[0].field_type.as_mut().unwrap().decimal = Some(6);
        let first_type = children[0].field_type.clone();
        let lowered = lower(&function, children).unwrap();
        assert_eq!(lowered.sig, signature("AddDateDatetimeInt"));
        assert_eq!(
            lowered.children[2].val.as_deref(),
            Some(b"YEAR_MONTH".as_slice())
        );
        assert_eq!(lowered.children[0].field_type, first_type);
    }

    #[test]
    fn duration_current_date_and_mode_less_week_stay_native() {
        for (name, codes, result) in [
            (
                "date_add_day_hour",
                vec![FieldTypeCode::Duration, FieldTypeCode::VarString],
                FieldTypeCode::Datetime,
            ),
            ("week", vec![FieldTypeCode::Date], FieldTypeCode::LongLong),
            ("date", vec![FieldTypeCode::Duration], FieldTypeCode::Date),
        ] {
            let (function, children) = call(name, &codes, result);
            assert!(lower(&function, children).is_none(), "{name}");
        }
    }

    #[test]
    fn hour_uses_duration_and_json_values_are_not_parsed_as_documents() {
        let (function, children) =
            call("hour", &[FieldTypeCode::Duration], FieldTypeCode::LongLong);
        assert_eq!(lower(&function, children).unwrap().sig, signature("Hour"));
        let (function, children) = call(
            "json_array",
            &[FieldTypeCode::VarString],
            FieldTypeCode::Json,
        );
        assert!(lower(&function, children).is_none());
        let (function, children) = call("json_array", &[FieldTypeCode::Json], FieldTypeCode::Json);
        assert_eq!(
            lower(&function, children).unwrap().sig,
            signature("JsonArraySig")
        );
    }

    #[test]
    fn json_text_results_wrap_native_json_and_preserve_sql_metadata() {
        use tidb_datatype::FieldTypeFlags;
        use FieldTypeCode::{Json as J, VarString as S};

        for (name, args, expected) in [
            ("json_array", vec![J], "JsonArraySig"),
            ("json_object", vec![S, J], "JsonObjectSig"),
            ("json_extract", vec![J, S], "JsonExtractSig"),
            ("json_remove", vec![J, S], "JsonRemoveSig"),
            ("json_keys", vec![J], "JsonKeysSig"),
            ("json_keys", vec![J, S], "JsonKeys2ArgsSig"),
            ("json_set", vec![J, S, J], "JsonSetSig"),
            ("json_insert", vec![J, S, J], "JsonInsertSig"),
            ("json_replace", vec![J, S, J], "JsonReplaceSig"),
            ("json_array_append", vec![J, S, J], "JsonArrayAppendSig"),
            ("json_merge_preserve", vec![J, J], "JsonMergeSig"),
            ("json_merge_patch", vec![J, J], "JsonMergePatchSig"),
        ] {
            let (mut function, mut children) = call(name, &args, S);
            let result = FieldType::new(S)
                .with_flen(2048)
                .with_decimal(-1)
                .with_flags(FieldTypeFlags::NOT_NULL | FieldTypeFlags::BINARY)
                .with_charset_name("utf8mb4")
                .with_collation_name("utf8mb4_bin");
            function.ret_type = Some(result.clone());
            // Satisfy the independently enforced key/base NULL gates. The
            // wrapper itself must not manufacture non-NULL input guarantees.
            for child in &mut children {
                child.field_type.as_mut().unwrap().flag = Some(FieldTypeFlags::NOT_NULL);
            }
            let original_children = children.clone();
            let lowered = lower(&function, children).unwrap();
            assert_eq!(lowered.sig, signature("CastJsonAsString"), "{name}");
            assert_eq!(
                lowered.field_type,
                crate::pushdown_catalog::field_type_to_pb(&result),
                "{name} SQL metadata"
            );
            assert_eq!(lowered.children.len(), 1, "{name}");
            let native = &lowered.children[0];
            assert_eq!(native.sig, signature(expected), "{name}");
            let native_type = native.field_type.as_ref().unwrap();
            assert_eq!(native_type.tp, Some(i32::from(J.mysql_type())), "{name}");
            assert_eq!(native_type.flag, Some(FieldTypeFlags::NOT_NULL), "{name}");
            assert_eq!(native.children, original_children, "{name} arguments");

            // Explicitly JSON-typed expression callers keep the native result;
            // only the SQL text-shaped boundary requires serialization.
            function.ret_type = Some(FieldType::new(J));
            let direct = lower(&function, original_children).unwrap();
            assert_eq!(direct.sig, signature(expected), "{name} native JSON");
        }
    }

    #[test]
    fn json_text_wrapper_does_not_bypass_null_diagnostic_or_argument_gates() {
        use FieldTypeCode::{Json as J, VarString as S};

        for (name, args) in [
            ("json_set", vec![J, S, J]),
            ("json_insert", vec![J, S, J]),
            ("json_replace", vec![J, S, J]),
            ("json_object", vec![S, J]),
            ("json_merge_preserve", vec![J, J]),
            ("json_merge", vec![J, J]),
            ("json_array", vec![S]),
        ] {
            let (function, children) = call(name, &args, S);
            assert!(lower(&function, children).is_none(), "{name}");
        }
        let (function, children) = call("json_type", &[J], S);
        assert_eq!(
            lower(&function, children).unwrap().sig,
            signature("JsonTypeSig")
        );
        let (function, mut children) = call("json_quote", &[S], S);
        children[0].tp = Some(ExprType::String as i32);
        children[0].val = Some(b"plain text".to_vec());
        assert_eq!(
            lower(&function, children).unwrap().sig,
            signature("JsonQuoteSig")
        );
    }

    #[test]
    fn nullable_json_modifier_and_missing_vector_parser_stay_native() {
        let (function, children) = call(
            "json_set",
            &[
                FieldTypeCode::Json,
                FieldTypeCode::VarString,
                FieldTypeCode::Json,
            ],
            FieldTypeCode::Json,
        );
        assert!(lower(&function, children).is_none());
        let (function, children) = call(
            "vec_from_text",
            &[FieldTypeCode::VarString],
            FieldTypeCode::VectorFloat32,
        );
        assert!(lower(&function, children).is_none());
    }
}
