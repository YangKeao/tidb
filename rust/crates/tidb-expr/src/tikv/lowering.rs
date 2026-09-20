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

//! Local embedding policy, independent of distributed predicate pushdown.
//!
//! Wire signature IDs are resolved against the pinned engine: the local prost
//! enum deliberately need not duplicate every upstream enum variant. Resolving
//! an ID does NOT prove capability; the native RPN builder is the final check.

use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};
use tidb_proto::tipb::{Expr as PbExpr, ExprType};
use tidb_query_expr::standalone::scalar_function_signature;

use crate::column::Column;
use crate::expression::Expression;
use crate::pushdown_catalog::{self, ColumnDescriptor};
use crate::scalar_function::ScalarFunction;

mod families;

fn wire_type(ty: &FieldType) -> Option<FieldType> {
    if ty
        .elems_snapshot()
        .iter()
        .any(|elem| elem.as_utf8().is_err())
    {
        // Protobuf enum labels are strings, not an arbitrary-byte transport.
        return None;
    }
    let mut result = ty.clone();
    if ty.flen() == i64::from(u32::MAX)
        && matches!(ty.eval_type(), EvalType::String | EvalType::Json)
    {
        // Go ToPBFieldType narrows MaxBlobWidth to int32(-1). Real SQL JSON /
        // LONG(BLOB/TEXT) metadata uses this width; checked conversion would
        // otherwise refuse it despite working hand-built flen=-1 fixtures.
        result = result.with_flen(-1);
    }
    i32::try_from(result.flen()).ok()?;
    Some(result)
}

pub(super) fn field_type_to_pb(ty: &FieldType) -> Option<tidb_proto::tipb::FieldType> {
    let wire = wire_type(ty)?;
    if wire.code() == FieldTypeCode::Set {
        // Go's `columnToPBExpr` refuses SET and GEOMETRY leaves for DISTRIBUTED
        // pushdown, and the shared catalog helper mirrors that on purpose. The
        // local embedding is not pushdown: the engine carries a SET column now
        // and the descriptor is ours, so build this one leaf here and leave the
        // shared refusal untouched.
        return Some(tidb_proto::tipb::FieldType {
            tp: Some(i32::from(wire.code().mysql_type())),
            flag: Some(wire.flags()),
            flen: Some(i32::try_from(wire.flen()).ok()?),
            decimal: Some(i32::try_from(wire.decimal()).ok()?),
            collate: Some(tidb_datatype::collation_to_proto(wire.collation_name())),
            charset: Some(wire.charset_name().to_owned()),
            elems: wire
                .elems_snapshot()
                .into_iter()
                .map(|elem| elem.to_string())
                .collect(),
            array: Some(wire.is_array()),
        });
    }
    pushdown_catalog::field_type_to_pb(&wire)
}

pub(super) fn admitted(expression: &Expression, context: &super::Context) -> bool {
    match admission_rejection(expression, context) {
        None => true,
        Some(reason) => {
            // The admission gate is a full third of the corpus gap and its
            // `NotAdmitted` is otherwise indistinguishable from a lowering or
            // engine refusal. `TIKV_EXPR_DEBUG_COMPILE` names the sub-rule.
            if super::debug_declines() {
                eprintln!("TIKV-EXPR-ADMIT-REJECT [{reason}] {expression:?}");
            }
            false
        }
    }
}

/// Why `admitted` refuses `expression`, for the debug channel only.
fn admission_rejection(expression: &Expression, context: &super::Context) -> Option<&'static str> {
    let ty = expression.static_type()?;
    if !super::bridge::supported_type(ty) {
        return Some("unsupported-result-type");
    }
    match expression {
        Expression::Column(_) => None,
        Expression::CorrelatedColumn(_) => Some("correlated-column"),
        Expression::Constant(value) => {
            if value.deferred_expr.is_some() {
                return Some("constant-deferred");
            }
            if value.param_marker.is_some() {
                return Some("constant-param-marker");
            }
            // The engine normalizes fixed-offset TIMESTAMP literals to UTC.
            // Named zones remain excluded: TiKV's earliest-fold rule differs
            // from Go time.Date in some zones. Follow Context::config priority.
            if ty.code() == FieldTypeCode::Timestamp
                && !match context
                    .time_zone_name
                    .as_deref()
                    .filter(|name| !name.is_empty())
                {
                    Some("UTC") | None => true,
                    Some(_) => false,
                }
            {
                return Some("constant-timestamp-timezone");
            }
            if matches!(
                ty.code(),
                FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
            ) && !temporal_constant_supported(&value.value, ty)
            {
                return Some("constant-temporal-shape");
            }
            if matches!(value.value, Datum::Real(v) | Datum::Float32(v) if !v.is_finite()) {
                return Some("constant-nonfinite-real");
            }
            // A binary/bit literal is NUMERIC in the native evaluator's
            // coercion (`b'1' + 0` is 1) but reaches the engine as bytes, where
            // the same expression is 0. Decline the literal.
            if matches!(
                value.value,
                Datum::Raw(_)
                    | Datum::MinNotNull
                    | Datum::MaxValue
                    | Datum::BinaryLiteral(_)
                    | Datum::Bit(_)
            ) {
                return Some("constant-binary-or-literal");
            }
            None
        }
        Expression::ScalarFunction(function) => {
            // The admission table is the only name-level gate. A name with no
            // row, or a row explicitly excluded, stays native. `Shape` carries
            // the lazy-child rules.
            let name = function.func_name.lowercase();
            let Some(row) = super::admission::admission(name) else {
                return Some("no-admission-row");
            };
            if row.decision != super::admission::Decision::Admitted {
                return Some(row.exclusion_reason);
            }
            // A FLOAT result needs an explicit rounding contract; numeric
            // kernels themselves return f64. FLOAT input columns remain usable.
            if ty.code() == FieldTypeCode::Float {
                return Some("float-result");
            }
            if !row.shape.permits(function) {
                return Some("lazy-shape");
            }
            if let Some(allowed) = binary_constant_integer_cast(function) {
                return (!allowed).then_some("binary-constant-integer-cast-shape");
            }
            function
                .args
                .iter()
                .find_map(|arg| admission_rejection(arg, context))
        }
    }
}

/// CastStringAsInt treats a binary-collated constant as a binary number. Only
/// actual literal provenance authorizes that kernel: ordinary bytes b"1" mean
/// decimal 1 natively, not the literal's 49. Keep this exception local to explicit
/// integer CAST; root/lazy forwarding still needs a literal-kind carrier.
fn binary_constant_integer_cast(function: &ScalarFunction) -> Option<bool> {
    let name = function.func_name.lowercase();
    if !matches!(name, "cast" | "cast_signed" | "cast_unsigned") {
        return None;
    }
    let target = function.get_static_type()?;
    if target.eval_type() != EvalType::Int || function.args.len() != 1 {
        return None;
    }
    let Expression::Constant(constant) = &function.args[0] else {
        return None;
    };
    let source = constant.ret_type.as_ref()?;
    if !source.is_binary_string() || matches!(constant.value, Datum::Null) {
        return None;
    }
    if constant.deferred_expr.is_some()
        || constant.param_marker.is_some()
        || !super::bridge::supported_type(source)
        || source.code() != FieldTypeCode::VarString
        || source.charset_name() != "binary"
        || target.code() != FieldTypeCode::LongLong
        || !matches!(
            (name, target.is_unsigned()),
            ("cast_signed", false) | ("cast_unsigned", true)
        )
    {
        return Some(false);
    }
    let Datum::BinaryLiteral(value) = &constant.value else {
        return Some(false);
    };
    let bytes = value.as_bytes();
    // Native signed CAST saturates a high-bit u64; the engine bitcasts it.
    // Reject that shape instead of silently changing the result.
    Some(bytes.len() <= 8 && (target.is_unsigned() || bytes.len() < 8 || bytes[0] < 0x80))
}

/// DATE/DATETIME payloads carry wall fields, not timezone instants.
/// Their kind/FSP live in protobuf metadata, so reject metadata that would
/// reinterpret the native scalar value. Packed zero additionally requires
/// warning-free engine compilation: standalone rejects decoder warnings/errors
/// before execution, so restrictive SQL modes remain explicit compile declines.
fn temporal_constant_supported(value: &Datum, ty: &FieldType) -> bool {
    if !(-1..=6).contains(&ty.decimal()) {
        return false;
    }
    let Datum::Time(time) = value else {
        return matches!(value, Datum::Null);
    };
    let kind = match ty.code() {
        FieldTypeCode::Date => tidb_datatype::TimeType::Date,
        FieldTypeCode::Datetime => tidb_datatype::TimeType::DateTime,
        FieldTypeCode::Timestamp => tidb_datatype::TimeType::Timestamp,
        _ => return false,
    };
    let fsp = if ty.code() == FieldTypeCode::Date {
        0
    } else {
        ty.decimal().max(0) as u8
    };
    time.kind() == kind && time.fsp() == fsp && super::bridge::check_time(*time, ty).is_ok()
}

#[cfg(test)]
mod temporal_literal_tests {
    use super::*;

    #[test]
    fn packed_temporal_literal_uses_existing_wire_format() {
        for (code, kind, fsp, hour, micro) in [
            (FieldTypeCode::Date, tidb_datatype::TimeType::Date, 0, 0, 0),
            (
                FieldTypeCode::Datetime,
                tidb_datatype::TimeType::DateTime,
                3,
                12,
                123456,
            ),
            (
                FieldTypeCode::Timestamp,
                tidb_datatype::TimeType::Timestamp,
                6,
                12,
                123456,
            ),
        ] {
            let ty = FieldType::new(code).with_decimal(fsp);
            let value =
                tidb_datatype::Time::from_date_checked(2024, 3, 14, hour, 34, 56, micro, kind, fsp)
                    .unwrap();
            let expr = Expression::Constant(crate::constant::Constant::new(Datum::Time(value), ty));
            assert!(admitted(&expr, &super::super::Context::default()));
            let pb = lower(&expr, &|_| None, &super::super::Context::default()).unwrap();
            let ymd = ((2024_u64 * 13 + 3) << 5) | 14;
            let hms = ((hour as u64) << 12) | (34 << 6) | 56;
            let packed = (((ymd << 17) | hms) << 24) | micro as u64;
            assert_eq!(pb.tp, Some(ExprType::MysqlTime as i32));
            assert_eq!(pb.val, Some(packed.to_be_bytes().to_vec()));
            let metadata = pb.field_type.unwrap();
            assert_eq!(metadata.tp, Some(i32::from(code.mysql_type())));
            assert_eq!(metadata.decimal, Some(fsp as i32));
        }
    }
}

fn leaf(expression: &Expression) -> bool {
    matches!(expression, Expression::Column(_) | Expression::Constant(_))
}

pub(super) fn lower(
    expression: &Expression,
    columns: &impl Fn(u32) -> Option<ColumnDescriptor>,
    context: &super::Context,
) -> Option<PbExpr> {
    let Expression::ScalarFunction(function) = expression else {
        return match expression {
            Expression::Column(column) => Some(PbExpr {
                tp: Some(ExprType::ColumnRef as i32),
                val: Some((column.index ^ i64::MIN).to_be_bytes().to_vec()),
                field_type: Some(field_type_to_pb(column.get_static_type()?)?),
                ..PbExpr::default()
            }),
            Expression::Constant(constant) => {
                let mut constant = constant.clone();
                constant.ret_type = Some(wire_type(constant.ret_type.as_ref()?)?);
                let timestamp = match &constant.value {
                    Datum::Time(value) if value.kind() == tidb_datatype::TimeType::Timestamp => {
                        Some(*value)
                    }
                    _ => None,
                };
                let mut encoded =
                    pushdown_catalog::expression_to_pb(&Expression::Constant(constant), columns)?;
                if let Some(value) = timestamp {
                    // Reuse TiKV's codec and exact round-trip checks. The
                    // distributed catalog and output bridge stay unchanged.
                    let engine_value = tidb_query_expr::standalone::date_time_from_chunk(
                        &value.go_raw().to_le_bytes(),
                    )
                    .ok()?;
                    let packed = context.pack_time_literal(&engine_value).ok()?;
                    encoded.val = Some(packed.to_be_bytes().to_vec());
                }
                Some(encoded)
            }
            _ => None,
        };
    };
    let children = function
        .args
        .iter()
        .map(|arg| lower(arg, columns, context))
        .collect::<Option<Vec<_>>>()?;
    local_call(function, children.clone())
        .or_else(|| families::lower(function, children.clone()))
        .or_else(|| catalog_call(function, children))
}

pub(super) fn node(signature: &str, children: Vec<PbExpr>, ty: &FieldType) -> Option<PbExpr> {
    Some(PbExpr {
        tp: Some(ExprType::ScalarFunc as i32),
        sig: Some(scalar_function_signature(signature)?),
        field_type: Some(field_type_to_pb(ty)?),
        children,
        ..PbExpr::default()
    })
}

fn family(ty: EvalType) -> &'static str {
    match ty {
        EvalType::Int => "Int",
        EvalType::Real => "Real",
        EvalType::Decimal => "Decimal",
        EvalType::String => "String",
        EvalType::Datetime | EvalType::Timestamp => "Time",
        EvalType::Duration => "Duration",
        EvalType::Json => "Json",
        EvalType::VectorFloat32 => "VectorFloat32",
    }
}

fn normalized(ty: EvalType) -> EvalType {
    if ty == EvalType::Timestamp {
        EvalType::Datetime
    } else {
        ty
    }
}

fn child_type(child: &PbExpr) -> Option<FieldType> {
    let ty = child.field_type.as_ref()?;
    Some(
        FieldType::new(FieldTypeCode::from_mysql_type(u8::try_from(ty.tp?).ok()?))
            .with_flags(ty.flag.unwrap_or_default())
            .with_flen(i64::from(ty.flen.unwrap_or(-1)))
            .with_decimal(i64::from(ty.decimal.unwrap_or(-1))),
    )
}

fn same_family(children: &[PbExpr], target: EvalType) -> bool {
    children.iter().all(|child| {
        child_type(child).is_some_and(|ty| {
            // Enum's SQL eval family is String, but its engine vector is Enum;
            // an actual cast is needed, and may not be speculated in lazy arms.
            ty.code() != FieldTypeCode::Enum && normalized(ty.eval_type()) == normalized(target)
        })
    })
}

/// The wire field type a cast to `target` declares.
fn target_code(target: EvalType) -> Option<FieldTypeCode> {
    Some(match target {
        EvalType::Int => FieldTypeCode::LongLong,
        EvalType::Real => FieldTypeCode::Double,
        EvalType::Decimal => FieldTypeCode::NewDecimal,
        EvalType::String => FieldTypeCode::VarString,
        EvalType::Datetime => FieldTypeCode::Datetime,
        EvalType::Timestamp => FieldTypeCode::Timestamp,
        EvalType::Duration => FieldTypeCode::Duration,
        EvalType::Json => FieldTypeCode::Json,
        EvalType::VectorFloat32 => return None,
    })
}

fn is_null_leaf(child: &PbExpr) -> bool {
    child.tp == Some(ExprType::Null as i32)
}

/// Prepares the children of a lazy arm, one target family per child.
///
/// A child already in `target`'s family passes through unchanged. A `NULL` leaf
/// is accepted and *retagged* to `target`: SQL's NULL has no type, but the
/// engine's argument validator reads the declared `FieldType`, so a NULL that
/// arrives labelled `Bytes` is rejected by a kernel that expects `Int`
/// (`coalesce(NULL, 1)`). Retagging adds no node, so the arm stays a leaf.
///
/// A numeric *constant* leaf may also be rendered as a string: Go wraps a lazy
/// arm's value with `WrapWithCastAsString`, that rendering is exact, and the
/// engine's `Cast{Int,Decimal}AsString` mirrors it, so `elt(1, 65)` answers
/// `65` on both paths. Every other coercion is refused, because section 7.2 of
/// the corpus plan shows a numeric coercion in an arm can change the value.
fn lazy_args(children: Vec<PbExpr>, targets: &[EvalType]) -> Option<Vec<PbExpr>> {
    if children.len() != targets.len() {
        return None;
    }
    children
        .into_iter()
        .zip(targets)
        .map(|(mut child, &target)| {
            if is_null_leaf(&child) {
                child.field_type = Some(field_type_to_pb(&FieldType::new(target_code(target)?))?);
                return Some(child);
            }
            if same_family(std::slice::from_ref(&child), target) {
                return Some(child);
            }
            if target == EvalType::String && is_numeric_constant_leaf(&child) {
                return coerce(child, target);
            }
            None
        })
        .collect()
}

/// Whether `child` is a constant leaf whose family is numeric.
fn is_numeric_constant_leaf(child: &PbExpr) -> bool {
    is_constant_leaf(child)
        && child_type(child).is_some_and(|ty| {
            matches!(
                ty.eval_type(),
                EvalType::Int | EvalType::Real | EvalType::Decimal
            )
        })
}

/// A leaf the adapter built from a literal rather than from a column or a call.
fn is_constant_leaf(child: &PbExpr) -> bool {
    !matches!(
        child.tp,
        Some(tp) if tp == ExprType::ScalarFunc as i32 || tp == ExprType::ColumnRef as i32
    )
}

pub(super) fn coerce(child: PbExpr, target: EvalType) -> Option<PbExpr> {
    let source = child_type(&child)?;
    if normalized(source.eval_type()) == normalized(target) && source.code() != FieldTypeCode::Enum
    {
        return Some(child);
    }
    let code = target_code(target)?;
    // Duration -> date/time uses today's date, not represented by Context.
    if source.eval_type() == EvalType::Duration
        && matches!(target, EvalType::Datetime | EvalType::Timestamp)
    {
        return None;
    }
    // A temporal cast over a constant used to be declined here because a
    // DATE-valued engine kernel can be typed `DateTime` internally (see
    // `bridge::check_time`, which now normalizes it the way Go's DATE decoder
    // does). The corpus dual-run covers the constant shapes: `date`, `time`,
    // `timestamp`, `extract`, `month`, `cast(<int|real> as datetime)` and
    // `last_day(<int literal>)` all agree with native.
    // JSON VALUE versus DOCUMENT coercion needs the caller's explicit policy.
    if target == EvalType::Json {
        return None;
    }
    let mut ty = FieldType::new(code);
    if target == EvalType::Decimal {
        // The general implicit DECIMAL cast keeps the source scale rather than
        // inventing a fixed scale and rounding arbitrary intermediates.
        ty = ty.with_flen(65).with_decimal(source.decimal());
    }
    if matches!(
        target,
        EvalType::Datetime | EvalType::Timestamp | EvalType::Duration
    ) {
        ty = ty.with_decimal(6);
    }
    node(
        &format!("Cast{}As{}", family(source.eval_type()), family(target)),
        vec![child],
        &ty,
    )
}

pub(super) fn cast_args(children: Vec<PbExpr>, targets: &[EvalType]) -> Option<Vec<PbExpr>> {
    if children.len() != targets.len() {
        return None;
    }
    children
        .into_iter()
        .zip(targets)
        .map(|(child, &ty)| coerce(child, ty))
        .collect()
}

fn all_as(children: Vec<PbExpr>, target: EvalType) -> Option<Vec<PbExpr>> {
    children
        .into_iter()
        .map(|child| coerce(child, target))
        .collect()
}

fn common_numeric(children: &[PbExpr]) -> Option<EvalType> {
    let types = children
        .iter()
        .map(child_type)
        .collect::<Option<Vec<_>>>()?;
    if types.iter().any(|ty| {
        !matches!(
            ty.eval_type(),
            EvalType::Int | EvalType::Real | EvalType::Decimal
        )
    }) {
        return None;
    }
    Some(if types.iter().any(|ty| ty.eval_type() == EvalType::Real) {
        EvalType::Real
    } else if types.iter().any(|ty| ty.eval_type() == EvalType::Decimal) {
        EvalType::Decimal
    } else {
        EvalType::Int
    })
}

fn local_call(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    // The admission table, not an ad-hoc chain, decides which local family a
    // name belongs to. A name the table routes to the temporal/JSON/vector
    // families or to the reused pushdown catalog returns `None` here so
    // `lower` reaches the next lowering site, exactly as the old chain did
    // when none of these functions matched.
    use super::admission::{Family, Signature};
    let family = match super::admission::admission(function.func_name.lowercase())?.signature {
        Signature::Family(family) => family,
        Signature::Resolved(_) | Signature::None => return None,
    };
    match family {
        Family::Arithmetic => arithmetic(function, children),
        Family::Comparison => comparison(function, children),
        Family::Control => control(function, children),
        Family::Math => math(function, children),
        // `regexp_extended` is reached through `strings`' final fallback.
        Family::String | Family::Regexp => strings(function, children),
        Family::Miscellaneous => miscellaneous(function, children),
        Family::Temporal
        | Family::DateArithmetic
        | Family::Json
        | Family::Vector
        | Family::Catalog
        | Family::None => None,
    }
}

fn arithmetic(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    use EvalType::{Decimal, Int, Real};
    let ty = function.get_static_type()?;
    let name = function.func_name.lowercase();
    // `cast` is the internal spelling. The rewriter mints `cast_signed`,
    // `cast_datetime`, ... for explicit `CAST(x AS <type>)`, and routing those
    // here looks interchangeable, but the dual-run corpus says otherwise: two
    // shapes diverge (a DATETIME result keeps scale 0 where native keeps the
    // promoted scale, and an INTERVAL argument rounds differently), so the
    // minted spellings stay native until that coercion is reproduced exactly.
    // `cast_signed`/`cast_unsigned` are the rewriter's spellings for an
    // explicit `CAST(x AS SIGNED|UNSIGNED)`. Every source family has a
    // `Cast{source}AsInt` kernel and the local arm derives it from the
    // function's own static type, so these two are the subset of the minted
    // spellings that needs no metadata of its own. The temporal/string
    // spellings are not interchangeable this way (corpus plan 7.4).
    if matches!(
        name,
        "cast"
            | "cast_signed"
            | "cast_unsigned"
            | "cast_char"
            | "cast_binary"
            | "cast_datetime"
            | "cast_date"
            | "cast_time"
    ) && children.len() == 1
    {
        let source = child_type(&children[0])?;
        if source.eval_type() == EvalType::Duration
            && matches!(ty.eval_type(), EvalType::Datetime | EvalType::Timestamp)
        {
            return None;
        }
        // TiDB and TiKV encode JSON temporal scalars differently; the bridge
        // currently declines those inputs and must not create them at output.
        if ty.eval_type() == EvalType::Json
            && matches!(
                source.eval_type(),
                EvalType::Datetime | EvalType::Timestamp | EvalType::Duration
            )
        {
            return None;
        }
        // Binary fixed-width casts can allocate/pad based on max_allowed_packet.
        if ty.is_binary_string() && ty.flen() >= 0 && ty.eval_type() == EvalType::String {
            return None;
        }
        return node(
            &format!(
                "Cast{}As{}",
                family(source.eval_type()),
                family(ty.eval_type())
            ),
            children,
            ty,
        );
    }
    if matches!(name, "not" | "unaryminus" | "bitneg") && children.len() == 1 {
        let input = child_type(&children[0])?.eval_type();
        let signature = match name {
            "not" if matches!(input, Int | Real | Decimal | EvalType::Json) => {
                format!("UnaryNot{}", family(input))
            }
            "unaryminus" if matches!(input, Int | Real | Decimal) => {
                format!("UnaryMinus{}", family(input))
            }
            "bitneg" => "BitNegSig".to_owned(),
            _ => return None,
        };
        let children = if name == "bitneg" {
            all_as(children, Int)?
        } else {
            children
        };
        return node(&signature, children, ty);
    }
    if children.len() != 2 {
        return None;
    }
    let bit_signature = match name {
        "bitand" => Some("BitAndSig"),
        "bitor" => Some("BitOrSig"),
        "bitxor" => Some("BitXorSig"),
        "leftshift" => Some("LeftShift"),
        "rightshift" => Some("RightShift"),
        _ => None,
    };
    if let Some(signature) = bit_signature {
        return node(signature, all_as(children, Int)?, ty);
    }
    let prefix = match name {
        "plus" => "Plus",
        "minus" => "Minus",
        "mul" => "Multiply",
        "div" => "Divide",
        "intdiv" => "IntDivide",
        "mod" => "Mod",
        _ => return None,
    };
    let domain = if name == "intdiv" {
        if same_family(&children, Int) {
            Int
        } else {
            Decimal
        }
    } else {
        ty.eval_type()
    };
    if !matches!(domain, Int | Real | Decimal) {
        return None;
    }
    if name == "minus"
        && domain == Int
        && !ty.is_unsigned()
        && children
            .iter()
            .any(|child| child_type(child).is_some_and(|ty| ty.is_unsigned()))
    {
        // TiKV's legacy MinusInt mapper does not implement the forced-signed
        // variants used by NO_UNSIGNED_SUBTRACTION.
        return None;
    }
    node(
        &format!("{prefix}{}", family(domain)),
        all_as(children, domain)?,
        ty,
    )
}

fn comparison(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    let ty = function.get_static_type()?;
    let name = function.func_name.lowercase();
    if children.len() == 1 {
        let input = child_type(&children[0])?.eval_type();
        let suffix = match name {
            "isnull" => "IsNull",
            "istrue" => "IsTrue",
            "isfalse" => "IsFalse",
            "istrue_with_null" => "IsTrueWithNull",
            "isfalse_with_null" => "IsFalseWithNull",
            _ => return None,
        };
        return node(
            &format!("{}{suffix}", family(input)),
            all_as(children, input)?,
            ty,
        );
    }
    // `NULLIF(a, b)` is `IF(a <=> b, NULL, a)`, and Go rewrites it that way.
    if name == "nullif" {
        return nullif(function, children);
    }
    let prefix = match name {
        "eq" => "Eq",
        "ne" => "Ne",
        "lt" => "Lt",
        "le" => "Le",
        "gt" => "Gt",
        "ge" => "Ge",
        "nulleq" => "NullEq",
        "in" => "In",
        "greatest" => "Greatest",
        "least" => "Least",
        "interval" => "Interval",
        "field" => "Field",
        _ => return None,
    };
    if children.len() < 2
        || (!matches!(name, "in" | "greatest" | "least" | "interval" | "field")
            && children.len() != 2)
    {
        return None;
    }
    let input = child_type(&children[0])?.eval_type();
    let domain = if same_family(&children, input) {
        input
    } else {
        common_numeric(&children)?
    };
    // Potentially skipped arguments must not acquire fallible implicit casts.
    if matches!(name, "in" | "greatest" | "least" | "interval" | "field")
        && !same_family(&children, domain)
    {
        return None;
    }
    // The engine compares string arguments bytewise, so a non-binary collation
    // would answer differently from the native evaluator (case/accent folding).
    if matches!(name, "greatest" | "least")
        && domain == EvalType::String
        && !function
            .args
            .iter()
            .all(|arg| arg.static_type().is_some_and(|ty| ty.is_binary_string()))
    {
        return None;
    }
    // The engine's legacy Int kernels compare the raw `i64` and ignore the
    // unsigned flag. For `interval` that is an ordering comparison, so an
    // UINT64 above `i64::MAX` sorts wrong; for `in`/`field` it is equality,
    // which survives unsigned values only while every argument is unsigned.
    if matches!(name, "interval" | "in" | "field") && domain == EvalType::Int {
        let unsigned = children
            .iter()
            .map(|child| child_type(child).is_some_and(|ty| ty.is_unsigned()))
            .collect::<Vec<_>>();
        if unsigned.iter().any(|flag| *flag)
            && (name == "interval" || !unsigned.iter().all(|flag| *flag))
        {
            return None;
        }
    }
    if matches!(name, "greatest" | "least")
        && domain == EvalType::Int
        && children
            .iter()
            .any(|child| child_type(child).is_some_and(|ty| ty.is_unsigned()))
    {
        // The legacy Int kernels compare raw i64 and ignore unsigned flags.
        // Reuse exact Decimal comparison instead, then the original typed cast;
        // this preserves UINT64 values above i64::MAX without f64 rounding.
        if !ty.is_unsigned()
            || !children
                .iter()
                .all(|child| child_type(child).is_some_and(|ty| ty.is_unsigned()))
        {
            return None;
        }
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(20)
            .with_decimal(0);
        let values = children
            .into_iter()
            .map(|child| node("CastIntAsDecimal", vec![child], &decimal))
            .collect::<Option<Vec<_>>>()?;
        let selected = node(&format!("{prefix}Decimal"), values, &decimal)?;
        return node("CastDecimalAsInt", vec![selected], ty);
    }
    node(
        &format!("{prefix}{}", family(domain)),
        all_as(children, domain)?,
        ty,
    )
}

/// Lowers `NULLIF(a, b)` as `IF(a <=> b, NULL, a)`.
///
/// The comparison promotes the pair the way the other comparisons do, and the
/// value arm then carries `a` coerced to that promoted type -- which is what
/// Go's `buildNullif` returns. A pair the comparison cannot promote stays
/// native.
fn nullif(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    if children.len() != 2 {
        return None;
    }
    let ty = function.get_static_type()?;
    let input = child_type(&children[0])?.eval_type();
    let domain = if same_family(&children, input) {
        input
    } else {
        common_numeric(&children)?
    };
    // The engine compares strings bytewise, so a non-binary collation would
    // make `NULLIF('a', 'A')` unequal where SQL says equal.
    if domain == EvalType::String
        && !function
            .args
            .iter()
            .all(|arg| arg.static_type().is_some_and(|ty| ty.is_binary_string()))
    {
        return None;
    }
    // `NullEqInt` compares the raw `i64` and ignores the unsigned flag, so a
    // mixed signedness pair would compare bit patterns where SQL compares
    // values.
    if domain == EvalType::Int {
        let unsigned = children
            .iter()
            .map(|child| child_type(child).is_some_and(|ty| ty.is_unsigned()))
            .collect::<Vec<_>>();
        if unsigned.iter().any(|flag| *flag) && !unsigned.iter().all(|flag| *flag) {
            return None;
        }
    }
    // The condition runs in the comparison's promoted type, but the value comes
    // back as the FIRST argument's type -- MySQL's NULLIF returns expr1, and
    // Go wraps it with `WrapWithCastAs<expr1's type>`. An `If` node whose
    // declared type disagreed with its value child is what the engine rejects
    // (`NULLIF(1, 1.0)` promotes to DECIMAL for the comparison and returns
    // BIGINT), so the two sides are built separately.
    let predicate = node(
        &format!("NullEq{}", family(domain)),
        all_as(children.clone(), domain)?,
        &FieldType::new(FieldTypeCode::LongLong),
    )?;
    let null = PbExpr {
        tp: Some(ExprType::Null as i32),
        field_type: Some(field_type_to_pb(ty)?),
        ..PbExpr::default()
    };
    let value = coerce(children.into_iter().next()?, ty.eval_type())?;
    node(
        &format!("If{}", family(ty.eval_type())),
        vec![predicate, null, value],
        ty,
    )
}

/// The child indexes of `name` that hold a condition.
fn condition_indexes(name: &str, len: usize) -> Vec<usize> {
    match name {
        "if" => vec![0],
        "case" | "casewhen" => (0..len)
            .filter(|index| index % 2 == 0 && index + 1 < len)
            .collect(),
        _ => Vec::new(),
    }
}

/// Whether `child`'s declared family is temporal.
fn is_temporal_typed(child: &PbExpr) -> bool {
    child_type(child).is_some_and(|ty| {
        matches!(
            ty.eval_type(),
            EvalType::Datetime | EvalType::Timestamp | EvalType::Duration
        )
    })
}

fn control(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    use EvalType::Int;
    let ty = function.get_static_type()?;
    let name = function.func_name.lowercase();
    let result = ty.eval_type();
    let mut children = children;
    // The condition of `if`/`case` is always evaluated, never a skipped arm, so
    // Go's own ETInt coercion applies there and the leaf-only rule does not.
    // Only a *temporal* condition is coerced: 7.2 and 7.10 measured that a
    // numeric condition must not be, because Go's truthiness and Go's integer
    // cast disagree for it. `case` conditions sit at even indexes.
    for index in condition_indexes(&name, children.len()) {
        if is_temporal_typed(&children[index]) {
            children[index] = coerce(children[index].clone(), Int)?;
        }
    }
    // A numeric *constant* index is deliberately NOT coerced here. Go's
    // `elt(1.1, '2.1', ...)` answers the FIRST element while the engine's
    // `CastDecimalAsInt(1.1)` index answered the second, so the index does not
    // follow the same coercion the value arms do; the corpus caught it in one
    // run. The value arms' numeric-to-string rendering is exact and is handled
    // in `lazy_args`.
    let signature = match name {
        "if" if children.len() == 3 => format!("If{}", family(result)),
        "ifnull" if children.len() == 2 => format!("IfNull{}", family(result)),
        "coalesce" if !children.is_empty() => format!("Coalesce{}", family(result)),
        "case" | "casewhen" if children.len() >= 2 => format!("CaseWhen{}", family(result)),
        "and" | "or" | "xor" if children.len() == 2 => match name {
            "and" => "LogicalAnd",
            "or" => "LogicalOr",
            _ => "LogicalXor",
        }
        .to_owned(),
        "elt" if children.len() >= 2 => "Elt".to_owned(),
        _ => return None,
    };
    let targets = match name {
        // `if(cond, a, b)`: only the condition is Int.
        "if" => vec![Int, result, result],
        // `case`: condition, value, condition, value, ... with the last child a
        // bare `else` value.
        "case" | "casewhen" => (0..children.len())
            .map(|index| {
                if index % 2 == 0 && index + 1 < children.len() {
                    Int
                } else {
                    result
                }
            })
            .collect(),
        // `elt(index, a, b, ...)`: index Int, values String.
        "elt" => std::iter::once(Int)
            .chain(std::iter::repeat(EvalType::String).take(children.len() - 1))
            .collect(),
        // `ifnull`/`coalesce`/`and`/`or`/`xor` are uniform.
        _ => vec![
            if matches!(name, "and" | "or" | "xor") {
                Int
            } else {
                result
            };
            children.len()
        ],
    };
    node(&signature, lazy_args(children, &targets)?, ty)
}

fn math(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    use EvalType::{Decimal, Int, Real, String as Bytes};
    let ty = function.get_static_type()?;
    let name = function.func_name.lowercase();
    let simple = match name {
        "pi" if children.is_empty() => return node("Pi", children, ty),
        "sin" => "Sin",
        "cos" => "Cos",
        "tan" => "Tan",
        "cot" => "Cot",
        "asin" => "Asin",
        "acos" => "Acos",
        "sqrt" => "Sqrt",
        "exp" => "Exp",
        "degrees" => "Degrees",
        "radians" => "Radians",
        "sign" => "Sign",
        "log2" => "Log2",
        "log10" => "Log10",
        "ln" => "Log1Arg",
        "log" if children.len() == 1 => "Log1Arg",
        "log" if children.len() == 2 => "Log2Args",
        "atan" if children.len() == 1 => "Atan1Arg",
        "atan" | "atan2" if children.len() == 2 => "Atan2Args",
        "pow" | "power" => "Pow",
        "crc32" if children.len() == 1 => return node("Crc32", all_as(children, Bytes)?, ty),
        "conv" => return node("Conv", cast_args(children, &[Bytes, Int, Int])?, ty),
        _ => "",
    };
    if !simple.is_empty() {
        let expected = if matches!(simple, "Log2Args" | "Atan2Args" | "Pow") {
            2
        } else {
            1
        };
        return (children.len() == expected)
            .then_some(())
            .and_then(|()| node(simple, all_as(children, Real)?, ty));
    }
    let input = child_type(children.first()?)?;
    let domain = match input.eval_type() {
        Int => Int,
        Decimal => Decimal,
        _ => Real,
    };
    let signature = match name {
        "abs" if children.len() == 1 => {
            if domain == Int && input.is_unsigned() {
                "AbsUInt".to_owned()
            } else {
                format!("Abs{}", family(domain))
            }
        }
        "ceil" | "ceiling" | "floor" if children.len() == 1 => {
            let prefix = if name == "floor" { "Floor" } else { "Ceil" };
            let suffix = match (domain, ty.eval_type()) {
                (Real, Real) => "Real",
                (Int, Int) => "IntToInt",
                (Int, Decimal) => "IntToDec",
                (Decimal, Int) => "DecToInt",
                (Decimal, Decimal) => "DecToDec",
                _ => return None,
            };
            format!("{prefix}{suffix}")
        }
        "round" if matches!(children.len(), 1 | 2) => {
            let prefix = if children.len() == 1 {
                "Round"
            } else {
                "RoundWithFrac"
            };
            format!(
                "{prefix}{}",
                if domain == Decimal {
                    "Dec"
                } else {
                    family(domain)
                }
            )
        }
        "truncate" if children.len() == 2 => format!(
            "Truncate{}",
            if domain == Int && input.is_unsigned() {
                "Uint"
            } else {
                family(domain)
            }
        ),
        _ => return None,
    };
    let args = if children.len() == 2 {
        cast_args(children, &[domain, Int])?
    } else {
        all_as(children, domain)?
    };
    node(&signature, args, ty)
}

fn strings(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    use EvalType::{Int, String as Bytes};
    let ty = function.get_static_type()?;
    let name = function.func_name.lowercase();
    if matches!(name, "to_binary" | "from_binary") {
        return (children.len() == 1 && same_family(&children, Bytes))
            .then_some(())
            .and_then(|()| {
                node(
                    if name == "to_binary" {
                        "ToBinary"
                    } else {
                        "FromBinary"
                    },
                    children,
                    ty,
                )
            });
    }
    let source_type = function.args.first()?.static_type()?;
    let binary = source_type.is_binary_string();
    if name == "ord" {
        // TiKV Ord(NULL) returns 0 (its own test pins that behavior), unlike
        // Go/TiDB's NULL. A leaf-only typed wrapper restores NULL propagation
        // without duplicating a fallible/warning-producing child expression.
        if children.len() != 1 || !leaf(&function.args[0]) || !same_family(&children, Bytes) {
            return None;
        }
        let source = children.into_iter().next()?;
        let predicate = node(
            "StringIsNull",
            vec![source.clone()],
            &FieldType::new(FieldTypeCode::LongLong),
        )?;
        let null = PbExpr {
            tp: Some(ExprType::Null as i32),
            field_type: Some(field_type_to_pb(ty)?),
            ..PbExpr::default()
        };
        // ORD consumes the argument's charset, not the integer result charset.
        let ord_type = ty
            .clone()
            .with_charset_name(source_type.charset_name())
            .with_collation_name(source_type.collation_name());
        let value = node("Ord", vec![source], &ord_type)?;
        return node("IfInt", vec![predicate, null, value], ty);
    }
    let unary = match name {
        "length" | "octet_length" => "Length",
        "bit_length" => "BitLength",
        "ascii" => "Ascii",
        "ord" => "Ord",
        "ltrim" => "LTrim",
        "rtrim" => "RTrim",
        "quote" => "Quote",
        "unhex" => "UnHex",
        "lower" | "lcase" => {
            if binary {
                "Lower"
            } else {
                "LowerUtf8"
            }
        }
        "upper" | "ucase" => {
            if binary {
                "Upper"
            } else {
                "UpperUtf8"
            }
        }
        "reverse" => {
            if binary {
                "Reverse"
            } else {
                "ReverseUtf8"
            }
        }
        "char_length" | "character_length" => {
            if binary {
                "CharLength"
            } else {
                "CharLengthUtf8"
            }
        }
        _ => "",
    };
    if !unary.is_empty() {
        return (children.len() == 1)
            .then_some(())
            .and_then(|()| node(unary, all_as(children, Bytes)?, ty));
    }
    let (signature, targets): (&str, &[EvalType]) = match name {
        "hex" if children.len() == 1 => {
            if child_type(&children[0])?.eval_type() == Bytes {
                ("HexStrArg", &[Bytes])
            } else {
                ("HexIntArg", &[Int])
            }
        }
        "bin" => ("Bin", &[Int]),
        "oct" if children.len() == 1 => {
            if child_type(&children[0])?.eval_type() == Int {
                ("OctInt", &[Int])
            } else {
                ("OctString", &[Bytes])
            }
        }
        "left" => (if binary { "Left" } else { "LeftUtf8" }, &[Bytes, Int]),
        "right" => (if binary { "Right" } else { "RightUtf8" }, &[Bytes, Int]),
        "substring" | "substr" | "mid" if children.len() == 2 => (
            if binary {
                "Substring2Args"
            } else {
                "Substring2ArgsUtf8"
            },
            &[Bytes, Int],
        ),
        "substring" | "substr" | "mid" if children.len() == 3 => (
            if binary {
                "Substring3Args"
            } else {
                "Substring3ArgsUtf8"
            },
            &[Bytes, Int, Int],
        ),
        "substring_index" => ("SubstringIndex", &[Bytes, Bytes, Int]),
        "strcmp" => ("Strcmp", &[Bytes, Bytes]),
        "find_in_set" => ("FindInSet", &[Bytes, Bytes]),
        "instr" => (if binary { "Instr" } else { "InstrUtf8" }, &[Bytes, Bytes]),
        "locate" | "position" if children.len() == 2 => (
            if binary {
                "Locate2Args"
            } else {
                "Locate2ArgsUtf8"
            },
            &[Bytes, Bytes],
        ),
        "locate" | "position" if children.len() == 3 => (
            if binary {
                "Locate3Args"
            } else {
                "Locate3ArgsUtf8"
            },
            &[Bytes, Bytes, Int],
        ),
        "replace" => ("Replace", &[Bytes, Bytes, Bytes]),
        "trim" if children.len() == 1 => ("Trim1Arg", &[Bytes]),
        "trim" if children.len() == 2 => ("Trim2Args", &[Bytes, Bytes]),
        "trim" if children.len() == 3 => ("Trim3Args", &[Bytes, Bytes, Int]),
        "like" => ("LikeSig", &[Bytes, Bytes, Int]),
        "regexp" | "rlike" => ("RegexpSig", &[Bytes, Bytes]),
        "regexp_like" if children.len() == 2 => ("RegexpLikeSig", &[Bytes, Bytes]),
        "regexp_like" if children.len() == 3 => ("RegexpLikeSig", &[Bytes, Bytes, Bytes]),
        _ => return regexp_extended(function, children),
    };
    // FIND_IN_SET compares with the argument's collation; the engine compares
    // bytes, so a non-binary collation would answer differently.
    if name == "find_in_set"
        && !function
            .args
            .iter()
            .all(|arg| arg.static_type().is_some_and(|ty| ty.is_binary_string()))
    {
        return None;
    }
    node(signature, cast_args(children, targets)?, ty)
}

fn regexp_extended(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    use EvalType::{Int, String as Bytes};
    let ty = function.get_static_type()?;
    let (signature, min, targets): (&str, usize, &[EvalType]) = match function.func_name.lowercase()
    {
        "regexp_substr" => ("RegexpSubstrSig", 2, &[Bytes, Bytes, Int, Int, Bytes]),
        "regexp_instr" => ("RegexpInStrSig", 2, &[Bytes, Bytes, Int, Int, Int, Bytes]),
        "regexp_replace" => (
            "RegexpReplaceSig",
            3,
            &[Bytes, Bytes, Bytes, Int, Int, Bytes],
        ),
        _ => return None,
    };
    let len = children.len();
    if !(min..=targets.len()).contains(&len) {
        return None;
    }
    node(signature, cast_args(children, &targets[..len])?, ty)
}

fn miscellaneous(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    use EvalType::{Int, String as Bytes};
    let ty = function.get_static_type()?;
    let (signature, targets): (&str, &[EvalType]) = match function.func_name.lowercase() {
        "bit_count" => ("BitCount", &[Int]),
        "md5" => ("Md5", &[Bytes]),
        "sha" | "sha1" => ("Sha1", &[Bytes]),
        "sha2" => ("Sha2", &[Bytes, Int]),
        "compress" => ("Compress", &[Bytes]),
        "uncompress" => ("Uncompress", &[Bytes]),
        "uncompressed_length" => ("UncompressedLength", &[Bytes]),
        "inet_aton" => ("InetAton", &[Bytes]),
        "inet_ntoa" => ("InetNtoa", &[Int]),
        "inet6_aton" => ("Inet6Aton", &[Bytes]),
        "inet6_ntoa" => ("Inet6Ntoa", &[Bytes]),
        "is_ipv4" => ("IsIPv4", &[Bytes]),
        "is_ipv6" => ("IsIPv6", &[Bytes]),
        "is_ipv4_compat" => ("IsIPv4Compat", &[Bytes]),
        "is_ipv4_mapped" => ("IsIPv4Mapped", &[Bytes]),
        "uuid_version" => ("UuidVersion", &[Bytes]),
        "uuid_timestamp" => ("UuidTimestamp", &[Bytes]),
        "any_value" if children.len() == 1 => {
            return node(&format!("{}AnyValue", family(ty.eval_type())), children, ty)
        }
        _ => return None,
    };
    // TiKV's `IsIPv4`/`IsIPv6`/compat/mapped kernels return 0 for NULL input,
    // while Go returns NULL. A leaf-only NULL mask restores the value without
    // duplicating a fallible child: the kernel itself cannot error or warn, so
    // the mask is correct whether the engine evaluates `IfInt` eagerly or
    // lazily.
    if matches!(
        function.func_name.lowercase(),
        "is_ipv4" | "is_ipv6" | "is_ipv4_compat" | "is_ipv4_mapped"
    ) {
        if children.len() != 1 || !leaf(&function.args[0]) {
            return None;
        }
        let source = children.into_iter().next()?;
        let predicate = node(
            "StringIsNull",
            vec![source.clone()],
            &FieldType::new(FieldTypeCode::LongLong),
        )?;
        let null = PbExpr {
            tp: Some(ExprType::Null as i32),
            field_type: Some(field_type_to_pb(ty)?),
            ..PbExpr::default()
        };
        let value = node(signature, vec![source], ty)?;
        return node("IfInt", vec![predicate, null, value], ty);
    }
    node(signature, cast_args(children, targets)?, ty)
}

/// Reuse a catalog's type selector/coercions without asking it to recursively
/// lower our real children. Otherwise it can reject new local-only descendants,
/// or silently replace nested Decimal/FSP/collation metadata with defaults.
fn catalog_call(function: &ScalarFunction, children: Vec<PbExpr>) -> Option<PbExpr> {
    // Refusal by a local safety/type rule must not be bypassed by the fallback.
    if families::owns(function.func_name.lowercase()) {
        return None;
    }
    if matches!(
        function.func_name.lowercase(),
        "cast"
            | "if"
            | "ifnull"
            | "coalesce"
            | "case"
            | "casewhen"
            | "in"
            | "and"
            | "or"
            | "elt"
            | "field"
            | "interval"
            | "greatest"
            | "least"
            | "week"
            | "hour"
            | "minute"
            | "second"
            | "microsecond"
            | "minus"
            | "ord"
    ) {
        return None;
    }
    let mut skeleton = function.clone();
    skeleton.ret_type = Some(wire_type(function.get_static_type()?)?);
    skeleton.args = function
        .args
        .iter()
        .enumerate()
        .map(|(index, arg)| {
            let mut column =
                Column::new(i64::try_from(index).ok()?, wire_type(arg.static_type()?)?);
            column.index = i64::try_from(index).ok()?;
            Some(Expression::Column(column))
        })
        .collect::<Option<_>>()?;
    let descriptor = |offset: u32| {
        let ty = function.args.get(offset as usize)?.static_type()?;
        Some(ColumnDescriptor {
            tp: i32::from(ty.code().mysql_type()),
            flag: ty.flags(),
            flen: i32::try_from(ty.flen()).ok()?,
            decimal: i32::try_from(ty.decimal()).ok()?,
            charset: ty.charset_name().to_owned(),
            collation: ty.collation_name().to_owned(),
            elems: ty
                .elems_snapshot()
                .into_iter()
                .map(|elem| elem.to_string())
                .collect(),
            array: ty.is_array(),
        })
    };
    let mut encoded =
        pushdown_catalog::expression_to_pb(&Expression::ScalarFunction(skeleton), &descriptor)?;
    fn substitute(expr: &mut PbExpr, children: &[PbExpr]) -> Option<()> {
        if expr.tp == Some(ExprType::ColumnRef as i32) {
            let bytes: [u8; 8] = expr.val.as_deref()?.try_into().ok()?;
            let index = i64::from_be_bytes(bytes) ^ i64::MIN;
            *expr = children.get(usize::try_from(index).ok()?)?.clone();
        } else {
            for child in &mut expr.children {
                substitute(child, children)?;
            }
        }
        Some(())
    }
    substitute(&mut encoded, &children)?;
    Some(encoded)
}
