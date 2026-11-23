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

//! `pkg/expression/scalar_function.go`: the `ScalarFunction` expression node.
//!
//! BRIDGE DECISION: Go keeps a `ScalarFunction`'s arguments and evaluation
//! behind a `Function builtinFunc` interface (implemented by hundreds of
//! per-signature structs). This port instead holds `args: Vec<Expression>`
//! directly on the node. Protobuf functions retain a typed implementation
//! selected by `tipb.ScalarFuncSig`; SQL-built functions still use the
//! existing name-based builder and shared evaluation kernels.
//!
//! Ported: the struct and its argument-structural methods, recursive
//! `Decorrelate`, const-level rules, the common `ReHashCode` path (including
//! `Grouping` metadata), structural `Hash64`/`Equals`, and evaluation for
//! operators plus the builtin families owned by the shared dispatch modules.
//! Unknown builtin names fail explicitly. `MemoryUsage` lives in
//! `crate::memory_usage`; the remaining structural gap is per-signature
//! collation.

mod pb_builtin;

pub(crate) use pb_builtin::PbBuiltin;

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use crate::context::{Columns, EvalError};
use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, SCALAR_FUNCTION_FLAG};
use crate::grouping::{GroupingMetadata, GroupingMetadataError, GroupingMode};
use crate::schema::Schema;
use tidb_ast::{BinaryOp, CiString, UnaryOp};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::row::Row;
use tidb_codec::{encode_compact_bytes, encode_int};
use tidb_datatype::{Datum, EvalType, FieldType, UNSPECIFIED_LENGTH};

const MAX_ADVISORY_LOCK_TIMEOUT_SECS: i64 = 1_073_741_824;

pub(crate) fn advisory_lock_spec(name: &str) -> Option<(&'static str, usize)> {
    Some(match name {
        "get_lock" => ("get_lock", 2),
        "release_lock" => ("release_lock", 1),
        "is_free_lock" => ("is_free_lock", 1),
        "is_used_lock" => ("is_used_lock", 1),
        "release_all_locks" => ("release_all_locks", 0),
        _ => return None,
    })
}

pub(crate) fn advisory_lock_name_value(
    value: &Datum,
    field: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<String, EvalError> {
    advisory_lock_name(crate::cast::cast_arg_as_string(value, field, ctx)?)
}

pub(crate) fn advisory_lock_value(
    name: &'static str,
    lock_name: &str,
    timeout: Option<(&Datum, Option<&FieldType>)>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    match name {
        "get_lock" => {
            let (value, field) = timeout.ok_or(EvalError::WrongParameterCount(name))?;
            let value = crate::cast::cast_arg_as_int(value, field, ctx)?;
            let mut timeout = crate::arg_eval_type::eval_int(&value)?.unwrap_or(0);
            if !(0..=MAX_ADVISORY_LOCK_TIMEOUT_SECS).contains(&timeout) {
                ctx.append_warning(
                    1292,
                    &format!("Truncated incorrect get_lock value: '{timeout}'"),
                );
                timeout = MAX_ADVISORY_LOCK_TIMEOUT_SECS;
            }
            let acquired = ctx
                .acquire_advisory_lock(lock_name, std::time::Duration::from_secs(timeout as u64))?;
            Ok(Datum::Int(i64::from(acquired)))
        }
        "release_lock" => Ok(Datum::Int(i64::from(ctx.release_advisory_lock(lock_name)?))),
        "is_free_lock" => Ok(Datum::Int(i64::from(
            ctx.advisory_lock_owner(lock_name)?.is_none(),
        ))),
        "is_used_lock" => Ok(ctx
            .advisory_lock_owner(lock_name)?
            .map_or(Datum::Null, |owner| Datum::Int(owner as i64))),
        "release_all_locks" => Ok(Datum::Int(
            i64::try_from(ctx.release_all_advisory_locks()?).unwrap_or(i64::MAX),
        )),
        _ => unreachable!("recognized advisory lock function"),
    }
}

fn advisory_lock_name(value: Datum) -> Result<String, EvalError> {
    let bytes = crate::arg_eval_type::eval_string(&value)?;
    let Some(bytes) = bytes else {
        return Err(EvalError::AdvisoryLock {
            code: 3057,
            message: "Incorrect user-level lock name 'NULL'.".to_owned(),
        });
    };
    let text = tidb_datatype::GoString::from_bytes(bytes).to_utf8_lossy_go();
    if text.is_empty() || text.chars().count() > 64 {
        return Err(EvalError::AdvisoryLock {
            code: 3057,
            message: format!("Incorrect user-level lock name '{text}'."),
        });
    }
    let normalized = tidb_mysql::to_lowercase(&text);
    if normalized.chars().count() > 64 {
        return Err(EvalError::IncorrectArguments(
            "Incorrect arguments to get_lock".to_owned(),
        ));
    }
    Ok(normalized)
}

/// Maps a Go binary-operator scalar-function name (`pkg/parser/ast`) to a
/// [`BinaryOp`]. Returns `None` for any function that is not a binary operator.
/// go `ToBool`'s string path parses the leading float and warns
/// `Truncated incorrect DOUBLE value` when the text is not fully numeric;
/// the logical operators' truthiness is that float != 0.
fn binary_op_for_name(name: &str) -> Option<BinaryOp> {
    Some(match name {
        "plus" => BinaryOp::Plus,
        "minus" => BinaryOp::Minus,
        "mul" => BinaryOp::Mul,
        "div" => BinaryOp::Div,
        "intdiv" => BinaryOp::IntDiv,
        "mod" => BinaryOp::Mod,
        "bitand" => BinaryOp::BitAnd,
        "bitor" => BinaryOp::BitOr,
        "bitxor" => BinaryOp::BitXor,
        "leftshift" => BinaryOp::LeftShift,
        "rightshift" => BinaryOp::RightShift,
        "eq" => BinaryOp::Eq,
        "nulleq" => BinaryOp::NullEq,
        "ne" => BinaryOp::Ne,
        "lt" => BinaryOp::Lt,
        "le" => BinaryOp::Le,
        "gt" => BinaryOp::Gt,
        "ge" => BinaryOp::Ge,
        "and" => BinaryOp::LogicAnd,
        "or" => BinaryOp::LogicOr,
        "xor" => BinaryOp::LogicXor,
        _ => return None,
    })
}

/// The eval-type family a [`Datum`] belongs to -- the inverse of the switch
/// Go's `ScalarFunction.Eval` performs on `RetType.EvalType()`. `None` is a
/// kind no eval type names (NULL, the range sentinels, an undecoded `Raw`).
///
/// `Datum::Time` is the value domain of BOTH Go's `ETDatetime` and
/// `ETTimestamp`, so it reports `Datetime` and a timestamp result type is
/// normalized onto it by [`same_eval_family`].
fn datum_eval_type(value: &Datum) -> Option<tidb_datatype::EvalType> {
    use tidb_datatype::EvalType;
    Some(match value {
        Datum::Int(_) | Datum::UInt(_) => EvalType::Int,
        Datum::Real(_) | Datum::Float32(_) => EvalType::Real,
        Datum::Decimal(_) => EvalType::Decimal,
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::BinaryLiteral(_)
        | Datum::Bit(_)
        | Datum::Enum(..)
        | Datum::Set(..) => EvalType::String,
        Datum::Time(_) => EvalType::Datetime,
        Datum::Duration(_) => EvalType::Duration,
        Datum::Json(_) => EvalType::Json,
        Datum::VectorFloat32(_) => EvalType::VectorFloat32,
        Datum::Null | Datum::MinNotNull | Datum::MaxValue | Datum::Raw(_) => return None,
    })
}

/// Whether `value` is already the family `ret_type` declares.
///
/// A HYBRID result type (`BIT`/`ENUM`/`SET`, Go `FieldType.Hybrid`) always
/// answers yes: its eval type is the domain it COMPARES in (`BIT` compares as
/// an integer), while `getFixedLen`'s default arm gives it a variable-length
/// cell that only the hybrid datum itself fits. Converting such a value onto
/// its eval type would put an integer in a var-length column -- the very
/// disagreement this check exists to prevent.
fn same_eval_family(value: &Datum, ret_type: &tidb_datatype::FieldType) -> bool {
    use tidb_datatype::EvalType;
    if ret_type.is_hybrid() {
        return true;
    }
    // The REAL family is the one place where sharing an eval type is not
    // enough. Go's `EvalReal` is float64-valued for both `FLOAT` and `DOUBLE`,
    // so a 4-byte `KindFloat32` datum is only ever a `FLOAT` COLUMN's own
    // cell, never an expression's result -- while `getFixedLen` gives
    // `TypeFloat` a 4-byte cell and `TypeDouble` an 8-byte one. A `Float32`
    // value under a `DOUBLE` result type therefore has to widen, or
    // `append_float32` writes 4 bytes into an 8-byte cell.
    if ret_type.eval_type() == EvalType::Real {
        return match value {
            Datum::Float32(_) => ret_type.code() == tidb_datatype::FieldTypeCode::Float,
            Datum::Real(_) => ret_type.code() != tidb_datatype::FieldTypeCode::Float,
            _ => false,
        };
    }
    let normalize = |t| match t {
        EvalType::Timestamp => EvalType::Datetime,
        other => other,
    };
    datum_eval_type(value).is_some_and(|got| normalize(got) == normalize(ret_type.eval_type()))
}

/// Go `unFoldableFunctions`: calls whose result cannot be frozen during
/// constant folding even when every argument is a strict literal.
#[must_use]
pub fn is_unfoldable_function(name: &str) -> bool {
    name.starts_with("getvar_")
        || matches!(
            name,
            "sysdate"
                | "found_rows"
                | "rand"
                | "uuid"
                | "uuid_v4"
                | "uuid_v7"
                | "sleep"
                | "row"
                | "values"
                | "setvar"
                | "getvar"
                | "getparam"
                | "benchmark"
                | "dayname"
                // These information functions read session properties in
                // Go (`CurrentDB`, `CurrentUserPropReader`, or
                // `SessionVarsPropReader`). Rust's planner fold has only
                // `NoColumns`, so keep them runtime-bound instead of
                // replacing a missing property with a frozen NULL.
                | "database"
                | "schema"
                | "current_user"
                | "current_role"
                | "current_resource_group"
                | "user"
                | "session_user"
                | "system_user"
                | "connection_id"
                | "row_count"
                | "version"
                | "tidb_version"
                | "nextval"
                | "lastval"
                | "setval"
                | "any_value"
        )
}

/// The Go scalar-function name for a binary operator (inverse of
/// [`binary_op_for_name`]); used when building a [`ScalarFunction`] from an AST
/// operator.
#[must_use]
pub fn binary_op_name(op: BinaryOp) -> &'static str {
    op.opcode().name()
}

/// The Go scalar-function name for a unary operator (inverse of
/// [`unary_op_for_name`]). `Not`/`NotKeyword` share the `not` function.
#[must_use]
pub fn unary_op_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Plus => "unaryplus",
        UnaryOp::Minus => "unaryminus",
        UnaryOp::BitNeg => "bitneg",
        UnaryOp::Not | UnaryOp::NotKeyword => "not",
    }
}

fn append_canonical_name(output: &mut Vec<u8>, name: &str) {
    encode_compact_bytes(output, name.as_bytes());
}

fn append_canonical_args(output: &mut Vec<u8>, args: &[Vec<u8>]) {
    for code in args {
        output.extend_from_slice(code);
    }
}

fn append_canonical_args_reversed(output: &mut Vec<u8>, args: &[Vec<u8>]) {
    for code in args.iter().rev() {
        output.extend_from_slice(code);
    }
}

#[derive(Debug, Default)]
struct RandCallIdentity(std::sync::OnceLock<usize>);

impl Clone for RandCallIdentity {
    fn clone(&self) -> Self {
        Self::default()
    }
}

/// Go `ScalarFunction`: the application of a built-in function to arguments.
#[derive(Clone, Debug, Default)]
pub struct ScalarFunction {
    /// Go `FuncName` (an `ast.CIStr`): the function's name.
    pub func_name: CiString,
    /// Go `RetType` (a `*types.FieldType`; `None` mirrors a nil pointer).
    pub ret_type: Option<FieldType>,
    /// The function arguments. In Go these live inside `Function.getArgs()`.
    pub args: Vec<Expression>,
    /// Go `builtinValues*Sig.offset`, carried by `NewValuesFunc`. `VALUES()`
    /// has no runtime argument: its column position is fixed when the
    /// expression is built and the value is read from the statement's
    /// current-insert row at evaluation time.
    values_offset: Option<usize>,
    /// Go Function: a protobuf-selected builtin, independent of FuncName.
    pb_builtin: Option<PbBuiltin>,
    rand_call_identity: RandCallIdentity,
    pub(crate) rand_occurrence_group: Option<usize>,
    /// Lazily-filled `HashCode` cache (Go `hashcode`).
    hashcode: Vec<u8>,
    /// Go `BuiltinGroupingImplSig` metadata installed by `SetMetadata`.
    ///
    /// Grouping is built through `NewFunctionWithInit`, so carrying the
    /// validated metadata on the node is what lets clones and substitution
    /// preserve the function's grouping-id semantics and hash identity.
    grouping_metadata: Option<GroupingMetadata>,

    /// Go embedded collation state (via the `Function`'s `collationInfo`).
    pub collation: CollationInfo,
}

pub(crate) fn last_insert_id_with_value(
    ctx: &dyn Columns,
    value: &Datum,
    field: Option<&FieldType>,
) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let cast = crate::cast::cast_arg_as_int(value, field, ctx)?;
    let recorded = match cast {
        Datum::Int(i) => i as u64,
        Datum::UInt(u) => u,
        _ => return Err(EvalError::Unsupported("LAST_INSERT_ID argument")),
    };
    ctx.set_last_insert_id(recorded);
    Ok(Datum::Int(recorded as i64))
}

fn arithmetic_symbol(op: tidb_ast::BinaryOp) -> Option<&'static str> {
    Some(match op {
        tidb_ast::BinaryOp::Plus => "+",
        tidb_ast::BinaryOp::Minus => "-",
        tidb_ast::BinaryOp::Mul => "*",
        tidb_ast::BinaryOp::Div => "/",
        tidb_ast::BinaryOp::IntDiv => "DIV",
        tidb_ast::BinaryOp::Mod => "%",
        _ => return None,
    })
}

fn numeric_expression_text(
    expression: &Expression,
    go_float_format: bool,
    ctx: &dyn Columns,
) -> Option<String> {
    match expression {
        Expression::Constant(constant) => match constant.eval_in(ctx).ok()? {
            Datum::Int(value) => Some(value.to_string()),
            Datum::UInt(value) => Some(value.to_string()),
            Datum::Float32(value) => {
                if go_float_format {
                    Some(tidb_datatype::format_float_g_shortest(value))
                } else {
                    Some(value.to_string())
                }
            }
            Datum::Real(value) => {
                if go_float_format {
                    Some(tidb_datatype::format_float_g_shortest(value))
                } else {
                    Some(value.to_string())
                }
            }
            Datum::Decimal(value) => Some(value.to_string()),
            Datum::BinaryLiteral(value) => Some(value.to_int().value().to_string()),
            Datum::Null => Some("NULL".to_owned()),
            value @ (Datum::String(_) | Datum::Bytes(_) | Datum::Json(_)) => {
                String::from_utf8(value.truncated_stringify().ok()?).ok()
            }
            _ => None,
        },
        Expression::Column(column) => {
            if column.is_hidden {
                if let Some(expression) = column.virtual_expr.as_deref() {
                    return numeric_expression_text(expression, go_float_format, ctx);
                }
            }
            Some(if column.orig_name.is_empty() {
                format!("Column#{}", column.unique_id)
            } else {
                column.orig_name.clone()
            })
        }
        Expression::CorrelatedColumn(column) => Some(if column.column.orig_name.is_empty() {
            format!("Column#{}", column.column.unique_id)
        } else {
            column.column.orig_name.clone()
        }),
        Expression::ScalarFunction(function) => {
            if function.func_name.lowercase() == "cast_json" {
                let [argument] = function.args.as_slice() else {
                    return None;
                };
                return Some(format!(
                    "cast({}, {})",
                    numeric_expression_text(argument, go_float_format, ctx)?,
                    function.get_static_type()?.source_string(),
                ));
            }
            let op = arithmetic_symbol(binary_op_for_name(function.func_name.lowercase())?)?;
            let [left, right] = function.args.as_slice() else {
                return None;
            };
            Some(format!(
                "({} {op} {})",
                numeric_expression_text(left, go_float_format, ctx)?,
                numeric_expression_text(right, go_float_format, ctx)?
            ))
        }
    }
}

// Go wraps non-decimal DIV arguments in a cast during construction. Strict
// constants fold that cast; columns and computed arguments retain it in errors.
fn numeric_argument_text(
    function: &ScalarFunction,
    expression: &Expression,
    float: bool,
    ctx: &dyn Columns,
    decimal: Option<&tidb_datatype::Decimal>,
) -> Option<String> {
    let field = expression.static_type()?;
    // Binary constant casts fold in Go, so diagnostics show the value in
    // the operator's domain, including signed carriers and real rounding.
    let text = match expression {
        Expression::Constant(constant) if is_numeric_binary_literal(expression) => {
            let Datum::BinaryLiteral(value) = &constant.value else {
                unreachable!()
            };
            let value = value.to_int().value();
            match function.numeric_operand_domain() {
                Some(EvalType::Int) if !field.is_unsigned() => (value as i64).to_string(),
                Some(EvalType::Real) => tidb_datatype::format_float_g_shortest(value as f64),
                _ => value.to_string(),
            }
        }
        Expression::ScalarFunction(child)
            if child.func_name.lowercase() == "unaryminus"
                && child.args.first().is_some_and(|value| {
                    matches!(value, Expression::Constant(constant) if constant.literal_value().is_some())
                })
                && decimal.is_some() =>
        {
            decimal.unwrap().to_string()
        }
        _ => numeric_expression_text(expression, float, ctx)?,
    };
    if function.func_name.lowercase() != "intdiv"
        || function.numeric_operand_domain() != Some(EvalType::Decimal)
        || field.eval_type() == EvalType::Decimal
        || matches!(expression, Expression::Constant(constant) if constant.literal_value().is_some())
    {
        return Some(text);
    }
    if matches!(expression, Expression::ScalarFunction(child) if child.func_name.lowercase() == "unaryminus")
        && decimal.is_some()
        && expression.const_level() == ConstLevel::STRICT
    {
        return Some(text);
    }
    let mut target = numeric_decimal_cast_type(field);
    if expression.const_level() == ConstLevel::STRICT {
        if let Some(value) = decimal {
            let (precision, fraction) = value.precision_and_frac();
            target.set_flen_under_limit(i64::from(precision));
            target.set_decimal_under_limit(i64::from(fraction));
        }
    }
    Some(format!(
        "cast({text}, decimal({},{}){} BINARY)",
        target.flen(),
        target.decimal().max(0),
        if field.is_unsigned() { " UNSIGNED" } else { "" }
    ))
}

/// Go WrapWithCastAsDecimal: retain declared precision/scale for non-integer
/// sources; integer casts use the decimal width of the integer storage type.
fn numeric_decimal_cast_type(field: &FieldType) -> FieldType {
    let mut target = FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal);
    if field.eval_type() == EvalType::Int {
        use tidb_datatype::FieldTypeCode::*;
        target.set_flen(match field.code() {
            Tiny => 3,
            Short => 5,
            Int24 => 8,
            Long => 10,
            LongLong => 20,
            Year => 4,
            _ => 20,
        });
        target.set_decimal(0);
    } else {
        target.set_flen(if field.flen() < 0 {
            65
        } else {
            field.flen().min(65)
        });
        target.set_decimal_under_limit(field.decimal());
    }
    target
}

fn integer_division_overflow(
    left: crate::coerce::Integer,
    right: crate::coerce::Integer,
) -> EvalError {
    use crate::coerce::Integer;
    let text = |value| match value {
        Integer::Signed(value) => value.to_string(),
        Integer::Unsigned(value) => value.to_string(),
    };
    EvalError::DataOutOfRange {
        value: if matches!(left, Integer::Unsigned(_)) || matches!(right, Integer::Unsigned(_)) {
            "BIGINT UNSIGNED"
        } else {
            "BIGINT"
        },
        expression: format!("({}, {})", text(left), text(right)),
    }
}

/// Renders the operand list Go's `StringWithCtx(errors.RedactLogDisable)` uses
/// in an arithmetic overflow. Resolved columns and nested arithmetic
/// functions retain their source names and shape; unknown expression kinds
/// keep the caller's safe fallback instead of inventing a display string.
fn arithmetic_overflow_expression(
    function: &ScalarFunction,
    op: tidb_ast::BinaryOp,
    go_float_format: bool,
    ctx: &dyn Columns,
) -> Option<String> {
    let symbol = arithmetic_symbol(op)?;
    let [left, right] = function.get_args() else {
        return None;
    };
    Some(format!(
        "({} {symbol} {})",
        numeric_argument_text(function, left, go_float_format, ctx, None)?,
        numeric_argument_text(function, right, go_float_format, ctx, None)?
    ))
}

/// Go's 1690 text for one binary integer overflow: the result type's integer
/// class (`BIGINT` / `BIGINT UNSIGNED`, from the function's declared result
/// type) and the rendered operand list.
fn arithmetic_overflow_error(
    function: &ScalarFunction,
    op: tidb_ast::BinaryOp,
    ctx: &dyn Columns,
) -> EvalError {
    let Some(operands) = arithmetic_overflow_expression(function, op, false, ctx) else {
        return EvalError::IntOverflow;
    };
    let class = match function.get_static_type() {
        Some(field_type) if field_type.is_unsigned() => "BIGINT UNSIGNED",
        _ => "BIGINT",
    };
    EvalError::DataOutOfRange {
        value: class,
        expression: operands,
    }
}

/// Go's 1690 text for one binary REAL overflow. The real signatures use the
/// same operand rendering as integer signatures, but always name `DOUBLE`.
fn real_arithmetic_overflow_error(
    function: &ScalarFunction,
    op: tidb_ast::BinaryOp,
    ctx: &dyn Columns,
) -> EvalError {
    let Some(operands) = arithmetic_overflow_expression(function, op, true, ctx) else {
        return EvalError::FloatOverflow;
    };
    EvalError::DataOutOfRange {
        value: "DOUBLE",
        expression: operands,
    }
}

/// Go's 1690 text for one binary DECIMAL overflow. The decimal signatures
/// carry the same source-shaped operand expression and name their domain.
fn decimal_arithmetic_overflow_error(
    function: &ScalarFunction,
    op: tidb_ast::BinaryOp,
    ctx: &dyn Columns,
) -> EvalError {
    let Some(operands) = arithmetic_overflow_expression(function, op, false, ctx) else {
        return EvalError::DecimalOverflow;
    };
    EvalError::DataOutOfRange {
        value: "DECIMAL",
        expression: operands,
    }
}

/// Renders the argument expression used by Go's math overflow signatures.
/// These signatures report their function name and source-shaped arguments,
/// rather than the datum-only `FloatOverflow` carrier returned by the shared
/// math implementation.
impl ScalarFunction {
    /// Go `cap(sf.hashcode)`, for `MemoryUsage`.
    pub(crate) fn hashcode_capacity(&self) -> usize {
        self.hashcode.capacity()
    }

    pub(crate) fn rand_call_id(&self) -> usize {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        *self
            .rand_call_identity
            .0
            .get_or_init(|| NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn share_rand_call(&mut self, copy: &mut Self) {
        let group = self
            .rand_occurrence_group
            .unwrap_or_else(|| self.rand_call_id());
        self.rand_occurrence_group = Some(group);
        copy.rand_occurrence_group = Some(group);
    }

    /// Builds a scalar-function node.
    #[must_use]
    pub fn new(func_name: CiString, ret_type: FieldType, args: Vec<Expression>) -> Self {
        ScalarFunction {
            func_name,
            ret_type: Some(ret_type),
            args,
            ..Default::default()
        }
    }

    /// Go newDistSQLFunctionBySig: retain the wire type and implementation.
    pub(crate) fn from_pb(builtin: PbBuiltin, ret_type: FieldType, args: Vec<Expression>) -> Self {
        Self {
            func_name: CiString::new(format!("sig_{:?}", builtin.signature())),
            ret_type: Some(ret_type),
            args,
            pb_builtin: Some(builtin),
            ..Default::default()
        }
    }

    /// The signature selected by protobuf decoding, preserved by Clone.
    #[must_use]
    pub fn pb_signature(&self) -> Option<tidb_proto::tipb::ScalarFuncSig> {
        self.pb_builtin.map(PbBuiltin::signature)
    }

    /// Builds Go's `NewValuesFunc(ctx, offset, retTp)` node. The offset is
    /// immutable build-time state, while the current insert row is supplied
    /// by [`Columns::current_insert_value`] for each evaluation.
    #[must_use]
    pub fn new_values(offset: usize, ret_type: FieldType) -> Self {
        Self {
            func_name: CiString::new("values"),
            ret_type: Some(ret_type),
            values_offset: Some(offset),
            ..Default::default()
        }
    }

    /// Invalidates memoized state derived from this function's arguments.
    ///
    /// Cached physical-plan rebuild replaces parameter constants inside the
    /// argument tree.  The expression node itself is retained, so every
    /// argument-derived cache must be discarded before the rebound tree is
    /// used by ranger or executor code.
    pub fn invalidate_cached_arguments(&mut self) {
        self.hashcode.clear();
        self.rand_call_identity = Default::default();
    }

    /// Go `BuiltinGroupingImplSig.SetMetadata`: install validated grouping
    /// mode/mark metadata on a `grouping` scalar function and invalidate its
    /// cached hash code. A failed replacement leaves the metadata
    /// uninitialized, matching the source signature's `isMetaInited` flag.
    pub fn set_grouping_metadata(
        &mut self,
        mode: GroupingMode,
        grouping_marks: Vec<BTreeSet<u64>>,
    ) -> Result<(), GroupingMetadataError> {
        self.clean_hash_code();
        self.grouping_metadata = None;
        let metadata = GroupingMetadata::new(mode, grouping_marks)?;
        self.grouping_metadata = Some(metadata);
        Ok(())
    }

    /// Returns the validated grouping metadata installed on this function.
    pub fn grouping_metadata(&self) -> Result<&GroupingMetadata, GroupingMetadataError> {
        self.grouping_metadata
            .as_ref()
            .ok_or(GroupingMetadataError::Uninitialized)
    }

    /// Whether this function has completed the source `SetMetadata` step.
    #[must_use]
    pub fn has_grouping_metadata(&self) -> bool {
        self.grouping_metadata.is_some()
    }

    /// Go `ScalarFunction.Decorrelate`: recursively decorrelate every
    /// argument and invalidate hashes/caches derived from the old tree.
    ///
    /// Expressions are owned values in Rust, so this method rebuilds a clone
    /// rather than mutating an aliased node as Go does.
    #[must_use]
    pub fn decorrelate(&self, schema: Option<&Schema>) -> Self {
        let mut decorrelated = self.clone();
        decorrelated.args = self
            .args
            .iter()
            .map(|argument| argument.decorrelate(schema))
            .collect();
        decorrelated.invalidate_cached_arguments();
        decorrelated
    }

    /// Go `GetStaticType` / `GetType` (which ignores its `EvalContext`).
    #[must_use]
    pub fn get_static_type(&self) -> Option<&FieldType> {
        self.ret_type.as_ref()
    }

    /// Go `GetArgs`.
    #[must_use]
    pub fn get_args(&self) -> &[Expression] {
        &self.args
    }

    /// Go `IsCorrelated`: correlated iff any argument is correlated.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        self.args.iter().any(Expression::is_correlated)
    }

    /// Go `HashCode` (`ReHashCode`), cached on first call:
    /// `[scalarFunctionFlag, EncodeCompactBytes(FuncName.L), arg.HashCode()...]`,
    /// plus, for `cast`, a trailing byte for the target `EvalType`, or, for
    /// `grouping`, the mode, mark count, each mark's size, and sorted keys.
    pub fn hash_code(&mut self) -> &[u8] {
        if !self.hashcode.is_empty() {
            return &self.hashcode;
        }
        self.hashcode.push(SCALAR_FUNCTION_FLAG);
        encode_compact_bytes(&mut self.hashcode, self.func_name.lowercase().as_bytes());
        // Collect the args' hash codes first to avoid overlapping borrows.
        let arg_codes: Vec<Vec<u8>> = self
            .args
            .iter_mut()
            .map(|a| a.hash_code().to_vec())
            .collect();
        for code in arg_codes {
            self.hashcode.extend_from_slice(&code);
        }
        let name = self.func_name.lowercase();
        if name == "values" {
            encode_int(
                &mut self.hashcode,
                self.values_offset
                    .map_or(-1, |offset| i64::try_from(offset).unwrap_or(i64::MAX)),
            );
        }
        // Cast is special: its result type is effectively an argument.
        if name == "cast" {
            if let Some(rt) = &self.ret_type {
                self.hashcode.push(rt.eval_type() as u8);
            }
        }
        if name == "grouping" {
            let metadata = self
                .grouping_metadata
                .as_ref()
                .expect("grouping metadata is not initialized");
            encode_int(&mut self.hashcode, metadata.mode() as u8 as i64);
            encode_int(
                &mut self.hashcode,
                i64::try_from(metadata.grouping_marks().len()).expect("grouping mark count fits"),
            );
            for mark in metadata.grouping_marks() {
                encode_int(
                    &mut self.hashcode,
                    i64::try_from(mark.len()).expect("grouping mark size fits"),
                );
                for key in mark {
                    // Go casts the uint64 key to int64 before EncodeInt;
                    // preserve the same two's-complement bit pattern.
                    encode_int(&mut self.hashcode, *key as i64);
                }
            }
        }
        &self.hashcode
    }

    /// Go `ScalarFunction.CanonicalHashCode` and
    /// `simpleCanonicalizedHashCode`: normalize commutative operators and
    /// equivalent directed comparisons before concatenating child hashes.
    /// The bytes are freshly owned so a rewrite cannot mutate a returned
    /// canonical key through an alias.
    #[must_use]
    pub fn canonical_hash_code(&self) -> Vec<u8> {
        let arg_codes: Vec<Vec<u8>> = self
            .args
            .iter()
            .map(Expression::canonical_hash_code)
            .collect();
        let name = self.func_name.lowercase();
        let mut canonical = vec![SCALAR_FUNCTION_FLAG];

        match name {
            "plus" | "mul" | "eq" | "in" | "or" | "and" => {
                append_canonical_name(&mut canonical, name);
                let mut sorted = arg_codes;
                sorted.sort();
                append_canonical_args(&mut canonical, &sorted);
            }
            "ge" | "le" => {
                append_canonical_name(&mut canonical, "ge");
                if name == "ge" {
                    append_canonical_args(&mut canonical, &arg_codes);
                } else {
                    append_canonical_args_reversed(&mut canonical, &arg_codes);
                }
            }
            "gt" | "lt" => {
                append_canonical_name(&mut canonical, "gt");
                if name == "gt" {
                    append_canonical_args(&mut canonical, &arg_codes);
                } else {
                    append_canonical_args_reversed(&mut canonical, &arg_codes);
                }
            }
            "not" => {
                if let Some(Expression::ScalarFunction(child)) = self.args.first() {
                    let child_args: Vec<Vec<u8>> = child
                        .args
                        .iter()
                        .map(Expression::canonical_hash_code)
                        .collect();
                    match child.func_name.lowercase() {
                        "gt" => {
                            append_canonical_name(&mut canonical, "ge");
                            append_canonical_args_reversed(&mut canonical, &child_args);
                        }
                        "lt" => {
                            append_canonical_name(&mut canonical, "ge");
                            append_canonical_args(&mut canonical, &child_args);
                        }
                        "ge" => {
                            append_canonical_name(&mut canonical, "gt");
                            append_canonical_args_reversed(&mut canonical, &child_args);
                        }
                        "le" => {
                            append_canonical_name(&mut canonical, "gt");
                            append_canonical_args(&mut canonical, &child_args);
                        }
                        // Go's inner switch has no default arm. Preserve its
                        // exact canonical bytes for a scalar child whose
                        // name is not one of the four comparison operators.
                        _ => {}
                    }
                } else {
                    append_canonical_name(&mut canonical, name);
                    append_canonical_args(&mut canonical, &arg_codes);
                }
            }
            _ => {
                append_canonical_name(&mut canonical, name);
                append_canonical_args(&mut canonical, &arg_codes);
                if name == "values" {
                    encode_int(
                        &mut canonical,
                        self.values_offset
                            .map_or(-1, |offset| i64::try_from(offset).unwrap_or(i64::MAX)),
                    );
                }
                if name == "cast" {
                    if let Some(ret_type) = &self.ret_type {
                        canonical.push(ret_type.eval_type() as u8);
                    }
                }
            }
        }
        canonical
    }

    /// Go `ScalarFunction.Hash64`: hash the function tag, lower-case name,
    /// nullable return type, argument count, and each argument recursively.
    /// This is the structural plan-key hash and intentionally differs from
    /// [`Self::canonical_hash_code`], which normalizes commutative operators.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = crate::column::Fnv64::default();
        SCALAR_FUNCTION_FLAG.hash(&mut hasher);
        self.func_name.lowercase().hash(&mut hasher);
        self.values_offset.hash(&mut hasher);
        match &self.ret_type {
            Some(ret_type) => {
                1_u8.hash(&mut hasher);
                ret_type.hash(&mut hasher);
            }
            None => 0_u8.hash(&mut hasher),
        }
        self.args.len().hash(&mut hasher);
        for argument in &self.args {
            crate::column::expression_hash64(argument).hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Go `ScalarFunction.Equals`: structural equality over name, nullable
    /// return type, and ordered argument trees. Hash caches and collation
    /// metadata are not part of the source method's contract.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self.func_name.lowercase() == other.func_name.lowercase()
            && self.values_offset == other.values_offset
            && self.ret_type == other.ret_type
            && self.args.len() == other.args.len()
            && self
                .args
                .iter()
                .zip(&other.args)
                .all(|(left, right)| crate::column::expression_equals(left, right))
    }

    /// Go `ScalarFunction.CleanHashCode` (`scalar_function.go:604`): drops the
    /// cached hash code so the next [`Self::hash_code`] recomputes it.
    ///
    /// Required by every rewrite that mutates [`Self::args`] in place --
    /// `SetExprColumnInOperand` and `ColumnSubstituteImpl`'s grouping arm both
    /// call it in Go. Canonical bytes are derived on demand, so there is no
    /// second cache to clear here.
    pub fn clean_hash_code(&mut self) {
        self.hashcode.clear();
    }

    /// Go `ConstLevel`.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        if is_unfoldable_function(self.func_name.lowercase()) {
            return ConstLevel::NONE;
        }
        self.args
            .iter()
            .map(Expression::const_level)
            .min()
            .unwrap_or(ConstLevel::STRICT)
    }

    /// The collation this function's own evaluation runs under: the one the
    /// expression rewriter's derivation stamped on its result type (Go's
    /// `baseBuiltinFunc.collator`, set from `ExprCollation.Collation`).
    ///
    /// A node built outside the rewriter carries no derived collation, and
    /// falls back to the connection collation the rest of the tier uses.
    #[must_use]
    pub fn derived_collation(&self) -> tidb_datatype::Collation {
        self.ret_type
            .as_ref()
            .and_then(|ft| tidb_datatype::Collation::from_name(ft.collation_name()))
            .unwrap_or(crate::ops::DERIVATION_FREE_COLLATION)
    }

    pub(crate) fn uservar_name(&self) -> Option<String> {
        let Expression::Constant(name) = self.args.first()? else {
            return None;
        };
        name.value.sql_string().ok()
    }

    pub(crate) fn shared_arithmetic_error(
        &self,
        domain: &'static str,
        ctx: &dyn Columns,
        scalar: bool,
        decimals: [Option<&tidb_datatype::Decimal>; 2],
    ) -> Option<EvalError> {
        let operation = binary_op_for_name(self.func_name.lowercase())?;
        if operation == BinaryOp::IntDiv {
            if !scalar || self.numeric_operand_domain()? == EvalType::Int {
                return None;
            }
            let [left, right] = self.args.as_slice() else {
                return None;
            };
            let left = numeric_argument_text(self, left, false, ctx, decimals[0])?;
            let right = numeric_argument_text(self, right, false, ctx, decimals[1])?;
            return Some(EvalError::DataOutOfRange {
                value: match domain {
                    "BIGINT" => "BIGINT",
                    "BIGINT UNSIGNED" => "BIGINT UNSIGNED",
                    _ => return None,
                },
                expression: format!("({left} DIV {right})"),
            });
        }
        Some(match domain {
            "DOUBLE" => real_arithmetic_overflow_error(self, operation, ctx),
            "DECIMAL" => decimal_arithmetic_overflow_error(self, operation, ctx),
            "BIGINT" | "BIGINT UNSIGNED" => arithmetic_overflow_error(self, operation, ctx),
            _ => return None,
        })
    }

    /// The argument evaluation domain selected by Go's arithmetic builder.
    /// DIV returns Int but consumes Decimal unless both arguments are Int.
    pub(crate) fn numeric_operand_domain(&self) -> Option<EvalType> {
        let op = binary_op_for_name(self.func_name.lowercase())?;
        if self.args.len() != 2
            || !matches!(
                op,
                BinaryOp::Plus
                    | BinaryOp::Minus
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::IntDiv
                    | BinaryOp::Mod
            )
        {
            return None;
        }
        let output = self.get_static_type()?.eval_type();
        let domain = if op == BinaryOp::IntDiv {
            if output != EvalType::Int {
                return None;
            }
            if self.args.iter().all(|arg| {
                crate::builtin_arithmetic::numeric_context_result_type(arg) == EvalType::Int
            }) {
                EvalType::Int
            } else {
                EvalType::Decimal
            }
        } else {
            output
        };
        if (op == BinaryOp::Div && domain == EvalType::Int)
            || !matches!(domain, EvalType::Int | EvalType::Real | EvalType::Decimal)
            || self.args.iter().any(|arg| {
                numeric_argument_source(arg, domain)
                    .is_none_or(|source| !numeric_argument_domain(source, domain))
            })
        {
            return None;
        }
        Some(domain)
    }

    /// Go builds implicit argument casts before deriving arithmetic metadata.
    /// Strict literal casts fold here, so their warnings occur once per build.
    pub(crate) fn prepare_numeric_arguments(&mut self, ctx: &dyn Columns) {
        let Some(domain) = self.numeric_operand_domain() else {
            return;
        };
        let mut probed_types = [None, None];
        for (index, argument) in self.args.iter_mut().enumerate() {
            if argument.const_level() != ConstLevel::STRICT {
                continue;
            }
            let Some(field) = argument.static_type() else {
                continue;
            };
            if field.eval_type() == domain {
                continue;
            }
            let literal_ref_id = match argument {
                Expression::Constant(constant) if constant.literal_value().is_some() => {
                    Some(constant.subquery_ref_id)
                }
                _ => None,
            };
            let field = argument.static_type().expect("typed numeric argument");
            let mut target = if domain == EvalType::Decimal {
                numeric_decimal_cast_type(field)
            } else if domain == EvalType::Int {
                let mut target = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
                target.set_flen(field.flen());
                target.set_decimal(0);
                target
            } else {
                crate::builtin_arithmetic::real_argument_type(field).into_owned()
            };
            target.add_flags(field.flags() & tidb_datatype::FieldTypeFlags::UNSIGNED);
            target.add_flags(tidb_datatype::FieldTypeFlags::BINARY);
            target.set_charset_name("binary");
            target.set_collation_name("binary");
            let refine_decimal = |target: &mut FieldType, value: &Datum| {
                if let Datum::Decimal(decimal) = value {
                    let (precision, fraction) = decimal.precision_and_frac();
                    target.set_flen_under_limit(i64::from(precision));
                    target.set_decimal_under_limit(i64::from(fraction));
                }
            };
            if let Some(subquery_ref_id) = literal_ref_id {
                // FoldConstant retains a failed cast for runtime evaluation.
                if let Ok(value) = eval_numeric_operand_row(argument, ctx, Row::empty(), domain) {
                    let value = match value {
                        Datum::Int(value) if target.is_unsigned() => Datum::UInt(value as u64),
                        value => value,
                    };
                    if !value.is_null() {
                        target.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
                    }
                    refine_decimal(&mut target, &value);
                    let mut folded = crate::constant::Constant::new(value, target);
                    folded.subquery_ref_id = subquery_ref_id;
                    *argument = Expression::Constant(folded);
                    continue;
                }
            }
            // WrapWithCastAsDecimal probes every ConstStrict cast, including
            // retained scalar trees and failed literal folds. Keep its warnings
            // and precision without folding a tree Go leaves for runtime.
            if domain == EvalType::Decimal {
                if let Ok(value) = eval_numeric_operand_row(argument, ctx, Row::empty(), domain) {
                    refine_decimal(&mut target, &value);
                    probed_types[index] = Some(target);
                }
            }
        }
        // Go's function classes read the cast argument types, including
        // fine-grained constant precision, after newBaseBuiltinFuncWithTp.
        let argument_type = |argument: &Expression| {
            let field = argument.static_type().expect("typed numeric argument");
            if field.eval_type() == domain {
                field.clone()
            } else if domain == EvalType::Decimal {
                numeric_decimal_cast_type(field)
            } else {
                crate::builtin_arithmetic::real_argument_type(field).into_owned()
            }
        };
        if matches!(domain, EvalType::Real | EvalType::Decimal) {
            let left = probed_types[0]
                .take()
                .unwrap_or_else(|| argument_type(&self.args[0]));
            let right = probed_types[1]
                .take()
                .unwrap_or_else(|| argument_type(&self.args[1]));
            if let Some(ret_type) = self.ret_type.as_mut() {
                crate::builtin_arithmetic::refine_cast_arithmetic_metadata(
                    self.func_name.lowercase(),
                    ret_type,
                    &left,
                    &right,
                    ctx.div_precision_increment(),
                );
            }
        }
    }

    /// Whether declared padding can be read without evaluating an operand.
    /// The numeric NULL signature never reads the value.
    pub(crate) fn weight_string_input_required(&self) -> Option<bool> {
        let padding = match self.args.as_slice() {
            [_] => None,
            [_, Expression::Constant(kind), Expression::Constant(length)] => Some(
                weight_string_padding(kind.literal_value()?, length.literal_value()?),
            ),
            _ => return None,
        };
        Some(self.weight_string_reads_value(padding))
    }

    fn weight_string_reads_value(&self, padding: Option<(bool, i64)>) -> bool {
        self.args[0]
            .static_type()
            .is_none_or(|field| !field.code().is_type_numeric())
            || matches!(padding, Some((true, _)))
    }

    pub(crate) fn requires_row_order(&self) -> bool {
        let name = self.func_name.lowercase();
        crate::expr_util::is_mutable_effects_function(name)
            || advisory_lock_spec(name).is_some()
            || matches!(name, "nextval" | "lastval" | "setval")
    }

    pub(crate) fn rejects_vector_json_cast(&self) -> bool {
        self.func_name.lowercase() == "cast_json"
            && matches!(self.args.as_slice(), [value]
                if value.static_type().is_some_and(|field| field.eval_type() == EvalType::VectorFloat32))
    }

    pub(crate) fn vector_json_cast_error() -> EvalError {
        EvalError::Vector("cannot cast from vector to json".to_owned())
    }

    pub(crate) fn native_vector_extremum(&self) -> Option<std::cmp::Ordering> {
        if self.args.len() < 2 || self.get_static_type()?.eval_type() != EvalType::VectorFloat32 {
            return None;
        }
        match self.func_name.lowercase() {
            "greatest" => Some(std::cmp::Ordering::Greater),
            "least" => Some(std::cmp::Ordering::Less),
            _ => None,
        }
    }

    pub(crate) fn native_vector_operator(&self) -> Option<BinaryOp> {
        if self.args.len() == 2
            && self
                .get_static_type()
                .is_some_and(|field| field.eval_type() == EvalType::VectorFloat32)
        {
            return binary_op_for_name(self.func_name.lowercase())
                .filter(|op| matches!(op, BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Mul));
        }
        None
    }

    pub(crate) fn accepts_eager_values(&self) -> bool {
        let name = self.func_name.lowercase();
        match (name, self.args.as_slice()) {
            ("cast_vector", [_]) | ("ilike", [_, _, _]) => true,
            ("ltrim_with" | "rtrim_with", [_, _]) => true,
            ("convert_using", [_, Expression::Constant(target)]) => {
                target.literal_value().is_some()
            }
            ("char_func", args) => !args.is_empty(),
            (_, args) => crate::builtin_ext::accepts_eager(name, args.len()),
        }
    }
}

fn coerce_value_to_ret_type(
    name: &str,
    field: Option<&FieldType>,
    value: Datum,
) -> Result<Datum, EvalError> {
    if value.is_null() {
        return Ok(value);
    }
    let Some(ret_type) = field else {
        return Ok(value);
    };
    // Go's `builtinCoalesceTimeSig`/`builtinCoalesceDurationSig` stamps
    // every selected temporal value with the merged result FSP after
    // evaluating it. `newBaseBuiltinFuncWithTp` does not wrap temporal
    // arguments, so the selected value can still carry the first
    // argument's original precision. Preserve the value's instant and
    // update only that metadata here; a general `ConvertTo` would also
    // round the instant, which is not what Coalesce's `SetFsp` does.
    if name.eq_ignore_ascii_case("coalesce") {
        let target_fsp = if ret_type.decimal() == tidb_datatype::UNSPECIFIED_LENGTH {
            0
        } else {
            ret_type.decimal()
        };
        match (&value, ret_type.code()) {
            (
                Datum::Time(time),
                tidb_datatype::FieldTypeCode::Date
                | tidb_datatype::FieldTypeCode::Datetime
                | tidb_datatype::FieldTypeCode::Timestamp,
            ) => {
                let mut time = *time;
                let _ = time.set_fsp(target_fsp);
                return Ok(Datum::Time(time));
            }
            (Datum::Duration(duration), tidb_datatype::FieldTypeCode::Duration) => {
                return Ok(Datum::Duration(
                    tidb_datatype::MySqlDuration::from_raw_parts(
                        duration.nanoseconds(),
                        target_fsp,
                    ),
                ));
            }
            _ => {}
        }
    }
    if same_eval_family(&value, ret_type) {
        // `mysql.TypeBit`'s eval type is `ETInt`, so the family check
        // would keep an integer here. A BIT value's canonical carrier is
        // instead the zero-padded byte string (`Datum::Bit`), which is
        // what Go's `chunk.AppendDatum` stores for `KindMysqlBit` and
        // what a var-length BIT chunk column can hold. A cast to BIT is
        // the scalar function that produces such an integer, so convert
        // it here; a value already carrying the bytes is left alone.
        if ret_type.code() == tidb_datatype::FieldTypeCode::Bit {
            let width = (ret_type.flen() > 0)
                .then(|| u8::try_from((ret_type.flen() + 7) / 8).ok())
                .flatten()
                .and_then(|bytes| tidb_datatype::BinaryLiteralWidth::try_from(bytes).ok());
            match value {
                Datum::Int(v) => {
                    return Ok(Datum::Bit(tidb_datatype::BinaryLiteral::from_uint(
                        v as u64, width,
                    )));
                }
                Datum::UInt(v) => {
                    return Ok(Datum::Bit(tidb_datatype::BinaryLiteral::from_uint(
                        v, width,
                    )));
                }
                _ => {}
            }
        }
        return Ok(match value {
            Datum::String(value) if !ret_type.is_binary_string() => {
                Datum::new_collation_string(value.into_bytes(), ret_type.collation())
            }
            value => value,
        });
    }
    // COALESCE is built with `newBaseBuiltinFuncWithTp`, so its ARGUMENTS
    // are cast only to the merged eval family -- but `getFunction` then
    // assigns `bf.tp = resultFieldType`, and that merged type is what the
    // selected value is presented as. `select coalesce(1, 2.55, 3)`
    // answers `1.00`, not `1`: the integer branch is widened to the
    // merged scale, exactly as IF/IFNULL/CASE widen theirs. Handled by
    // the shared conversion below rather than a COALESCE-only rule.
    match value.convert_to(ret_type, tidb_datatype::DEFAULT_STATEMENT_FLAGS) {
        Ok(converted) => Ok(converted.value),
        Err(_) => Ok(value),
    }
}

/// A session input whose identity and literal operands are fixed at binding.
pub(crate) struct PreparedContextInput {
    name: String,
    arity: usize,
    values_offset: Option<usize>,
    arguments: Option<Vec<Datum>>,
    builtin: PreparedLocalBuiltin,
}

impl PreparedContextInput {
    pub(crate) fn new(function: &ScalarFunction) -> Self {
        Self {
            name: function.func_name.lowercase().to_owned(),
            arity: function.args.len(),
            values_offset: function.values_offset,
            arguments: function
                .args
                .iter()
                .map(|expression| match expression {
                    Expression::Constant(constant) => constant.literal_value().cloned(),
                    _ => None,
                })
                .collect(),
            builtin: PreparedLocalBuiltin::new(function),
        }
    }

    pub(crate) fn read(&self, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        if let Some(result) = self.context_value(ctx) {
            return coerce_value_to_ret_type(&self.name, self.builtin.get_static_type(), result?);
        }
        let arguments = self.arguments.as_ref().ok_or(EvalError::Unsupported(
            "a bound context input needs literal operands",
        ))?;
        self.builtin.evaluate(arguments.clone(), ctx)
    }

    fn context_value(&self, ctx: &dyn Columns) -> Option<Result<Datum, EvalError>> {
        let name = self.name.as_str();
        if matches!(name, "values" | "tidb_is_ddl_owner") && self.arity != 0 {
            return Some(Err(EvalError::WrongParameterCount(if name == "values" {
                "values"
            } else {
                "tidb_is_ddl_owner"
            })));
        }
        if self.arity != 0 {
            return None;
        }
        let value = match name {
            "values" => {
                return Some(
                    self.values_offset
                        .ok_or(EvalError::Unsupported("VALUES offset is not initialized"))
                        .and_then(|offset| ctx.current_insert_value(offset))
                        .map(|value| value.unwrap_or(Datum::Null)),
                )
            }
            "tidb_is_ddl_owner" => {
                return Some(
                    ctx.ddl_owner_info()
                        .map(|owner| Datum::Int(i64::from(owner))),
                )
            }
            "database" | "schema" => ctx
                .current_database()
                .map(|v| Datum::new_string(v.into_bytes())),
            "version" => ctx.sysvar(None, "version"),
            "tidb_version" => Some(Datum::new_string(ctx.tidb_info().into_bytes())),
            "current_user" => ctx
                .current_user()
                .map(|v| Datum::new_string(v.into_bytes())),
            "current_role" => ctx
                .current_role()
                .map(|v| Datum::new_string(v.into_bytes())),
            "current_resource_group" => ctx
                .current_resource_group()
                .map(|v| Datum::new_string(v.into_bytes())),
            "user" | "session_user" | "system_user" => {
                ctx.login_user().map(|v| Datum::new_string(v.into_bytes()))
            }
            "connection_id" => ctx.connection_id().map(Datum::UInt),
            "found_rows" => ctx.found_rows().map(Datum::UInt),
            "row_count" => ctx.row_count().map(Datum::Int),
            "last_insert_id" => ctx.last_insert_id().map(Datum::UInt),
            _ => return None,
        };
        Some(Ok(value.unwrap_or(Datum::Null)))
    }
}

/// The declaration and small caches a remaining native value callback needs.
/// It owns no expression tree and receives operands from the shared program.
pub(crate) struct PreparedLocalBuiltin {
    pub(crate) name: String,
    ret_type: Option<FieldType>,
    argument_types: Box<[Option<FieldType>]>,
    all_literal: bool,
    explicit_first_collation: bool,
    collation: tidb_datatype::Collation,
    cache_pattern: bool,
    like: crate::builtin_ext::BuiltinFuncCache<crate::like::CompiledLikePattern>,
    ilike: crate::builtin_ext::BuiltinFuncCache<crate::like::CompiledIlikePattern>,
    regexp: crate::builtin_ext::BuiltinFuncCache<crate::regexp::CachedRegexp>,
    find_in_set: crate::builtin_ext::BuiltinFuncCache<crate::builtin_ext::FindInSetLookup>,
    extremum_signature: Option<crate::builtin_ext::GlSignature>,
}

impl PreparedLocalBuiltin {
    pub(crate) fn new(function: &ScalarFunction) -> Self {
        Self {
            collation: function.derived_collation(),
            cache_pattern: function.args.iter().skip(1).all(|argument| {
                matches!(
                    argument,
                    Expression::Constant(value)
                        if value.literal_value().is_some()
                )
            }),
            like: Default::default(),
            ilike: Default::default(),
            regexp: Default::default(),
            find_in_set: Default::default(),
            extremum_signature: matches!(function.func_name.lowercase(), "greatest" | "least")
                .then(|| crate::rewriter::result_type::gl_signature(&function.args))
                .flatten(),
            name: function.func_name.lowercase().to_ascii_uppercase(),
            ret_type: function.ret_type.clone(),
            argument_types: function
                .args
                .iter()
                .map(|argument| argument.static_type().cloned())
                .collect(),
            all_literal: function
                .args
                .iter()
                .all(|argument| matches!(argument, Expression::Constant(_))),
            explicit_first_collation: function.args.first().is_some_and(|argument| {
                crate::collation_derive::coercibility_of(argument)
                    == crate::expr_collation::Coercibility::EXPLICIT
            }),
        }
    }

    fn get_static_type(&self) -> Option<&FieldType> {
        self.ret_type.as_ref()
    }

    pub(crate) fn evaluate(
        &self,
        values: Vec<Datum>,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        coerce_value_to_ret_type(
            &self.name,
            self.get_static_type(),
            self.eval_eager_values(values, ctx)?,
        )
    }

    fn weight_string_value(
        &self,
        value: &Datum,
        padding: Option<(bool, i64)>,
        ctx: &dyn Columns,
    ) -> Result<Datum, EvalError> {
        let field = self.argument_types[0].as_ref();
        let value = crate::cast::cast_arg_as_string(value, field, ctx)?;
        let fallback = FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let string_type = crate::cast::cast_arg_as_string_type(
            field.unwrap_or(&fallback),
            self.explicit_first_collation,
            ctx.connection_charset_info(),
        );
        let collation = tidb_datatype::Collation::from_name(string_type.collation_name())
            .unwrap_or(crate::ops::DERIVATION_FREE_COLLATION);
        crate::string_packet::weight_string(&value, padding, collation, ctx)
    }

    fn eval_eager_values(&self, vals: Vec<Datum>, ctx: &dyn Columns) -> Result<Datum, EvalError> {
        let upper = self.name.as_str();
        let arg_types = self.argument_types.as_ref();
        if let [left, right] = vals.as_slice() {
            if self
                .ret_type
                .as_ref()
                .is_some_and(|field| field.eval_type() == EvalType::VectorFloat32)
            {
                if let Some(operator) = binary_op_for_name(&upper.to_ascii_lowercase()) {
                    return crate::ops::eval_binary_in(operator, left.clone(), right.clone(), ctx);
                }
            }
        }
        if let [date, amount] = vals.as_slice() {
            let interval = upper
                .strip_prefix("DATE_ADD_")
                .map(|unit| (unit, 1))
                .or_else(|| upper.strip_prefix("DATE_SUB_").map(|unit| (unit, -1)));
            if let Some((unit, sign)) = interval {
                let fsp = crate::time_fn::calendar::date_add_result_fsp(
                    unit,
                    arg_types[0].as_ref(),
                    arg_types[1].as_ref(),
                );
                let target = self.ret_type.as_ref().map(FieldType::code);
                if target == Some(tidb_datatype::FieldTypeCode::Duration) {
                    return crate::time_fn::add_sub::date_add_duration(
                        ctx,
                        unit,
                        date,
                        amount,
                        arg_types[1].as_ref(),
                        sign,
                        i64::from(fsp.unwrap_or(0)),
                    );
                }
                let cast;
                let date = if target == Some(tidb_datatype::FieldTypeCode::Datetime)
                    && matches!(date, Datum::Duration(_))
                {
                    cast = crate::cast::cast_arg_as_datetime(date, arg_types[0].as_ref(), ctx)?;
                    &cast
                } else {
                    date
                };
                let value = crate::time_fn::calendar::date_add_with_result_fsp(
                    unit, date, amount, sign, fsp, ctx,
                )?;
                return match self.ret_type.as_ref().map(FieldType::eval_type) {
                    _ if target == Some(tidb_datatype::FieldTypeCode::Date) => {
                        crate::cast::parse_computed_time(
                            &value,
                            ctx,
                            tidb_datatype::TimeType::Date,
                            Some(0),
                        )
                    }
                    Some(EvalType::Datetime | EvalType::Timestamp) => {
                        crate::cast::parse_computed_time(
                            &value,
                            ctx,
                            tidb_datatype::TimeType::DateTime,
                            self.ret_type.as_ref().map(FieldType::decimal),
                        )
                    }
                    Some(EvalType::Duration) => crate::cast::parse_computed_duration(&value, ctx),
                    _ => Ok(value),
                };
            }
        }
        match (upper, vals.as_slice()) {
            ("MINUS", [left, right]) => {
                return crate::ops::eval_binary_in(
                    BinaryOp::Minus,
                    left.clone(),
                    right.clone(),
                    ctx,
                )
                .map_err(|error| match error {
                    EvalError::IntOverflow => EvalError::DataOutOfRange {
                        value: "BIGINT",
                        expression: format!(
                            "({} - {})",
                            left.sql_string().unwrap_or_default(),
                            right.sql_string().unwrap_or_default()
                        ),
                    },
                    error => error,
                });
            }
            ("INSTR", [haystack, needle]) => {
                return crate::string_fn::locate(needle, haystack, self.collation);
            }
            ("FIND_IN_SET", [needle, list]) => {
                if self.cache_pattern {
                    let lookup = self.find_in_set.get_or_init_cache(ctx.context_id(), || {
                        crate::builtin_ext::build_find_in_set_lookup(list, self.collation)
                    })?;
                    return crate::builtin_ext::find_in_set_lookup(needle, &lookup, self.collation);
                }
                return crate::builtin_ext::find_in_set_with_collation(&vals, self.collation);
            }
            ("GREATEST" | "LEAST", values) => {
                let want = if upper == "GREATEST" {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                };
                let decimals = arg_types
                    .iter()
                    .map(|field| field.as_ref().map_or(-1, FieldType::decimal))
                    .collect::<Vec<_>>();
                return crate::builtin_ext::extremum_with_signature(
                    values,
                    want,
                    self.extremum_signature,
                    &decimals,
                    self.all_literal,
                    self.collation,
                    ctx,
                );
            }
            ("DATE", [value]) => {
                let mut date =
                    crate::cast::cast_arg_as_datetime(value, arg_types[0].as_ref(), ctx)?;
                if matches!(value, Datum::String(_) | Datum::Bytes(_)) {
                    if let Datum::Time(time) = &mut date {
                        if time.invalid_zero() && !time.is_zero() {
                            let _ = time.set_fsp(6);
                        }
                    }
                }
                return crate::time_fn::date(&[date], ctx);
            }
            ("FROM_DAYS", [_]) => {
                let date = crate::time_fn::calendar::from_days(&vals)?;
                if date.as_raw_bytes() == Some(b"0000-00-00") {
                    return Ok(Datum::Time(
                        tidb_datatype::Time::from_go_raw(0xf)
                            .map_err(|_| EvalError::Unsupported("zero DATE carrier"))?,
                    ));
                }
                return crate::cast::parse_computed_time(
                    &date,
                    ctx,
                    tidb_datatype::TimeType::Date,
                    Some(0),
                );
            }
            ("MAKEDATE", [_, _]) => {
                let date = crate::time_fn::makedate(&vals)?;
                return crate::cast::parse_computed_time(
                    &date,
                    ctx,
                    tidb_datatype::TimeType::Date,
                    Some(0),
                );
            }
            ("MAKETIME", [_, _, _]) => {
                let duration = crate::time_fn::maketime(&vals, ctx)?;
                return crate::cast::parse_computed_duration(&duration, ctx);
            }
            ("POW" | "POWER" | "COT", _) => {
                if let Some(result) = crate::math_fn::dispatch_values(upper, &vals, ctx) {
                    return result.map_err(|error| {
                        if !matches!(error, EvalError::FloatOverflow) {
                            return error;
                        }
                        let numbers = vals
                            .iter()
                            .map(|value| match value {
                                Datum::Float32(value) | Datum::Real(value) => {
                                    tidb_datatype::format_float_g_shortest(*value)
                                }
                                _ => value.sql_string().unwrap_or_else(|_| "NULL".to_owned()),
                            })
                            .collect::<Vec<_>>();
                        let name = upper.to_ascii_lowercase();
                        EvalError::DataOutOfRange {
                            value: "DOUBLE",
                            expression: format!("{name}({})", numbers.join(", ")),
                        }
                    });
                }
            }
            ("EXTRACT", [unit, value]) => {
                let unit = match unit {
                    Datum::Bit(unit) => String::from_utf8_lossy(unit.as_bytes()).into_owned(),
                    value => {
                        let Some(unit) = crate::coerce::coerce_str(value)? else {
                            return Ok(Datum::Null);
                        };
                        unit
                    }
                };
                return crate::time_fn::extract::extract(
                    &unit.to_ascii_uppercase(),
                    value,
                    arg_types[1].as_ref(),
                    ctx,
                );
            }
            ("TO_BINARY" | "FROM_BINARY", [value]) => {
                let field = if upper == "TO_BINARY" {
                    arg_types[0].as_ref()
                } else {
                    self.ret_type.as_ref()
                };
                let charset = field.map_or("binary", FieldType::charset_name);
                return if upper == "TO_BINARY" {
                    crate::convert_charset::to_binary(value, charset)
                } else {
                    crate::convert_charset::from_binary(value, charset)
                };
            }
            ("CONVERT_USING", [value, charset]) => {
                if value.is_null() {
                    return Ok(Datum::Null);
                }
                let charset = charset
                    .sql_string()
                    .map_err(|_| EvalError::Unsupported("invalid CONVERT charset"))?;
                let source = arg_types[0]
                    .as_ref()
                    .ok_or(EvalError::Unsupported("CONVERT source type"))?;
                let value = crate::cast::cast_arg_as_string(value, Some(source), ctx)?;
                let string = crate::cast::cast_arg_as_string_type(
                    source,
                    self.explicit_first_collation,
                    ctx.connection_charset_info(),
                );
                return crate::convert_charset::convert_using(&value, &string, &charset);
            }
            ("LIKE" | "ILIKE", [text, pattern, escape]) => {
                if text.is_null() || pattern.is_null() || escape.is_null() {
                    return Ok(Datum::Null);
                }
                let text = text
                    .sql_bytes()
                    .map_err(|_| EvalError::Unsupported("invalid LIKE operand scalar domain"))?;
                let pattern = pattern
                    .sql_bytes()
                    .map_err(|_| EvalError::Unsupported("invalid LIKE pattern scalar domain"))?;
                let escape = crate::cast::cast_arg_as_int(escape, arg_types[2].as_ref(), ctx)?;
                let Some(escape) = crate::arg_eval_type::eval_int(&escape)? else {
                    return Ok(Datum::Null);
                };
                let escape = escape as u8;
                let matched = if upper == "ILIKE" {
                    if self.cache_pattern {
                        self.ilike
                            .get_or_init_cache(ctx.context_id(), || {
                                Ok::<_, EvalError>(crate::like::CompiledIlikePattern::new(
                                    &pattern,
                                    escape,
                                    self.collation,
                                ))
                            })?
                            .is_match(&text)
                    } else {
                        crate::like::ilike_match_with_collation(
                            text,
                            pattern,
                            escape,
                            self.collation,
                        )
                    }
                } else if self.cache_pattern {
                    self.like
                        .get_or_init_cache(ctx.context_id(), || {
                            Ok::<_, EvalError>(crate::like::CompiledLikePattern::new(
                                &pattern,
                                escape,
                                self.collation,
                            ))
                        })?
                        .is_match(&text)
                } else {
                    crate::like_match_with_collation(text, pattern, Some(escape), self.collation)
                };
                return Ok(Datum::Int(i64::from(matched)));
            }
            ("REGEXP" | "REGEXP_LIKE", [text, pattern] | [text, pattern, _]) => {
                if vals.iter().any(Datum::is_null) {
                    return Ok(Datum::Null);
                }
                let text = text
                    .sql_string()
                    .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP operand"))?;
                let pattern = pattern
                    .sql_string()
                    .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP pattern"))?;
                let option = vals
                    .get(2)
                    .map(Datum::sql_string)
                    .transpose()
                    .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP match type"))?
                    .unwrap_or_default();
                let option =
                    crate::regexp::regexp_match_type_with_collation(&option, self.collation);
                let regex = crate::regexp::get_cached_regexp(
                    &self.regexp,
                    ctx.context_id(),
                    self.cache_pattern,
                    &pattern,
                    &option,
                )
                .map_err(|error| {
                    if upper == "REGEXP" {
                        EvalError::SharedEvaluation {
                            code: 1139,
                            message: match error {
                                EvalError::Unsupported(message) => message.to_owned(),
                                error => format!("{error:?}"),
                            },
                        }
                    } else {
                        error
                    }
                })?;
                return Ok(Datum::Int(i64::from(regex.is_match(&text))));
            }
            _ => {}
        }
        if upper == "WEIGHT_STRING" {
            let padding = match vals.as_slice() {
                [_, kind, length] => Some(weight_string_padding(kind, length)),
                _ => None,
            };
            return self.weight_string_value(&vals[0], padding, ctx);
        }
        // `JSON_ARRAY`/`JSON_OBJECT`/`JSON_{SET,INSERT,REPLACE}`/
        // `JSON_ARRAY_{APPEND,INSERT}`: Go builds each value argument through
        // an implicit `CAST(... AS JSON)`, so a genuine BINARY-charset
        // argument must render as a JSON `Opaque` value rather than an
        // ordinary JSON string -- see `builtin_ext::json::dispatch_typed`.
        // Only this chunk path has each argument's static `FieldType`
        // (`Expression::static_type`); the row/AST evaluator
        // (`crate::func::eval_func`) still falls through to the untyped
        // `dispatch` below and stays the documented partial boundary.
        if upper == "HEX" {
            return crate::string_fn::hex_with_type(
                &vals,
                arg_types.first().and_then(Option::as_ref),
                ctx,
            );
        }
        // Go's `newBaseBuiltinFuncWithTp` argument-cast layer
        // (`crate::arg_eval_type`), the one point where a builtin's declared
        // argument eval types are imposed. Only this tier has the static
        // types Go's `WrapWithCastAsTime` reads, so only here can a `YEAR`
        // argument take Go's `ParseTimeFromYear`.
        let vals = crate::arg_eval_type::wrap_datetime_args(upper, vals, arg_types, ctx)?;
        let vals = crate::arg_eval_type::wrap_int_args(upper, vals, arg_types, ctx)?;
        let vals = crate::arg_eval_type::wrap_string_args(upper, vals, arg_types, ctx)?;
        if upper == "ORD" {
            return crate::string_fn::ord_with_type(
                &vals,
                arg_types.first().and_then(Option::as_ref),
            );
        }
        if let Some(result) = crate::builtin_ext::json_dispatch_typed(upper, &vals, arg_types) {
            return result;
        }
        // `ADDTIME`/`SUBTIME` are the other family Go types from the
        // ARGUMENTS' `FieldType`s rather than from their values: twelve
        // signatures over the `(tp1, tp2)` cross product, whose arms differ
        // in the result fsp and in whether the answer is NULL at all. Only
        // this tier has those types; `time_fn::dispatch` below still serves
        // the AST tier on Go's `default` branch.
        if matches!(upper, "ADDTIME" | "SUBTIME") && vals.len() == 2 {
            use crate::time_fn::add_sub::{add_sub_time, kind_of, TemporalKind};
            let kinds: [TemporalKind; 2] = [
                kind_of(arg_types[0].as_ref(), &vals[0]),
                kind_of(arg_types[1].as_ref(), &vals[1]),
            ];
            // A call whose arguments are all CONSTANT is what Go folds, and
            // folding runs the ROW body. Anything reading a column runs the
            // vectorized one.
            let sign = if upper == "SUBTIME" { -1 } else { 1 };
            let result = add_sub_time(&vals, kinds, sign, self.all_literal, ctx)?;
            return match self.get_static_type().map(FieldType::code) {
                Some(tidb_datatype::FieldTypeCode::Datetime) => crate::cast::parse_computed_time(
                    &result,
                    ctx,
                    tidb_datatype::TimeType::DateTime,
                    None,
                ),
                Some(tidb_datatype::FieldTypeCode::Duration) => {
                    crate::cast::parse_computed_duration(&result, ctx)
                }
                _ => Ok(result),
            };
        }
        if upper == "TIMESTAMP" {
            let result = crate::time_fn::add_sub::timestamp(&vals, ctx)?;
            return crate::cast::parse_computed_time(
                &result,
                ctx,
                tidb_datatype::TimeType::DateTime,
                None,
            );
        }
        if upper == "SYSDATE" {
            let result = crate::time_fn::add_sub::sysdate(&vals, ctx)?;
            return crate::cast::parse_computed_time(
                &result,
                ctx,
                tidb_datatype::TimeType::DateTime,
                self.get_static_type().map(FieldType::decimal),
            );
        }
        if matches!(
            upper,
            "NOW"
                | "CURRENT_TIMESTAMP"
                | "LOCALTIME"
                | "LOCALTIMESTAMP"
                | "UTC_TIMESTAMP"
                | "CURDATE"
                | "CURRENT_DATE"
                | "UTC_DATE"
                | "CURTIME"
                | "CURRENT_TIME"
                | "UTC_TIME"
                | "LAST_DAY"
                | "FROM_DAYS"
                | "SEC_TO_TIME"
                | "STR_TO_DATE"
                | "TIMEDIFF"
                | "CONVERT_TZ"
                | "FROM_UNIXTIME"
        ) {
            let mut result = crate::time_fn::dispatch(upper, &vals, ctx)
                .expect("the native temporal family is registered")?;
            if upper == "STR_TO_DATE"
                && self.get_static_type().map(FieldType::code)
                    == Some(tidb_datatype::FieldTypeCode::Datetime)
            {
                if let Ok(text) = result.sql_string() {
                    if text.contains(':') && !text.contains('-') {
                        if ctx.date_modes().no_zero_date {
                            return Ok(Datum::Null);
                        }
                        result = Datum::new_string(format!("0000-00-00 {text}"));
                    }
                }
            }
            return match self.get_static_type().map(FieldType::code) {
                Some(tidb_datatype::FieldTypeCode::Datetime) => crate::cast::parse_computed_time(
                    &result,
                    ctx,
                    tidb_datatype::TimeType::DateTime,
                    self.get_static_type().map(FieldType::decimal),
                ),
                Some(tidb_datatype::FieldTypeCode::Date) => crate::cast::parse_computed_time(
                    &result,
                    ctx,
                    tidb_datatype::TimeType::Date,
                    Some(0),
                ),
                Some(tidb_datatype::FieldTypeCode::Duration) => {
                    crate::cast::parse_computed_duration(&result, ctx)
                }
                _ => Ok(result),
            };
        }
        if matches!(upper, "ROUND" | "TRUNCATE") && matches!(vals.first(), Some(Datum::Decimal(_)))
        {
            return crate::math_fn::round_or_truncate_with_result_decimal(
                &vals,
                upper == "ROUND",
                self.ret_type.as_ref().map(FieldType::decimal),
                ctx,
            );
        }
        if matches!(upper, "CEIL" | "CEILING" | "FLOOR")
            && matches!(vals.first(), Some(Datum::Decimal(_)))
        {
            return crate::math_fn::ceil_floor_with_result_domain(
                &vals,
                upper != "FLOOR",
                self.ret_type
                    .as_ref()
                    .map(|field_type| field_type.eval_type() == tidb_datatype::EvalType::Decimal),
                ctx,
            );
        }
        if let Some(result) = crate::math_fn::dispatch_values(upper, &vals, ctx) {
            return result;
        }
        if let Some(result) = crate::func::eval_func_values_in(upper, &vals, ctx) {
            return result;
        }
        // The date/time family reads the statement clock and the session
        // zone, so it takes the context rather than values alone.
        if let Some(result) = crate::time_fn::dispatch(upper, &vals, ctx) {
            return result;
        }
        Err(EvalError::Unsupported(
            "this scalar function is not yet ported",
        ))
    }
}

pub(crate) fn grouping_value(
    metadata: Option<&GroupingMetadata>,
    value: &Datum,
) -> Result<Datum, EvalError> {
    let grouping_id = crate::arg_eval_type::eval_int(value)?;
    let Some(grouping_id) = grouping_id else {
        return Ok(Datum::Null);
    };
    let metadata = metadata.ok_or(EvalError::Unsupported("Meta data is not initialized"))?;
    let result = metadata.eval(grouping_id as u64);
    Ok(Datum::UInt(result))
}

pub(crate) fn sleep_value(value: &Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
    if value.is_null() {
        ctx.handle_sleep_incorrect_argument()?;
        return Ok(Datum::Int(0));
    }
    let seconds = crate::ops::to_f64_with_mysql_string(value, ctx)?;
    if seconds < 0.0 {
        ctx.handle_sleep_incorrect_argument()?;
        return Ok(Datum::Int(0));
    }
    if seconds > f64::MAX / 1_000_000_000.0 {
        return Err(EvalError::IncorrectArguments(
            "Incorrect arguments to sleep".to_owned(),
        ));
    }
    if seconds <= 0.0 {
        return Ok(Datum::Int(0));
    }
    let duration = std::time::Duration::try_from_secs_f64(seconds)
        .map_err(|_| EvalError::IncorrectArguments("Incorrect arguments to sleep".to_owned()))?;
    Ok(Datum::Int(i64::from(ctx.sleep_for(duration))))
}

/// The cast a `cast_*` function name describes, with the width and scale its
/// result type carries. Go stores the same fact as the chosen
/// `builtinCast*As*Sig`.
/// Converts a user variable's stored value onto the kind its `getvar_<kind>`
/// call declared. NULL stays NULL, and a value already of that kind passes
/// through untouched -- the conversion only matters when an assignment made
/// during this same statement changed the kind out from under the plan.
pub(crate) fn uservar_as_kind(
    kind: &str,
    value: Datum,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    use tidb_ast::CastType;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let target = match (kind, &value) {
        ("int", Datum::Int(_)) | ("uint", Datum::UInt(_)) | ("real", Datum::Real(_)) => {
            return Ok(value)
        }
        ("decimal", Datum::Decimal(_)) => return Ok(value),
        ("time", Datum::Time(_)) => return Ok(value),
        ("string", Datum::String(_) | Datum::Bytes(_)) => return Ok(value),
        ("int", _) => CastType::Signed,
        ("uint", _) => CastType::Unsigned,
        ("real", _) => CastType::Double,
        ("decimal", _) => CastType::Decimal { flen: 0, scale: 0 },
        ("time", _) => return crate::cast::cast_arg_as_datetime(&value, None, ctx),
        ("string", _) => CastType::Char {
            len: None,
            charset: None,
        },
        _ => return Err(EvalError::Unsupported("unknown user-variable kind")),
    };
    // No session resolver is threaded here on purpose: every target above is
    // numeric or string, so no arm reads the date modes or raises a warning.
    // Every target above is numeric or string, so no arm reads the
    // source type.
    crate::cast::eval_cast(&target, value, None, ctx)
}

fn weight_string_padding(kind: &Datum, length: &Datum) -> (bool, i64) {
    (
        kind.sql_string().is_ok_and(|kind| kind == "BINARY"),
        crate::cast::to_i64_signed(length),
    )
}

/// Evaluates the Go numeric typed entrypoint without changing generic Eval.
pub(crate) fn eval_numeric_row(
    expression: &Expression,
    ctx: &dyn Columns,
    row: Row<'_>,
    target: EvalType,
) -> Result<Datum, EvalError> {
    let value = match expression {
        Expression::Constant(constant) => constant.eval_typed_as_on_row(ctx, row, target)?,
        Expression::CorrelatedColumn(column) => column.eval_typed(ctx, target)?,
        _ => {
            let value = expression.eval(ctx, row)?;
            if target == EvalType::Int
                && !value.is_null()
                && expression.static_type().is_some_and(FieldType::is_hybrid)
            {
                if let Datum::Bit(value) = value {
                    let warnings = crate::constant::ConversionWarnings(ctx);
                    let zone = ctx.time_zone();
                    let context = tidb_datatype::ConversionContext::new(
                        ctx.type_flags(),
                        tidb_datatype::ConversionLocation::from_time_zone(&zone),
                        &warnings,
                    );
                    let (integer, error) = value.to_int_with_context(&context);
                    if let Some(error) = error {
                        return Err(EvalError::Conversion(error));
                    }
                    Datum::Int(integer as i64)
                } else {
                    convert_numeric_datum(
                        value,
                        &FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        ctx,
                    )?
                }
            } else {
                value
            }
        }
    };
    Ok(match (target, value) {
        (EvalType::Int, Datum::UInt(value)) => Datum::Int(value as i64),
        (EvalType::Real, Datum::Float32(value)) => Datum::Real(value),
        (_, value) => value,
    })
}

// Go's hybrid cast signatures read EvalInt. Binary constants are special:
// EvalReal/EvalDecimal reads their unsigned literal value directly.
fn numeric_argument_source(expression: &Expression, target: EvalType) -> Option<EvalType> {
    let field = expression.static_type()?;
    Some(if is_numeric_binary_literal(expression) {
        target
    } else if field.is_hybrid() {
        EvalType::Int
    } else {
        field.eval_type()
    })
}

fn is_numeric_binary_literal(expression: &Expression) -> bool {
    expression.binary_literal_source().is_some()
}

fn numeric_argument_domain(source: EvalType, target: EvalType) -> bool {
    source == target
        || (matches!(source, EvalType::String | EvalType::Json)
            && matches!(target, EvalType::Real | EvalType::Decimal))
        || (matches!(
            source,
            EvalType::Datetime | EvalType::Timestamp | EvalType::Duration
        ) && matches!(target, EvalType::Int | EvalType::Real | EvalType::Decimal))
        || (target == EvalType::Real && matches!(source, EvalType::Int | EvalType::Decimal))
        || (target == EvalType::Decimal && matches!(source, EvalType::Int | EvalType::Real))
}

// Go's builder inserts numeric argument casts. Native arithmetic retains
// those source FieldTypes, so apply the same conversion before the operator.
pub(crate) fn cast_numeric_argument(
    expression: &Expression,
    value: Datum,
    target: EvalType,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let field = expression
        .static_type()
        .expect("numeric argument has a type");
    cast_numeric_value(field, value, target, ctx, false, || {
        numeric_expression_text(expression, true, ctx)
    })
}

pub(crate) fn cast_numeric_value(
    field: &FieldType,
    value: Datum,
    target: EvalType,
    ctx: &dyn Columns,
    vectorized: bool,
    source_text: impl FnOnce() -> Option<String>,
) -> Result<Datum, EvalError> {
    if value.is_null() || field.eval_type() == target {
        return Ok(value);
    }
    if target == EvalType::Int {
        if let Datum::Decimal(value) = &value {
            return Ok(Datum::Int(value.round_to_i64_saturating()));
        }
    }
    let value = match value {
        Datum::Int(bits) if field.is_unsigned() => Datum::UInt(bits as u64),
        Datum::Float32(value) => Datum::Real(f64::from(value as f32)),
        value => value,
    };
    let code = match target {
        EvalType::Int => tidb_datatype::FieldTypeCode::LongLong,
        EvalType::Real => tidb_datatype::FieldTypeCode::Double,
        EvalType::Decimal => tidb_datatype::FieldTypeCode::NewDecimal,
        _ => return Err(EvalError::Unsupported("numeric argument cast domain")),
    };
    if field.eval_type() == EvalType::String && !field.is_hybrid() {
        let bytes = match &value {
            Datum::String(value) => value.bytes(),
            Datum::Bytes(value) => value.as_slice(),
            _ => return Err(EvalError::Unsupported("string arithmetic argument domain")),
        };
        return cast_string_numeric_argument(field, bytes, target, ctx, vectorized);
    }
    if let Datum::Json(json) = &value {
        if target == EvalType::Real {
            let converted = tidb_datatype::json_to_float(json);
            if converted.event.is_some() {
                if let Some(bytes) = json.as_string() {
                    let text = String::from_utf8_lossy(bytes);
                    ctx.handle_truncate(&format!(
                        "Truncated incorrect DOUBLE value: '{}'",
                        tidb_datatype::float_warning_input(&text),
                    ))?;
                } else {
                    ctx.handle_truncate(&format!("Truncated incorrect FLOAT value: '{json}'"))?;
                }
            }
            return Ok(Datum::Real(converted.value));
        }
        if target == EvalType::Int {
            // go `builtinCastJSONAsIntSig`: the document's text re-reads as
            // an integer (StrToInt), raising go's 1292 truncation warning
            // when the text is not a clean integer — captured:
            // `bitand(j, j)` over `{}` warns twice and answers 0, while the
            // JSON number `3` coerces silently.
            let as_text = Datum::new_string(json.to_string());
            crate::cast::report_int_truncation(&as_text, ctx)?;
            return Ok(Datum::Int(crate::cast::to_i64_signed(&as_text)));
        }
    }
    let value = if target == EvalType::Decimal
        && matches!(value, Datum::Time(_) | Datum::Duration(_) | Datum::Json(_))
    {
        // Time/Duration.ToNumber and ConvertJSONToDecimal precede target
        // precision fitting. JSON's integer path must never pass through f64.
        let warnings = crate::constant::ConversionWarnings(ctx);
        let zone = ctx.time_zone();
        let context = tidb_datatype::ConversionContext::new(
            ctx.type_flags(),
            tidb_datatype::ConversionLocation::from_time_zone(&zone),
            &warnings,
        );
        let (decimal, error) = value
            .to_decimal_with_context(&context)
            .map_err(|_| EvalError::Unsupported("numeric decimal argument conversion failed"))?;
        if let Some(error) = error {
            return Err(EvalError::Conversion(error));
        }
        Datum::Decimal(decimal)
    } else {
        value
    };
    let value = if let (EvalType::Decimal, Datum::Real(real)) = (target, &value) {
        // Every integral double in [-2^53, 2^53] has the same exact integer
        // decimal as Go's shortest-float formatting followed by FromString.
        // Negative zero keeps the source parser's sign behavior below.
        if real.abs() <= 9_007_199_254_740_992.0
            && real.fract() == 0.0
            && (*real != 0.0 || !real.is_sign_negative())
        {
            let value = Datum::Decimal(tidb_datatype::Decimal::from_int(*real as i64));
            let target = numeric_decimal_cast_type(field);
            return if target.decimal() < 0 {
                Ok(value)
            } else {
                convert_numeric_datum(value, &target, ctx)
            };
        }
        let (decimal, error) = tidb_datatype::MyDecimal::from_float64(*real);
        match error {
            Some(tidb_datatype::DecimalError::Overflow) => {
                if ctx.truncate_level() == crate::context::ErrorLevel::Error {
                    return Err(EvalError::Conversion(tidb_datatype::ERR_OVERFLOW.clone()));
                }
                ctx.handle_truncate(&format!(
                    "Truncated incorrect DECIMAL value: '{}'",
                    source_text().unwrap_or_else(|| tidb_datatype::format_float_g_shortest(*real))
                ))?;
            }
            Some(tidb_datatype::DecimalError::Truncated) | None => {}
            Some(_) => return Err(EvalError::Conversion(tidb_datatype::ERR_BAD_NUMBER.clone())),
        }
        Datum::Decimal(tidb_datatype::Decimal::from_my_decimal(&decimal))
    } else {
        value
    };
    let target_field = if target == EvalType::Decimal {
        numeric_decimal_cast_type(field)
    } else {
        FieldType::new(code)
    };
    // IntAsDecimal constructs the decimal before applying source metadata.
    // ProduceDecWithSpecifiedTp is a no-op when scale is unspecified.
    if target == EvalType::Decimal && target_field.decimal() < 0 {
        return Ok(match value {
            Datum::Int(value) => Datum::Decimal(tidb_datatype::Decimal::from_int(value)),
            Datum::UInt(value) => Datum::Decimal(tidb_datatype::Decimal::from_uint(value)),
            value => value,
        });
    }
    convert_numeric_datum(value, &target_field, ctx)
}

fn cast_string_numeric_argument(
    field: &FieldType,
    bytes: &[u8],
    target: EvalType,
    ctx: &dyn Columns,
    vectorized: bool,
) -> Result<Datum, EvalError> {
    if target == EvalType::Real {
        return crate::ops::bytes_to_f64(bytes, ctx).map(Datum::Real);
    }
    let text = String::from_utf8_lossy(bytes);
    let text = text.trim();
    let (decimal, error) = tidb_datatype::MyDecimal::from_string(text.as_bytes());
    if let Some(error) = error {
        use tidb_datatype::DecimalError;
        if error == DecimalError::TruncatedWrongValue
            || (error == DecimalError::Truncated && !vectorized)
        {
            ctx.handle_truncate(&format!("Truncated incorrect DECIMAL value: '{text}'"))?;
        } else {
            let error = match error {
                DecimalError::Truncated => tidb_datatype::ERR_TRUNCATED.clone(),
                DecimalError::Overflow => tidb_datatype::ERR_OVERFLOW.clone(),
                _ => tidb_datatype::ERR_BAD_NUMBER.clone(),
            };
            match ctx.truncate_level() {
                crate::ErrorLevel::Error => return Err(EvalError::Conversion(error)),
                crate::ErrorLevel::Warn => {
                    let warning = error.to_sql_error();
                    ctx.append_warning(warning.code, &warning.message);
                }
                crate::ErrorLevel::Ignore => {}
            }
        }
    }
    let value = Datum::Decimal(tidb_datatype::Decimal::from_my_decimal(&decimal));
    let target = numeric_decimal_cast_type(field);
    if target.decimal() < 0 {
        Ok(value)
    } else {
        convert_numeric_datum(value, &target, ctx)
    }
}

fn convert_numeric_datum(
    value: Datum,
    target: &FieldType,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let warnings = crate::constant::ConversionWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let converted = value
        .convert_to_in_context(target, &context, &zone)
        .map_err(|_| EvalError::Unsupported("numeric argument conversion failed"))?;
    if let Some(error) = converted.error {
        return Err(EvalError::Conversion(error));
    }
    Ok(converted.value)
}

fn eval_numeric_operand_row(
    expression: &Expression,
    ctx: &dyn Columns,
    row: Row<'_>,
    target: EvalType,
) -> Result<Datum, EvalError> {
    let source = numeric_argument_source(expression, target).expect("numeric argument has a type");
    if is_numeric_binary_literal(expression) {
        return eval_numeric_row(expression, ctx, row, target);
    }
    cast_numeric_argument(
        expression,
        eval_numeric_row(expression, ctx, row, source)?,
        target,
        ctx,
    )
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::column::Column;
    use crate::constant::Constant;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    #[test]
    fn string_division_preserves_go_decimal_parser_diagnostics() {
        struct Context {
            level: crate::ErrorLevel,
            warnings: std::cell::RefCell<Vec<(u16, String)>>,
        }
        impl crate::Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                self.level
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        use crate::ErrorLevel::{Error, Ignore, Warn};
        for (case, text) in [
            "",
            "abc",
            " 1x ",
            "1e999",
            "1e-999",
            "1e18446744073709551620",
            &"9".repeat(82),
        ]
        .into_iter()
        .enumerate()
        {
            for level in [Error, Warn, Ignore] {
                for vectorized in [false, true] {
                    let ctx = Context {
                        level,
                        warnings: Default::default(),
                    };
                    let field = FieldType::new(FieldTypeCode::VarString);
                    let args = (0..2)
                        .map(|index| {
                            let mut column =
                                crate::expression::Column::new(index + 1, field.clone());
                            column.index = index;
                            Expression::Column(column)
                        })
                        .collect::<Vec<_>>();
                    let expression = Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new("intdiv"),
                        FieldType::new(FieldTypeCode::LongLong),
                        args,
                    ));
                    let mut chunk = Chunk::new_with_capacity(&[field.clone(), field], 1);
                    chunk.append_datum(0, &Datum::new_string(text));
                    chunk.append_datum(1, &Datum::new_string("1"));
                    let result = if vectorized {
                        crate::try_evaluate_batch(&expression, &ctx, &chunk)
                            .map(|batch| batch.unwrap().remove(0))
                    } else {
                        expression.eval(&ctx, chunk.get_row(0))
                    };
                    let error = match case {
                        3 | 6 => EvalError::Conversion(tidb_datatype::ERR_OVERFLOW.clone()),
                        5 => EvalError::Conversion(tidb_datatype::ERR_BAD_NUMBER.clone()),
                        2 | 4 if vectorized => {
                            EvalError::Conversion(tidb_datatype::ERR_TRUNCATED.clone())
                        }
                        _ => EvalError::TruncatedWrongValue(format!(
                            "Truncated incorrect DECIMAL value: '{}'",
                            text.trim()
                        )),
                    };
                    let warning = match &error {
                        EvalError::Conversion(error) => {
                            let e = error.to_sql_error();
                            (e.code, e.message)
                        }
                        EvalError::TruncatedWrongValue(message) => (1292, message.clone()),
                        _ => unreachable!(),
                    };
                    let mut warnings = if level == Warn { vec![warning] } else { vec![] };
                    let expected = if level == Error {
                        Err(error)
                    } else if matches!(case, 3 | 6) {
                        if level == Warn {
                            warnings.push((
                                1292,
                                format!("Truncated incorrect DECIMAL value: '{}'", "9".repeat(81)),
                            ));
                        }
                        Err(EvalError::DataOutOfRange {
                            value: "BIGINT",
                            expression: if vectorized {
                                format!("({} DIV 1)", "9".repeat(81))
                            } else {
                                "(cast(Column#1, decimal(65,0) BINARY) DIV cast(Column#2, decimal(65,0) BINARY))".to_owned()
                            },
                        })
                    } else {
                        Ok(Datum::Int(if case == 2 { 1 } else { 0 }))
                    };
                    assert_eq!(result, expected, "{text:?}/{level:?}/{vectorized}");
                    assert_eq!(
                        *ctx.warnings.borrow(),
                        warnings,
                        "{text:?}/{level:?}/{vectorized}"
                    );
                }
            }
        }
    }

    #[test]
    fn string_arithmetic_preserves_go_cast_and_batch_order() {
        #[derive(Default)]
        struct Context {
            errors: bool,
            warnings: std::cell::RefCell<Vec<(u16, String)>>,
        }
        impl crate::Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                if self.errors {
                    crate::ErrorLevel::Error
                } else {
                    crate::ErrorLevel::Warn
                }
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }
        let mut mismatches = Vec::new();
        for name in ["plus", "minus", "mul", "div", "intdiv", "mod"] {
            for mode in ["warnings", "nulls", "error"] {
                for (vectorized, selected) in
                    [(false, false), (false, true), (true, false), (true, true)]
                {
                    let field = FieldType::new(FieldTypeCode::VarString);
                    let args = (0..2)
                        .map(|index| {
                            let mut column =
                                crate::expression::Column::new(index + 1, field.clone());
                            column.index = index;
                            Expression::Column(column)
                        })
                        .collect::<Vec<_>>();
                    let output =
                        crate::builtin_arithmetic::infer_arithmetic_type(name, &args[0], &args[1])
                            .unwrap();
                    let expression = Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new(name),
                        output,
                        args,
                    ));
                    let mut chunk = Chunk::new_with_capacity(&[field.clone(), field], 2);
                    let rows = match mode {
                        "warnings" => [[Some("1x"), Some("3z")], [Some("2y"), Some("4w")]],
                        "nulls" => [[None, Some("3z")], [Some("2y"), None]],
                        _ => [[Some("1"), Some("3z")], [Some("2y"), Some("4")]],
                    };
                    for row in rows {
                        for (i, value) in row.into_iter().enumerate() {
                            chunk.append_datum(
                                i,
                                &value.map(Datum::new_string).unwrap_or(Datum::Null),
                            );
                        }
                    }
                    if selected {
                        chunk.set_sel(Some(vec![1, 0]));
                    }
                    let ctx = Context {
                        errors: mode == "error",
                        ..Context::default()
                    };
                    let scalar = || {
                        (0..2)
                            .map(|i| expression.eval(&ctx, chunk.get_row(i)))
                            .collect::<Result<Vec<_>, _>>()
                    };
                    let actual = if vectorized {
                        crate::try_evaluate_batch(&expression, &ctx, &chunk)
                            .and_then(|batch| batch.map(Ok).unwrap_or_else(scalar))
                    } else {
                        scalar()
                    };
                    let class = if name == "intdiv" {
                        "DECIMAL"
                    } else {
                        "DOUBLE"
                    };
                    let message = |value| format!("Truncated incorrect {class} value: '{value}'");
                    let mut expected = if mode == "error" {
                        if name == "intdiv" && vectorized {
                            Err(EvalError::Conversion(tidb_datatype::ERR_TRUNCATED.clone()))
                        } else {
                            Err(EvalError::TruncatedWrongValue(message(
                                if vectorized || selected { "2y" } else { "3z" },
                            )))
                        }
                    } else if mode == "nulls" {
                        Ok(vec![Datum::Null; 2])
                    } else {
                        Ok(match name {
                            "plus" => vec![Datum::Real(4.0), Datum::Real(6.0)],
                            "minus" => vec![Datum::Real(-2.0); 2],
                            "mul" => vec![Datum::Real(3.0), Datum::Real(8.0)],
                            "div" => vec![Datum::Real(1.0 / 3.0), Datum::Real(0.5)],
                            "intdiv" => vec![Datum::Int(0); 2],
                            _ => vec![Datum::Real(1.0), Datum::Real(2.0)],
                        })
                    };
                    if selected {
                        if let Ok(values) = &mut expected {
                            values.reverse();
                        }
                    }
                    let warning_inputs: &[&str] = match (mode, vectorized) {
                        ("warnings", false) if selected => &["2y", "4w", "1x", "3z"],
                        ("warnings", true) if selected => &["2y", "1x", "4w", "3z"],
                        ("nulls", true) if selected => &["2y", "3z"],
                        ("nulls", false) if selected && matches!(name, "plus" | "mod") => {
                            &["2y", "3z"]
                        }
                        ("warnings", false) => &["1x", "3z", "2y", "4w"],
                        ("warnings", true) => &["1x", "2y", "3z", "4w"],
                        ("nulls", true) => &["2y", "3z"],
                        ("nulls", false) if matches!(name, "plus" | "mod") => &["3z", "2y"],
                        ("nulls", false) => &["2y"],
                        _ => &[],
                    };
                    let warnings = warning_inputs
                        .iter()
                        .map(|v| {
                            if name == "intdiv" && vectorized {
                                (1265, "Data truncated for column '%s' at row %d".to_owned())
                            } else {
                                (1292, message(v))
                            }
                        })
                        .collect::<Vec<_>>();
                    if actual != expected || *ctx.warnings.borrow() != warnings {
                        mismatches.push(format!("{name}/{mode}/{vectorized}/{selected}: {actual:?}, warnings {:?}; expected {expected:?}, {warnings:?}", ctx.warnings.borrow()));
                    }
                }
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    fn ft() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// A hand-built node must be given the result type its function actually
    /// reports, exactly as the rewriter's `builtin_return_type` would: since
    /// [`ScalarFunction::eval`] reconciles the evaluated value with the
    /// declared type (Go's own `Eval` does the same by construction), a node
    /// declared `Long` for a string- or real-valued builtin is a node Go
    /// cannot build, and asserting on one would assert nothing.
    fn text_ft() -> FieldType {
        FieldType::new(FieldTypeCode::VarString)
    }

    fn real_ft() -> FieldType {
        FieldType::new(FieldTypeCode::Double)
    }

    #[test]
    fn numeric_real_cast_fast_path_preserves_decimal_value_and_scale() {
        for value in [
            -9_007_199_254_740_994.0,
            -9_007_199_254_740_992.0,
            -1.0,
            -0.0,
            0.0,
            1.0,
            9_007_199_254_740_992.0,
            9_007_199_254_740_994.0,
            0.3,
            0.39,
            f64::MIN_POSITIVE,
        ] {
            let expression = Expression::Constant(Constant::new(Datum::Real(value), real_ft()));
            let actual = cast_numeric_argument(
                &expression,
                Datum::Real(value),
                EvalType::Decimal,
                &crate::NoColumns,
            )
            .unwrap();
            let Datum::Decimal(actual) = actual else {
                panic!("expected decimal");
            };
            let (expected, error) = tidb_datatype::MyDecimal::from_float64(value);
            assert!(error.is_none() || error == Some(tidb_datatype::DecimalError::Truncated));
            let expected = tidb_datatype::Decimal::from_my_decimal(&expected);
            assert_eq!(
                (actual.storage_string(), actual.scale()),
                (expected.storage_string(), expected.scale()),
                "{value:?}"
            );
        }
    }

    #[test]
    fn binary_operator_names_delegate_to_the_opcode_authority() {
        for operator in [
            BinaryOp::Plus,
            BinaryOp::Minus,
            BinaryOp::Mul,
            BinaryOp::Div,
            BinaryOp::IntDiv,
            BinaryOp::Mod,
            BinaryOp::BitAnd,
            BinaryOp::BitOr,
            BinaryOp::BitXor,
            BinaryOp::LeftShift,
            BinaryOp::RightShift,
            BinaryOp::Eq,
            BinaryOp::NullEq,
            BinaryOp::Ne,
            BinaryOp::Lt,
            BinaryOp::Le,
            BinaryOp::Gt,
            BinaryOp::Ge,
            BinaryOp::LogicAnd,
            BinaryOp::LogicOr,
            BinaryOp::LogicXor,
        ] {
            assert_eq!(binary_op_name(operator), operator.opcode().name());
        }
    }

    #[derive(Default)]
    struct InfoColumns {
        current_user: Option<String>,
        current_role: Option<String>,
        connection_id: Option<u64>,
        tidb_info: Option<String>,
    }

    impl Columns for InfoColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn current_user(&self) -> Option<String> {
            self.current_user.clone()
        }

        fn current_role(&self) -> Option<String> {
            self.current_role.clone()
        }

        fn connection_id(&self) -> Option<u64> {
            self.connection_id
        }

        fn tidb_info(&self) -> String {
            self.tidb_info
                .clone()
                .unwrap_or_else(|| tidb_util::printer::get_tidb_info())
        }
    }

    fn eval_info(name: &str, result_type: FieldType, ctx: &InfoColumns) -> Datum {
        crate::expression::Expression::ScalarFunction(ScalarFunction::new(
            CiString::new(name),
            result_type,
            vec![],
        ))
        .eval(ctx, tidb_chunk::row::Row::empty())
        .expect("session information builtin must evaluate")
    }

    struct PacketColumns {
        limit: u64,
        warnings: RefCell<Vec<(u16, String)>>,
    }

    #[derive(Default)]
    struct WarningColumns {
        warnings: RefCell<Vec<(u16, String)>>,
    }

    impl Columns for WarningColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }

        fn warning_count(&self) -> usize {
            self.warnings.borrow().len()
        }

        fn truncate_warnings(&self, bookmark: usize) {
            self.warnings.borrow_mut().truncate(bookmark);
        }
    }

    #[test]
    fn logical_and_interval_skip_unreachable_warning_arguments() {
        let ctx = WarningColumns::default();
        let integer = |value| Expression::Constant(Constant::new(Datum::Int(value), ft()));
        let division = || {
            let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
            decimal.set_flen(15);
            decimal.set_decimal(4);
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("div"),
                decimal,
                vec![integer(1), integer(0)],
            ))
        };

        for (name, first, expected) in [("and", 0, 0), ("or", 1, 1)] {
            let function =
                ScalarFunction::new(CiString::new(name), ft(), vec![integer(first), division()]);
            assert_eq!(
                crate::expression::Expression::ScalarFunction(function)
                    .eval(&ctx, tidb_chunk::row::Row::empty())
                    .unwrap(),
                Datum::Int(expected)
            );
            assert!(ctx.warnings.borrow().is_empty());
        }

        let interval = ScalarFunction::new(
            CiString::new("interval"),
            ft(),
            vec![integer(1), integer(0), integer(1), integer(2), division()],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(interval)
                .eval(&ctx, tidb_chunk::row::Row::empty())
                .unwrap(),
            Datum::Int(2)
        );
        assert!(ctx.warnings.borrow().is_empty());

        let mut not_null_int = ft();
        not_null_int.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        let integer =
            |value| Expression::Constant(Constant::new(Datum::Int(value), not_null_int.clone()));
        let unreachable = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("not_a_function"),
            not_null_int.clone(),
            vec![],
        ));
        let interval = ScalarFunction::new(
            CiString::new("interval"),
            ft(),
            vec![integer(1), integer(0), integer(1), integer(2), unreachable],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(interval)
                .eval(&ctx, tidb_chunk::row::Row::empty())
                .unwrap(),
            Datum::Int(2)
        );
    }

    impl Columns for PacketColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            self.limit
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    #[test]
    fn concat_family_stops_at_the_packet_limit_before_later_arguments() {
        let ctx = PacketColumns {
            limit: 3,
            warnings: RefCell::new(Vec::new()),
        };
        let string =
            |value: &[u8]| Expression::Constant(Constant::new(Datum::new_string(value), text_ft()));
        let missing_provider = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("tidb_is_ddl_owner"),
            ft(),
            vec![],
        ));
        let concat = ScalarFunction::new(
            CiString::new("concat"),
            text_ft(),
            vec![string(b"abcd"), missing_provider],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(concat)
                .eval(&ctx, tidb_chunk::row::Row::empty())
                .unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.warnings.borrow()[0].0, 1301);
        assert!(ctx.warnings.borrow()[0].1.contains("concat()"));

        ctx.warnings.borrow_mut().clear();
        let concat_ws = ScalarFunction::new(
            CiString::new("concat_ws"),
            text_ft(),
            vec![string(b"--"), string(b"a"), string(b"b")],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(concat_ws)
                .eval(&ctx, tidb_chunk::row::Row::empty())
                .unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.warnings.borrow()[0].0, 1301);
        assert!(ctx.warnings.borrow()[0].1.contains("concat_ws()"));

        ctx.warnings.borrow_mut().clear();
        assert_eq!(
            crate::func::eval_func_values_in(
                "CONCAT",
                &[Datum::new_string("abcd"), Datum::new_string("x")],
                &ctx,
            )
            .unwrap()
            .unwrap(),
            Datum::Null
        );
        assert_eq!(ctx.warnings.borrow()[0].0, 1301);
    }

    #[test]
    fn decimal_round_and_truncate_cap_dynamic_scale_by_the_result_type() {
        let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_type.set_flen(10);
        decimal_type.set_decimal(2);
        let decimal = || {
            Expression::Constant(Constant::new(
                Datum::Decimal(tidb_datatype::Decimal::from_literal("1.23")),
                decimal_type.clone(),
            ))
        };
        let scale = || Expression::Constant(Constant::new(Datum::Int(5), ft()));

        for name in ["round", "truncate"] {
            let function = ScalarFunction::new(
                CiString::new(name),
                decimal_type.clone(),
                vec![decimal(), scale()],
            );
            let value = crate::expression::Expression::ScalarFunction(function)
                .eval(&crate::context::NoColumns, tidb_chunk::row::Row::empty())
                .expect("decimal ROUND/TRUNCATE must evaluate");
            let Datum::Decimal(value) = value else {
                panic!("{name} returned a non-decimal value")
            };
            assert_eq!(value.to_string(), "1.23", "{name}");
        }
    }

    // Go TestCurrentUser.
    #[test]
    fn test_current_user() {
        let ctx = InfoColumns {
            current_user: Some("root@localhost".to_owned()),
            ..InfoColumns::default()
        };
        assert_eq!(
            eval_info("current_user", text_ft(), &ctx),
            Datum::new_string(b"root@localhost".to_vec())
        );
        assert_eq!(
            eval_info("current_user", text_ft(), &InfoColumns::default()),
            Datum::Null
        );
    }

    // Go TestCurrentRole.
    #[test]
    fn test_current_role() {
        for (roles, expected) in [
            ("NONE", "NONE"),
            ("`r_1`@`%`,`r_2`@`localhost`", "`r_1`@`%`,`r_2`@`localhost`"),
        ] {
            let ctx = InfoColumns {
                current_role: Some(roles.to_owned()),
                ..InfoColumns::default()
            };
            assert_eq!(
                eval_info("current_role", text_ft(), &ctx),
                Datum::new_string(expected.as_bytes().to_vec())
            );
        }
    }

    // Go TestConnectionID.
    #[test]
    fn test_connection_id() {
        let ctx = InfoColumns {
            connection_id: Some(1),
            ..InfoColumns::default()
        };
        let mut result_type = FieldType::new(FieldTypeCode::LongLong);
        result_type.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        assert_eq!(
            eval_info("connection_id", result_type, &ctx),
            Datum::UInt(1)
        );
    }

    // Go TestTiDBVersion.
    #[test]
    fn test_tidb_version() {
        let ctx = InfoColumns {
            tidb_info: Some("Release Version: test\nKernel Type: Classic".to_owned()),
            ..InfoColumns::default()
        };
        assert_eq!(
            eval_info("tidb_version", text_ft(), &ctx),
            Datum::new_string(b"Release Version: test\nKernel Type: Classic".to_vec())
        );
    }

    fn plus(args: Vec<Expression>) -> ScalarFunction {
        ScalarFunction::new(CiString::new("plus"), ft(), args)
    }

    #[test]
    fn args_and_static_type() {
        let sf = plus(vec![
            Expression::Constant(Constant::new(Datum::Int(1), ft())),
            Expression::Constant(Constant::new(Datum::Int(2), ft())),
        ]);
        assert_eq!(sf.get_args().len(), 2);
        assert!(sf.get_static_type().is_some());
        assert_eq!(sf.const_level(), ConstLevel::STRICT);
    }

    #[test]
    fn const_level_matches_the_source_function_and_argument_rules() {
        let literal = || Expression::Constant(Constant::new(Datum::Int(1), ft()));
        let parameter = || {
            let mut constant = Constant::new(Datum::Int(1), ft());
            constant.param_marker = Some(crate::constant::ParamMarker { order: 0 });
            Expression::Constant(constant)
        };
        let call = |name: &str, args| ScalarFunction::new(CiString::new(name), ft(), args);

        assert_eq!(
            call("abs", vec![literal()]).const_level(),
            ConstLevel::STRICT
        );
        assert_eq!(
            plus(vec![literal(), parameter()]).const_level(),
            ConstLevel::ONLY_IN_CONTEXT
        );
        assert_eq!(
            plus(vec![literal(), Expression::Column(Column::new(1, ft()))]).const_level(),
            ConstLevel::NONE
        );
        for name in [
            "sysdate",
            "found_rows",
            "rand",
            "uuid",
            "uuid_v4",
            "uuid_v7",
            "sleep",
            "row",
            "values",
            "setvar",
            "getvar",
            "getvar_string",
            "getparam",
            "benchmark",
            "dayname",
            "nextval",
            "lastval",
            "setval",
            "any_value",
        ] {
            assert_eq!(
                call(name, vec![literal()]).const_level(),
                ConstLevel::NONE,
                "{name}"
            );
        }
    }

    #[test]
    fn is_correlated_follows_args() {
        let plain = plus(vec![Expression::Column(Column::new(1, ft()))]);
        assert!(!plain.is_correlated());

        let corr = plus(vec![Expression::CorrelatedColumn(
            crate::column::CorrelatedColumn {
                column: Column::new(1, ft()),
                data: Default::default(),
            },
        )]);
        assert!(corr.is_correlated());
    }

    #[test]
    fn hash_code_is_flag_name_and_arg_codes() {
        let mut c1 = Column::new(1, ft());
        let mut c2 = Column::new(2, ft());
        let mut sf = plus(vec![
            Expression::Column(Column::new(1, ft())),
            Expression::Column(Column::new(2, ft())),
        ]);

        let mut expected = vec![SCALAR_FUNCTION_FLAG];
        encode_compact_bytes(&mut expected, b"plus");
        expected.extend_from_slice(c1.hash_code());
        expected.extend_from_slice(c2.hash_code());
        assert_eq!(sf.hash_code(), expected.as_slice());
        // Cached.
        assert_eq!(sf.hash_code(), expected.as_slice());
    }

    #[test]
    fn cast_hash_code_includes_eval_type_byte() {
        let mut sf = ScalarFunction::new(
            CiString::new("cast"),
            FieldType::new(FieldTypeCode::Long),
            vec![Expression::Column(Column::new(1, ft()))],
        );
        let hc = sf.hash_code().to_vec();
        // Ends with the target EvalType byte (Long -> Int == 0).
        assert_eq!(*hc.last().unwrap(), tidb_datatype::EvalType::Int as u8);
    }

    #[test]
    fn eval_bridges_values_only_builtins() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let chk = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        let mut chk = chk;
        chk.append_int64(0, 0);
        let row = chk.get_row(0);
        let konst = |value: Datum| {
            let field = match value {
                Datum::String(_) => text_ft(),
                _ => ft(),
            };
            Expression::Constant(Constant::new(value, field))
        };

        // ABS(-5) = 5 via math_fn's shared values-only dispatch.
        let abs = ScalarFunction::new(CiString::new("abs"), ft(), vec![konst(Datum::Int(-5))]);
        assert_eq!(
            crate::expression::Expression::ScalarFunction(abs)
                .eval(&NoColumns, row)
                .unwrap(),
            Datum::Int(5)
        );

        // CONCAT('a', 'b') = 'ab' via the shared string arm.
        let concat = ScalarFunction::new(
            CiString::new("concat"),
            text_ft(),
            vec![konst(Datum::new_string("a")), konst(Datum::new_string("b"))],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(concat)
                .eval(&NoColumns, row)
                .unwrap(),
            Datum::new_string("ab")
        );

        // COALESCE(NULL, 7) = 7 (eager over values, matching the old
        // AST-level evaluator).
        let coalesce = ScalarFunction::new(
            CiString::new("coalesce"),
            ft(),
            vec![konst(Datum::Null), konst(Datum::Int(7))],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(coalesce)
                .eval(&NoColumns, row)
                .unwrap(),
            Datum::Int(7)
        );

        // `InferType4ControlFuncs` widens the result metadata to the widest
        // branch scale, and `bf.tp = resultFieldType` makes that merged type
        // the one the selected argument is presented as: recorded TiDB
        // answers `select coalesce(1, 2.55, 3)` with `1.00`.
        let mut decimal_four = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_four.set_flen(15);
        decimal_four.set_decimal(4);
        let decimal_coalesce = ScalarFunction::new(
            CiString::new("coalesce"),
            decimal_four,
            vec![konst(Datum::Int(1)), konst(Datum::Null)],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(decimal_coalesce)
                .eval(&NoColumns, row)
                .unwrap(),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("1.0000"))
        );

        // A column argument feeds the builtin from the chunk row: ABS(col).
        let mut col = Column::new(1, ft());
        col.index = 0;
        let mut chk2 = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        chk2.append_int64(0, -9);
        let abs_col =
            ScalarFunction::new(CiString::new("abs"), ft(), vec![Expression::Column(col)]);
        assert_eq!(
            crate::expression::Expression::ScalarFunction(abs_col)
                .eval(&NoColumns, chk2.get_row(0))
                .unwrap(),
            Datum::Int(9)
        );

        // `IF` is a lazy control form now, so it evaluates here: the
        // condition picks the branch (this used to be the example of a
        // function outside the values-only entry).
        let iff = ScalarFunction::new(
            CiString::new("if"),
            ft(),
            vec![
                konst(Datum::Int(1)),
                konst(Datum::Int(2)),
                konst(Datum::Int(3)),
            ],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(iff)
                .eval(&NoColumns, row)
                .unwrap(),
            Datum::Int(2)
        );

        // A function with no ported builtin at all still is unsupported.
        let unknown = ScalarFunction::new(
            CiString::new("no_such_builtin"),
            ft(),
            vec![konst(Datum::Int(1))],
        );
        assert!(matches!(
            crate::expression::Expression::ScalarFunction(unknown).eval(&NoColumns, row),
            Err(EvalError::Unsupported(_))
        ));
    }

    /// `JSON_ARRAY`/`CAST(... AS JSON)` over a column whose static `FieldType`
    /// is BINARY-charset render the JSON `Opaque` value real TiDB produces,
    /// not a plain JSON string -- the chunk path threads
    /// `Expression::static_type()` into `builtin_ext::json::dispatch_typed`/
    /// `cast_as_json_typed`. Captured: `SELECT JSON_ARRAY(vb), CAST(vb AS
    /// JSON) FROM t` where `vb varbinary(8)` holds `'ab'`
    /// (`zz_dump_frozjson_test.go`, `TestZZDumpFrozJSONBinaryOpaque`).
    #[test]
    fn eval_renders_binary_charset_column_as_json_opaque() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;
        use tidb_datatype::{Collation, FieldTypeCode};

        let varbinary_type =
            FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&varbinary_type), 1);
        chk.append_bytes(0, b"ab");
        let row = chk.get_row(0);

        let mut col = Column::new(1, varbinary_type);
        col.index = 0;

        let json_array = ScalarFunction::new(
            CiString::new("json_array"),
            FieldType::new(FieldTypeCode::Json),
            vec![Expression::Column(col.clone())],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(json_array)
                .eval(&NoColumns, row)
                .unwrap()
                .sql_string()
                .unwrap(),
            r#"["base64:type15:YWI="]"#
        );

        let cast_json = ScalarFunction::new(
            CiString::new("cast_json"),
            FieldType::new(FieldTypeCode::Json),
            vec![Expression::Column(col)],
        );
        let Datum::Json(got) = crate::expression::Expression::ScalarFunction(cast_json)
            .eval(&NoColumns, row)
            .unwrap()
        else {
            panic!("CAST AS JSON did not retain the JSON domain")
        };
        let opaque = got.opaque().expect("opaque binary JSON");
        assert_eq!(opaque.type_code, 15);
        assert_eq!(opaque.bytes, b"ab");
    }

    #[test]
    fn rand_reuses_one_generator_per_node_for_a_constant_seed() {
        use std::cell::Cell;
        use tidb_chunk::chunk::Chunk;

        // A minimal session whose `rand_seeded_next` records the key it was
        // called with and returns a fixed value, so this asserts the SAME
        // key reaches it across repeated evaluations of one `ScalarFunction`
        // node -- exactly the identity the AST evaluator gets from the
        // `Expr` node's own address.
        struct RandColumns {
            keys: Cell<Vec<usize>>,
        }
        impl Columns for RandColumns {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn rand_seeded_next(&self, key: usize, _seed: i64) -> Option<f64> {
                let mut keys = self.keys.take();
                keys.push(key);
                self.keys.set(keys);
                Some(0.5)
            }
        }

        let chk = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        let row = chk.get_row(0);
        let konst = |d: Datum| Expression::Constant(Constant::new(d, ft()));
        let rand_five =
            ScalarFunction::new(CiString::new("rand"), real_ft(), vec![konst(Datum::Int(5))]);
        let columns = RandColumns {
            keys: Cell::new(Vec::new()),
        };
        let rand_five = Expression::ScalarFunction(rand_five);

        assert_eq!(rand_five.eval(&columns, row).unwrap(), Datum::Real(0.5));
        assert_eq!(rand_five.eval(&columns, row).unwrap(), Datum::Real(0.5));
        let keys = columns.keys.into_inner();
        assert_eq!(keys.len(), 2);
        // The SAME node produced the same key both times.
        assert_eq!(keys[0], keys[1]);

        // A DIFFERENT node (a different RAND(5) call site) gets a different
        // key, because each owns its own generator.
        let rand_five_again =
            ScalarFunction::new(CiString::new("rand"), real_ft(), vec![konst(Datum::Int(5))]);
        let columns2 = RandColumns {
            keys: Cell::new(Vec::new()),
        };
        crate::expression::Expression::ScalarFunction(rand_five_again)
            .eval(&columns2, row)
            .unwrap();
        assert_ne!(columns2.keys.into_inner()[0], keys[0]);
    }

    #[test]
    fn rand_with_no_args_reads_the_running_generator() {
        use tidb_chunk::chunk::Chunk;

        struct SeqColumns {
            next: std::cell::Cell<f64>,
        }
        impl Columns for SeqColumns {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn rand_next(&self) -> Option<f64> {
                Some(self.next.get())
            }
        }

        let chk = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        let row = chk.get_row(0);
        let rand = ScalarFunction::new(CiString::new("rand"), real_ft(), vec![]);
        let columns = SeqColumns {
            next: std::cell::Cell::new(0.75),
        };
        assert_eq!(
            crate::expression::Expression::ScalarFunction(rand)
                .eval(&columns, row)
                .unwrap(),
            Datum::Real(0.75)
        );
    }

    #[test]
    fn different_functions_hash_differently() {
        let mut a = plus(vec![Expression::Column(Column::new(1, ft()))]);
        let mut b = ScalarFunction::new(
            CiString::new("minus"),
            ft(),
            vec![Expression::Column(Column::new(1, ft()))],
        );
        assert_ne!(a.hash_code(), b.hash_code());
    }

    #[test]
    fn string_in_matches_collated_strict_literals() {
        use tidb_chunk::chunk::Chunk;

        let mut result_type = ft();
        result_type.set_collation_name("utf8mb4_general_ci");
        let string_type = text_ft();
        let string = |value: &str| {
            Expression::Constant(Constant::new(Datum::new_string(value), string_type.clone()))
        };
        let function = ScalarFunction::new(
            CiString::new("in"),
            result_type,
            vec![string("a"), string("A"), string("b")],
        );

        let chunk = Chunk::new_with_capacity(&[], 1);
        assert_eq!(
            crate::expression::Expression::ScalarFunction(function)
                .eval(&crate::NoColumns, chunk.get_row(0))
                .unwrap(),
            Datum::Int(1)
        );
    }

    #[test]
    fn string_in_raw_key_probe_preserves_go_collation_semantics() {
        use tidb_chunk::chunk::Chunk;
        use tidb_datatype::Collation;

        let string =
            |value: &str| Expression::Constant(Constant::new(Datum::new_string(value), text_ft()));
        let chunk = Chunk::new_with_capacity(&[], 1);

        // Go's `builtinInStringSig.buildHashMapForConstArgs` stores the
        // `collator.Key` result and `evalInt` probes the same key. Binary and
        // derived-binary collators return the source bytes directly, so the
        // Rust probe may borrow them without changing membership behavior.
        let binary_type = text_ft().with_collation(Collation::Binary);
        let binary = ScalarFunction::new(
            CiString::new("in"),
            ft().with_collation(binary_type.collation()),
            vec![string("a"), string("a")],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(binary)
                .eval(&crate::NoColumns, chunk.get_row(0))
                .unwrap(),
            Datum::Int(1)
        );

        // PAD SPACE collations still allocate a trimmed sort key. This is
        // the source behavior for `utf8mb4_bin`: `a` and `a ` compare equal.
        let pad_type = text_ft().with_collation(Collation::Utf8Mb4Bin);
        let pad = ScalarFunction::new(
            CiString::new("in"),
            ft().with_collation(pad_type.collation()),
            vec![string("a "), string("a")],
        );
        assert_eq!(
            crate::expression::Expression::ScalarFunction(pad)
                .eval(&crate::NoColumns, chunk.get_row(0))
                .unwrap(),
            Datum::Int(1)
        );

        // A large fixed literal list still uses the same typed IN operation.
        let mut many = Vec::with_capacity(1001);
        many.push(string("probe"));
        for value in 0..1000 {
            many.push(string(&format!("maker-{value}")));
        }
        let large =
            Expression::ScalarFunction(ScalarFunction::new(CiString::new("in"), ft(), many));
        assert_eq!(
            large.eval(&crate::NoColumns, chunk.get_row(0)).unwrap(),
            Datum::Int(0)
        );
    }
    /// The stock-level shape: `ge(ol_o_id, minus(d_next_o_id, 20))` over a
    /// joined chunk row -- the exact residual predicate class jTPCC's
    /// stock-level statement evaluates once per hash-join candidate, mixing
    /// PROBE-side and BUILD-side columns. The shared dispatch evaluates the composed tree.
    #[test]
    fn typed_join_residual_composes_comparison_and_integer_arithmetic() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let probe_col = || {
            let mut col = Column::new(1, ft());
            col.index = 0;
            Expression::Column(col)
        };
        let build_col = || {
            let mut col = Column::new(2, ft());
            col.index = 1;
            Expression::Column(col)
        };
        let konst = |d: Datum| Expression::Constant(Constant::new(d, ft()));

        // Joined row: ol_o_id = 100, d_next_o_id = 105 -> minus(...) = 85 ->
        // 100 >= 85 is true. One below the join boundary: ol_o_id = 84 ->
        // 84 >= 85 is false.
        let inner = ScalarFunction::new(
            CiString::new("minus"),
            ft(),
            vec![build_col(), konst(Datum::Int(20))],
        );
        let ge = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("ge"),
            ft(),
            vec![probe_col(), Expression::ScalarFunction(inner)],
        ));

        let mut above = Chunk::new_with_capacity(&[ft(), ft()], 1);
        above.append_int64(0, 100);
        above.append_int64(1, 105);
        assert_eq!(
            ge.eval(&NoColumns, above.get_row(0)).unwrap(),
            Datum::Int(1)
        );

        let mut below = Chunk::new_with_capacity(&[ft(), ft()], 1);
        below.append_int64(0, 84);
        below.append_int64(1, 105);
        assert_eq!(
            ge.eval(&NoColumns, below.get_row(0)).unwrap(),
            Datum::Int(0)
        );
    }

    /// Arithmetic and comparison both propagate NULL.
    #[test]
    fn typed_integer_operators_propagate_null() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let chk = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        let mut chk = chk;
        chk.append_int64(0, 7);
        let row = chk.get_row(0);
        let konst = |d: Datum, t: &FieldType| Expression::Constant(Constant::new(d, t.clone()));

        for name in ["plus", "minus", "mul", "eq", "ne", "lt", "le", "gt", "ge"] {
            let function = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                ft(),
                vec![konst(Datum::Null, &ft()), konst(Datum::Int(3), &ft())],
            ));
            assert_eq!(
                function.eval(&NoColumns, row).unwrap(),
                Datum::Null,
                "{name} NULL lhs"
            );
            let function = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                ft(),
                vec![konst(Datum::Int(3), &ft()), konst(Datum::Null, &ft())],
            ));
            assert_eq!(
                function.eval(&NoColumns, row).unwrap(),
                Datum::Null,
                "{name} NULL rhs"
            );
        }
    }

    /// A mixed signed/unsigned comparison orders by VALUE, not by bit
    /// pattern: `-1 < 1u` because Go's compare treats the signed side as
    /// negative against an unsigned partner. Each column carries its own type.
    #[test]
    fn typed_integer_comparison_orders_mixed_signedness_by_value() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let signed_type = ft();
        let unsigned_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
        assert!(unsigned_type.is_unsigned());

        let signed_col = {
            let mut col = Column::new(1, signed_type.clone());
            col.index = 0;
            move || Expression::Column(col.clone())
        };
        let unsigned_col = {
            let mut col = Column::new(2, unsigned_type.clone());
            col.index = 1;
            move || Expression::Column(col.clone())
        };

        // The joined rows are assembled left-then-right in one scratch chunk:
        // column 0 reads the SIGNED value, column 1 the UNSIGNED one. Build
        // that exact two-column row and compare across it.
        let mut joined = Chunk::new_with_capacity(&[signed_type, unsigned_type], 1);
        joined.append_int64(0, -1);
        joined.append_uint64(1, 1);
        let joined_row = joined.get_row(0);

        for (name, op_name, want) in [
            ("lt", "lt", 1u8),
            ("le", "le", 1),
            ("gt", "gt", 0),
            ("ge", "ge", 0),
            ("eq", "eq", 0),
            ("ne", "ne", 1),
        ] {
            let function = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(op_name),
                ft(),
                vec![signed_col(), unsigned_col()],
            ));
            assert_eq!(
                function.eval(&NoColumns, joined_row).unwrap(),
                Datum::Int(want as i64),
                "{name}(-1, 1u)"
            );
        }
    }

    /// Unsigned subtraction past zero is `ErrOverflow` with Go's BIGINT
    /// UNSIGNED wording (1690), not a wrapped value: `1u - 2` overflows.
    #[test]
    fn typed_unsigned_subtraction_reports_overflow() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let unsigned_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);

        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&unsigned_type), 1);
        chk.append_uint64(0, 1);
        let row = chk.get_row(0);
        let mut col = Column::new(1, unsigned_type.clone());
        col.index = 0;
        let konst = |d: Datum, t: &FieldType| Expression::Constant(Constant::new(d, t.clone()));

        let function = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("minus"),
            unsigned_type.clone(),
            vec![
                Expression::Column(col),
                konst(Datum::Int(2), &unsigned_type),
            ],
        ));
        let error = function
            .eval(&NoColumns, row)
            .expect_err("1u - 2 must overflow");
        assert_eq!(
            error,
            EvalError::DataOutOfRange {
                value: "BIGINT UNSIGNED",
                expression: "(Column#1 - 2)".to_owned(),
            }
        );
    }

    /// A typed string comparison uses the declared string domain.
    #[test]
    fn typed_string_comparison_preserves_lexicographic_order() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let string_type = text_ft();
        let chk = Chunk::new_with_capacity(std::slice::from_ref(&string_type), 1);
        let mut chk = chk;
        chk.append_string(0, b"a".as_slice());
        let row = chk.get_row(0);
        let konst = |d: Datum| Expression::Constant(Constant::new(d, string_type.clone()));

        let function = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("ge"),
            ft(),
            vec![konst(Datum::new_string("a")), konst(Datum::new_string("b"))],
        ));
        assert_eq!(function.eval(&NoColumns, row).unwrap(), Datum::Int(0));
    }

    /// TiDB capture over `create table g (i int, d decimal(10,3))` holding
    /// `(-5, 2.500)`: `select least(i, d)` is `-5` with datum frac 0 -- the
    /// winner keeps the winning ARGUMENT's own decimal (Go casts integers to
    /// `SetDecimal(0)`), not the aggregated max (3), and `select least(1, d)`
    /// is `1` the same way. A fully-constant call is the one exception: the
    /// planner folds it to the RETURN type's scale, so `select least(1, 2.5)`
    /// is `1.0` (max argument decimal).
    #[test]
    fn extremum_over_typed_columns_keeps_the_winner_own_scale() {
        let int_ft = FieldType::new(FieldTypeCode::LongLong);
        let mut dec_ft = FieldType::new(FieldTypeCode::NewDecimal);
        dec_ft.set_flen(10);
        dec_ft.set_decimal(3);
        let mut ret_ft = FieldType::new(FieldTypeCode::NewDecimal);
        ret_ft.set_flen(11);
        ret_ft.set_decimal(3);

        let mut col_i = crate::column::Column::new(1, int_ft.clone());
        col_i.index = 0;
        let mut col_d = crate::column::Column::new(2, dec_ft.clone());
        col_d.index = 1;
        let least = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("least"),
            ret_ft.clone(),
            vec![Expression::Column(col_i), Expression::Column(col_d)],
        ));

        let mut chunk =
            tidb_chunk::chunk::Chunk::new_with_capacity(&[int_ft.clone(), dec_ft.clone()], 1);
        chunk.append_datum(0, &Datum::Int(-5));
        chunk.append_datum(
            1,
            &Datum::Decimal(tidb_datatype::Decimal::from_literal("2.500")),
        );
        let datum = least
            .eval(&crate::context::NoColumns, chunk.get_row(0))
            .unwrap();
        let Datum::Decimal(dec) = &datum else {
            panic!("least over a decimal aggregate answers a decimal");
        };
        assert_eq!(dec.scale(), 0, "the integer winner keeps its own scale 0");
        assert_eq!(dec.to_string(), "-5");

        // Mixed constant + column: `least(1, d)` is `1` (frac 0), not `1.000`.
        let mut col_d = crate::column::Column::new(2, dec_ft.clone());
        col_d.index = 0;
        let least_mixed = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("least"),
            ret_ft,
            vec![
                Expression::Constant(Constant::new(Datum::Int(1), int_ft.clone())),
                Expression::Column(col_d),
            ],
        ));
        let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&[dec_ft], 1);
        chunk.append_datum(
            0,
            &Datum::Decimal(tidb_datatype::Decimal::from_literal("2.500")),
        );
        let datum = least_mixed
            .eval(&crate::context::NoColumns, chunk.get_row(0))
            .unwrap();
        let Datum::Decimal(dec) = &datum else {
            panic!("least over a decimal aggregate answers a decimal");
        };
        assert_eq!(
            dec.scale(),
            0,
            "the constant winner keeps its own scale 0 too"
        );
        assert_eq!(dec.to_string(), "1");
    }

    fn empty_row() -> tidb_chunk::row::Row<'static> {
        let chunk = Box::leak(Box::new(tidb_chunk::chunk::Chunk::new_empty(&[])));
        chunk.get_row(0)
    }

    /// Go `builtinTiDBIsDDLOwnerSig.evalInt` (`builtin_info.go:627`) reads
    /// the `DDLOwnerInfo` optional eval prop and answers 1/0, never NULL.
    /// A context without the provider fails with Go's `getPropProvider`
    /// error -- NOT the generic not-yet-ported fallback -- and an extra
    /// argument is 1582, the same refusal the other zero-arg builtins raise.
    #[test]
    fn tidb_is_ddl_owner_reports_the_missing_provider_like_go() {
        let function = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("tidb_is_ddl_owner"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![],
        ));
        let error = function
            .eval(&crate::context::NoColumns, empty_row())
            .unwrap_err();
        assert!(
            matches!(
                &error,
                EvalError::Unsupported(message)
                    if message.contains(
                        "optional property: 'OptPropDDLOwnerInfo' not exists in EvalContext"
                    )
            ),
            "{error:?}"
        );

        let function = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("tidb_is_ddl_owner"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Constant(Constant::new(
                Datum::Int(1),
                FieldType::new(FieldTypeCode::LongLong),
            ))],
        ));
        assert_eq!(
            function.eval(&crate::context::NoColumns, empty_row()),
            Err(EvalError::WrongParameterCount("tidb_is_ddl_owner"))
        );
    }
}
