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
//! directly on the node and identifies the function by name; the evaluation
//! dispatch (the `builtinFunc` `eval*` methods, keyed by `tipb.ScalarFuncSig`)
//! is a separate, larger unit built on `EvalContext`/`chunk.Row`.
//!
//! Ported: the struct and its argument-structural methods, recursive
//! `Decorrelate`, const-level rules, the common `ReHashCode` path (including
//! `Grouping` metadata), structural `Hash64`/`Equals`, and evaluation for
//! operators plus the builtin families owned by the shared dispatch modules.
//! Unknown builtin names fail explicitly. Remaining structural gaps are
//! per-signature collation and `MemoryUsage`.

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

/// Maps a Go unary-operator scalar-function name (`pkg/parser/ast`) to a
/// [`UnaryOp`]. Returns `None` for any function that is not a unary operator.
fn unary_op_for_name(name: &str) -> Option<UnaryOp> {
    Some(match name {
        "unaryplus" => UnaryOp::Plus,
        "unaryminus" => UnaryOp::Minus,
        "bitneg" => UnaryOp::BitNeg,
        "not" => UnaryOp::Not,
        _ => return None,
    })
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
    /// Go `builtinInStringSig.hashSet`, prepared after collation derivation.
    /// Only strict string/byte literals enter it; every other argument keeps
    /// the ordinary evaluation path so casts, warnings and errors are not
    /// skipped.
    in_string_hash_set: Option<std::collections::HashSet<Vec<u8>>>,
    in_string_non_const_args: Vec<usize>,
    in_string_has_null: bool,
    json_schema_cache: crate::builtin_ext::JsonSchemaCache,
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
    fn render(expression: &Expression, go_float_format: bool, ctx: &dyn Columns) -> Option<String> {
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
                Datum::Null => Some("NULL".to_owned()),
                _ => None,
            },
            Expression::Column(column) if !column.orig_name.is_empty() => {
                Some(column.orig_name.clone())
            }
            Expression::CorrelatedColumn(column) if !column.column.orig_name.is_empty() => {
                Some(column.column.orig_name.clone())
            }
            Expression::ScalarFunction(function) => {
                let op = arithmetic_symbol(binary_op_for_name(function.func_name.lowercase())?)?;
                let [left, right] = function.args.as_slice() else {
                    return None;
                };
                Some(format!(
                    "({} {op} {})",
                    render(left, go_float_format, ctx)?,
                    render(right, go_float_format, ctx)?
                ))
            }
            _ => None,
        }
    }
    let [left, right] = function.get_args() else {
        return None;
    };
    Some(format!(
        "({} {symbol} {})",
        render(left, go_float_format, ctx)?,
        render(right, go_float_format, ctx)?
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

impl ScalarFunction {
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
        self.in_string_hash_set = None;
        self.in_string_non_const_args.clear();
        self.in_string_has_null = false;
        self.json_schema_cache = Default::default();
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

    /// THE guarantee Go's `ScalarFunction.Eval` provides and this tier must
    /// reproduce: a function's evaluated value is in the EVAL-TYPE FAMILY of
    /// its own declared result type.
    ///
    /// Go gets this by construction. `ScalarFunction.Eval`
    /// (`pkg/expression/scalar_function.go`) does not ask the signature for
    /// "a value" -- it switches on `sf.GetType().EvalType()` and calls
    /// `EvalInt`/`EvalReal`/`EvalDecimal`/`EvalString`/`EvalTime`/
    /// `EvalDuration`/`EvalJSON` accordingly, so the returned datum's kind is
    /// DERIVED from the result type and cannot disagree with it. Downstream,
    /// `chunk.AppendDatum` dispatches on the datum kind while the column's
    /// cell width came from the field type, and only that construction keeps
    /// the two in step.
    ///
    /// This tier evaluates on Datums and dispatches on operand kinds, so the
    /// guarantee is restored here instead, once, for every function -- rather
    /// than at the handful of call sites whose mismatch had been noticed
    /// (`IF`/`CASE`), which left the same defect reachable from every other
    /// one. A value already in the right family is returned untouched, so the
    /// result type's LENGTH constraints are applied only where Go's own
    /// argument cast would have applied them.
    fn coerce_to_ret_type(&self, value: Datum) -> Result<Datum, EvalError> {
        if value.is_null() {
            return Ok(value);
        }
        let Some(ret_type) = self.get_static_type() else {
            return Ok(value);
        };
        // Go's `builtinCoalesceTimeSig`/`builtinCoalesceDurationSig` stamps
        // every selected temporal value with the merged result FSP after
        // evaluating it. `newBaseBuiltinFuncWithTp` does not wrap temporal
        // arguments, so the selected value can still carry the first
        // argument's original precision. Preserve the value's instant and
        // update only that metadata here; a general `ConvertTo` would also
        // round the instant, which is not what Coalesce's `SetFsp` does.
        if self.func_name.lowercase() == "coalesce" {
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
            return Ok(value);
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

    /// Builds Go `builtinInStringSig.buildHashMapForConstArgs` for the strict
    /// string literals this expression model can evaluate without a statement
    /// context. Other constant families remain in the row path because their
    /// implicit casts can raise warnings or errors.
    pub(crate) fn prepare_in_string_hash_set(&mut self) {
        if self.func_name.lowercase() != "in"
            || self.args.len() < 2
            || self
                .args
                .first()
                .and_then(Expression::static_type)
                .is_none_or(|field_type| field_type.eval_type() != tidb_datatype::EvalType::String)
        {
            return;
        }
        let collator = tidb_datatype::get_collator(self.derived_collation().name());
        // Go's `pkg/expression/builtin_other.go::builtinInStringSig` keeps
        // these immutable literal keys in a per-function map and probes it
        // for every row. Reserve the complete
        // literal-list capacity up front, matching the source map's intended
        // read-mostly shape without changing its collision-resistant hasher or
        // any membership/collation semantics.
        let mut hash_set =
            std::collections::HashSet::with_capacity(self.args.len().saturating_sub(1));
        let mut non_const_args = Vec::new();
        let mut has_null = false;
        for (index, argument) in self.args.iter().enumerate().skip(1) {
            let Expression::Constant(constant) = argument else {
                non_const_args.push(index);
                continue;
            };
            if constant.const_level() != crate::expression::ConstLevel::STRICT {
                non_const_args.push(index);
                continue;
            }
            match &constant.value {
                Datum::String(value) => {
                    hash_set.insert(collator.key(value.bytes()));
                }
                Datum::Bytes(value) => {
                    hash_set.insert(collator.key(value));
                }
                Datum::Null => has_null = true,
                _ => non_const_args.push(index),
            }
        }
        self.in_string_hash_set = Some(hash_set);
        self.in_string_non_const_args = non_const_args;
        self.in_string_has_null = has_null;
    }
}

/// The cast a `cast_*` function name describes, with the width and scale its
/// result type carries. Go stores the same fact as the chosen
/// `builtinCast*As*Sig`.
/// Converts a user variable's stored value onto the kind its `getvar_<kind>`
/// call declared. NULL stays NULL, and a value already of that kind passes
/// through untouched -- the conversion only matters when an assignment made
/// during this same statement changed the kind out from under the plan.
fn uservar_as_kind(kind: &str, value: Datum, ctx: &dyn Columns) -> Result<Datum, EvalError> {
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

fn cast_type_of(target: &str, ret_type: &FieldType) -> Result<tidb_ast::CastType, EvalError> {
    use tidb_ast::CastType;
    let len = || u32::try_from(ret_type.flen()).ok();
    Ok(match target {
        "signed" => CastType::Signed,
        "unsigned" => CastType::Unsigned,
        "unsigned_in_union" => CastType::UnsignedInUnion,
        "char" => CastType::Char {
            len: len(),
            // A binary-charset CHAR target truncates in BYTES
            // (`ProduceStrWithSpecifiedTp`'s `chs == CharsetBin` branch);
            // carry that through the reconstructed cast type.
            // For character targets retain the resolved target charset too:
            // a BINARY source must pass through Go's `from_binary` decoder
            // before `CAST AS CHAR`, and an explicit `CHARACTER SET` target
            // must not silently fall back to the session default.
            charset: Some(if ret_type.is_binary_string() {
                "BINARY".to_owned()
            } else {
                ret_type.charset_name().to_owned()
            }),
        },
        "binary" => {
            // Go pads NUL bytes only the FIXED TypeString target
            // (`padZeroForBinaryType`'s `TypeString` gate,
            // `builtin_cast.go:2251`): a lengthless `BINARY` keeps
            // TypeVarString, so its adjusted result flen (e.g. 20 for an
            // integer source) must not turn into BINARY(N) padding here.
            if ret_type.code() == tidb_datatype::FieldTypeCode::String {
                CastType::Binary { len: len() }
            } else {
                CastType::Binary { len: None }
            }
        }
        "decimal" => CastType::Decimal {
            flen: u32::try_from(ret_type.flen()).unwrap_or(0),
            // `WrapWithCastAsDecimal` preserves Go's `UnspecifiedLength`
            // scale on non-decimal sources.  The AST cast fields are unsigned,
            // so carry that state through the internal dispatch with the
            // sentinel understood by `cast::eval_cast` instead of silently
            // turning it into scale 0 (which rounds REAL 123.555 to 124).
            scale: if ret_type.decimal() == UNSPECIFIED_LENGTH {
                crate::cast::UNSPECIFIED_CAST_SCALE
            } else {
                u32::try_from(ret_type.decimal()).unwrap_or(0)
            },
        },
        "date" => CastType::Date,
        "datetime" => CastType::DateTime {
            fsp: u32::try_from(ret_type.decimal()).ok(),
        },
        "time" => CastType::Time {
            fsp: u32::try_from(ret_type.decimal()).ok(),
        },
        "year" => CastType::Year,
        "double" => CastType::Double,
        "json" => CastType::Json,
        "vector" => CastType::Vector { dimensions: len() },
        _ => return Err(EvalError::Unsupported("this cast target is not ported")),
    })
}

/// One numeric value of a column-wise comparison, in the comparison's
/// evaluation family (Go `getBaseCmpType`: `ETInt` when both sides are
/// integers, `ETDecimal` when a decimal meets an integer or a decimal).
#[derive(Clone, Copy)]
enum CompareValue {
    Int(crate::coerce::Integer),
    Decimal(tidb_datatype::MyDecimal),
}

impl CompareValue {
    fn cmp(self, other: Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => crate::coerce::integer_cmp(a, b),
            (a, b) => a.to_my_decimal().compare(&b.to_my_decimal()),
        }
    }

    fn to_my_decimal(self) -> tidb_datatype::MyDecimal {
        match self {
            Self::Int(crate::coerce::Integer::Signed(value)) => {
                tidb_datatype::MyDecimal::from_int(value)
            }
            Self::Int(crate::coerce::Integer::Unsigned(value)) => {
                tidb_datatype::MyDecimal::from_uint(value)
            }
            Self::Decimal(value) => value,
        }
    }
}

/// One argument of a numeric comparison as Go's `VecEvalInt`/`VecEvalDecimal`
/// reads it: a chunk column's 8-byte integer or 40-byte decimal cells, or
/// one strict constant.
enum CompareOperand<'a> {
    IntColumn {
        column: tidb_chunk::ColumnRead<'a>,
        signed: bool,
    },
    DecimalColumn(tidb_chunk::ColumnRead<'a>),
    Constant(Option<CompareValue>),
}

impl<'a> CompareOperand<'a> {
    /// Returns the operand and whether it is in the decimal family.
    fn of(expression: &Expression, input: &'a Chunk) -> Option<(Self, bool)> {
        use tidb_datatype::FieldTypeCode;
        let field_type = expression.static_type()?;
        // Only the integer codes store the value as the 8-byte cell
        // `GetInt64` reads; BIT and the ENUM/SET-as-int forms (Go's hybrid
        // types) keep the row path.
        let is_int = matches!(
            field_type.code(),
            FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
                | FieldTypeCode::Year
        );
        let is_decimal = field_type.code() == FieldTypeCode::NewDecimal;
        if !is_int && !is_decimal {
            return None;
        }
        // Go `mysql.HasUnsignedFlag(args[i].GetType(ctx).GetFlag())`: the
        // argument's field type decides how its int64 bits are read.
        let signed = !field_type.is_unsigned();
        match expression {
            Expression::Column(column) => {
                let index = usize::try_from(column.index).ok()?;
                if index >= input.num_cols() {
                    return None;
                }
                let column = input.column(index);
                if is_int && column.type_size() == 8 {
                    Some((Self::IntColumn { column, signed }, false))
                } else if is_decimal
                    && column.type_size() == tidb_chunk::column::MY_DECIMAL_STRUCT_SIZE
                {
                    Some((Self::DecimalColumn(column), true))
                } else {
                    None
                }
            }
            Expression::Constant(constant) => {
                let value = match constant.literal_value()? {
                    Datum::Null => None,
                    Datum::Int(value) if is_int => Some(CompareValue::Int(if signed {
                        crate::coerce::Integer::Signed(*value)
                    } else {
                        crate::coerce::Integer::Unsigned(*value as u64)
                    })),
                    Datum::UInt(value) if is_int => {
                        Some(CompareValue::Int(crate::coerce::Integer::Unsigned(*value)))
                    }
                    Datum::Decimal(value) if is_decimal => {
                        Some(CompareValue::Decimal(value.to_my_decimal().ok()?))
                    }
                    _ => return None,
                };
                Some((Self::Constant(value), is_decimal))
            }
            _ => None,
        }
    }

    fn get(&self, physical: usize) -> Option<CompareValue> {
        match self {
            Self::IntColumn { column, signed } => {
                if column.is_null(physical) {
                    return None;
                }
                let bits = column.get_int64(physical);
                Some(CompareValue::Int(if *signed {
                    crate::coerce::Integer::Signed(bits)
                } else {
                    crate::coerce::Integer::Unsigned(bits as u64)
                }))
            }
            Self::DecimalColumn(column) => {
                if column.is_null(physical) {
                    return None;
                }
                Some(CompareValue::Decimal(column.get_my_decimal(physical)))
            }
            Self::Constant(value) => *value,
        }
    }
}

impl ScalarFunction {
    /// Go `builtin{EQ,NE,LT,LE,GT,GE}{Int,Decimal}Sig.vecEvalInt`: a numeric
    /// comparison over the live physical rows `sel` of `input`, column-wise.
    ///
    /// `is_zero` receives one entry per row of `sel` in Go's `VecEvalBool`
    /// encoding (`-1` NULL, `0` false, `1` true). Returns `Ok(false)`, with
    /// `is_zero` untouched, for every shape this kernel does not cover -- an
    /// argument that is neither an integer/decimal column nor a strict
    /// integer/decimal constant, or a result type the row evaluator would
    /// still convert -- so the caller keeps the row evaluator as the
    /// behavior contract there. The covered shapes produce exactly what
    /// [`Self::eval`] produces row by row: NULL when either side is NULL,
    /// the signedness of each integer side read from its argument's field
    /// type (`ops::integer_binary_typed`), and an integer meeting a decimal
    /// compared exactly in the decimal domain (`ops::decimal_binary`).
    pub(crate) fn vec_eval_numeric_compare(
        &self,
        input: &Chunk,
        sel: &[usize],
        is_zero: &mut Vec<i8>,
    ) -> Result<bool, EvalError> {
        if self.args.len() != 2 {
            return Ok(false);
        }
        let op = match binary_op_for_name(self.func_name.lowercase()) {
            Some(
                op @ (BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge),
            ) => op,
            _ => return Ok(false),
        };
        // `coerce_to_ret_type` leaves an integer alone only for an integer
        // result type that is not BIT.
        let Some(ret_type) = self.get_static_type() else {
            return Ok(false);
        };
        if ret_type.eval_type() != EvalType::Int
            || ret_type.code() == tidb_datatype::FieldTypeCode::Bit
        {
            return Ok(false);
        }
        let (Some((lhs, _)), Some((rhs, _))) = (
            CompareOperand::of(&self.args[0], input),
            CompareOperand::of(&self.args[1], input),
        ) else {
            return Ok(false);
        };
        is_zero.clear();
        is_zero.reserve(sel.len());
        for &physical in sel {
            let (Some(a), Some(b)) = (lhs.get(physical), rhs.get(physical)) else {
                is_zero.push(-1);
                continue;
            };
            let ordering = a.cmp(b);
            let truth = match op {
                BinaryOp::Eq => ordering.is_eq(),
                BinaryOp::Ne => !ordering.is_eq(),
                BinaryOp::Lt => ordering.is_lt(),
                BinaryOp::Le => ordering.is_le(),
                BinaryOp::Gt => ordering.is_gt(),
                _ => ordering.is_ge(),
            };
            is_zero.push(i8::from(truth));
        }
        Ok(true)
    }

    /// Go `VecEvalBool`'s column-wise leg over one filter node: the numeric
    /// comparisons of [`Self::vec_eval_numeric_compare`] and `NOT` over a node
    /// this kernel covers (`builtinUnaryNotIntSig.vecEvalInt`: NULL stays
    /// NULL, zero becomes one, anything else zero). `is_zero` and the
    /// `Ok(false)` contract are those of the comparison kernel; removed
    /// families return `Ok(false)` before any local value dispatch.
    pub(crate) fn vec_eval_bool(
        &self,
        input: &Chunk,
        sel: &[usize],
        is_zero: &mut Vec<i8>,
    ) -> Result<bool, EvalError> {
        let name = self.func_name.lowercase();
        if name != "not" {
            return self.vec_eval_numeric_compare(input, sel, is_zero);
        }
        // `coerce_to_ret_type` leaves an integer alone only for an integer
        // result type that is not BIT.
        let Some(ret_type) = self.get_static_type() else {
            return Ok(false);
        };
        if ret_type.eval_type() != EvalType::Int
            || ret_type.code() == tidb_datatype::FieldTypeCode::Bit
        {
            return Ok(false);
        }
        match (name, self.args.as_slice()) {
            ("not", [Expression::ScalarFunction(argument)]) => {
                if !argument.vec_eval_bool(input, sel, is_zero)? {
                    return Ok(false);
                }
                for code in is_zero.iter_mut() {
                    if *code >= 0 {
                        *code = i8::from(*code == 0);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// One decimal value of a column-wise arithmetic evaluation, in the shape
/// the value-layer `Decimal` fast paths compute in: a signed `i128`
/// coefficient at `storage_scale` fraction digits, of which `scale` are the
/// SQL-visible result scale.
#[derive(Clone, Copy)]
struct DecimalValue {
    coefficient: i128,
    storage_scale: u32,
    scale: u32,
}

impl DecimalValue {
    fn from_my_decimal(value: &tidb_datatype::MyDecimal) -> Option<Self> {
        // `Decimal::from_my_decimal`: the stored fraction digits are padded
        // up to the result scale when the cell declares more of them.
        let (coefficient, digits_frac) = value.to_i128_scaled()?;
        let result_frac = u32::try_from(value.result_frac()).ok()?;
        let storage_scale = digits_frac.max(result_frac);
        let coefficient = if storage_scale > digits_frac {
            coefficient.checked_mul(10i128.checked_pow(storage_scale - digits_frac)?)?
        } else {
            coefficient
        };
        Some(Self {
            coefficient,
            storage_scale,
            scale: result_frac,
        })
    }

    fn from_integer(value: crate::coerce::Integer) -> Self {
        let coefficient = match value {
            crate::coerce::Integer::Signed(value) => i128::from(value),
            crate::coerce::Integer::Unsigned(value) => i128::from(value),
        };
        Self {
            coefficient,
            storage_scale: 0,
            scale: 0,
        }
    }

    /// `Decimal::add` (`try_add_fast`): operands aligned to the wider storage
    /// scale, the visible scale the wider of the two.
    fn add(self, other: Self) -> Option<Self> {
        let storage_scale = self.storage_scale.max(other.storage_scale);
        let left = self.aligned(storage_scale)?;
        let right = other.aligned(storage_scale)?;
        Some(Self {
            coefficient: left.checked_add(right)?,
            storage_scale,
            scale: self.scale.max(other.scale),
        })
    }

    fn sub(self, other: Self) -> Option<Self> {
        self.add(Self {
            coefficient: other.coefficient.checked_neg()?,
            ..other
        })
    }

    /// `Decimal::mul_mysql` (`try_mul_mysql_fast`): scales add; a result
    /// scale past MySQL's 30 takes the general path.
    fn mul(self, other: Self) -> Option<Self> {
        let scale = self.scale.checked_add(other.scale)?;
        if scale > 30 {
            return None;
        }
        Some(Self {
            coefficient: self.coefficient.checked_mul(other.coefficient)?,
            storage_scale: self.storage_scale.checked_add(other.storage_scale)?,
            scale,
        })
    }

    fn aligned(self, storage_scale: u32) -> Option<i128> {
        if storage_scale == self.storage_scale {
            Some(self.coefficient)
        } else {
            self.coefficient
                .checked_mul(10i128.checked_pow(storage_scale - self.storage_scale)?)
        }
    }

    /// `Decimal::to_chunk_my_decimal`: the cell the row path appends.
    fn to_my_decimal(self) -> Option<tidb_datatype::MyDecimal> {
        tidb_datatype::MyDecimal::from_scaled_i128(self.coefficient, self.storage_scale, self.scale)
    }
}

/// Go `VecEvalDecimal` over one expression node, restricted to the shapes
/// whose row evaluation is `ops::decimal_binary`'s exact fast arithmetic:
/// integer and decimal columns, strict integer and decimal constants, and
/// `+`/`-`/`*` nodes with a decimal result type over them where at least
/// one argument is a decimal (an integer-only node is integer arithmetic).
/// `None` means the shape is not covered or a value left the `i128` fast
/// path; the caller then evaluates the whole column row by row.
fn vec_eval_decimal(
    expression: &Expression,
    input: &Chunk,
    physical: &[usize],
) -> Option<Vec<Option<DecimalValue>>> {
    use tidb_datatype::FieldTypeCode;
    let field_type = expression.static_type()?;
    let is_int = matches!(
        field_type.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year
    );
    let is_decimal = field_type.code() == FieldTypeCode::NewDecimal;
    if !is_int && !is_decimal {
        return None;
    }
    let signed = !field_type.is_unsigned();
    match expression {
        Expression::Column(column) => {
            let index = usize::try_from(column.index).ok()?;
            if index >= input.num_cols() {
                return None;
            }
            let column = input.column(index);
            if is_int && column.type_size() == 8 {
                Some(
                    physical
                        .iter()
                        .map(|&row| {
                            (!column.is_null(row)).then(|| {
                                let bits = column.get_int64(row);
                                DecimalValue::from_integer(if signed {
                                    crate::coerce::Integer::Signed(bits)
                                } else {
                                    crate::coerce::Integer::Unsigned(bits as u64)
                                })
                            })
                        })
                        .collect(),
                )
            } else if is_decimal && column.type_size() == tidb_chunk::column::MY_DECIMAL_STRUCT_SIZE
            {
                physical
                    .iter()
                    .map(|&row| {
                        if column.is_null(row) {
                            Some(None)
                        } else {
                            DecimalValue::from_my_decimal(&column.get_my_decimal(row)).map(Some)
                        }
                    })
                    .collect()
            } else {
                None
            }
        }
        Expression::Constant(constant) => {
            let value = match constant.literal_value()? {
                Datum::Null => None,
                Datum::Int(value) if is_int => Some(DecimalValue::from_integer(if signed {
                    crate::coerce::Integer::Signed(*value)
                } else {
                    crate::coerce::Integer::Unsigned(*value as u64)
                })),
                Datum::UInt(value) if is_int => Some(DecimalValue::from_integer(
                    crate::coerce::Integer::Unsigned(*value),
                )),
                Datum::Decimal(value) if is_decimal => {
                    Some(DecimalValue::from_my_decimal(&value.to_my_decimal().ok()?)?)
                }
                _ => return None,
            };
            Some(vec![value; physical.len()])
        }
        Expression::ScalarFunction(function) => {
            if !is_decimal {
                return None;
            }
            vec_eval_decimal_function(function, input, physical)
        }
        Expression::CorrelatedColumn(_) => None,
    }
}

/// The `+`/`-`/`*` node of [`vec_eval_decimal`].
fn vec_eval_decimal_function(
    function: &ScalarFunction,
    input: &Chunk,
    physical: &[usize],
) -> Option<Vec<Option<DecimalValue>>> {
    use tidb_datatype::FieldTypeCode;
    if function.func_name.lowercase() == "cast_decimal" {
        return vec_eval_cast_int_as_decimal(function, input, physical);
    }
    {
        {
            if function.args.len() != 2 {
                return None;
            }
            let op = match binary_op_for_name(function.func_name.lowercase()) {
                Some(op @ (BinaryOp::Plus | BinaryOp::Minus | BinaryOp::Mul)) => op,
                _ => return None,
            };
            // `eval_binary_full` reaches `decimal_binary` only when an
            // operand is a decimal; two integers are integer arithmetic.
            let decimal_argument = function.args.iter().any(|argument| {
                argument
                    .static_type()
                    .is_some_and(|ty| ty.code() == FieldTypeCode::NewDecimal)
            });
            if !decimal_argument {
                return None;
            }
            let lhs = vec_eval_decimal(&function.args[0], input, physical)?;
            let rhs = vec_eval_decimal(&function.args[1], input, physical)?;
            lhs.into_iter()
                .zip(rhs)
                .map(|(left, right)| match (left, right) {
                    (Some(left), Some(right)) => match op {
                        BinaryOp::Plus => left.add(right),
                        BinaryOp::Minus => left.sub(right),
                        _ => left.mul(right),
                    }
                    .map(Some),
                    _ => Some(None),
                })
                .collect()
        }
    }
}

/// Go `builtinCastIntAsDecimalSig.vecEvalDecimal`, restricted to the shape
/// whose row evaluation (`cast::eval_cast`'s DECIMAL arm over an integer,
/// `Decimal::from_int` then `cast_to_precision`) is the integer itself: an
/// integer column cast to a `DECIMAL(flen, scale)` whose integer digits hold
/// every value the column's signedness can take (19 signed, 20 unsigned), so
/// `ProduceDecWithSpecifiedTp` neither clamps nor warns and the cell is the
/// integer padded to `scale` fraction digits. An unspecified target shape
/// keeps the row path (it returns the source unchanged), as does a value
/// whose padding leaves `i128`.
fn vec_eval_cast_int_as_decimal(
    function: &ScalarFunction,
    input: &Chunk,
    physical: &[usize],
) -> Option<Vec<Option<DecimalValue>>> {
    use tidb_datatype::FieldTypeCode;
    let [Expression::Column(column)] = function.args.as_slice() else {
        return None;
    };
    let source = column.get_static_type()?;
    if !matches!(
        source.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year
    ) {
        return None;
    }
    let signed = !source.is_unsigned();
    let target = function.get_static_type()?;
    if target.decimal() == UNSPECIFIED_LENGTH {
        return None;
    }
    let flen = u32::try_from(target.flen()).ok()?;
    let scale = u32::try_from(target.decimal()).ok()?;
    if flen == 0 || flen < scale.checked_add(if signed { 19 } else { 20 })? {
        return None;
    }
    let factor = 10i128.checked_pow(scale)?;
    let index = usize::try_from(column.index).ok()?;
    if index >= input.num_cols() {
        return None;
    }
    let column = input.column(index);
    if column.type_size() != 8 {
        return None;
    }
    physical
        .iter()
        .map(|&row| {
            if column.is_null(row) {
                return Some(None);
            }
            let bits = column.get_int64(row);
            let value = if signed {
                i128::from(bits)
            } else {
                i128::from(bits as u64)
            };
            Some(Some(DecimalValue {
                coefficient: value.checked_mul(factor)?,
                storage_scale: scale,
                scale,
            }))
        })
        .collect()
}

impl ScalarFunction {
    /// Go `builtinArithmetic{Plus,Minus,Multiply}DecimalSig.vecEvalDecimal`:
    /// appends this expression's value for every row of `input` to column
    /// `output_index` of `output`, column-wise. Returns `Ok(false)` with
    /// nothing appended when the shape is not covered (see
    /// [`vec_eval_decimal`]); the appended cells are the ones the row
    /// evaluator would append through `Chunk::append_datum`.
    pub(crate) fn vec_eval_decimal_arithmetic(
        &self,
        input: &Chunk,
        output: &mut Chunk,
        output_index: usize,
    ) -> Result<bool, EvalError> {
        if !self
            .get_static_type()
            .is_some_and(|ty| ty.code() == tidb_datatype::FieldTypeCode::NewDecimal)
        {
            return Ok(false);
        }
        let rows = input.num_rows();
        let physical: Vec<usize> = (0..rows).map(|row| input.get_row(row).idx()).collect();
        let Some(values) = vec_eval_decimal_function(self, input, &physical) else {
            return Ok(false);
        };
        let mut cells = Vec::with_capacity(rows);
        for value in values {
            match value {
                Some(value) => match value.to_my_decimal() {
                    Some(cell) => cells.push(Some(cell)),
                    None => return Ok(false),
                },
                None => cells.push(None),
            }
        }
        for cell in &cells {
            match cell {
                Some(cell) => output.append_my_decimal(output_index, cell),
                None => output.append_null(output_index),
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::column::Column;
    use crate::constant::Constant;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

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
        ScalarFunction::new(CiString::new(name), result_type, vec![])
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
    fn logical_short_circuit_survives_while_interval_refuses() {
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
                function.eval(&ctx, tidb_chunk::row::Row::empty()).unwrap(),
                Datum::Int(expected)
            );
            assert!(ctx.warnings.borrow().is_empty());
        }

        let interval = ScalarFunction::new(
            CiString::new("interval"),
            ft(),
            vec![integer(1), integer(0), integer(1), integer(2), division()],
        );
        assert!(matches!(
            interval.eval(&ctx, tidb_chunk::row::Row::empty()),
            Err(EvalError::Unsupported(_))
        ));
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
        assert!(matches!(
            interval.eval(&ctx, tidb_chunk::row::Row::empty()),
            Err(EvalError::Unsupported(_))
        ));
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
    fn concat_family_refuses_before_children_or_packet_warning() {
        let ctx = PacketColumns {
            limit: 3,
            warnings: RefCell::new(Vec::new()),
        };
        let string =
            |value: &[u8]| Expression::Constant(Constant::new(Datum::new_string(value), text_ft()));
        let unsupported = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("not_a_function"),
            text_ft(),
            vec![],
        ));
        let concat = ScalarFunction::new(
            CiString::new("concat"),
            text_ft(),
            vec![string(b"abcd"), unsupported],
        );
        assert_eq!(
            concat.eval(&ctx, tidb_chunk::row::Row::empty()),
            Err(EvalError::Unsupported(
                "native packet-limited string evaluation was removed; function unsupported"
            ))
        );
        assert!(ctx.warnings.borrow().is_empty());

        ctx.warnings.borrow_mut().clear();
        let concat_ws = ScalarFunction::new(
            CiString::new("concat_ws"),
            text_ft(),
            vec![string(b"--"), string(b"a"), string(b"b")],
        );
        assert_eq!(
            concat_ws.eval(&ctx, tidb_chunk::row::Row::empty()),
            Err(EvalError::Unsupported(
                "native packet-limited string evaluation was removed; function unsupported"
            ))
        );
        assert!(ctx.warnings.borrow().is_empty());

        assert_eq!(
            crate::func::eval_func_values_in(
                "CONCAT",
                &[Datum::new_string("abcd"), Datum::new_string("x")],
                &ctx,
            ),
            Some(Err(EvalError::Unsupported(
                "native packet-limited string evaluation was removed; function unsupported"
            )))
        );
        assert!(ctx.warnings.borrow().is_empty());
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
            assert!(matches!(
                function.eval(&crate::context::NoColumns, tidb_chunk::row::Row::empty()),
                Err(EvalError::Unsupported(_))
            ));
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
        let konst = |d: Datum| Expression::Constant(Constant::new(d, ft()));

        // Native math entry points fail closed after the kernel deletion.
        let abs = ScalarFunction::new(CiString::new("abs"), ft(), vec![konst(Datum::Int(-5))]);
        assert!(matches!(
            abs.eval(&NoColumns, row),
            Err(EvalError::Unsupported(_))
        ));

        // CONCAT no longer has a native scalar arm.
        let concat = ScalarFunction::new(
            CiString::new("concat"),
            text_ft(),
            vec![konst(Datum::new_string("a")), konst(Datum::new_string("b"))],
        );
        assert_eq!(
            concat.eval(&NoColumns, row),
            Err(EvalError::Unsupported(
                "native packet-limited string evaluation was removed; function unsupported"
            ))
        );

        // COALESCE(NULL, 7) = 7 (eager over values, matching the old
        // AST-level evaluator).
        let coalesce = ScalarFunction::new(
            CiString::new("coalesce"),
            ft(),
            vec![konst(Datum::Null), konst(Datum::Int(7))],
        );
        assert_eq!(coalesce.eval(&NoColumns, row).unwrap(), Datum::Int(7));

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
            decimal_coalesce.eval(&NoColumns, row).unwrap(),
            Datum::Decimal(tidb_datatype::Decimal::from_literal("1.0000"))
        );

        // A column argument feeds the builtin from the chunk row: ABS(col).
        let mut col = Column::new(1, ft());
        col.index = 0;
        let mut chk2 = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        chk2.append_int64(0, -9);
        let abs_col =
            ScalarFunction::new(CiString::new("abs"), ft(), vec![Expression::Column(col)]);
        assert!(matches!(
            abs_col.eval(&NoColumns, chk2.get_row(0)),
            Err(EvalError::Unsupported(_))
        ));

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
        assert_eq!(iff.eval(&NoColumns, row).unwrap(), Datum::Int(2));

        // A function with no ported builtin at all still is unsupported.
        let unknown = ScalarFunction::new(
            CiString::new("no_such_builtin"),
            ft(),
            vec![konst(Datum::Int(1))],
        );
        assert!(matches!(
            unknown.eval(&NoColumns, row),
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
        let json_text_type = text_ft();
        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&varbinary_type), 1);
        chk.append_bytes(0, b"ab");
        let row = chk.get_row(0);

        let mut col = Column::new(1, varbinary_type);
        col.index = 0;

        let json_array = ScalarFunction::new(
            CiString::new("json_array"),
            json_text_type,
            vec![Expression::Column(col.clone())],
        );
        assert_eq!(
            json_array.eval(&NoColumns, row).unwrap(),
            Datum::new_string(r#"["base64:type15:YWI="]"#.to_string())
        );

        let cast_json = ScalarFunction::new(
            CiString::new("cast_json"),
            FieldType::new(FieldTypeCode::Json),
            vec![Expression::Column(col)],
        );
        let Datum::Json(got) = cast_json.eval(&NoColumns, row).unwrap() else {
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

        assert!(matches!(
            rand_five.eval(&columns, row),
            Err(EvalError::Unsupported(_))
        ));
        assert!(matches!(
            rand_five.eval(&columns, row),
            Err(EvalError::Unsupported(_))
        ));
        assert!(columns.keys.into_inner().is_empty());
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
        assert!(matches!(
            rand.eval(&columns, row),
            Err(EvalError::Unsupported(_))
        ));
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
    fn string_in_prepares_collation_keys_for_strict_literals() {
        use tidb_chunk::chunk::Chunk;

        let mut result_type = ft();
        result_type.set_collation_name("utf8mb4_general_ci");
        let string_type = text_ft();
        let string = |value: &str| {
            Expression::Constant(Constant::new(Datum::new_string(value), string_type.clone()))
        };
        let mut function = ScalarFunction::new(
            CiString::new("in"),
            result_type,
            vec![string("a"), string("A"), string("b")],
        );
        function.prepare_in_string_hash_set();

        assert_eq!(function.in_string_hash_set.as_ref().unwrap().len(), 2);
        assert!(function.in_string_non_const_args.is_empty());
        let chunk = Chunk::new_with_capacity(&[], 1);
        assert_eq!(
            function.eval(&crate::NoColumns, chunk.get_row(0)).unwrap(),
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
        let mut binary = ScalarFunction::new(
            CiString::new("in"),
            binary_type,
            vec![string("a"), string("a")],
        );
        binary.prepare_in_string_hash_set();
        assert_eq!(
            binary.eval(&crate::NoColumns, chunk.get_row(0)).unwrap(),
            Datum::Bytes(b"1".to_vec())
        );

        // PAD SPACE collations still allocate a trimmed sort key. This is
        // the source behavior for `utf8mb4_bin`: `a` and `a ` compare equal.
        let pad_type = text_ft().with_collation(Collation::Utf8Mb4Bin);
        let mut pad = ScalarFunction::new(
            CiString::new("in"),
            pad_type,
            vec![string("a "), string("a")],
        );
        pad.prepare_in_string_hash_set();
        assert_eq!(
            pad.eval(&crate::NoColumns, chunk.get_row(0)).unwrap(),
            Datum::new_string("1")
        );

        // The Go map is built once for the immutable literal list. Reserving
        // that list's complete size avoids growth/rehash when a query has a
        // large `IN (...)` predicate (the hbx swap query has 1000 literals).
        let mut many = Vec::with_capacity(1001);
        many.push(string("probe"));
        for value in 0..1000 {
            many.push(string(&format!("maker-{value}")));
        }
        let mut large = ScalarFunction::new(CiString::new("in"), text_ft(), many);
        large.prepare_in_string_hash_set();
        assert!(large.in_string_hash_set.as_ref().unwrap().capacity() >= 1000);
    }
    /// The stock-level shape: `ge(ol_o_id, minus(d_next_o_id, 20))` over a
    /// joined chunk row -- the exact residual predicate class jTPCC's
    /// stock-level statement evaluates once per hash-join candidate, mixing
    /// PROBE-side and BUILD-side columns. Pins that the typed fast path
    /// answers the composed tree.
    #[test]
    fn fast_integer_path_answers_the_join_residual_shape() {
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
        let ge = ScalarFunction::new(
            CiString::new("ge"),
            ft(),
            vec![probe_col(), Expression::ScalarFunction(inner)],
        );

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

    /// NULL propagation on the fast path matches the ladder's answer for
    /// every covered operator family: arithmetic stays NULL, comparisons
    /// stay NULL (never false).
    #[test]
    fn fast_integer_path_propagates_null_like_the_ladder() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let chk = Chunk::new_with_capacity(std::slice::from_ref(&ft()), 1);
        let mut chk = chk;
        chk.append_int64(0, 7);
        let row = chk.get_row(0);
        let konst = |d: Datum, t: &FieldType| Expression::Constant(Constant::new(d, t.clone()));

        for name in ["plus", "minus", "mul", "eq", "ne", "lt", "le", "gt", "ge"] {
            let function = ScalarFunction::new(
                CiString::new(name),
                ft(),
                vec![konst(Datum::Null, &ft()), konst(Datum::Int(3), &ft())],
            );
            assert_eq!(
                function.eval(&NoColumns, row).unwrap(),
                Datum::Null,
                "{name} NULL lhs"
            );
            let function = ScalarFunction::new(
                CiString::new(name),
                ft(),
                vec![konst(Datum::Int(3), &ft()), konst(Datum::Null, &ft())],
            );
            assert_eq!(
                function.eval(&NoColumns, row).unwrap(),
                Datum::Null,
                "{name} NULL rhs"
            );
        }
    }

    /// A mixed signed/unsigned comparison orders by VALUE, not by bit
    /// pattern: `-1 < 1u` because Go's compare treats the signed side as
    /// negative against an unsigned partner. The fast path reinterprets each
    /// operand through its own argument field type before comparing.
    #[test]
    fn fast_integer_path_orders_mixed_signedness_by_value() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let signed_type = ft();
        let unsigned_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
        assert!(unsigned_type.is_unsigned());

        let mut signed_chk = Chunk::new_with_capacity(std::slice::from_ref(&signed_type), 1);
        signed_chk.append_int64(0, -1);
        let signed_row = signed_chk.get_row(0);

        let unsigned_chk = Chunk::new_with_capacity(std::slice::from_ref(&unsigned_type), 1);
        let mut unsigned_chk = unsigned_chk;
        unsigned_chk.append_uint64(0, 1);
        let unsigned_row = unsigned_chk.get_row(0);

        let signed_col = {
            let mut col = Column::new(1, signed_type.clone());
            col.index = 0;
            move || Expression::Column(col.clone())
        };
        let unsigned_col = {
            let mut col = Column::new(2, unsigned_type.clone());
            col.index = 0;
            move || Expression::Column(col.clone())
        };

        // The joined rows are assembled left-then-right in one scratch chunk:
        // column 0 reads the SIGNED value, column 1 the UNSIGNED one. Build
        // that exact two-column row and compare across it.
        let mut joined = Chunk::new_with_capacity(&[signed_type, unsigned_type], 1);
        joined.append_int64(0, -1);
        joined.append_uint64(1, 1);
        let joined_row = joined.get_row(0);
        let _ = (signed_row, unsigned_row);

        for (name, op_name, want) in [
            ("lt", "lt", 1u8),
            ("le", "le", 1),
            ("gt", "gt", 0),
            ("ge", "ge", 0),
            ("eq", "eq", 0),
            ("ne", "ne", 1),
        ] {
            let function = ScalarFunction::new(
                CiString::new(op_name),
                ft(),
                vec![signed_col(), unsigned_col()],
            );
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
    fn fast_integer_path_reports_unsigned_subtraction_overflow() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let unsigned_type = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);

        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&unsigned_type), 1);
        chk.append_uint64(0, 1);
        let row = chk.get_row(0);
        let mut col = Column::new(1, unsigned_type.clone());
        col.index = 0;
        let konst = |d: Datum, t: &FieldType| Expression::Constant(Constant::new(d, t.clone()));

        let function = ScalarFunction::new(
            CiString::new("minus"),
            unsigned_type.clone(),
            vec![
                Expression::Column(col),
                konst(Datum::Int(2), &unsigned_type),
            ],
        );
        let error = function
            .eval(&NoColumns, row)
            .expect_err("1u - 2 must overflow");
        assert!(
            matches!(error, EvalError::IntOverflow),
            "unexpected error: {error:?}"
        );
    }

    /// A non-ETInt argument keeps the generic ladder: string comparison
    /// semantics are untouched by the gate.
    #[test]
    fn fast_integer_gate_leaves_string_operands_on_the_ladder() {
        use crate::context::NoColumns;
        use tidb_chunk::chunk::Chunk;

        let string_type = text_ft();
        let chk = Chunk::new_with_capacity(std::slice::from_ref(&string_type), 1);
        let mut chk = chk;
        chk.append_string(0, b"a".as_slice());
        let row = chk.get_row(0);
        let konst = |d: Datum| Expression::Constant(Constant::new(d, string_type.clone()));

        let function = ScalarFunction::new(
            CiString::new("ge"),
            ft(),
            vec![konst(Datum::new_string("a")), konst(Datum::new_string("b"))],
        );
        assert_eq!(function.eval(&NoColumns, row).unwrap(), Datum::Int(0));
    }

    /// TiDB capture over `create table g (i int, d decimal(10,3))` holding
    /// `(-5, 2.500)`: `select least(i, d)` is `-5` with datum frac 0 -- the
    /// winner keeps the winning ARGUMENT's own decimal (Go casts integers to
    /// `SetDecimal(0)`), not the aggregated max (3), and `select least(1, d)`
    /// is `1` the same way. A fully-constant call is the one exception: the
    /// planner folds it to the RETURN type's scale, so `select least(1, 2.5)`
    /// is `1.0` (max argument decimal). The values remain independent oracle
    /// evidence while this native typed-column path now refuses explicitly.
    #[test]
    fn typed_extremum_preserves_the_former_scale_oracle_and_refuses() {
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
            ret_ft,
            vec![Expression::Column(col_i), Expression::Column(col_d)],
        ));
        let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&[int_ft, dec_ft], 1);
        chunk.append_datum(0, &Datum::Int(-5));
        chunk.append_datum(
            1,
            &Datum::Decimal(tidb_datatype::Decimal::from_literal("2.500")),
        );
        assert!(matches!(
            least.eval(&crate::context::NoColumns, chunk.get_row(0)),
            Err(EvalError::Unsupported(_))
        ));
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
