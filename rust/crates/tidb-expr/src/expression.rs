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

//! The `pkg/expression` node hierarchy spine (from `expression.go`).
//!
//! DESIGN DECISION: Go's `Expression` is an interface implemented by a *closed*
//! set of node types (`Column`, `Constant`, `ScalarFunction`,
//! `CorrelatedColumn`), and callers pervasively type-switch (`expr.(*Column)`).
//! The faithful, idiomatic Rust model is therefore an **enum**, not a
//! `Box<dyn Expression>`: matching replaces the type-switches, and `Clone`/`Eq`
//! are cheap and structural.
//!
//! SEED SCOPE (grown incrementally, like `meta/model` was): the [`Column`]
//! variant plus [`Schema`] are ported here (the type every plan node exposes).
//! DEFERRED behavior: the ~30 `Eval*`/`GetType(ctx)`/`ResolveIndices`/
//! `ExplainInfo` methods of the interface (they need `EvalContext`,
//! `chunk.Row`, and the `builtinFunc` dispatch). Structural, context-free
//! methods (identity, decorrelation, type propagation, hash code, canonical
//! semantic hash, const-level) are ported now for all four node variants.

pub use crate::column::{Column, CorrelatedColumn};
pub use crate::constant::{Constant, ParamMarker};
pub use crate::scalar_function::ScalarFunction;
pub use crate::schema::{KeyInfo, Schema};
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode, MAX_DECIMAL_WIDTH};

// Type tags written as the first byte of an expression `HashCode`
// (`pkg/expression/expression.go`).
pub(crate) const CONSTANT_FLAG: u8 = 0;
pub(crate) const COLUMN_FLAG: u8 = 1;
pub(crate) const SCALAR_FUNCTION_FLAG: u8 = 3;
pub(crate) const PARAMETER_FLAG: u8 = 4;

/// Go `mysql.NotFixedDec` (`pkg/parser/mysql/type.go`).
const NOT_FIXED_DEC: i64 = 31;
/// Go `mysql.MaxRealWidth` (`pkg/parser/mysql/type.go`).
const MAX_REAL_WIDTH: i64 = 23;

/// Go `ConstLevel` (a `uint`): how constant an expression is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConstLevel(pub u32);

impl ConstLevel {
    /// Not a constant; may differ per input row (Go `ConstNone`, the zero value).
    pub const NONE: ConstLevel = ConstLevel(0);
    /// Constant only within one context/execution, e.g. a plan-cache `?`
    /// placeholder (Go `ConstOnlyInContext`).
    pub const ONLY_IN_CONTEXT: ConstLevel = ConstLevel(1);
    /// Always the same regardless of context or row (Go `ConstStrict`).
    pub const STRICT: ConstLevel = ConstLevel(2);
}

/// Go `Expression`: a scalar expression node.
///
/// A closed enum over the concrete node types: [`Column`](Expression::Column),
/// [`Constant`](Expression::Constant),
/// [`CorrelatedColumn`](Expression::CorrelatedColumn), and
/// [`ScalarFunction`](Expression::ScalarFunction) -- the full Go variant set.
#[derive(Clone, Debug)]
pub enum Expression {
    /// A column reference (Go `*Column`).
    Column(Column),
    /// A literal / deferred / parameter constant (Go `*Constant`).
    Constant(Constant),
    /// A column bound to an outer query's value (Go `*CorrelatedColumn`).
    CorrelatedColumn(CorrelatedColumn),
    /// A built-in function applied to arguments (Go `*ScalarFunction`).
    ScalarFunction(ScalarFunction),
}

/// Go `ExpressionsSemanticEqual`: compare expression trees using their
/// canonicalized hash bytes, so commutative operators and equivalent directed
/// comparisons share one semantic identity.
#[must_use]
pub fn expressions_semantic_equal(left: &Expression, right: &Expression) -> bool {
    left.canonical_hash_code() == right.canonical_hash_code()
}

/// The two facts needed to prove that a predicate rejects an outer-join row
/// after its inner-side columns have been replaced by SQL `NULL`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct NullRejectProof {
    non_true: bool,
    must_null: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NullRejectTestMode {
    ReturnsFalse,
    KeepsNull,
}

// `pkg/planner/util/null_misc_builtins.go`. These functions return NULL when
// any argument is NULL. Keep the complete source table here: an omitted entry
// loses a valid outer-join simplification, while a spurious entry can make the
// optimizer return wrong rows.
const NULL_REJECT_NULL_PRESERVING_FUNCTIONS: &[&str] = &[
    "cast",
    "not",
    "unaryminus",
    "bitneg",
    "greatest",
    "least",
    "bit_count",
    "ge",
    "le",
    "eq",
    "ne",
    "lt",
    "gt",
    "xor",
    "plus",
    "minus",
    "mod",
    "div",
    "mul",
    "intdiv",
    "bitand",
    "leftshift",
    "rightshift",
    "bitor",
    "bitxor",
    "like",
    "ilike",
    "regexp",
    "regexp_like",
    "regexp_substr",
    "regexp_instr",
    "regexp_replace",
    "strcmp",
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "ceil",
    "ceiling",
    "conv",
    "cos",
    "cot",
    "crc32",
    "degrees",
    "exp",
    "floor",
    "ln",
    "log",
    "log2",
    "log10",
    "pow",
    "power",
    "radians",
    "round",
    "sign",
    "sin",
    "sqrt",
    "tan",
    "ascii",
    "bin",
    "bit_length",
    "char_length",
    "character_length",
    "concat",
    "find_in_set",
    "from_base64",
    "hex",
    "insert_func",
    "instr",
    "lcase",
    "left",
    "length",
    "locate",
    "lower",
    "lpad",
    "ltrim",
    "mid",
    "oct",
    "octet_length",
    "ord",
    "position",
    "repeat",
    "replace",
    "reverse",
    "right",
    "rpad",
    "rtrim",
    "space",
    "substr",
    "substring",
    "substring_index",
    "to_base64",
    "translate",
    "trim",
    "ucase",
    "unhex",
    "upper",
    "weight_string",
    "adddate",
    "date_add",
    "subdate",
    "date_sub",
    "addtime",
    "convert_tz",
    "date",
    "date_format",
    "datediff",
    "day",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "extract",
    "from_days",
    "from_unixtime",
    "hour",
    "last_day",
    "makedate",
    "maketime",
    "microsecond",
    "minute",
    "month",
    "monthname",
    "period_add",
    "period_diff",
    "quarter",
    "sec_to_time",
    "second",
    "str_to_date",
    "subtime",
    "time",
    "timediff",
    "time_format",
    "time_to_sec",
    "timestamp",
    "timestampadd",
    "timestampdiff",
    "to_days",
    "to_seconds",
    "unix_timestamp",
    "weekday",
    "weekofyear",
    "year",
    "compress",
    "md5",
    "sha1",
    "sha",
    "sha2",
    "sm3",
    "uncompress",
    "uncompressed_length",
    "json_type",
    "json_extract",
    "json_unquote",
    "json_remove",
    "json_merge",
    "json_merge_preserve",
    "json_contains",
    "json_contains_path",
    "json_overlaps",
    "json_memberof",
    "json_valid",
    "json_pretty",
    "json_quote",
    "json_storage_free",
    "json_storage_size",
    "json_depth",
    "json_keys",
    "json_length",
    "inet_aton",
    "inet_ntoa",
    "inet6_aton",
    "inet6_ntoa",
];

const NULL_REJECT_REJECT_NULL_TESTS: &[(&str, NullRejectTestMode)] = &[
    ("istrue", NullRejectTestMode::ReturnsFalse),
    ("istrue_with_null", NullRejectTestMode::KeepsNull),
    ("isfalse", NullRejectTestMode::ReturnsFalse),
];

/// Proves whether a predicate can be true after every listed inner-side
/// column is replaced by SQL `NULL`.
///
/// This is Go `pkg/planner/util.IsNullRejected`. It tracks both "cannot be
/// true" and the stronger "must be NULL" fact, because SQL three-valued
/// logic needs both for `NOT`, `AND`, and `OR`. Before symbolic reasoning it
/// also tries Go's nullify-then-fold bridge for constant subtrees.
#[must_use]
pub fn is_null_rejected(inner_column_ids: &[i64], predicate: &Expression) -> bool {
    prove_null_rejected(inner_column_ids, predicate, true).non_true
}

fn prove_null_rejected(
    inner_column_ids: &[i64],
    expression: &Expression,
    allow_nullified_fold: bool,
) -> NullRejectProof {
    if allow_nullified_fold {
        if let Some(constant) = try_fold_nullified_constant(inner_column_ids, expression) {
            return proof_from_constant(&constant);
        }
    }

    match expression {
        Expression::Column(column) if inner_column_ids.contains(&column.unique_id) => {
            NullRejectProof {
                non_true: true,
                must_null: true,
            }
        }
        Expression::Constant(constant) => {
            if constant.param_marker.is_none() {
                if let Some(deferred) = constant.deferred_expr.as_deref() {
                    return prove_null_rejected(inner_column_ids, deferred, false);
                }
            }
            proof_from_constant(constant)
        }
        Expression::ScalarFunction(function) => {
            prove_null_rejected_function(inner_column_ids, function, allow_nullified_fold)
        }
        Expression::Column(_) | Expression::CorrelatedColumn(_) => NullRejectProof::default(),
    }
}

fn prove_null_rejected_function(
    inner_column_ids: &[i64],
    function: &ScalarFunction,
    allow_nullified_fold: bool,
) -> NullRejectProof {
    let name = function.func_name.lowercase();
    let prove = |argument: &Expression| {
        prove_null_rejected(inner_column_ids, argument, allow_nullified_fold)
    };
    match (name, function.args.as_slice()) {
        ("and", [left, right]) => {
            let left = prove(left);
            let right = prove(right);
            return NullRejectProof {
                non_true: left.non_true || right.non_true,
                must_null: left.must_null && right.must_null,
            };
        }
        ("or", [left, right]) => {
            let left = prove(left);
            let right = prove(right);
            return NullRejectProof {
                non_true: left.non_true && right.non_true,
                must_null: left.must_null && right.must_null,
            };
        }
        ("not", [Expression::ScalarFunction(child)])
            if child.func_name.lowercase() == "isnull" && child.args.len() == 1 =>
        {
            return NullRejectProof {
                non_true: prove(&child.args[0]).must_null,
                must_null: false,
            };
        }
        ("not", [child]) => {
            let child = prove(child);
            return NullRejectProof {
                non_true: child.must_null,
                must_null: child.must_null,
            };
        }
        ("in", arguments) => {
            let Some((value, list)) = arguments.split_first() else {
                return NullRejectProof::default();
            };
            if prove(value).must_null || list.iter().all(|item| prove(item).must_null) {
                return NullRejectProof {
                    non_true: true,
                    must_null: true,
                };
            }
            return NullRejectProof::default();
        }
        ("isnull", _) => return NullRejectProof::default(),
        ("week" | "yearweek", [date, ..]) => {
            if prove(date).must_null {
                return NullRejectProof {
                    non_true: true,
                    must_null: true,
                };
            }
            return NullRejectProof::default();
        }
        _ => {}
    }

    if let Some((_, mode)) = NULL_REJECT_REJECT_NULL_TESTS
        .iter()
        .find(|(candidate, _)| *candidate == name)
    {
        let child = function
            .args
            .first()
            .map_or_else(NullRejectProof::default, prove);
        return NullRejectProof {
            non_true: child.must_null,
            must_null: child.must_null && *mode == NullRejectTestMode::KeepsNull,
        };
    }

    if NULL_REJECT_NULL_PRESERVING_FUNCTIONS.contains(&name)
        && function
            .args
            .iter()
            .any(|argument| prove(argument).must_null)
    {
        return NullRejectProof {
            non_true: true,
            must_null: true,
        };
    }
    NullRejectProof::default()
}

fn try_fold_nullified_constant(
    inner_column_ids: &[i64],
    expression: &Expression,
) -> Option<Constant> {
    match expression {
        Expression::Column(column) if inner_column_ids.contains(&column.unique_id) => Some(
            Constant::new(Datum::Null, column.get_static_type()?.clone()),
        ),
        Expression::Constant(constant)
            if constant.param_marker.is_none() && constant.deferred_expr.is_none() =>
        {
            Some(constant.clone())
        }
        Expression::ScalarFunction(function) => {
            try_fold_nullified_function(inner_column_ids, function)
        }
        Expression::Column(_) | Expression::Constant(_) | Expression::CorrelatedColumn(_) => None,
    }
}

fn try_fold_nullified_function(
    inner_column_ids: &[i64],
    function: &ScalarFunction,
) -> Option<Constant> {
    let name = function.func_name.lowercase();
    let result_type = function.get_static_type()?.clone();
    if matches!(name, "coalesce" | "ifnull") {
        for argument in &function.args {
            let constant = try_fold_nullified_constant(inner_column_ids, argument)?;
            if !constant.value.is_null() {
                return Some(constant);
            }
        }
        return Some(Constant::new(Datum::Null, result_type));
    }
    if name == "if" {
        let [condition, when_true, when_false] = function.args.as_slice() else {
            return None;
        };
        let take_true = match condition {
            Expression::ScalarFunction(condition)
                if condition.func_name.lowercase() == "isnull" && condition.args.len() == 1 =>
            {
                try_fold_nullified_constant(inner_column_ids, &condition.args[0])?
                    .value
                    .is_null()
            }
            condition => {
                let condition = try_fold_nullified_constant(inner_column_ids, condition)?;
                crate::truthy_of(&condition.value).ok()? == Some(true)
            }
        };
        return try_fold_nullified_constant(
            inner_column_ids,
            if take_true { when_true } else { when_false },
        );
    }
    if name == "truncate"
        && result_type.eval_type() == tidb_datatype::EvalType::Int
        && function
            .args
            .get(1)
            .and_then(Expression::static_type)
            .is_some_and(tidb_datatype::FieldType::is_unsigned)
    {
        // Both Go integer TRUNCATE signatures inspect an unsigned scale's
        // FieldType before evaluating its value. Even a nullable unsigned
        // scale therefore returns X unchanged instead of propagating NULL.
        return try_fold_nullified_constant(inner_column_ids, function.args.first()?);
    }

    let arguments = function
        .args
        .iter()
        .map(|argument| try_fold_nullified_constant(inner_column_ids, argument))
        .collect::<Option<Vec<_>>>()?;
    if NULL_REJECT_NULL_PRESERVING_FUNCTIONS.contains(&name)
        && arguments.iter().any(|argument| argument.value.is_null())
    {
        return Some(Constant::new(Datum::Null, result_type));
    }
    let folded = Expression::ScalarFunction(ScalarFunction::new(
        function.func_name.clone(),
        result_type.clone(),
        arguments.into_iter().map(Expression::Constant).collect(),
    ));
    let value = crate::eval_expression_once(&folded, &crate::NoColumns).ok()?;
    Some(Constant::new(value, result_type))
}

fn proof_from_constant(constant: &Constant) -> NullRejectProof {
    if constant.param_marker.is_some() || constant.deferred_expr.is_some() {
        return NullRejectProof::default();
    }
    if constant.value.is_null() {
        return NullRejectProof {
            non_true: true,
            must_null: true,
        };
    }
    NullRejectProof {
        non_true: crate::truthy_of(&constant.value).ok() == Some(Some(false)),
        must_null: false,
    }
}

/// Borrows an expression's own declared result type for context-free metadata
/// propagation.
fn ret_type_mut(expr: &mut Expression) -> Option<&mut FieldType> {
    match expr {
        Expression::Column(column) => column.ret_type.as_mut(),
        Expression::Constant(constant) => constant.ret_type.as_mut(),
        Expression::CorrelatedColumn(column) => column.column.ret_type.as_mut(),
        Expression::ScalarFunction(function) => function.ret_type.as_mut(),
    }
}

/// Replaces an expression's result metadata with the type propagated by an
/// enclosing cast. This is the context-free portion of Go's
/// `Expression.PropagateType` (`pkg/expression/expression.go:1238-1308`).
///
/// Go currently has only one caller, `builtinCastDecimalAsRealSig`, and only
/// implements `ETReal`. Taking the expression by mutable reference keeps the
/// same observable metadata update while Rust ownership makes Go's defensive
/// leaf clones unnecessary.
pub(crate) fn propagate_type(expr: &mut Expression, eval_type: EvalType) {
    if eval_type != EvalType::Real {
        return;
    }
    let Some(source) = expr.static_type().cloned() else {
        return;
    };
    let old_flen = source.flen();
    let old_decimal = source.decimal();
    let mut new_decimal = NOT_FIXED_DEC;
    let mut new_flen = if old_decimal != NOT_FIXED_DEC {
        15 + 2 + new_decimal
    } else {
        MAX_REAL_WIDTH
    };
    // For float(M,D), double(M,D), or decimal(M,D), M must be >= D.
    if new_flen < new_decimal {
        new_flen = old_flen - old_decimal + new_decimal;
    }
    if old_flen != new_flen || old_decimal != new_decimal {
        if source.code() == FieldTypeCode::NewDecimal {
            if new_decimal > tidb_datatype::MAX_DECIMAL_SCALE {
                new_decimal = tidb_datatype::MAX_DECIMAL_SCALE;
            }
            // The input data must not overflow under the new type. Extend the
            // fractional part only as far as the DECIMAL precision limit
            // allows while preserving the original integer width.
            if old_flen - old_decimal > new_flen - new_decimal {
                if new_decimal > old_decimal {
                    let increment = (new_decimal - old_decimal).min(MAX_DECIMAL_WIDTH - old_flen);
                    new_flen = old_flen + increment;
                    new_decimal = old_decimal + increment;
                } else {
                    new_flen = old_flen;
                    new_decimal = old_decimal;
                }
            }
        }
        if let Some(target) = ret_type_mut(expr) {
            target.set_flen_under_limit(new_flen);
            target.set_decimal_under_limit(new_decimal);
        }
    }
}

impl Expression {
    /// Go `Expression.HashCode`: the type-tagged canonical byte encoding used as
    /// a map/dedup key. Structural and context-free.
    pub fn hash_code(&mut self) -> &[u8] {
        match self {
            Expression::Column(c) => c.hash_code(),
            Expression::Constant(c) => c.hash_code(),
            Expression::CorrelatedColumn(c) => c.hash_code(),
            Expression::ScalarFunction(c) => c.hash_code(),
        }
    }

    /// Go `Expression.CanonicalHashCode`: leaves retain their ordinary
    /// type-tagged bytes while scalar functions normalize commutative and
    /// directed-comparison forms recursively.
    #[must_use]
    pub fn canonical_hash_code(&self) -> Vec<u8> {
        match self {
            Expression::Column(column) => {
                let mut column = column.clone();
                column.hash_code().to_vec()
            }
            Expression::Constant(constant) => constant.canonical_hash_code(),
            Expression::CorrelatedColumn(column) => {
                let mut column = column.clone();
                column.hash_code().to_vec()
            }
            Expression::ScalarFunction(function) => function.canonical_hash_code(),
        }
    }

    /// Go `Expression.IsCorrelated`.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        match self {
            Expression::Column(c) => c.is_correlated(),
            Expression::Constant(c) => c.is_correlated(),
            Expression::CorrelatedColumn(c) => c.is_correlated(),
            Expression::ScalarFunction(c) => c.is_correlated(),
        }
    }

    /// Go `Expression.ConstLevel`.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        match self {
            Expression::Column(c) => c.const_level(),
            Expression::Constant(c) => c.const_level(),
            Expression::CorrelatedColumn(c) => c.const_level(),
            Expression::ScalarFunction(c) => c.const_level(),
        }
    }

    /// Go `Expression.Decorrelate`: recursively remove correlated references
    /// that belong to the supplied outer schema.
    ///
    /// Rust returns a rebuilt expression tree because nodes are owned values;
    /// this is equivalent to Go's in-place mutation for scalar functions and
    /// preserves the input expression for callers that still need it. `None`
    /// models Go's nil schema pointer: it is valid for constants and plain
    /// columns, while a correlated node panics on the same invalid
    /// dereference as Go.
    #[must_use]
    pub fn decorrelate(&self, schema: Option<&Schema>) -> Expression {
        match self {
            Expression::Column(column) => column.decorrelate(),
            Expression::Constant(constant) => Expression::Constant(constant.clone()),
            Expression::CorrelatedColumn(column) => {
                let schema = schema.expect("CorrelatedColumn::decorrelate requires a schema");
                column.decorrelate(schema)
            }
            Expression::ScalarFunction(function) => {
                Expression::ScalarFunction(function.decorrelate(schema))
            }
        }
    }

    /// The context-free subset of Go `Expression.Equal(ctx, e)`.
    ///
    /// Columns compare by `UniqueID`. Constants compare their retained typed
    /// value, and scalar functions compare their normalized name, return type,
    /// and arguments recursively. The latter is the contract optimizer rules
    /// such as `InjectProjBelowAgg` use to share one projected expression
    /// between an aggregate argument and an identical group item.
    #[must_use]
    pub fn equal(&self, other: &Expression) -> bool {
        match self {
            Expression::Column(c) => c.equal_column(other),
            Expression::CorrelatedColumn(c) => c.equal_column(other),
            // Go `Constant.Equal` (`constant.go:508`): both constants,
            // values binary-compare equal. The deferred-expression Eval
            // legs are this port's materialized values.
            Expression::Constant(c) => {
                let Expression::Constant(y) = other else {
                    return false;
                };
                c.value
                    .compare(&y.value, tidb_datatype::Collation::Binary)
                    .map(|order| order == std::cmp::Ordering::Equal)
                    .unwrap_or(false)
            }
            // Go `ScalarFunction.Equal` (`scalar_function.go:377`): same
            // lowercased name, equal return types, pairwise-equal
            // arguments.
            Expression::ScalarFunction(sf) => {
                let Expression::ScalarFunction(fun) = other else {
                    return false;
                };
                if sf.func_name.lowercase() != fun.func_name.lowercase() {
                    return false;
                }
                match (&sf.ret_type, &fun.ret_type) {
                    (Some(left), Some(right)) if left.equal(right) => {}
                    (None, None) => {}
                    _ => return false,
                }
                sf.args.len() == fun.args.len()
                    && sf
                        .args
                        .iter()
                        .zip(&fun.args)
                        .all(|(left, right)| left.equal(right))
            }
        }
    }

    /// Go `Expression.GetType` without an `EvalContext`: the expression's static
    /// result type. `None` mirrors a nil `RetType`.
    ///
    /// For a [`ScalarFunction`] this is the placeholder result type set at
    /// construction (faithful type inference is not yet ported).
    #[must_use]
    pub fn static_type(&self) -> Option<&tidb_datatype::FieldType> {
        match self {
            Expression::Column(c) => c.get_static_type(),
            Expression::Constant(c) => c.get_static_type(),
            Expression::CorrelatedColumn(c) => c.get_static_type(),
            Expression::ScalarFunction(c) => c.get_static_type(),
        }
    }

    /// Borrows the inner [`Column`] when this expression is a column reference.
    #[must_use]
    pub fn as_column(&self) -> Option<&Column> {
        match self {
            Expression::Column(c) => Some(c),
            _ => None,
        }
    }
}
