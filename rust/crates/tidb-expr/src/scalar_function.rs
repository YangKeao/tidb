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
            .unwrap_or(tidb_datatype::Collation::Utf8Mb4Bin)
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
