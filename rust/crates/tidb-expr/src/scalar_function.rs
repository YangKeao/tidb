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
//! Retained: the struct and its argument-structural methods, recursive
//! `Decorrelate`, const-level rules, the common `ReHashCode` path (including
//! `Grouping` metadata), and structural `Hash64`/`Equals`. Runtime evaluation
//! is owned exclusively by the shared TiKV engine.

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, SCALAR_FUNCTION_FLAG};
use crate::grouping::{GroupingMetadata, GroupingMetadataError, GroupingMode};
use crate::schema::Schema;
use tidb_ast::{BinaryOp, CiString, UnaryOp};
use tidb_codec::{encode_compact_bytes, encode_int};
use tidb_datatype::{FieldType, UNSPECIFIED_LENGTH};

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
    /// by [`crate::context::Columns::current_insert_value`] for each evaluation.
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
