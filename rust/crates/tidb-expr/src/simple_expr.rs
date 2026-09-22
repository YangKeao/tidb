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

//! SEED of Go `pkg/expression`, covering the "build one expression against a
//! table, with limited context" surface that DDL, partition pruning and the
//! DDL coprocessor reach for -- and nothing else. `pkg/expression` is far too
//! large to complete in one unit; this module is explicitly a SEED with the
//! boundaries named below.
//!
//! Ported symbol groups, each with its Go home:
//!
//! - **Simple-expression building** (`simple_rewriter.go`, `expression.go`):
//!   [`BuildOptions`] with its complete option surface --
//!   [`BuildOptions::with_table_info`] (Go `WithTableInfo`),
//!   [`BuildOptions::with_input_schema_and_names`] (`WithInputSchemaAndNames`),
//!   [`BuildOptions::with_allow_cast_array`] (`WithAllowCastArray`),
//!   [`BuildOptions::with_cast_expr_to`] (`WithCastExprTo`) and
//!   [`BuildOptions::with_use_new_collate`] (`WithUseNewCollate`) -- plus
//!   [`build_simple_expr`] (Go `BuildSimpleExpr`, whose body lives in
//!   `pkg/planner/core/expression_rewriter.go:108` `buildSimpleExpr`),
//!   [`parse_simple_expr`] (`simple_rewriter.go:37`) and
//!   [`parse_simple_expr_with_table_info`] (`simple_rewriter.go:31`).
//! - **Name resolution** (`simple_rewriter.go:63`, `:92`): ALREADY PORTED in
//!   this crate as [`crate::find_field_name`] /
//!   [`crate::find_field_name_index_by_column`]; this module adds only the
//!   [`SchemaNameResolver`] that binds an `Expr::Column` path through them.
//! - **Condition composition** (`expression.go:824-848`):
//!   [`compose_cnf_condition`], [`compose_dnf_condition`] and their shared
//!   `compose_condition_with_binary_op`.
//! - **Column extraction** (`util.go:127`, `:140`, `:164`):
//!   [`extract_columns`], [`extract_cor_columns`] and
//!   [`extract_columns_from_expressions`].
//! - **Column-info conversion** (`expression.go:1109`, `:1115`):
//!   [`column_infos_to_columns_and_names`] and
//!   [`column_infos_to_columns_and_names_with_collate`], over the
//!   [`ColumnInfoSource`] view described below, plus the `ResolveIndices`
//!   walk they finish a virtual generated column with,
//!   [`resolve_indices_in_place`].
//!
//! # Boundaries (this is a seed, not the package)
//!
//! - `// boundary:` Go `model.TableInfo`/`model.ColumnInfo`. `tidb-expr` sits
//!   BELOW `tidb-model` in the workspace and must not depend on it, so the
//!   column-info conversions take a [`ColumnInfoSource`] view: six accessors
//!   naming exactly the `ColumnInfo` fields Go reads here. Callers that hold
//!   real `tidb_model::ColumnInfo` values implement it in one place. No model
//!   type is duplicated.
//! - `// boundary:` Go `generatedexpr.SimpleResolveName`. Go resolves a stored
//!   generated-column string's names against the `TableInfo` BEFORE building,
//!   because its rewriter needs `ColumnNameExpr.Refer`. This port resolves
//!   names through [`SchemaNameResolver`] during the single rewrite walk, so
//!   the pre-pass has no counterpart and the `tblInfo` argument of
//!   `WithInputSchemaAndNames` is not carried.
//! - `// boundary:` Go `DEFAULT(col)`. `buildSimpleExpr`'s `SourceTable` also
//!   feeds `DEFAULT(col)`, which needs `ColumnInfo.DefaultValue` /
//!   `DefaultIsExpr`. That leg is NOT ported; it stays available to callers
//!   through [`ColumnResolver::resolve_default`], which
//!   [`SchemaNameResolver`] forwards to its base context.
//! - `// boundary:` `WithAllowCastArray(true)`. The flag is stored and
//!   reported, but this crate's rewriter rejects every `CAST(.. AS .. ARRAY)`
//!   (`rewriter.rs`, "a CAST with the ARRAY modifier is not supported yet"),
//!   so the permissive leg cannot yet be exercised.
//! - `// boundary:` `WithUseNewCollate`. Stored and reported, but this crate
//!   derives collation from the process-wide
//!   `tidb_datatype::new_collation_enabled()` rather than a
//!   per-build flag, so the value does not yet steer derivation.
//! - `// boundary:` Go's `sqlexec.SQLParser` fast path in `ParseSimpleExpr`
//!   (reuse of the session's pooled parser). This port always calls
//!   [`tidb_parser::parse`]; the parse RESULT is identical, only the pooling
//!   is absent. Go's `AppendWarning(util.SyntaxWarn(..))` loop over parser
//!   warnings likewise has no warning sink here.
//! - `// boundary:` unknown-column diagnostics. Go raises
//!   `[planner:1054]Unknown column 'a' in 'expression'`; this crate's
//!   rewriter reports `EvalError::Unsupported("unresolved column
//!   reference")`, which is the shared spelling every existing resolver
//!   already produces. Changing it is an `EvalError` change, not a change
//!   here.
//! - `// boundary:` `NewFunctionInternal`'s constant folding. Go composes each
//!   CNF/DNF node through `NewFunctionInternal`, which folds. The composers
//!   here build the node and stop, because their callers compose predicates
//!   that were already folded when they were built.
//!
//! NOT ported from the extractor family (`util.go`), by name, so the omission
//! is greppable: `ExtractDependentColumns`, `ExtractColumnsMapFromExpressions`,
//! `ExtractColumnsMapFromExpressionsWithReusedMap`,
//! `ExtractAllColumnsFromExpressionsInUsedSlices`,
//! `ExtractAllColumnsFromExpressions`, `ExtractColumnsSetFromExpressions`,
//! `ExtractColumnsAndCorColumnsFromExpressions`, `ExtractColumnsFromColOpCol`,
//! `GetUniqueIDToColumnMap`/`PutUniqueIDToColumnMap`. Also not ported here:
//! `FlattenCNFConditions`/`FlattenDNFConditions` (the inverse of the
//! composers) and `TableInfo2SchemaAndNames` (needs `TableInfo.Indices`,
//! i.e. the model boundary above).

use std::collections::BTreeMap;

use tidb_ast::{CiString, Expr, SelectField, Stmt};
use tidb_datatype::{
    FieldName, FieldNameMetadata, FieldType, FieldTypeCode, FieldTypeFlags, IdentifierMetadata,
    QualifiedColumnName,
};

use crate::column::{Column, CorrelatedColumn};
use crate::constant_fold::ConstantFoldMode;
use crate::exprctx::PlanColumnIdAllocator;
use crate::expression::{Expression, ScalarFunction};
use crate::field_name::{find_field_name, NonUniqueFieldName};
use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};
use crate::schema::Schema;
use crate::EvalError;

/// Why a simple-expression build failed.
///
/// The four fixed messages are Go's own `errors.New` strings, byte for byte,
/// so a caller that surfaces them matches TiDB's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimpleExprError {
    /// Go `ParseSimpleExpr`: the expression string was empty.
    EmptyExpressionString,
    /// Go `buildSimpleExpr`: names were given without a schema.
    NamesWithoutSchema,
    /// Go `buildSimpleExpr`: schema and names disagree in length.
    SchemaNamesLengthMismatch,
    /// Go `errNonUniq` (1052), raised by the already-ported
    /// [`crate::find_field_name`]: the reference matches several visible
    /// fields.
    NonUniqueColumn(NonUniqueFieldName),
    /// Go `Column.ResolveIndices`: a bound column is absent from the schema.
    ColumnNotInSchema(i64),
    /// The parser rejected `select <expr>`.
    Parse(String),
    /// The rewrite could not build the expression in the ported domain.
    Build(EvalError),
}

impl std::fmt::Display for SimpleExprError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyExpressionString => {
                formatter.write_str("expression should not be an empty string")
            }
            Self::NamesWithoutSchema => formatter
                .write_str("InputSchema and InputNames should be specified at the same time"),
            Self::SchemaNamesLengthMismatch => {
                formatter.write_str("InputSchema and InputNames should be the same length")
            }
            Self::NonUniqueColumn(error) => write!(formatter, "{error}"),
            Self::ColumnNotInSchema(unique_id) => {
                write!(
                    formatter,
                    "Can't find column with UniqueID {unique_id} in schema"
                )
            }
            Self::Parse(message) => write!(formatter, "{message}"),
            Self::Build(error) => write!(formatter, "{error:?}"),
        }
    }
}

impl std::error::Error for SimpleExprError {}

impl From<NonUniqueFieldName> for SimpleExprError {
    fn from(error: NonUniqueFieldName) -> Self {
        Self::NonUniqueColumn(error)
    }
}

impl From<EvalError> for SimpleExprError {
    fn from(error: EvalError) -> Self {
        Self::Build(error)
    }
}

/// The `model.ColumnInfo` fields `ColumnInfos2ColumnsAndNamesWithCollate`
/// reads, as a view.
///
/// boundary: `tidb-expr` may not depend on `tidb-model` (the model crate is
/// the higher layer). Rather than copy a `ColumnInfo` struct into this crate
/// -- a duplicate that would silently drift -- the conversion is generic over
/// this trait, and the one caller that owns real column metadata implements
/// it once.
pub trait ColumnInfoSource {
    /// Go `ColumnInfo.Name`.
    fn column_name(&self) -> &CiString;
    /// Go `ColumnInfo.ID`.
    fn column_id(&self) -> i64;
    /// Go `ColumnInfo.Offset`: the column's position in the TABLE, which
    /// becomes `Column.Index` before `ResolveIndices` remaps it.
    fn column_offset(&self) -> i64;
    /// Go `ColumnInfo.FieldType`.
    fn column_field_type(&self) -> &FieldType;
    /// Go `ColumnInfo.Hidden`.
    fn column_hidden(&self) -> bool {
        false
    }
    /// Go `ColumnInfo.GeneratedExprString`, but only when
    /// `ColumnInfo.IsVirtualGenerated()` holds -- the exact condition under
    /// which Go builds the expression. A STORED generated column returns
    /// `None`: its value is read from the row, never recomputed.
    fn virtual_generated_expr(&self) -> Option<&str> {
        None
    }
}

/// Go `BuildOptions` (`expression.go:55`): the optional settings a simple
/// build accepts.
///
/// Go applies variadic `BuildOption` closures; Rust uses consuming builder
/// methods, one per Go option, with the same names.
#[derive(Debug, Clone, Default)]
pub struct BuildOptions {
    /// Go `InputSchema`.
    pub input_schema: Option<Schema>,
    /// Go `InputNames`.
    pub input_names: Vec<FieldName>,
    /// Go `SourceTableDB`.
    pub source_table_db: IdentifierMetadata,
    /// Go `AllowCastArray`. See this module's boundary note: stored, but the
    /// permissive leg is not reachable yet.
    pub allow_cast_array: bool,
    /// Go `TargetFieldType`: when set, the built expression is wrapped in a
    /// cast to it.
    pub target_field_type: Option<FieldType>,
    /// Go `UseNewCollate`. See this module's boundary note.
    pub use_new_collate: bool,
}

impl BuildOptions {
    /// Go's zero `BuildOptions` with `UseNewCollate` seeded from the process
    /// collation mode, which is what `buildSimpleExpr` does before applying
    /// any option.
    #[must_use]
    pub fn new() -> Self {
        Self {
            use_new_collate: tidb_datatype::new_collation_enabled(),
            ..Self::default()
        }
    }

    /// Go `WithInputSchemaAndNames(schema, names, table)`.
    ///
    /// The `table` argument is dropped: it exists in Go only to reach
    /// `DEFAULT(col)` metadata, which this seed does not port (see the module
    /// boundary note).
    #[must_use]
    pub fn with_input_schema_and_names(mut self, schema: Schema, names: Vec<FieldName>) -> Self {
        self.input_schema = Some(schema);
        self.input_names = names;
        self
    }

    /// Go `WithTableInfo(db, tblInfo)`.
    ///
    /// Go stores the table and lets `buildSimpleExpr` call
    /// `ColumnInfos2ColumnsAndNames` if no schema was supplied. Because that
    /// conversion needs a column-id allocator and a build context, this port
    /// performs it here -- the observable result (a schema and names over the
    /// table's columns, qualified by `db`) is the same, and it fails at the
    /// point the caller can see why.
    pub fn with_table_info<C: ColumnInfoSource>(
        mut self,
        ctx: &dyn ColumnResolver,
        ids: &dyn PlanColumnIdAllocator,
        db: &str,
        table_name: &CiString,
        col_infos: &[C],
    ) -> Result<Self, SimpleExprError> {
        self.source_table_db = IdentifierMetadata::from_parts(db, CiString::new(db).lowercase());
        if self.input_schema.is_none() {
            let (columns, names) = column_infos_to_columns_and_names(
                ctx,
                ids,
                &self.source_table_db,
                table_name,
                col_infos,
            )?;
            self.input_schema = Some(Schema::new(columns));
            self.input_names = names;
        }
        Ok(self)
    }

    /// Go `WithAllowCastArray(allow)`.
    #[must_use]
    pub fn with_allow_cast_array(mut self, allow: bool) -> Self {
        self.allow_cast_array = allow;
        self
    }

    /// Go `WithCastExprTo(targetFt)`.
    #[must_use]
    pub fn with_cast_expr_to(mut self, target: FieldType) -> Self {
        self.target_field_type = Some(target);
        self
    }

    /// Go `WithUseNewCollate(useNewCollate)`.
    #[must_use]
    pub fn with_use_new_collate(mut self, use_new_collate: bool) -> Self {
        self.use_new_collate = use_new_collate;
        self
    }
}

/// Go `expressionRewriter`'s schema/name scope, as a [`ColumnResolver`].
///
/// It resolves an `Expr::Column` path through [`find_field_name`] (Go
/// `FindFieldName`) into the position of a [`FieldName`], then hands back the
/// schema column at that position UNCHANGED -- Go's `toColumn` returns
/// `schema.Columns[idx]` itself, so `ID`, `OrigName`, `IsHidden` and
/// `VirtualExpr` must survive the binding.
///
/// Every non-column decision (session zone, connection charset, fold mode,
/// `DEFAULT` resolution, ...) is forwarded to the base context, which is Go's
/// `ctx BuildContext`.
pub struct SchemaNameResolver<'a> {
    base: &'a dyn ColumnResolver,
    schema: &'a Schema,
    names: &'a [FieldName],
}

impl<'a> SchemaNameResolver<'a> {
    /// Binds `schema`/`names` as the column scope over the `base` context.
    #[must_use]
    pub fn new(base: &'a dyn ColumnResolver, schema: &'a Schema, names: &'a [FieldName]) -> Self {
        Self {
            base,
            schema,
            names,
        }
    }
}

impl ColumnResolver for SchemaNameResolver<'_> {
    fn param_value(&self, order: usize) -> Result<tidb_datatype::Datum, crate::EvalError> {
        self.base.param_value(order)
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let column = self.resolve_column(path)?;
        Some((
            usize::try_from(column.index).ok()?,
            column.ret_type.clone()?,
            column.unique_id,
        ))
    }

    /// narrowing: [`ColumnResolver::resolve_column`] answers `Option`, so an
    /// AMBIGUOUS reference (Go's 1052 from `FindFieldName`) collapses into the
    /// same "unresolved" answer as an unknown one. The distinction is
    /// preserved in [`SimpleExprError::NonUniqueColumn`] for callers that
    /// reach [`crate::find_field_name`] directly; carrying it through the
    /// rewrite would require an error channel the resolver trait does not
    /// have.
    fn resolve_column(&self, path: &[String]) -> Option<Column> {
        let index = find_field_name(self.names, &qualified_name_of(path)).ok()??;
        self.schema.columns.get(index).cloned()
    }

    fn resolve_default(&self, path: &[String]) -> Option<Expression> {
        self.base.resolve_default(path)
    }

    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.base.time_zone()
    }

    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.base.date_modes()
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        self.base.connection_charset_info()
    }

    fn tidb_info_len(&self) -> usize {
        self.base.tidb_info_len()
    }

    fn like_default_escape(&self) -> u8 {
        self.base.like_default_escape()
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.base.no_unsigned_subtraction()
    }

    fn div_precision_increment(&self) -> u32 {
        self.base.div_precision_increment()
    }

    fn current_database(&self) -> Option<String> {
        self.base.current_database()
    }

    fn fold_mode(&self) -> ConstantFoldMode {
        self.base.fold_mode()
    }

    fn fold_constant(&self, expression: &mut Expression, mode: ConstantFoldMode) {
        self.base.fold_constant(expression, mode);
    }

    fn eval_constant(
        &self,
        expression: &Expression,
    ) -> Result<tidb_datatype::Datum, crate::EvalError> {
        self.base.eval_constant(expression)
    }
}

/// The `ast.ColumnName` a rewriter path denotes: `["db","t","a"]`,
/// `["t","a"]` or `["a"]`.
fn qualified_name_of(path: &[String]) -> QualifiedColumnName {
    let part = |offset: usize| -> IdentifierMetadata {
        path.len()
            .checked_sub(offset)
            .and_then(|at| path.get(at))
            .map(|raw| IdentifierMetadata::from_parts(raw, CiString::new(raw).lowercase()))
            .unwrap_or_default()
    };
    QualifiedColumnName {
        database: part(3),
        table: part(2),
        column: part(1),
    }
}

/// Go `ParseSimpleExpr` (`simple_rewriter.go:37`): parses `expr_str` as the
/// sole select field of `select <expr_str>` and builds it.
pub fn parse_simple_expr(
    ctx: &dyn ColumnResolver,
    expr_str: &str,
    options: &BuildOptions,
) -> Result<Expression, SimpleExprError> {
    if expr_str.is_empty() {
        // Go asserts in intest builds and returns this exact message
        // otherwise, because reaching it means a caller bug.
        return Err(SimpleExprError::EmptyExpressionString);
    }
    let node = parse_select_field_expr(expr_str)?;
    build_simple_expr(ctx, &node, options)
}

/// Go `ParseSimpleExprWithTableInfo` (`simple_rewriter.go:31`), kept because
/// Go keeps it: a deprecated shorthand for `ParseSimpleExpr` with
/// `WithTableInfo("", tableInfo)`.
pub fn parse_simple_expr_with_table_info<C: ColumnInfoSource>(
    ctx: &dyn ColumnResolver,
    ids: &dyn PlanColumnIdAllocator,
    expr_str: &str,
    table_name: &CiString,
    col_infos: &[C],
) -> Result<Expression, SimpleExprError> {
    let options = BuildOptions::new().with_table_info(ctx, ids, "", table_name, col_infos)?;
    parse_simple_expr(ctx, expr_str, &options)
}

/// The `select <expr>` trick both `ParseSimpleExpr` and
/// `generatedexpr.ParseExpression` use to reach the expression grammar.
fn parse_select_field_expr(expr_str: &str) -> Result<Expr, SimpleExprError> {
    let stmt = tidb_parser::parse(&format!("select {expr_str}"))
        .map_err(|error| SimpleExprError::Parse(error.message))?;
    let Stmt::Query(query) = stmt else {
        return Err(SimpleExprError::Parse("expected a query".to_owned()));
    };
    let tidb_ast::QueryStmt::Select(select) = &*query else {
        return Err(SimpleExprError::Parse("expected a SELECT".to_owned()));
    };
    match select.fields.fields().first() {
        Some(SelectField::Expr { expr, .. }) => Ok(expr.clone()),
        _ => Err(SimpleExprError::Parse(
            "expected an expression field".to_owned(),
        )),
    }
}

/// Go `BuildSimpleExpr` (`expression.go:126`, implemented by
/// `pkg/planner/core/expression_rewriter.go:108` `buildSimpleExpr`): builds an
/// expression from one AST node with limited context.
///
/// Subqueries, window and aggregate functions and the other planner-only
/// constructs Go lists are outside this crate's rewriter as well, so they fail
/// as `EvalError::Unsupported` rather than being silently accepted.
pub fn build_simple_expr(
    ctx: &dyn ColumnResolver,
    node: &Expr,
    options: &BuildOptions,
) -> Result<Expression, SimpleExprError> {
    if options.input_schema.is_none() && !options.input_names.is_empty() {
        return Err(SimpleExprError::NamesWithoutSchema);
    }
    if let Some(schema) = &options.input_schema {
        if schema.columns.len() != options.input_names.len() {
            return Err(SimpleExprError::SchemaNamesLengthMismatch);
        }
    }

    // Go falls back to an EMPTY schema when no scope was supplied, so an
    // unqualified name is "unknown column" rather than a panic.
    let empty = Schema::default();
    let schema = options.input_schema.as_ref().unwrap_or(&empty);
    let resolver = SchemaNameResolver::new(ctx, schema, &options.input_names);

    let expr = rewrite_expr_resolved(node, &resolver)?;
    match &options.target_field_type {
        Some(target) => Ok(build_cast_function(expr, target.clone(), false)?),
        None => Ok(expr),
    }
}

/// Go `WrapWithCastAsInt`/`WrapWithCastAsReal` as the hybrid push uses them
/// (`builtin_cast.go:2909-2923`): the wrapped branch becomes
/// ETInt/ETReal-typed so the rebuilt control function infers a numeric
/// result. The enum `ENUM_SET_AS_INT` stamp is unnecessary in this shape:
/// the built cast node evaluates the ordinal through `cast_arg_as_int`'s
/// hybrid short-circuit.
fn wrap_cast_for_hybrid_push(
    expr: Expression,
    real: bool,
    target_unsigned: bool,
) -> Result<Expression, EvalError> {
    if let Some(ft) = expr.static_type() {
        let wanted = if real {
            tidb_datatype::EvalType::Real
        } else {
            tidb_datatype::EvalType::Int
        };
        if ft.eval_type() == wanted {
            return Ok(expr);
        }
    }
    let source_flen = expr.static_type().map_or(0, FieldType::flen);
    let not_null = expr
        .static_type()
        .is_some_and(|ft| ft.has_flag(FieldTypeFlags::NOT_NULL));
    let source_unsigned = expr
        .static_type()
        .is_some_and(|ft| ft.has_flag(FieldTypeFlags::UNSIGNED));
    let mut tp = if real {
        // Go WrapWithCastAsReal: Double + MaxRealWidth + unspecified decimal,
        // flags inheriting UnsignedFlag|NotNullFlag from the source.
        let mut tp = FieldType::new(FieldTypeCode::Double);
        tp.set_flen(22);
        tp.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
        tp
    } else {
        // Go WrapWithCastAsInt: LongLong + source flen + decimal 0, flags
        // inheriting NotNullFlag from the source and UnsignedFlag from the
        // TARGET.
        let mut tp = FieldType::new(FieldTypeCode::LongLong);
        tp.set_flen(source_flen);
        tp.set_decimal(0);
        tp
    };
    tp.set_charset_name("binary");
    tp.set_collation_name("binary");
    tp.add_flags(FieldTypeFlags::BINARY);
    let mut inherited = if not_null {
        FieldTypeFlags::NOT_NULL
    } else {
        0
    };
    if real {
        if source_unsigned {
            inherited |= FieldTypeFlags::UNSIGNED;
        }
    } else if target_unsigned {
        inherited |= FieldTypeFlags::UNSIGNED;
    }
    tp.add_flags(inherited);
    build_cast_function(expr, tp, false)
}

pub(crate) fn build_cast_function(
    mut expr: Expression,
    mut target: FieldType,
    in_union: bool,
) -> Result<Expression, EvalError> {
    // Go's BuildCastFunctionWithCheck mutates only its DeepCopy of the target:
    // a nullable source makes the cast result nullable even when the caller's
    // requested target carries NotNullFlag. Keep the option-owned target
    // untouched while matching the source nullability in the built node.
    if expr
        .static_type()
        .is_some_and(|source| !source.has_flag(FieldTypeFlags::NOT_NULL))
    {
        target.del_flags(FieldTypeFlags::NOT_NULL);
    }
    // Go `castAsStringFunctionClass.getFunction` → `adjustRetFtForCastString`:
    // an unspecified-width CHAR target takes the produced value's width (and
    // a JSON source widens the code to LongBlob).
    if target.code() == FieldTypeCode::VarString {
        if let Some(arg_ft) = expr.static_type() {
            crate::rewriter::adjust_ret_ft_for_cast_string(&mut target, arg_ft);
        }
    }
    // Go `TryPushCastIntoControlFunctionForHybridType` (builtin_cast.go:2898):
    // a numeric-target cast over IF/CASE/ELT pushes INTO the branches when a
    // branch is a hybrid type (Enum/Set — Bit excluded, issue 24725): the
    // control function rebuilds over cast-wrapped branches so a branch's enum
    // ORDINAL flows forward, where the unpushed shape would route the enum
    // NAME through the string result and answer 0 for arithmetic.
    if matches!(
        target.eval_type(),
        tidb_datatype::EvalType::Int | tidb_datatype::EvalType::Real
    ) {
        if let Expression::ScalarFunction(control) = &expr {
            let name = control.func_name.lowercase();
            if matches!(name, "if" | "case" | "elt") {
                let is_hybrid = |e: &Expression| {
                    e.static_type()
                        .is_some_and(|ft| ft.is_hybrid() && ft.code() != FieldTypeCode::Bit)
                };
                let len = control.args.len();
                let branch_indexes: Vec<usize> = match name {
                    "if" => vec![1, 2],
                    "case" => {
                        let mut indexes: Vec<usize> = (1..len).step_by(2).collect();
                        if len % 2 == 1 {
                            indexes.push(len - 1);
                        }
                        indexes
                    }
                    _ => (1..len).collect(),
                };
                if branch_indexes.iter().any(|&i| is_hybrid(&control.args[i])) {
                    let unsigned_flag = target.flags() & FieldTypeFlags::UNSIGNED != 0;
                    let real = target.eval_type() == tidb_datatype::EvalType::Real;
                    let mut args = control.args.clone();
                    let mut pushed = true;
                    for &i in &branch_indexes {
                        match wrap_cast_for_hybrid_push(args[i].clone(), real, unsigned_flag) {
                            Ok(wrapped) => args[i] = wrapped,
                            Err(_) => {
                                pushed = false;
                                break;
                            }
                        }
                    }
                    if pushed {
                        // Go rebuilds the control function over the wrapped
                        // args and adopts the rebuilt signature's ret type;
                        // the OUTER cast still wraps the rebuilt node.
                        let inferred = if name == "case" {
                            let branches: Vec<Expression> = args
                                .iter()
                                .skip(1)
                                .step_by(2)
                                .chain((args.len() % 2 == 1).then(|| args.last()).flatten())
                                .cloned()
                                .collect();
                            crate::rewriter::builtin_return_type("case", &branches)
                        } else if name == "elt" {
                            crate::rewriter::builtin_return_type("elt", &args)
                        } else {
                            crate::rewriter::infer_type4_control_funcs("if", &args)
                        };
                        if let Some(ret_type) = inferred {
                            expr = Expression::ScalarFunction(ScalarFunction::new(
                                control.func_name.clone(),
                                ret_type,
                                args,
                            ));
                        }
                        // Inference failure keeps the unpushed node, which is
                        // Go's own `return expr` on error.
                    }
                }
            }
        }
    }
    let unsigned = target.flags() & FieldTypeFlags::UNSIGNED != 0;
    let source_eval_type = expr.static_type().map(FieldType::eval_type);
    let name = match target.code() {
        FieldTypeCode::Year => "cast_year",
        FieldTypeCode::Date | FieldTypeCode::NewDate => "cast_date",
        FieldTypeCode::Datetime | FieldTypeCode::Timestamp => "cast_datetime",
        FieldTypeCode::Duration => "cast_time",
        FieldTypeCode::NewDecimal if in_union => match source_eval_type {
            // Go's decimal target has source-specific inUnion signatures.
            // REAL and integer sources clamp a negative signed value before
            // ProduceDecWithSpecifiedTp; string/decimal sources take that
            // branch only when the merged target is UNSIGNED.
            Some(tidb_datatype::EvalType::Real) => "cast_real_to_decimal_in_union",
            Some(tidb_datatype::EvalType::Int) if unsigned => "cast_int_to_decimal_in_union",
            Some(tidb_datatype::EvalType::String) if unsigned => "cast_string_to_decimal_in_union",
            Some(tidb_datatype::EvalType::Decimal) if unsigned => "cast_decimal_in_union",
            _ => "cast_decimal",
        },
        FieldTypeCode::NewDecimal => "cast_decimal",
        FieldTypeCode::Float | FieldTypeCode::Double => {
            if in_union && target.flags() & FieldTypeFlags::UNSIGNED != 0 {
                // Go `builtinCastRealAsRealSig.evalReal`
                // (`builtin_cast.go:1346-1352`): an in-union unsigned-target
                // cast clamps a negative to 0.
                "cast_real_in_union"
            } else {
                "cast_double"
            }
        }
        FieldTypeCode::Json => "cast_json",
        FieldTypeCode::VectorFloat32 => "cast_vector",
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong
        | FieldTypeCode::Bit => {
            if unsigned {
                if in_union {
                    "cast_unsigned_in_union"
                } else {
                    "cast_unsigned"
                }
            } else {
                "cast_signed"
            }
        }
        FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::String
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob
        | FieldTypeCode::Enum
        | FieldTypeCode::Set => {
            if target.charset_name() == "binary" {
                "cast_binary"
            } else {
                "cast_char"
            }
        }
        _ => return Err(EvalError::Unsupported("this cast target is not ported")),
    };
    Ok(Expression::ScalarFunction(ScalarFunction::new(
        CiString::new(name),
        target,
        vec![expr],
    )))
}

/// Go `composeConditionWithBinaryOp` (`expression.go:825`): folds
/// `conditions` into a BALANCED binary tree, which is what keeps the
/// coprocessor's protobuf encoder/decoder shallow.
///
/// `None` is Go's nil for an empty slice; a single condition is returned
/// untouched.
fn compose_condition_with_binary_op(
    mut conditions: Vec<Expression>,
    func_name: &str,
) -> Option<Expression> {
    match conditions.len() {
        0 => None,
        1 => conditions.pop(),
        length => {
            let right = conditions.split_off(length / 2);
            let left = compose_condition_with_binary_op(conditions, func_name)?;
            let right = compose_condition_with_binary_op(right, func_name)?;
            let ret_type = crate::builtin_op::infer_op_type(func_name)
                .expect("`and`/`or` are in the logical-op result-type table");
            Some(Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(func_name),
                ret_type,
                vec![left, right],
            )))
        }
    }
}

/// Go `ComposeCNFCondition` (`expression.go:842`): the conjunction of
/// `conditions` as a balanced `AND` tree.
#[must_use]
pub fn compose_cnf_condition(conditions: Vec<Expression>) -> Option<Expression> {
    compose_condition_with_binary_op(conditions, "and")
}

/// Go `ComposeDNFCondition` (`expression.go:847`): the disjunction of
/// `conditions` as a balanced `OR` tree.
#[must_use]
pub fn compose_dnf_condition(conditions: Vec<Expression>) -> Option<Expression> {
    compose_condition_with_binary_op(conditions, "or")
}

/// Go `extractColumns` (`util.go:263`): the private walk both public
/// extractors share.
fn extract_columns_into(
    result: &mut BTreeMap<i64, Column>,
    expr: &Expression,
    filter: Option<&dyn Fn(&Column) -> bool>,
) {
    match expr {
        Expression::Column(column) => {
            if filter.is_none_or(|keep| keep(column)) {
                result.insert(column.unique_id, column.clone());
            }
        }
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_columns_into(result, arg, filter);
            }
        }
        _ => {}
    }
}

/// Go `ExtractColumns` (`util.go:127`): every distinct `*Column` under
/// `expr`, deduplicated by `UniqueID` and sorted by it.
///
/// Go deduplicates through a map and then sorts, precisely because a map's
/// iteration order is not stable; a `BTreeMap` gives the same set in the same
/// order without the sort.
#[must_use]
pub fn extract_columns(expr: &Expression) -> Vec<Column> {
    let mut result = BTreeMap::new();
    extract_columns_into(&mut result, expr, None);
    result.into_values().collect()
}

/// Go `ExtractColumnsFromExpressions` (`util.go:164`): [`extract_columns`]
/// over a batch, with an optional filter applied while walking so a caller
/// never allocates the columns it would discard.
#[must_use]
pub fn extract_columns_from_expressions(
    exprs: &[Expression],
    filter: Option<&dyn Fn(&Column) -> bool>,
) -> Vec<Column> {
    if exprs.is_empty() {
        return Vec::new();
    }
    let mut result = BTreeMap::new();
    for expr in exprs {
        extract_columns_into(&mut result, expr, filter);
    }
    result.into_values().collect()
}

/// Go `ExtractCorColumns` (`util.go:140`): the correlated columns under
/// `expr`, in walk order and WITHOUT deduplication -- Go appends, so a column
/// referenced twice appears twice.
#[must_use]
pub fn extract_cor_columns(expr: &Expression) -> Vec<CorrelatedColumn> {
    let mut result = Vec::new();
    extract_cor_columns_into(&mut result, expr);
    result
}

fn extract_cor_columns_into(result: &mut Vec<CorrelatedColumn>, expr: &Expression) {
    match expr {
        Expression::CorrelatedColumn(column) => result.push(column.clone()),
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_cor_columns_into(result, arg);
            }
        }
        _ => {}
    }
}

/// Go `Expression.ResolveIndices(schema)` restricted to the node kinds a
/// simple expression can contain: rebinds every `Column`'s `Index` to its
/// POSITION in `schema`.
///
/// `ColumnInfos2ColumnsAndNamesWithCollate` needs this because a column's
/// `Index` starts as its offset in the TABLE, which is not its position in a
/// schema built over a subset of the table's columns.
pub fn resolve_indices_in_place(
    expr: &mut Expression,
    schema: &Schema,
) -> Result<(), SimpleExprError> {
    match expr {
        Expression::Column(column) => {
            let at = schema.column_index(column);
            if at < 0 {
                return Err(SimpleExprError::ColumnNotInSchema(column.unique_id));
            }
            column.index = at as i64;
            Ok(())
        }
        Expression::ScalarFunction(function) => {
            for arg in &mut function.args {
                resolve_indices_in_place(arg, schema)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Go `ColumnInfos2ColumnsAndNames` (`expression.go:1109`):
/// [`column_infos_to_columns_and_names_with_collate`] under the process-wide
/// collation mode.
pub fn column_infos_to_columns_and_names<C: ColumnInfoSource>(
    ctx: &dyn ColumnResolver,
    ids: &dyn PlanColumnIdAllocator,
    db_name: &IdentifierMetadata,
    tbl_name: &CiString,
    col_infos: &[C],
) -> Result<(Vec<Column>, Vec<FieldName>), SimpleExprError> {
    column_infos_to_columns_and_names_with_collate(
        ctx,
        ids,
        db_name,
        tbl_name,
        col_infos,
        tidb_datatype::new_collation_enabled(),
    )
}

/// Go `ColumnInfos2ColumnsAndNamesWithCollate` (`expression.go:1115`): turns
/// column metadata into planner columns plus their field names, then resolves
/// each VIRTUAL generated column's expression against the columns just built.
///
/// The two-pass shape is Go's and is load-bearing: a generated column may name
/// a column that appears after it, so every column must exist before any
/// expression is built.
pub fn column_infos_to_columns_and_names_with_collate<C: ColumnInfoSource>(
    ctx: &dyn ColumnResolver,
    ids: &dyn PlanColumnIdAllocator,
    db_name: &IdentifierMetadata,
    tbl_name: &CiString,
    col_infos: &[C],
    use_new_collate: bool,
) -> Result<(Vec<Column>, Vec<FieldName>), SimpleExprError> {
    let table = IdentifierMetadata::from_parts(tbl_name.original(), tbl_name.lowercase());
    let mut columns = Vec::with_capacity(col_infos.len());
    let mut names = Vec::with_capacity(col_infos.len());
    for col in col_infos {
        let column_name = IdentifierMetadata::from_parts(
            col.column_name().original(),
            col.column_name().lowercase(),
        );
        let name = FieldName::new(FieldNameMetadata {
            original_table: table.clone(),
            original_column: column_name.clone(),
            database: db_name.clone(),
            table: table.clone(),
            column: column_name,
        });
        let mut column = Column::new(ids.alloc_plan_column_id(), col.column_field_type().clone());
        column.id = col.column_id();
        column.index = col.column_offset();
        // Go reads `names[i].String()` -- the name built just above, before
        // any hidden-column suppression is applied to it.
        column.orig_name = name.display_name();
        column.is_hidden = col.column_hidden();
        columns.push(column);
        names.push(name);
    }

    let mock_schema = Schema::new(columns.clone());
    for (at, col) in col_infos.iter().enumerate() {
        let Some(generated) = col.virtual_generated_expr() else {
            continue;
        };
        // boundary: Go wraps `ctx` with `CtxWithHandleTruncateErrLevel(
        // errctx.LevelIgnore)` on the first virtual column so a generated
        // expression's truncation does not warn twice. The static expression
        // context now carries that wrapper; this live `ColumnResolver` path
        // still has no truncate-level knob, so its warning suppression remains
        // a higher-layer boundary while the built expression is the same.
        let node = parse_select_field_expr(generated)?;
        let options = BuildOptions::new()
            .with_input_schema_and_names(mock_schema.clone(), names.clone())
            .with_allow_cast_array(true)
            .with_use_new_collate(use_new_collate);
        let mut virtual_expr = build_simple_expr(ctx, &node, &options)?;
        resolve_indices_in_place(&mut virtual_expr, &mock_schema)?;
        columns[at].virtual_expr = Some(Box::new(virtual_expr));
    }
    Ok((columns, names))
}
