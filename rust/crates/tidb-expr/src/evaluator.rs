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

//! `pkg/expression/evaluator.go`: evaluate a projection's calculated
//! expressions before transferring any direct input-column owners.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_util::ColumnSwapHelper;
use tidb_datatype::{Datum, FieldTypeCode};

use crate::context::{Columns, EvalError};
use crate::expression::Expression;

/// Go `HasGetSetVarFunc`: whether an expression contains a user-variable read
/// or assignment at any depth.
#[must_use]
pub fn has_get_set_var_func(expression: &Expression) -> bool {
    let Expression::ScalarFunction(function) = expression else {
        return false;
    };

    let name = function.func_name.lowercase();
    name == "setvar"
        || name == "getvar"
        || name.starts_with("getvar_")
        || function.get_args().iter().any(has_get_set_var_func)
}

/// Go `Vectorizable`: whether expressions may be evaluated column by column.
///
/// User-variable functions require select-list order for every row. Sequence
/// functions also require row-major order when a top-level `nextval` is mixed
/// with `lastval`/`setval`, or when more than one top-level `nextval` appears.
#[must_use]
pub fn vectorizable(expressions: &[Expression]) -> bool {
    if expressions.iter().any(has_get_set_var_func) {
        return false;
    }

    let mut nextval = 0;
    let mut lastval = 0;
    let mut setval = 0;
    for expression in expressions {
        let Expression::ScalarFunction(function) = expression else {
            continue;
        };
        match function.func_name.lowercase() {
            "nextval" => nextval += 1,
            "lastval" => lastval += 1,
            "setval" => setval += 1,
            _ => {}
        }
    }

    !((nextval > 0 && (lastval > 0 || setval > 0)) || nextval > 1)
}

/// Retained predicate programs. Evaluation order and NULL policy remain owned
/// by the filtering operation; each predicate has an independent engine cache.
pub struct FilterProgram {
    filters: Vec<Expression>,
    suites: Vec<EvaluatorSuite>,
    vectorizable: bool,
}

impl FilterProgram {
    /// Build metadata without evaluating any predicate or input row.
    pub fn new(filters: Vec<Expression>) -> Self {
        Self {
            vectorizable: vectorizable(&filters),
            suites: filters
                .iter()
                .map(|expr| EvaluatorSuite::new(vec![expr.clone()], true))
                .collect(),
            filters,
        }
    }

    /// Physical-mask filtering with the existing input-selection and NULL rules.
    pub fn consider_null<C: Columns>(
        &self,
        ctx: &C,
        vec_enabled: bool,
        input: &Chunk,
        selected: Vec<bool>,
        nulls: Vec<bool>,
    ) -> Result<(Vec<bool>, Vec<bool>), EvalError> {
        let (mut selected, nulls) = filter_physical_rows(
            ctx,
            vec_enabled,
            &self.filters,
            input,
            selected,
            nulls,
            !vec_enabled || !self.vectorizable,
            Some(&self.suites),
        )?;
        apply_input_selection(input, &mut selected);
        Ok((selected, nulls))
    }

    /// Ordinary WHERE matching: evaluate in order and stop at false OR NULL.
    /// The row index is already physical; never apply the chunk selection twice.
    pub fn matches_row<C: Columns>(
        &self,
        ctx: &C,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<bool, EvalError> {
        let virtual_input = row.chunk().is_none().then(|| {
            let mut input = Chunk::new_with_capacity(&[], 1);
            input.set_num_virtual_rows(1);
            input
        });
        let input = row
            .chunk()
            .or(virtual_input.as_ref())
            .expect("filter input");
        let physical = if virtual_input.is_some() {
            0
        } else {
            row.idx()
        };
        for suite in &self.suites {
            let value = eval_filter_row(suite, ctx, input, physical)?;
            if crate::truthy_of(&value)? != Some(true) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Number of engine compilations retained by this predicate set.
    #[cfg(feature = "tikv-expr")]
    pub fn tikv_compilations(&self) -> u64 {
        self.suites
            .iter()
            .map(|suite| suite.program.tikv_compilations())
            .sum()
    }
}

fn eval_filter_row<C: Columns>(
    suite: &EvaluatorSuite,
    ctx: &C,
    input: &Chunk,
    physical: usize,
) -> Result<Datum, EvalError> {
    suite
        .eval_selected_for_cast(ctx, input, &[physical])
        .map_err(into_eval_error)?
        .pop()
        .ok_or_else(|| EvalError::Unsupported("filter returned no value"))
}

/// Go `expression.VecEvalBool`/`VectorizedFilterConsiderNull`.
///
/// The returned mask is indexed by the physical rows of `input`, just like
/// Go's `selected` slice. The caller-supplied `selected` and `nulls` vectors
/// are output buffers (their previous contents are discarded); the input
/// chunk's selection vector is reapplied after evaluating physical rows.
/// As in Go, the vector path preserves NULL from equality rewritten from IN
/// until later conjuncts reject the row; ordinary NULL rejects it immediately.
///
/// Every predicate executes through an [`EvaluatorSuite`] backed by TiKV.
/// Filters remain filter-major and rejected rows are removed before the next
/// filter, so a later expression never observes a row rejected earlier. A
/// declined shape is a structured error; there is no row-evaluator fallback.
pub fn vectorized_filter_consider_null<C: Columns>(
    ctx: &C,
    vec_enabled: bool,
    filters: &[Expression],
    input: &Chunk,
    selected: Vec<bool>,
    nulls: Vec<bool>,
) -> Result<(Vec<bool>, Vec<bool>), EvalError> {
    let (mut selected, nulls) = filter_physical_rows(
        ctx,
        vec_enabled,
        filters,
        input,
        selected,
        nulls,
        !vec_enabled || !vectorizable(filters),
        None,
    )?;
    apply_input_selection(input, &mut selected);
    Ok((selected, nulls))
}

/// Go VecEvalBool: filter-major three-valued evaluation of physical rows.
/// Unlike VectorizedFilterConsiderNull, disabling typed vector evaluation
/// does not switch to rowBasedFilter's NULL handling or intersect input Sel.
pub fn vec_eval_bool<C: Columns>(
    ctx: &C,
    vec_enabled: bool,
    filters: &[Expression],
    input: &Chunk,
    selected: Vec<bool>,
    nulls: Vec<bool>,
) -> Result<(Vec<bool>, Vec<bool>), EvalError> {
    filter_physical_rows(
        ctx,
        vec_enabled,
        filters,
        input,
        selected,
        nulls,
        false,
        None,
    )
}

fn filter_physical_rows<C: Columns>(
    ctx: &C,
    vec_enabled: bool,
    filters: &[Expression],
    input: &Chunk,
    mut selected: Vec<bool>,
    mut nulls: Vec<bool>,
    row_based: bool,
    programs: Option<&[EvaluatorSuite]>,
) -> Result<(Vec<bool>, Vec<bool>), EvalError> {
    // `Chunk::num_rows` is selection-aware in Rust. Go's VecEvalBool instead
    // clears the input selection while evaluating and returns a mask sized to
    // all physical rows, then reapplies the original selection. Derive that
    // physical width without mutating the caller's chunk.
    let physical_rows = input.physical_rows();
    selected.clear();
    selected.resize(physical_rows, true);
    nulls.clear();
    nulls.resize(physical_rows, false);
    if filters.is_empty() {
        return Ok((selected, nulls));
    }

    // The demo has one execution path: every predicate enters the TiKV suite.
    // A missing context or declined shape is a structured error, never a reason
    // to execute the native scalar/vector kernels below this abstraction.
    let owned_programs = programs.is_none().then(|| {
        filters
            .iter()
            .map(|expr| EvaluatorSuite::new(vec![expr.clone()], true))
            .collect::<Vec<_>>()
    });
    let programs = programs
        .or(owned_programs.as_deref())
        .expect("owned programs exist when the caller supplied none");

    // Go falls back to rowBasedFilter when vectorization is disabled or any
    // filter is not vectorizable. Keep the same filter-major order and
    // three-valued truth handling in that branch.
    if row_based {
        for (position, filter) in filters.iter().enumerate() {
            let int_type = filter
                .static_type()
                .is_some_and(|ty| ty.eval_type() == tidb_datatype::EvalType::Int);
            for row_index in 0..physical_rows {
                if !selected[row_index] {
                    continue;
                }
                let value = eval_filter_row(&programs[position], ctx, input, row_index)?;
                let truth = crate::truthy_of(&value)?;
                if truth.is_none() && int_type {
                    nulls[row_index] = true;
                }
                selected[row_index] = truth == Some(true);
            }
        }
        return Ok((selected, nulls));
    }

    // Go `VecEvalBool`: `sel` is the live physical row set. Each filter runs
    // over it and removes the rows it rejects, so a later filter never
    // evaluates a row an earlier one dropped.
    let mut sel: Vec<usize> = (0..physical_rows).collect();
    // Go's `isZero`: -1 NULL, 0 false, 1 true, one entry per row of `sel`.
    let mut is_zero: Vec<i8> = Vec::new();
    let truth_code = |value: &Datum| -> Result<i8, EvalError> {
        Ok(match crate::truthy_of(value)? {
            None => -1,
            Some(false) => 0,
            Some(true) => 1,
        })
    };
    for (position, filter) in filters.iter().enumerate() {
        if sel.is_empty() {
            break;
        }
        is_zero.clear();
        if !vec_enabled || !vectorizable(std::slice::from_ref(filter)) {
            for &physical in &sel {
                is_zero.push(truth_code(&eval_filter_row(
                    &programs[position],
                    ctx,
                    input,
                    physical,
                )?)?);
            }
        } else {
            for value in programs[position]
                .eval_selected_for_cast(ctx, input, &sel)
                .map_err(into_eval_error)?
            {
                is_zero.push(truth_code(&value)?);
            }
        }
        let mut kept = 0;
        let eq_from_in = is_eq_cond_from_in(filter);
        for index in 0..sel.len() {
            let physical = sel[index];
            match is_zero[index] {
                -1 if !eq_from_in => continue,
                0 => {
                    nulls[physical] = false;
                    continue;
                }
                _ => {
                    if is_zero[index] == -1 {
                        nulls[physical] = true;
                    }
                    sel[kept] = physical;
                    kept += 1;
                }
            }
        }
        sel.truncate(kept);
    }
    selected.fill(false);
    for &physical in &sel {
        selected[physical] = !nulls[physical];
    }
    Ok((selected, nulls))
}

fn apply_input_selection(input: &Chunk, selected: &mut [bool]) {
    if let Some(sel) = input.sel() {
        let mut in_selection = vec![false; selected.len()];
        for &physical in sel {
            in_selection[physical] = true;
        }
        for (kept, present) in selected.iter_mut().zip(in_selection) {
            *kept &= present;
        }
    }
}

/// Go IsEQCondFromIn, without materializing a map of the matching columns.
fn is_eq_cond_from_in(expr: &Expression) -> bool {
    fn in_operand(expr: &Expression) -> bool {
        match expr {
            Expression::Column(column) => column.in_operand,
            Expression::ScalarFunction(function) => function.get_args().iter().any(in_operand),
            _ => false,
        }
    }
    matches!(expr, Expression::ScalarFunction(function)
        if function.func_name.lowercase() == "eq"
            && function.ret_type.as_ref().is_some_and(|ty| ty.eval_type() == tidb_datatype::EvalType::Int)
            && function.get_args().iter().any(in_operand))
}

/// Convenience form matching Go `VectorizedFilter` when the caller does not
/// need the per-row NULL mask.
pub fn vectorized_filter<C: Columns>(
    ctx: &C,
    vec_enabled: bool,
    filters: &[Expression],
    input: &Chunk,
    selected: Vec<bool>,
) -> Result<Vec<bool>, EvalError> {
    vectorized_filter_consider_null(ctx, vec_enabled, filters, input, selected, Vec::new())
        .map(|(selected, _)| selected)
}

/// A failure from [`EvaluatorSuite::run`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvaluatorError {
    /// A calculated expression failed.
    Eval(EvalError),
    /// The chunk ownership transfer rejected an invalid chunk state.
    Chunk(&'static str),
}

impl From<EvalError> for EvaluatorError {
    fn from(error: EvalError) -> Self {
        EvaluatorError::Eval(error)
    }
}

/// Unpacks [`EvaluatorError`] into the `EvalError` a caller-facing signature
/// wants, without an `impl From<EvaluatorError> for EvalError`.
///
/// The explicit conversion keeps caller-facing error boundaries visible and
/// avoids adding a blanket `Into<EvalError>` inference candidate.
pub fn into_eval_error(error: EvaluatorError) -> EvalError {
    match error {
        EvaluatorError::Eval(error) => error,
        EvaluatorError::Chunk(message) => EvalError::Unsupported(message),
    }
}

/// Evaluates one expression for **every row** of `input`, through the suite.
///
/// This is the seam for the per-row call sites outside projections: a row loop
/// that already holds its chunk should evaluate the expression once for the
/// whole chunk and index the result, which is what a projection does and what
/// the loop cannot do for itself. The suite executes admitted expressions only
/// in TiKV and returns a structured error for every decline; the returned vector
/// has one datum per input row.
///
/// Unlike [`eval_constant_row`] and [`eval_row_values`], this reads a chunk the
/// caller already built, so it costs no per-call chunk construction and works
/// for an expression that references only some of the chunk's columns. The
/// compiled program is still not cached: a loop that evaluates the same
/// expression for many chunks should retain an [`EvaluatorSuite`] and call its
/// [`EvaluatorSuite::eval_chunk`] method instead.
pub fn eval_chunk(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &Chunk,
) -> Result<Vec<Datum>, EvaluatorError> {
    EvaluatorSuite::new(vec![expression.clone()], true).eval_chunk(ctx, input)
}

/// Evaluates a **constant** expression through the suite, with no input
/// columns. Native scalar Datum kinds are preserved for subsequent SQL casts.
///
/// A row-at-a-time call site that has no input chunk -- the shape
/// `expression.eval(ctx, dual.get_row(0))` over an empty one-row chunk -- can be
/// re-pointed at the engine by calling this instead. The suite executes through
/// TiKV or returns its structured decline; it never invokes the native evaluator.
///
/// The compiled program is not cached: every call compiles the expression for
/// the caller's engine context. That is what makes this suitable for the
/// once-per-statement call sites (DDL partition values, defaults, pruning
/// bounds) and unsuitable for a per-row loop, which should move the evaluation
/// out of the loop instead.
pub fn eval_constant_row(
    expression: &Expression,
    ctx: &dyn Columns,
) -> Result<Datum, EvaluatorError> {
    let mut input = Chunk::new_empty(&[]);
    input.set_num_virtual_rows(1);
    eval_scalar_row(expression, ctx, &input)
}

/// Evaluates `expression` for **one row** whose column values are indexed by the
/// expression's own `Column::index`.
///
/// This is the row-at-a-time shape that has an input chunk
/// (`expression.eval(ctx, row)` where the row carries the expression's
/// dependencies). The suite runs TiKV for admitted expressions and returns a
/// structured decline otherwise.
///
/// Values retain their original positions, including unreferenced columns;
/// sparse references are valid when their indexes exist in `values`. Empty
/// values represent one virtual row, not zero rows. Native scalar Datum kinds
/// are preserved for subsequent SQL casts.
///
/// The optional return type is retained for compatibility, but success now
/// always returns `Some`, with or without the engine feature. Errors (including
/// out-of-bounds references and required-engine refusals) are returned directly;
/// this helper no longer asks callers to perform their own native fallback.
///
/// Like [`eval_constant_row`], the compiled program is not cached.
pub fn eval_row_values(
    expression: &Expression,
    ctx: &dyn Columns,
    values: &[Datum],
) -> Result<Option<Datum>, EvaluatorError> {
    if values.is_empty() {
        return eval_constant_row(expression, ctx).map(Some);
    }
    let input = tidb_chunk::mutrow::MutRow::from_datums(values);
    let row = input.to_row();
    let chunk = row
        .chunk()
        .ok_or(EvaluatorError::Chunk("missing scalar input row"))?;
    eval_scalar_row(expression, ctx, chunk).map(Some)
}

fn eval_scalar_row(
    expression: &Expression,
    ctx: &dyn Columns,
    input: &Chunk,
) -> Result<Datum, EvaluatorError> {
    EvaluatorSuite::new(vec![expression.clone()], true)
        .eval_selected_for_cast(ctx, input, &[0])?
        .pop()
        .ok_or(EvaluatorError::Chunk("scalar evaluation returned no value"))
}

pub struct EvaluatorProgram {
    calculated_output_indexes: Vec<usize>,
    calculated: Vec<Expression>,
    vectorizable: bool,
    column_mapping: HashMap<usize, Vec<usize>>,
    /// Compiled engine programs for this plan, shared by every worker that
    /// runs it. The compiled programs themselves are immutable and `Sync`; the
    /// lock covers only the compile-on-context-change step, so evaluation does
    /// not serialize the workers.
    #[cfg(feature = "tikv-expr")]
    tikv: std::sync::Mutex<crate::tikv::ProjectionCache>,
}

impl EvaluatorProgram {
    /// Compile the context-independent part of Go `NewEvaluatorSuite`.
    ///
    /// When `avoid_column_evaluator` is true, direct columns are calculated
    /// cell by cell like any other expression. Otherwise their resolved input
    /// indexes are grouped into one [`ColumnSwapHelper`].
    #[must_use]
    pub fn new(exprs: Vec<Expression>, avoid_column_evaluator: bool) -> Self {
        let mut calculated = Vec::with_capacity(exprs.len());
        let mut calculated_output_indexes = Vec::with_capacity(exprs.len());
        let mut column_mapping = HashMap::<usize, Vec<usize>>::new();

        for (output_index, expression) in exprs.into_iter().enumerate() {
            if !avoid_column_evaluator {
                if let Expression::Column(column) = &expression {
                    let input_index = usize::try_from(column.index)
                        .expect("projection column index must be resolved");
                    column_mapping
                        .entry(input_index)
                        .or_default()
                        .push(output_index);
                    continue;
                }
            }
            calculated_output_indexes.push(output_index);
            calculated.push(expression);
        }

        let vectorizable = vectorizable(&calculated);
        Self {
            calculated_output_indexes,
            calculated,
            vectorizable,
            column_mapping,
            #[cfg(feature = "tikv-expr")]
            tikv: std::sync::Mutex::new(crate::tikv::ProjectionCache::default()),
        }
    }

    /// How many times this plan has actually compiled an engine program.
    ///
    /// A shared plan compiles once per statement policy, not once per worker or
    /// per chunk; tests use this to prove the cache rather than assume it.
    #[cfg(feature = "tikv-expr")]
    #[must_use]
    pub fn tikv_compilations(&self) -> u64 {
        self.tikv.lock().map_or(0, |cache| cache.compilations())
    }
}

/// Go `EvaluatorSuite`: executes a projection program with an execution-local
/// column ownership cache. Calculated expressions finish before owner moves,
/// so an evaluation error cannot leave the input chunk half-consumed.
pub struct EvaluatorSuite {
    program: Arc<EvaluatorProgram>,
    column_swap_helper: Option<ColumnSwapHelper>,
}

impl EvaluatorSuite {
    /// Go `NewEvaluatorSuite`: compile and instantiate a fresh program.
    #[must_use]
    pub fn new(exprs: Vec<Expression>, avoid_column_evaluator: bool) -> Self {
        Self::from_program(Arc::new(EvaluatorProgram::new(
            exprs,
            avoid_column_evaluator,
        )))
    }

    /// Instantiate without cloning or reclassifying expression trees. Go's
    /// merged column mapping depends on the first input chunk of this execution.
    #[must_use]
    pub fn from_program(program: Arc<EvaluatorProgram>) -> Self {
        let column_swap_helper = (!program.column_mapping.is_empty())
            .then(|| ColumnSwapHelper::from_mapping(program.column_mapping.clone()));
        Self {
            program,
            column_swap_helper,
        }
    }

    /// Go `EvaluatorSuite.Vectorizable`.
    #[must_use]
    pub fn vectorizable(&self) -> bool {
        self.program.vectorizable
    }

    /// Go `EvaluatorSuite.Run`.
    ///
    /// Safe expressions are evaluated column by column. Expressions with
    /// order-sensitive side effects are evaluated in select-list order for
    /// each row. Both modes finish before the helper transfers the first
    /// direct-column owner.
    pub fn run<C: Columns>(
        &self,
        ctx: &C,
        input: &mut Chunk,
        output: &mut Chunk,
    ) -> Result<(), EvaluatorError> {
        self.evaluate_rows(ctx, input, output)?;
        if let Some(helper) = &self.column_swap_helper {
            helper
                .swap_columns(input, output)
                .map_err(EvaluatorError::Chunk)?;
        }
        Ok(())
    }

    /// [`run`](Self::run) for a caller that cannot hand over the input chunk.
    ///
    /// Only the direct-column *move* needs `&mut` input, and that move exists
    /// only for a program built with `avoid_column_evaluator = false`. This
    /// entry point is for the other kind -- one expression evaluated for its
    /// values, which is what [`eval_chunk`] needs -- and it never takes
    /// ownership of an input column, so the chunk is left exactly as it was.
    pub fn run_with_shared_input<C: Columns>(
        &self,
        ctx: &C,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<(), EvaluatorError> {
        if self.column_swap_helper.is_some() {
            return Err(EvaluatorError::Chunk(
                "a suite that moves direct columns needs a mutable input chunk",
            ));
        }
        self.evaluate_rows(ctx, input, output)
    }

    /// Evaluates this suite's single calculated expression for every input row.
    ///
    /// Unlike [`run`](Self::run), this keeps `input` shared: it is for a caller
    /// that needs values rather than a projection that may transfer a direct
    /// column owner. Reusing the suite reuses its immutable
    /// [`EvaluatorProgram`] and its compiled-engine cache across chunks.
    /// Suites with a direct-column ownership transfer, or with any output shape
    /// other than one calculated expression, are rejected rather than silently
    /// omitting an output.
    pub fn eval_chunk(
        &self,
        ctx: &dyn Columns,
        input: &Chunk,
    ) -> Result<Vec<Datum>, EvaluatorError> {
        self.eval_single_input(ctx, input, None, false)
    }

    /// Evaluate one calculated expression for explicit physical row indices.
    /// Ignores `input.sel()` without mutating/copying the input chunk. Repeats
    /// and reordering are preserved; the shared program retains its engine
    /// cache. Invalid indices are rejected before any expression is evaluated.
    pub fn eval_selected(
        &self,
        ctx: &dyn Columns,
        input: &Chunk,
        physical_rows: &[usize],
    ) -> Result<Vec<Datum>, EvaluatorError> {
        self.eval_single_input(ctx, input, Some(physical_rows), false)
    }

    /// Evaluate TiKV values for a subsequent SQL/table cast. Admission,
    /// structured declines and no-error-replay rules are identical to
    /// `eval_selected`; this is not additional engine type support.
    pub fn eval_selected_for_cast(
        &self,
        ctx: &dyn Columns,
        input: &Chunk,
        physical_rows: &[usize],
    ) -> Result<Vec<Datum>, EvaluatorError> {
        self.eval_single_input(ctx, input, Some(physical_rows), true)
    }

    fn eval_single_input(
        &self,
        ctx: &dyn Columns,
        input: &Chunk,
        selection: Option<&[usize]>,
        preserve_native_datums: bool,
    ) -> Result<Vec<Datum>, EvaluatorError> {
        if self.program.calculated.len() != 1
            || self.program.calculated_output_indexes.as_slice() != [0]
        {
            return Err(EvaluatorError::Eval(EvalError::Unsupported(
                "eval_chunk needs exactly one calculated expression",
            )));
        }
        let ty = self.program.calculated[0]
            .static_type()
            .cloned()
            .ok_or_else(|| {
                EvaluatorError::Eval(EvalError::Unsupported(
                    "an expression without a static type cannot be evaluated",
                ))
            })?;
        if self.column_swap_helper.is_some() {
            return Err(EvaluatorError::Chunk(
                "a suite that moves direct columns needs a mutable input chunk",
            ));
        }
        let rows = selection.map_or_else(|| input.num_rows(), <[usize]>::len);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&ty), rows);
        self.evaluate_rows_selected(ctx, input, &mut output, selection)?;
        let values: Vec<Datum> = (0..rows)
            .map(|row| output.get_row(row).get_datum(0, &ty))
            .collect();
        // The hybrid flag describes the scalar value exposed to the caller,
        // even when a materialized/engine column stores the ENUM/SET carrier.
        // Projection APIs keep their existing typed-column representation.
        if preserve_native_datums && ty.has_flag(tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT) {
            return Ok(values
                .into_iter()
                .map(|value| match (ty.code(), value) {
                    (FieldTypeCode::Enum, Datum::Enum(value, _)) => Datum::UInt(value.value()),
                    (FieldTypeCode::Set, Datum::Set(value, _)) => Datum::UInt(value.value()),
                    (_, value) => value,
                })
                .collect());
        }
        Ok(values)
    }

    /// The evaluation half of [`run`](Self::run): everything except the
    /// direct-column ownership transfer.
    fn evaluate_rows<C: Columns>(
        &self,
        ctx: &C,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<(), EvaluatorError> {
        self.evaluate_rows_selected(ctx, input, output, None)
    }

    fn evaluate_rows_selected(
        &self,
        ctx: &dyn Columns,
        input: &Chunk,
        output: &mut Chunk,
        selection: Option<&[usize]>,
    ) -> Result<(), EvaluatorError> {
        if let Some(selection) = selection {
            let physical_rows = input.physical_rows();
            if selection.iter().any(|&row| row >= physical_rows) {
                return Err(EvaluatorError::Chunk("physical selection is out of bounds"));
            }
        }
        // A direct-column-only projection moves columns in `run`; it has no
        // expression to evaluate and therefore needs neither engine nor native code.
        if self.program.calculated.is_empty() {
            return Ok(());
        }

        #[cfg(not(feature = "tikv-expr"))]
        {
            let _ = (ctx, input, output, selection);
            return Err(EvalError::Unsupported(
                "the engine-only expression demo requires the tikv-expr feature",
            )
            .into());
        }

        #[cfg(feature = "tikv-expr")]
        {
            let context =
                ctx.tikv_expression_context()
                    .ok_or_else(|| EvalError::ExternalEngine {
                        code: 1105,
                        message:
                            "the engine-only expression demo requires a TiKV expression context"
                                .to_owned(),
                    })?;
            let program = &self.program;
            if !program.vectorizable {
                ctx.record_tikv_expression_fallback(crate::tikv::FallbackReason::NotAdmitted);
                return Err(EvalError::ExternalEngine {
                    code: 1105,
                    message: "TiKV expression engine does not admit row-major programs".to_owned(),
                }
                .into());
            }

            let programs = {
                let mut cache = program.tikv.lock().map_err(|_| EvalError::ExternalEngine {
                    code: 1105,
                    message: "TiKV expression execution cache was poisoned".to_owned(),
                })?;
                cache.prepare(&program.calculated, &context)?
            };
            for (expression_index, output_index) in
                program.calculated_output_indexes.iter().enumerate()
            {
                let compiled = match programs.get(expression_index) {
                    Some(Ok(compiled)) => compiled,
                    Some(Err(reason)) => {
                        ctx.record_tikv_expression_fallback(*reason);
                        return Err(EvalError::ExternalEngine {
                            code: 1105,
                            message: format!(
                                "TiKV expression engine declined the expression: {reason:?}"
                            ),
                        }
                        .into());
                    }
                    None => {
                        let reason = crate::tikv::FallbackReason::NotAdmitted;
                        ctx.record_tikv_expression_fallback(reason);
                        return Err(EvalError::ExternalEngine {
                            code: 1105,
                            message: format!(
                                "TiKV expression engine declined the expression: {reason:?}"
                            ),
                        }
                        .into());
                    }
                };
                let declined = match selection {
                    Some(selected) => crate::tikv::evaluate_shared_selected(
                        compiled,
                        ctx,
                        input,
                        Some(selected),
                        output,
                        *output_index,
                    )?,
                    None => {
                        crate::tikv::evaluate_shared(compiled, ctx, input, output, *output_index)?
                    }
                };
                if let Some(reason) = declined {
                    ctx.record_tikv_expression_fallback(reason);
                    return Err(EvalError::ExternalEngine {
                        code: 1105,
                        message: format!(
                            "TiKV expression engine declined the expression: {reason:?}"
                        ),
                    }
                    .into());
                }
            }
            Ok(())
        }
    }
}
