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

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, Decimal, FieldType, FieldTypeCode};

    use crate::column::Column;
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::ScalarFunction;
    use crate::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn string() -> FieldType {
        FieldType::new(FieldTypeCode::VarString)
    }

    fn input_column(index: i64) -> Expression {
        let mut column = Column::new(index + 1, long());
        column.index = index;
        Expression::Column(column)
    }

    fn decimal_column(index: i64, field_type: &FieldType) -> Expression {
        let mut column = Column::new(index + 1, field_type.clone());
        column.index = index;
        Expression::Column(column)
    }

    fn int_const(value: i64) -> Expression {
        Expression::Constant(Constant::new(Datum::Int(value), long()))
    }

    fn string_const(value: &str) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Bytes(value.as_bytes().to_vec()),
            string(),
        ))
    }

    fn scalar(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), long(), args))
    }

    struct CountedParameter {
        value: Result<Datum, EvalError>,
        reads: Cell<usize>,
    }

    impl Columns for CountedParameter {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            assert_eq!(order, 0);
            self.reads.set(self.reads.get() + 1);
            self.value.clone()
        }
    }

    fn parameter(field_type: FieldType) -> Expression {
        let mut constant = Constant::new(Datum::Null, field_type);
        constant.param_marker = Some(ParamMarker { order: 0 });
        Expression::Constant(constant)
    }

    #[test]
    fn scalar_row_helpers_preserve_values_indexes_and_errors() {
        let reordered = scalar("minus", vec![input_column(1), input_column(0)]);
        assert_eq!(
            eval_row_values(&reordered, &NoColumns, &[Datum::Int(3), Datum::Int(10)]),
            Ok(Some(Datum::Int(7)))
        );
        assert_eq!(
            eval_row_values(
                &input_column(2),
                &NoColumns,
                &[Datum::Null, Datum::Int(9), Datum::Int(42)],
            ),
            Ok(Some(Datum::Int(42)))
        );
        assert!(eval_row_values(&input_column(2), &NoColumns, &[Datum::Int(42)]).is_err());
        assert_eq!(
            eval_row_values(&int_const(42), &NoColumns, &[]),
            Ok(Some(Datum::Int(42)))
        );
        let value = Datum::BinaryLiteral(vec![0x41].into());
        let binary = Expression::Constant(Constant::new(value.clone(), string()));
        assert_eq!(eval_constant_row(&binary, &NoColumns), Ok(value.clone()));
        assert_eq!(eval_row_values(&binary, &NoColumns, &[]), Ok(Some(value)));
        let error = EvalError::Unsupported("unbound prepared parameter");
        let ctx = CountedParameter {
            value: Err(error.clone()),
            reads: Cell::new(0),
        };
        assert_eq!(
            eval_constant_row(&parameter(long()), &ctx),
            Err(EvaluatorError::Eval(error.clone()))
        );
        assert_eq!(ctx.reads.get(), 1);
        assert_eq!(
            eval_row_values(&parameter(long()), &ctx, &[]),
            Err(EvaluatorError::Eval(error))
        );
        assert_eq!(ctx.reads.get(), 2);
    }

    #[test]
    fn vector_filter_reads_current_parameter_once_per_nonempty_chunk() {
        let filters = vec![parameter(long())];
        for value in [Datum::Null, Datum::Int(0), Datum::Int(1), Datum::Int(-1)] {
            for rows in [0, 1, 8] {
                let ctx = CountedParameter {
                    value: Ok(value.clone()),
                    reads: Cell::new(0),
                };
                let mut input = Chunk::new(&[], rows, rows.max(1));
                input.set_num_virtual_rows(rows);
                let (selected, nulls) = vectorized_filter_consider_null(
                    &ctx,
                    true,
                    &filters,
                    &input,
                    Vec::new(),
                    Vec::new(),
                )
                .unwrap();
                assert_eq!(
                    selected,
                    vec![crate::truthy_of(&value).unwrap() == Some(true); rows]
                );
                assert_eq!(nulls, vec![false; rows]);
                assert_eq!(ctx.reads.get(), usize::from(rows > 0));
            }
        }
    }

    #[test]
    fn constant_batch_reads_current_parameter_once_per_nonempty_chunk() {
        // Go Constant.VecEval* -> genVecFromConstExpr evaluates once, not
        // once per row. Reusing the suite must not freeze that execution.
        for (field_type, values) in [
            (long(), vec![Datum::Int(7), Datum::Null, Datum::Int(-9)]),
            (
                string(),
                vec![
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Bytes(vec![]),
                    Datum::Null,
                ],
            ),
        ] {
            let suite = EvaluatorSuite::new(vec![parameter(field_type.clone())], false);
            for value in values {
                for rows in [0, 1, 1024] {
                    let ctx = CountedParameter {
                        value: Ok(value.clone()),
                        reads: Cell::new(0),
                    };
                    let mut input = Chunk::new_with_capacity(&[], rows);
                    input.set_num_virtual_rows(rows);
                    let mut output =
                        Chunk::new_with_capacity(std::slice::from_ref(&field_type), rows);
                    suite.run(&ctx, &mut input, &mut output).unwrap();
                    assert_eq!(ctx.reads.get(), usize::from(rows != 0));
                    assert_eq!(output.num_rows(), rows);
                    for row in 0..rows {
                        let row = output.get_row(row);
                        match &value {
                            Datum::Null => assert!(row.is_null(0)),
                            Datum::Bytes(bytes) => assert_eq!(row.get_bytes(0), bytes.as_slice()),
                            _ => assert_eq!(row.get_datum(0, &field_type), value),
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn constant_batch_preserves_deferred_rows_and_side_effect_ordering() {
        // A deferred constant delegates to its expression in Go; its saved
        // value is not permission to broadcast the first input row.
        let mut deferred = Constant::new(Datum::Int(99), long());
        deferred.deferred_expr = Some(Box::new(input_column(0)));
        let suite = EvaluatorSuite::new(vec![Expression::Constant(deferred)], false);
        let mut input = Chunk::new_with_capacity(&[long()], 3);
        for value in [3, 7, 11] {
            input.append_int64(0, value);
        }
        let mut output = Chunk::new_with_capacity(&[long()], 3);
        suite.run(&NoColumns, &mut input, &mut output).unwrap();
        for (row, expected) in [3, 7, 11].into_iter().enumerate() {
            assert_eq!(output.get_row(row).get_int64(0), expected);
        }

        let suite = EvaluatorSuite::new(
            vec![
                parameter(long()),
                scalar("getvar_int", vec![string_const("v")]),
            ],
            false,
        );
        assert!(!suite.vectorizable());
        let ctx = CountedParameter {
            value: Ok(Datum::Int(8)),
            reads: Cell::new(0),
        };
        let mut output = Chunk::new_with_capacity(&[long(), long()], 3);
        suite.run(&ctx, &mut input, &mut output).unwrap();
        assert_eq!(ctx.reads.get(), 3);
        for row in 0..3 {
            assert_eq!(output.get_row(row).get_int64(0), 8);
            assert!(output.get_row(row).is_null(1));
        }
    }

    #[test]
    fn constant_batch_error_preserves_input_owners_and_skips_empty_input() {
        let suite = EvaluatorSuite::new(vec![input_column(0), parameter(long())], false);
        for rows in [0, 3] {
            let error = EvalError::Unsupported("unbound prepared parameter");
            let ctx = CountedParameter {
                value: Err(error.clone()),
                reads: Cell::new(0),
            };
            let mut input = Chunk::new_with_capacity(&[long()], rows);
            for _ in 0..rows {
                input.append_int64(0, 7);
            }
            let input_owner = input.column_handle(0);
            let mut output = Chunk::new_with_capacity(&[long(), long()], rows);
            let result = suite.run(&ctx, &mut input, &mut output);
            assert_eq!(ctx.reads.get(), usize::from(rows != 0));
            if rows == 0 {
                assert_eq!(result, Ok(()));
            } else {
                assert_eq!(result, Err(EvaluatorError::Eval(error)));
                assert!(input_owner.same_identity(&input.column_handle(0)));
                assert_eq!(input.num_rows(), rows);
            }
            assert_eq!(output.num_rows(), 0);
        }
    }

    #[test]
    fn shared_input_rejects_a_suite_that_moves_direct_columns() {
        let suite = EvaluatorSuite::new(vec![input_column(0)], false);
        let mut input = Chunk::new_with_capacity(&[long()], 1);
        input.append_int64(0, 7);
        let input_owner = input.column_handle(0);
        let mut output = Chunk::new_with_capacity(&[long()], 1);

        assert_eq!(
            suite.run_with_shared_input(&NoColumns, &input, &mut output),
            Err(EvaluatorError::Chunk(
                "a suite that moves direct columns needs a mutable input chunk"
            ))
        );
        assert!(input_owner.same_identity(&input.column_handle(0)));
        assert_eq!(input.num_rows(), 1);
        assert_eq!(output.num_rows(), 0);
    }

    #[derive(Default)]
    struct UserVariables(RefCell<HashMap<String, Datum>>);

    impl Columns for UserVariables {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn get_uservar(&self, name: &str) -> Option<Datum> {
            self.0.borrow().get(&name.to_ascii_lowercase()).cloned()
        }

        fn set_uservar(&self, name: &str, value: Datum) {
            self.0.borrow_mut().insert(name.to_ascii_lowercase(), value);
        }
    }

    #[test]
    fn user_variable_side_effects_follow_select_list_order_for_each_row() {
        let mut column = Column::new(1, string());
        column.index = 0;
        let setvar = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("setvar"),
            string(),
            vec![string_const("v"), Expression::Column(column)],
        ));
        let getvar = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("getvar_string"),
            string(),
            vec![string_const("v")],
        ));
        let suite = EvaluatorSuite::new(vec![setvar, getvar], false);
        assert!(!suite.vectorizable());

        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&string()), 3);
        input.append_string(0, "a");
        input.append_string(0, "b");
        input.append_string(0, "c");
        let mut output = Chunk::new_with_capacity(&[string(), string()], 3);

        suite
            .run(&UserVariables::default(), &mut input, &mut output)
            .unwrap();

        assert_eq!(output.num_rows(), 3);
        for (row_index, expected) in [b"a", b"b", b"c"].into_iter().enumerate() {
            let row = output.get_row(row_index);
            assert_eq!(row.get_bytes(0), expected);
            assert_eq!(row.get_bytes(1), expected);
        }
    }

    #[test]
    fn vectorizable_matches_user_variable_and_sequence_ordering_rules() {
        let nested_getvar = scalar(
            "plus",
            vec![scalar("getvar_int", vec![string_const("v")]), int_const(1)],
        );
        assert!(has_get_set_var_func(&nested_getvar));
        assert!(!vectorizable(&[nested_getvar]));
        assert!(!vectorizable(&[scalar("getvar", vec![])]));

        assert!(vectorizable(&[scalar("nextval", vec![])]));
        assert!(vectorizable(&[
            scalar("lastval", vec![]),
            scalar("setval", vec![]),
        ]));
        assert!(!vectorizable(&[
            scalar("nextval", vec![]),
            scalar("lastval", vec![]),
        ]));
        assert!(!vectorizable(&[
            scalar("nextval", vec![]),
            scalar("setval", vec![]),
        ]));
        assert!(!vectorizable(&[
            scalar("nextval", vec![]),
            scalar("nextval", vec![]),
        ]));

        let nested_nextval = scalar("plus", vec![scalar("nextval", vec![]), int_const(1)]);
        assert!(vectorizable(&[nested_nextval]));
    }

    #[test]
    fn vectorized_filter_preserves_selection_and_null_mask() {
        for incomplete in [false, true] {
            let mut input =
                Chunk::new_with_capacity(&if incomplete { vec![long()] } else { vec![] }, 5);
            input.set_incomplete_chunk(incomplete);
            input.set_num_virtual_rows(5);
            input.set_sel(Some(vec![4, 2]));
            for vectorized in [false, true] {
                let (selected, nulls) = vectorized_filter_consider_null(
                    &NoColumns,
                    vectorized,
                    &[int_const(1)],
                    &input,
                    Vec::new(),
                    Vec::new(),
                )
                .unwrap();
                assert_eq!(selected, [false, false, true, false, true]);
                assert_eq!(nulls, [false; 5]);
                assert_eq!(input.sel(), Some([4, 2].as_slice()));
            }
        }
        // Go TestVectorizedFilterConsiderNull: evaluate physical rows first,
        // then intersect the output with the original selection. Even an empty
        // selection must not suppress an expression error in a physical row.
        for vectorized in [false, true] {
            let mut input = Chunk::new_with_capacity(&[long()], 1);
            input.append_int64(0, 1);
            input.set_sel(Some(vec![]));
            let ctx = CountedParameter {
                value: Err(EvalError::ParamIndexExceedParamCounts),
                reads: Cell::new(0),
            };
            assert_eq!(
                vectorized_filter_consider_null(
                    &ctx,
                    vectorized,
                    &[parameter(long())],
                    &input,
                    Vec::new(),
                    Vec::new()
                ),
                Err(EvalError::ParamIndexExceedParamCounts)
            );
            assert_eq!(input.sel(), Some([].as_slice()));
        }
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&long()), 5);
        for value in [0, 1, 2, 3] {
            input.append_int64(0, value);
        }
        input.append_null(0);
        // Go evaluates physical rows, then intersects with this selection.
        input.set_sel(Some(vec![3, 2, 1]));
        let filter = scalar("gt", vec![input_column(0), int_const(1)]);
        let (selected, nulls) = vectorized_filter_consider_null(
            &NoColumns,
            true,
            &[filter],
            &input,
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(selected, vec![false, false, true, true, false]);
        assert_eq!(nulls, vec![false, false, false, false, false]);

        input.set_sel(Some(vec![3, 4, 1]));
        let filter = scalar("gt", vec![input_column(0), int_const(1)]);
        let (selected, nulls) = vectorized_filter_consider_null(
            &NoColumns,
            true,
            &[filter],
            &input,
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(selected, vec![false, false, false, true, false]);
        assert_eq!(nulls, vec![false; 5]);

        // Go VecEvalBool keeps an IN-derived equality's NULL alive until
        // later predicates run. False clears that NULL; true preserves it.
        let mut operand = Column::new(1, long());
        operand.index = 0;
        operand.in_operand = true;
        let eq = scalar("eq", vec![Expression::Column(operand), int_const(1)]);
        for tail in [0, 1] {
            let (selected, nulls) = vectorized_filter_consider_null(
                &NoColumns,
                true,
                &[eq.clone(), int_const(tail)],
                &input,
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
            assert_eq!(selected, vec![false, tail == 1, false, false, false]);
            assert_eq!(nulls, vec![false, false, false, false, tail == 1]);
        }
    }

    #[test]
    fn calculated_columns_finish_before_direct_owners_move() {
        let mut input = Chunk::new_with_capacity(&[long(), long()], 2);
        input.append_int64(0, 10);
        input.append_int64(1, 20);
        input.append_int64(0, 30);
        input.append_int64(1, 40);
        let original_input_owner = input.column_handle(0);

        let plus_one = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![input_column(1), int_const(1)],
        ));
        let suite = EvaluatorSuite::new(vec![input_column(0), plus_one, input_column(0)], false);
        let mut output = Chunk::new_with_capacity(&[long(), long(), long()], 2);

        suite.run(&NoColumns, &mut input, &mut output).unwrap();

        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.get_row(0).get_int64(0), 10);
        assert_eq!(output.get_row(0).get_int64(1), 21);
        assert_eq!(output.get_row(0).get_int64(2), 10);
        assert_eq!(output.get_row(1).get_int64(0), 30);
        assert_eq!(output.get_row(1).get_int64(1), 41);
        assert_eq!(output.get_row(1).get_int64(2), 30);
        assert!(output.columns_share_identity(0, &output, 2));
        assert!(original_input_owner.same_identity(&output.column_handle(0)));
        assert_eq!(input.num_rows(), 0);
    }

    #[test]
    fn expression_error_does_not_move_a_direct_column_owner() {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&long()), 1);
        input.append_int64(0, 7);
        let mut output = Chunk::new_with_capacity(&[long(), long()], 1);
        let input_before = input.column_handle(0);
        let output_before = output.column_handle(0);
        let unsupported = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("not_a_function"),
            long(),
            vec![],
        ));
        let suite = EvaluatorSuite::new(vec![input_column(0), unsupported], false);

        assert_eq!(
            suite.run(&NoColumns, &mut input, &mut output),
            Err(EvaluatorError::Eval(EvalError::Unsupported(
                "this scalar function is not yet ported"
            )))
        );

        assert!(input_before.same_identity(&input.column_handle(0)));
        assert!(output_before.same_identity(&output.column_handle(0)));
        assert!(!input_before.same_identity(&output.column_handle(0)));
        assert_eq!(input.get_row(0).get_int64(0), 7);
        assert_eq!(output.num_rows(), 0);
    }

    #[test]
    fn avoiding_column_evaluator_copies_without_transferring() {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&long()), 1);
        input.append_int64(0, 9);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&long()), 1);
        let input_before = input.column_handle(0);
        let suite = EvaluatorSuite::new(vec![input_column(0)], true);

        suite.run(&NoColumns, &mut input, &mut output).unwrap();

        assert_eq!(output.get_row(0).get_int64(0), 9);
        assert!(input_before.same_identity(&input.column_handle(0)));
        assert!(!input_before.same_identity(&output.column_handle(0)));
    }

    #[test]
    fn decimal_revenue_expression_uses_the_general_expression_evaluator() {
        let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
        decimal.set_flen(15);
        decimal.set_decimal(2);
        let one = Expression::Constant(Constant::new(Datum::Int(1), long()));
        let discount = decimal_column(1, &decimal);
        let discounted_fraction = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("minus"),
            decimal.clone(),
            vec![one, discount],
        ));
        let revenue = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("mul"),
            decimal.clone(),
            vec![decimal_column(0, &decimal), discounted_fraction],
        ));
        let suite = EvaluatorSuite::new(vec![revenue], false);

        let mut input = Chunk::new_with_capacity(&[decimal.clone(), decimal.clone()], 3);
        for (price, discount) in [("100.00", "0.10"), ("12.50", "0.20")] {
            input.append_datum(0, &Datum::Decimal(Decimal::from_literal(price)));
            input.append_datum(1, &Datum::Decimal(Decimal::from_literal(discount)));
        }
        input.append_null(0);
        input.append_datum(1, &Datum::Decimal(Decimal::from_literal("0.15")));
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&decimal), 3);

        suite.run(&NoColumns, &mut input, &mut output).unwrap();

        assert_eq!(
            output.get_row(0).get_datum(0, &decimal),
            Datum::Decimal(Decimal::from_literal("90.00"))
        );
        assert_eq!(
            output.get_row(1).get_datum(0, &decimal),
            Datum::Decimal(Decimal::from_literal("10.00"))
        );
        assert!(output.get_row(2).is_null(0));
    }

    fn unsigned_long() -> FieldType {
        let mut field_type = long();
        field_type.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        field_type
    }

    fn typed_input_column(index: i64, field_type: FieldType) -> Expression {
        let mut column = Column::new(index + 1, field_type);
        column.index = index;
        Expression::Column(column)
    }

    /// Go `builtinGTIntSig.vecEvalInt`: the integer comparison runs over the
    /// column cells, NULL when either side is NULL, and reads each side's
    /// signedness from its argument type (`VecCompareUI`: an unsigned value
    /// above `MaxInt64` is greater than any signed one).
    #[test]
    fn vector_filter_compares_integer_columns_column_wise() {
        let mut input = Chunk::new_with_capacity(&[long(), unsigned_long()], 4);
        for value in [1, 5, 7] {
            input.append_int64(0, value);
            input.append_uint64(1, u64::MAX);
        }
        input.append_null(0);
        input.append_null(1);
        let ctx = NoColumns;

        let filters = vec![scalar("gt", vec![input_column(0), int_const(4)])];
        let (selected, nulls) =
            vectorized_filter_consider_null(&ctx, true, &filters, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![false, true, true, false]);
        assert_eq!(nulls, vec![false; 4]);

        // The same bits read through a signed type are -1, through an
        // unsigned type 18446744073709551615.
        let unsigned = vec![scalar(
            "gt",
            vec![typed_input_column(1, unsigned_long()), int_const(-1)],
        )];
        let (selected, _) =
            vectorized_filter_consider_null(&ctx, true, &unsigned, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![true, true, true, false]);
        let signed = vec![scalar(
            "gt",
            vec![typed_input_column(1, long()), int_const(-1)],
        )];
        let (selected, _) =
            vectorized_filter_consider_null(&ctx, true, &signed, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![false, false, false, false]);

        // A second filter sees only the rows the first kept, and an input
        // selection is honored.
        let both = vec![
            scalar("gt", vec![input_column(0), int_const(4)]),
            scalar("lt", vec![input_column(0), int_const(7)]),
        ];
        input.set_sel(Some(vec![0, 2, 3]));
        let (selected, _) =
            vectorized_filter_consider_null(&ctx, true, &both, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![false, false, false, false]);
        input.set_sel(Some(vec![1, 3]));
        let (selected, _) =
            vectorized_filter_consider_null(&ctx, true, &both, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![false, true, false, false]);
    }

    #[test]
    fn filter_scalar_truth_keeps_binary_literal_kind() {
        struct Context(bool);
        impl Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            #[cfg(feature = "tikv-expr")]
            fn tikv_expression_context(&self) -> Option<crate::tikv::Context> {
                self.0.then(crate::tikv::Context::default)
            }
        }
        let mut input = Chunk::new_with_capacity(&[], 2);
        input.set_num_virtual_rows(2);
        input.set_sel(Some(vec![1]));
        let filter = FilterProgram::new(vec![Expression::Constant(Constant::new(
            Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![16])),
            string(),
        ))]);
        for engine in [false, true] {
            let ctx = Context(engine);
            assert!(filter.matches_row(&ctx, input.physical_row(0)).unwrap());
            for vectorized in [false, true] {
                let (selected, _) = filter
                    .consider_null(&ctx, vectorized, &input, vec![], vec![])
                    .unwrap();
                assert_eq!(selected, vec![false, true]);
            }
        }
    }

    #[test]
    fn late_cast_values_apply_hybrid_numeric_flag() {
        for code in [FieldTypeCode::Enum, FieldTypeCode::Set] {
            let mut ty = FieldType::new(code).with_elems(["a", "b"]);
            ty.add_flags(tidb_datatype::FieldTypeFlags::ENUM_SET_AS_INT);
            let (value, numeric) = if code == FieldTypeCode::Enum {
                (
                    Datum::Enum(tidb_datatype::MysqlEnum::new("b", 2), ty.collation()),
                    2,
                )
            } else {
                (
                    Datum::Set(tidb_datatype::MysqlSet::new("a,b", 3), ty.collation()),
                    3,
                )
            };
            let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 1);
            input.append_datum(0, &value);
            let mut column = crate::column::Column::new(1, ty.clone());
            column.index = 0;
            for expr in [
                Expression::Constant(Constant::new(value, ty)),
                Expression::Column(column),
            ] {
                let suite = EvaluatorSuite::new(vec![expr], true);
                assert_eq!(
                    suite
                        .eval_selected_for_cast(&crate::NoColumns, &input, &[0])
                        .unwrap(),
                    vec![Datum::UInt(numeric)]
                );
                #[cfg(feature = "tikv-expr")]
                {
                    struct Engine;
                    impl Columns for Engine {
                        fn get(&self, _: &[String]) -> Option<Datum> {
                            None
                        }
                        fn tikv_expression_context(&self) -> Option<crate::tikv::Context> {
                            Some(crate::tikv::Context::default())
                        }
                    }
                    assert_eq!(
                        suite.eval_selected_for_cast(&Engine, &input, &[0]).unwrap(),
                        vec![Datum::UInt(numeric)]
                    );
                }
            }
        }
    }

    #[test]
    fn late_cast_values_keep_native_datum_kinds_without_changing_projection() {
        let value = Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![0, 16]));
        let literal = Expression::Constant(Constant::new(value.clone(), string()));
        let mut deferred = Constant::new(Datum::Null, string());
        deferred.deferred_expr = Some(Box::new(literal.clone()));
        let mut input = Chunk::new_with_capacity(&[], 2);
        input.set_num_virtual_rows(2);
        for expression in [literal, Expression::Constant(deferred)] {
            let expected = expression
                .eval(&crate::NoColumns, tidb_chunk::row::Row::empty())
                .unwrap();
            let suite = EvaluatorSuite::new(vec![expression], true);
            assert_eq!(
                suite
                    .eval_selected_for_cast(&crate::NoColumns, &input, &[1, 0, 1])
                    .unwrap(),
                vec![expected.clone(); 3]
            );
            let typed = suite
                .eval_selected(&crate::NoColumns, &input, &[0])
                .unwrap();
            assert!(!matches!(&typed[0], Datum::BinaryLiteral(_)));
            assert_eq!(typed[0].as_raw_bytes(), Some([0, 16].as_slice()));
            #[cfg(feature = "tikv-expr")]
            {
                struct EngineContext {
                    required: bool,
                    fallbacks: Cell<usize>,
                }
                impl Columns for EngineContext {
                    fn get(&self, _: &[String]) -> Option<Datum> {
                        None
                    }
                    fn tikv_expression_context(&self) -> Option<crate::tikv::Context> {
                        Some(crate::tikv::Context::default())
                    }
                    fn tikv_expression_required(&self) -> bool {
                        self.required
                    }
                    fn record_tikv_expression_fallback(&self, _: crate::tikv::FallbackReason) {
                        self.fallbacks.set(self.fallbacks.get() + 1);
                    }
                }
                for required in [false, true] {
                    let ctx = EngineContext {
                        required,
                        fallbacks: Cell::new(0),
                    };
                    let result = suite.eval_selected_for_cast(&ctx, &input, &[0]);
                    assert_eq!(ctx.fallbacks.get(), 1);
                    if required {
                        assert!(matches!(
                            result,
                            Err(EvaluatorError::Eval(EvalError::ExternalEngine {
                                code: 1105,
                                ..
                            }))
                        ));
                    } else {
                        assert_eq!(result.unwrap(), vec![expected.clone()]);
                    }
                }
            }
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn retained_filters_execute_live_physical_rows_and_cache_programs() {
        struct EngineContext {
            rows: Cell<usize>,
        }
        impl Columns for EngineContext {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn tikv_expression_context(&self) -> Option<crate::tikv::Context> {
                Some(crate::tikv::Context::default())
            }
            fn tikv_expression_required(&self) -> bool {
                true
            }
            fn record_tikv_expression_rows(&self, rows: usize) {
                self.rows.set(self.rows.get() + rows);
            }
        }
        let ctx = EngineContext { rows: Cell::new(0) };
        let filters = FilterProgram::new(vec![
            scalar("lt", vec![input_column(0), int_const(10)]),
            scalar("plus", vec![input_column(0), int_const(1)]),
        ]);
        let mut input = Chunk::new_with_capacity(&[long()], 3);
        for value in [0, 2, i64::MAX] {
            input.append_int64(0, value);
        }
        input.set_sel(Some(vec![1]));
        for run in 1..=2 {
            let (selected, nulls) = filters
                .consider_null(&ctx, true, &input, vec![], vec![])
                .unwrap();
            assert_eq!(selected, vec![false, true, false]);
            assert_eq!(nulls, vec![false; 3]);
            // Evaluate all physical rows before intersecting the input Sel,
            // but never evaluate PLUS on the row rejected by LT.
            assert_eq!(ctx.rows.get(), run * 5);
            assert_eq!(filters.tikv_compilations(), 2);
        }
        assert_eq!(input.sel(), Some([1].as_slice()));
        assert!(filters.matches_row(&ctx, input.physical_row(0)).unwrap());
        assert_eq!(ctx.rows.get(), 12);
        assert_eq!(filters.tikv_compilations(), 2);

        // Ordinary NULL ends demand; NULL from an IN-rewritten equality is
        // carried until a later false, exactly as in the existing vector API.
        let mut in_col = input_column(0);
        if let Expression::Column(col) = &mut in_col {
            col.in_operand = true;
        }
        let null_eq = scalar("eq", vec![in_col, int_const(0)]);
        let mut null_input = Chunk::new_with_capacity(&[long()], 1);
        null_input.append_null(0);
        let cnf = FilterProgram::new(vec![null_eq, int_const(0)]);
        assert_eq!(
            cnf.consider_null(&ctx, true, &null_input, vec![], vec![])
                .unwrap(),
            (vec![false], vec![false])
        );
        let before = ctx.rows.get();
        assert!(!cnf.matches_row(&ctx, null_input.physical_row(0)).unwrap());
        assert_eq!(ctx.rows.get(), before + 1);
        let null_first = FilterProgram::new(vec![
            input_column(0),
            scalar("plus", vec![int_const(i64::MAX), int_const(1)]),
        ]);
        assert_eq!(
            null_first
                .consider_null(&ctx, true, &null_input, vec![], vec![])
                .unwrap()
                .0,
            vec![false]
        );
        assert_eq!(null_first.tikv_compilations(), 1);
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn filtering_must_not_bypass_required_engine() {
        struct RequiredContext(Cell<usize>);
        impl Columns for RequiredContext {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn tikv_expression_context(&self) -> Option<crate::tikv::Context> {
                Some(crate::tikv::Context::default())
            }
            fn tikv_expression_required(&self) -> bool {
                true
            }
            fn get_uservar(&self, _: &str) -> Option<Datum> {
                self.0.set(self.0.get() + 1);
                Some(Datum::Bytes(b"1".to_vec()))
            }
        }
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("getvar"),
            string(),
            vec![string_const("x")],
        ));
        let mut input = Chunk::new_with_capacity(&[], 1);
        input.set_num_virtual_rows(1);
        for vectorized in [false, true] {
            let ctx = RequiredContext(Cell::new(0));
            assert!(matches!(
                vectorized_filter_consider_null(
                    &ctx,
                    vectorized,
                    std::slice::from_ref(&filter),
                    &input,
                    vec![],
                    vec![],
                ),
                Err(EvalError::ExternalEngine { code: 1105, .. })
            ));
            assert_eq!(ctx.0.get(), 0, "native side effect must not execute");
            assert!(matches!(
                vec_eval_bool(
                    &ctx,
                    vectorized,
                    std::slice::from_ref(&filter),
                    &input,
                    vec![],
                    vec![],
                ),
                Err(EvalError::ExternalEngine { code: 1105, .. })
            ));
            assert_eq!(ctx.0.get(), 0);
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn required_engine_rejects_row_major_native_dispatch() {
        struct RequiredContext {
            reads: Cell<usize>,
            fallbacks: Cell<usize>,
        }
        impl Columns for RequiredContext {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn tikv_expression_context(&self) -> Option<crate::tikv::Context> {
                Some(crate::tikv::Context::default())
            }
            fn tikv_expression_required(&self) -> bool {
                true
            }
            fn record_tikv_expression_fallback(&self, reason: crate::tikv::FallbackReason) {
                assert_eq!(reason, crate::tikv::FallbackReason::NotAdmitted);
                self.fallbacks.set(self.fallbacks.get() + 1);
            }
            fn get_uservar(&self, _: &str) -> Option<Datum> {
                self.reads.set(self.reads.get() + 1);
                Some(Datum::Bytes(b"value".to_vec()))
            }
        }
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("getvar"),
            string(),
            vec![string_const("x")],
        ));
        let suite = EvaluatorSuite::new(vec![expression], true);
        assert!(!suite.vectorizable());
        let mut input = Chunk::new_with_capacity(&[], 1);
        input.set_num_virtual_rows(1);
        for selected in [false, true] {
            let ctx = RequiredContext {
                reads: Cell::new(0),
                fallbacks: Cell::new(0),
            };
            let result = if selected {
                suite.eval_selected(&ctx, &input, &[0])
            } else {
                suite.eval_chunk(&ctx, &input)
            };
            assert!(matches!(
                result,
                Err(EvaluatorError::Eval(EvalError::ExternalEngine { .. }))
            ));
            assert_eq!(
                ctx.reads.get(),
                0,
                "required engine must never invoke native side effects"
            );
            assert_eq!(ctx.fallbacks.get(), 1);
        }
    }

    #[test]
    fn selected_decimal_suite_does_not_reapply_chunk_selection() {
        let mut ty = FieldType::new(FieldTypeCode::NewDecimal);
        ty.set_flen(20);
        ty.set_decimal(2);
        let expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            ty.clone(),
            vec![decimal_column(0, &ty), decimal_column(0, &ty)],
        ));
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&ty), 3);
        for value in ["1.25", "7.50"] {
            input.append_my_decimal(0, &Decimal::from_literal(value).to_my_decimal().unwrap());
        }
        input.append_null(0);
        input.set_sel(Some(vec![1]));
        let selected = [2, 0, 2];
        let expected: Vec<_> = selected
            .iter()
            .map(|&row| expr.eval(&NoColumns, input.physical_row(row)).unwrap())
            .collect();
        let suite = EvaluatorSuite::new(vec![expr], true);
        assert_eq!(
            suite.eval_selected(&NoColumns, &input, &selected).unwrap(),
            expected
        );
        assert_eq!(input.sel(), Some(&[1][..]));
        assert!(suite.eval_selected(&NoColumns, &input, &[3]).is_err());
    }

    /// Go `builtinArithmetic*DecimalSig.vecEvalDecimal`: the projection's
    /// column-wise decimal arithmetic appends exactly the cells the row
    /// evaluator appends, for nested `+`/`-`/`*` over decimal and integer
    /// columns and constants, NULLs included.
    #[test]
    fn projection_decimal_arithmetic_matches_the_row_evaluator_cell_for_cell() {
        let mut price_type = FieldType::new(FieldTypeCode::NewDecimal);
        price_type.set_flen(15);
        price_type.set_decimal(2);
        let mut result_type = FieldType::new(FieldTypeCode::NewDecimal);
        result_type.set_flen(21);
        result_type.set_decimal(4);
        let mut input =
            Chunk::new_with_capacity(&[price_type.clone(), price_type.clone(), long()], 8);
        let rows: [(Option<&str>, Option<&str>, Option<i64>); 8] = [
            (Some("36901.00"), Some("0.04"), Some(17)),
            (Some("-9999999.99"), Some("1.00"), Some(-3)),
            (Some("0.01"), Some("0.99"), Some(0)),
            (Some("999999999.99"), Some("-0.10"), Some(1_000_000_000)),
            (None, Some("0.05"), Some(1)),
            (Some("12.34"), None, Some(2)),
            (Some("0.00"), Some("0.00"), None),
            (Some("123456789012.34"), Some("0.30"), Some(7)),
        ];
        for (price, discount, quantity) in rows {
            match price {
                Some(text) => input
                    .append_my_decimal(0, &Decimal::from_literal(text).to_my_decimal().unwrap()),
                None => input.append_null(0),
            }
            match discount {
                Some(text) => input
                    .append_my_decimal(1, &Decimal::from_literal(text).to_my_decimal().unwrap()),
                None => input.append_null(1),
            }
            match quantity {
                Some(value) => input.append_int64(2, value),
                None => input.append_null(2),
            }
        }
        let typed = |name: &str, args: Vec<Expression>, ty: &FieldType| {
            Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty.clone(), args))
        };
        let revenue = typed(
            "mul",
            vec![
                decimal_column(0, &price_type),
                typed(
                    "minus",
                    vec![int_const(1), decimal_column(1, &price_type)],
                    &price_type,
                ),
            ],
            &result_type,
        );
        let expressions = vec![
            revenue.clone(),
            typed(
                "minus",
                vec![
                    revenue,
                    typed(
                        "mul",
                        vec![decimal_column(1, &price_type), input_column(2)],
                        &price_type,
                    ),
                ],
                &result_type,
            ),
            typed(
                "plus",
                vec![decimal_column(0, &price_type), int_const(-5)],
                &price_type,
            ),
            typed(
                "mul",
                vec![input_column(2), decimal_column(0, &price_type)],
                &result_type,
            ),
        ];
        let ctx = NoColumns;
        for expression in &expressions {
            let Expression::ScalarFunction(function) = expression else {
                unreachable!()
            };
            let mut expected = Chunk::new_with_capacity(
                std::slice::from_ref(function.get_static_type().unwrap()),
                8,
            );
            for row in 0..input.num_rows() {
                expected.append_datum(0, &expression.eval(&ctx, input.get_row(row)).unwrap());
            }
            let mut output = Chunk::new_with_capacity(
                std::slice::from_ref(function.get_static_type().unwrap()),
                8,
            );
            assert!(function
                .vec_eval_decimal_arithmetic(&input, &mut output, 0)
                .unwrap());
            assert_eq!(output, expected);
            // A selection on the input is honored.
            input.set_sel(Some(vec![1, 3, 6]));
            let mut expected = Chunk::new_with_capacity(
                std::slice::from_ref(function.get_static_type().unwrap()),
                3,
            );
            for row in 0..input.num_rows() {
                expected.append_datum(0, &expression.eval(&ctx, input.get_row(row)).unwrap());
            }
            let mut output = Chunk::new_with_capacity(
                std::slice::from_ref(function.get_static_type().unwrap()),
                3,
            );
            assert!(function
                .vec_eval_decimal_arithmetic(&input, &mut output, 0)
                .unwrap());
            assert_eq!(output, expected);
            input.set_sel(None);
        }
        // Go `builtinCastIntAsDecimalSig.vecEvalDecimal`: an integer column
        // cast to a DECIMAL wide enough for every integer is the padded
        // integer, at scale 0 and at a positive scale.
        let mut wide = FieldType::new(FieldTypeCode::NewDecimal);
        wide.set_flen(20);
        wide.set_decimal(0);
        let mut wide_scaled = FieldType::new(FieldTypeCode::NewDecimal);
        wide_scaled.set_flen(22);
        wide_scaled.set_decimal(2);
        for target in [&wide, &wide_scaled] {
            let product = typed(
                "mul",
                vec![
                    decimal_column(0, &price_type),
                    typed("cast_decimal", vec![input_column(2)], target),
                ],
                &result_type,
            );
            let Expression::ScalarFunction(function) = &product else {
                unreachable!()
            };
            let mut expected = Chunk::new_with_capacity(
                std::slice::from_ref(function.get_static_type().unwrap()),
                8,
            );
            for row in 0..input.num_rows() {
                expected.append_datum(0, &product.eval(&ctx, input.get_row(row)).unwrap());
            }
            let mut output = Chunk::new_with_capacity(
                std::slice::from_ref(function.get_static_type().unwrap()),
                8,
            );
            assert!(function
                .vec_eval_decimal_arithmetic(&input, &mut output, 0)
                .unwrap());
            assert_eq!(output, expected);
        }
        // A target that can clamp (`ProduceDecWithSpecifiedTp`) keeps the
        // row path.
        let mut narrow = FieldType::new(FieldTypeCode::NewDecimal);
        narrow.set_flen(10);
        narrow.set_decimal(0);
        let Expression::ScalarFunction(clamping) = typed(
            "mul",
            vec![
                decimal_column(0, &price_type),
                typed("cast_decimal", vec![input_column(2)], &narrow),
            ],
            &result_type,
        ) else {
            unreachable!()
        };
        let mut output = Chunk::new_with_capacity(&[result_type.clone()], 8);
        assert!(!clamping
            .vec_eval_decimal_arithmetic(&input, &mut output, 0)
            .unwrap());
        assert_eq!(output.num_rows(), 0);
        // Two integers stay integer arithmetic: not covered.
        let Expression::ScalarFunction(ints) =
            typed("plus", vec![input_column(2), int_const(1)], &result_type)
        else {
            unreachable!()
        };
        let mut output = Chunk::new_with_capacity(&[result_type.clone()], 8);
        assert!(!ints
            .vec_eval_decimal_arithmetic(&input, &mut output, 0)
            .unwrap());
        assert_eq!(output.num_rows(), 0);
    }

    /// Go `builtinGTDecimalSig.vecEvalInt`: a decimal column against an
    /// integer or decimal constant, and an integer column against a decimal
    /// constant, compare exactly in the decimal domain.
    #[test]
    fn vector_filter_compares_decimals_column_wise() {
        let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_type.set_flen(15);
        decimal_type.set_decimal(2);
        let mut input = Chunk::new_with_capacity(&[decimal_type.clone(), long()], 4);
        for (value, int_value) in [("313.99", 313), ("314.00", 314), ("314.01", 315)] {
            input.append_my_decimal(0, &Decimal::from_literal(value).to_my_decimal().unwrap());
            input.append_int64(1, int_value);
        }
        input.append_null(0);
        input.append_null(1);
        let ctx = NoColumns;
        let decimal_const = |text: &str| {
            Expression::Constant(Constant::new(
                Datum::Decimal(Decimal::from_literal(text)),
                decimal_type.clone(),
            ))
        };

        let cases: [(&str, Expression, Expression, [bool; 4]); 5] = [
            (
                "gt",
                decimal_column(0, &decimal_type),
                int_const(314),
                [false, false, true, false],
            ),
            (
                "ge",
                decimal_column(0, &decimal_type),
                int_const(314),
                [false, true, true, false],
            ),
            (
                "lt",
                decimal_column(0, &decimal_type),
                decimal_const("314.005"),
                [true, true, false, false],
            ),
            (
                "eq",
                input_column(1),
                decimal_const("314.00"),
                [false, true, false, false],
            ),
            (
                "ne",
                decimal_const("314.00"),
                decimal_column(0, &decimal_type),
                [true, false, true, false],
            ),
        ];
        for (name, lhs, rhs, expected) in cases {
            let filters = vec![scalar(name, vec![lhs, rhs])];
            let (selected, nulls) = vectorized_filter_consider_null(
                &ctx,
                true,
                &filters,
                &input,
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
            assert_eq!(selected, expected, "{name}");
            assert_eq!(nulls, vec![false; 4], "{name}");
        }
    }

    /// Go `builtinUnaryNotIntSig.vecEvalInt` and `builtin*IsNullSig.vecEvalInt`:
    /// `NOT` over a covered node and `IS NULL` over a column run column-wise
    /// and select exactly the rows the row evaluator selects, NULL rows
    /// included; a `NOT` over an uncovered node keeps the row evaluator.
    #[test]
    fn vector_filter_evaluates_not_and_isnull_column_wise() {
        let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_type.set_flen(15);
        decimal_type.set_decimal(2);
        let mut input = Chunk::new_with_capacity(&[decimal_type.clone(), long()], 5);
        for (value, int_value) in [
            (Some("0.00"), Some(0)),
            (None, Some(5)),
            (Some("2.50"), None),
            (None, None),
            (Some("7.00"), Some(7)),
        ] {
            match value {
                Some(text) => input
                    .append_my_decimal(0, &Decimal::from_literal(text).to_my_decimal().unwrap()),
                None => input.append_null(0),
            }
            match int_value {
                Some(int_value) => input.append_int64(1, int_value),
                None => input.append_null(1),
            }
        }
        let ctx = NoColumns;
        let cases: [(&str, Expression, [bool; 5], [bool; 5]); 4] = [
            (
                "isnull",
                scalar("isnull", vec![decimal_column(0, &decimal_type)]),
                [false, true, false, true, false],
                [false; 5],
            ),
            (
                "not isnull",
                scalar(
                    "not",
                    vec![scalar("isnull", vec![decimal_column(0, &decimal_type)])],
                ),
                [true, false, true, false, true],
                [false; 5],
            ),
            (
                "not gt",
                scalar(
                    "not",
                    vec![scalar("gt", vec![input_column(1), int_const(4)])],
                ),
                [true, false, false, false, false],
                [false, false, true, true, false],
            ),
            (
                "not column",
                scalar("not", vec![input_column(1)]),
                [true, false, false, false, false],
                [false, false, true, true, false],
            ),
        ];
        for (name, filter, expected_selected, expected_nulls) in cases {
            let filters = vec![filter];
            let vectorized = vectorized_filter_consider_null(
                &ctx,
                true,
                &filters,
                &input,
                Vec::new(),
                Vec::new(),
            );
            if name.contains("isnull") {
                assert!(
                    matches!(vectorized, Err(EvalError::Unsupported(_))),
                    "{name}"
                );
                continue;
            }
            let (selected, nulls) = vectorized.unwrap();
            assert_eq!(selected, expected_selected, "{name}");
            assert_eq!(nulls, [false; 5], "{name}");
            let (row_selected, row_nulls) = vectorized_filter_consider_null(
                &ctx,
                false,
                &filters,
                &input,
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
            assert_eq!(selected, row_selected, "{name} against the row evaluator");
            // Go rowBasedFilter records EvalInt NULL directly; VecEvalBool
            // exposes NULL only for equality rewritten from IN.
            assert_eq!(row_nulls, expected_nulls, "{name} row evaluator");
        }
    }
}
