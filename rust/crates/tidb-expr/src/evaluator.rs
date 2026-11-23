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

use std::sync::Arc;
use std::{cell::RefCell, collections::HashMap};

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_util::ColumnSwapHelper;
use tidb_datatype::Datum;

use crate::context::{Columns, EvalError};
use crate::expression::Expression;
use crate::rpn::PreparedExpression;

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

pub(crate) fn has_mutable_effects(expression: &Expression) -> bool {
    contains_effect(expression, &|function| function.requires_row_order())
}

pub(crate) fn has_scalar_effects(expression: &Expression) -> bool {
    contains_effect(expression, &|function| {
        // Seeded streams belong to the program. Preserve the established
        // per-mode demand order when NULLIF shares a seeded occurrence.
        function.requires_row_order()
            && (function.func_name.lowercase() != "rand" || function.args.is_empty())
    })
}

fn contains_effect(
    expression: &Expression,
    matches: &impl Fn(&crate::scalar_function::ScalarFunction) -> bool,
) -> bool {
    match expression {
        Expression::ScalarFunction(function) => {
            matches(function)
                || function
                    .args
                    .iter()
                    .any(|arg| contains_effect(arg, matches))
        }
        Expression::Constant(constant) => constant
            .deferred_expr
            .as_deref()
            .is_some_and(|arg| contains_effect(arg, matches)),
        Expression::Column(_) | Expression::CorrelatedColumn(_) => false,
    }
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

/// Filter definitions and their execution-local programs. One instance
/// evaluates the same predicates across every batch of a Selection or
/// residual filter and starts again when that execution reopens.
pub struct FilterExecution {
    predicates: Arc<[Expression]>,
    prepared: RefCell<Vec<Option<Option<PreparedExpression>>>>,
}

impl FilterExecution {
    /// Bind the predicates of one filter execution.
    #[must_use]
    pub fn new(predicates: Vec<Expression>) -> Self {
        Self {
            prepared: RefCell::new(empty_filter_programs(predicates.len())),
            predicates: predicates.into(),
        }
    }

    /// Open another execution over the same immutable predicates.
    #[must_use]
    pub fn start_execution(&self) -> Self {
        Self {
            predicates: Arc::clone(&self.predicates),
            prepared: RefCell::new(empty_filter_programs(self.predicates.len())),
        }
    }

    /// Restore fresh execution state when reopening the same filter.
    pub fn reset(&mut self) {
        for program in self.prepared.get_mut().iter_mut().flatten().flatten() {
            program.reset_state();
        }
    }

    /// Borrow the bound predicates.
    #[must_use]
    pub fn predicates(&self) -> &[Expression] {
        &self.predicates
    }

    /// Match Go's filtered physical mask and intersect the chunk's selection.
    pub fn filter<C: Columns>(
        &self,
        ctx: &C,
        vec_enabled: bool,
        input: &Chunk,
        selected: Vec<bool>,
        nulls: Vec<bool>,
    ) -> Result<(Vec<bool>, Vec<bool>), EvalError> {
        let (mut selected, nulls) = self.filter_physical(
            ctx,
            vec_enabled,
            input,
            selected,
            nulls,
            !vec_enabled || !vectorizable(&self.predicates),
        )?;
        apply_input_selection(input, &mut selected);
        Ok((selected, nulls))
    }

    /// Match Go's three-valued physical mask before applying any selection.
    pub fn vec_eval_bool<C: Columns>(
        &self,
        ctx: &C,
        vec_enabled: bool,
        input: &Chunk,
        selected: Vec<bool>,
        nulls: Vec<bool>,
    ) -> Result<(Vec<bool>, Vec<bool>), EvalError> {
        self.filter_physical(ctx, vec_enabled, input, selected, nulls, false)
    }

    fn filter_physical<C: Columns>(
        &self,
        ctx: &C,
        vec_enabled: bool,
        input: &Chunk,
        selected: Vec<bool>,
        nulls: Vec<bool>,
        row_based: bool,
    ) -> Result<(Vec<bool>, Vec<bool>), EvalError> {
        filter_physical_rows(
            ctx,
            vec_enabled,
            &self.predicates,
            &mut self.prepared.borrow_mut(),
            input,
            selected,
            nulls,
            row_based,
        )
    }

    /// Evaluate a single predicate when the caller must process rows in order.
    pub fn predicate_on_row(
        &self,
        ctx: &dyn Columns,
        predicate: usize,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<Datum, EvalError> {
        let expression = &self.predicates[predicate];
        evaluate_prepared_row(
            expression,
            &mut self.prepared.borrow_mut()[predicate],
            ctx,
            row,
        )
    }
}

fn empty_filter_programs(count: usize) -> Vec<Option<Option<PreparedExpression>>> {
    (0..count).map(|_| None).collect()
}

fn prepare_expression<'a>(
    slot: &'a mut Option<Option<PreparedExpression>>,
    expr: &Expression,
    ctx: &dyn Columns,
    columns: usize,
) -> Result<Option<&'a mut PreparedExpression>, EvalError> {
    if slot.is_none() {
        *slot = Some(PreparedExpression::new(expr, ctx, columns)?);
    }
    Ok(slot.as_mut().and_then(Option::as_mut))
}

fn evaluate_prepared_row(
    expr: &Expression,
    slot: &mut Option<Option<PreparedExpression>>,
    ctx: &dyn Columns,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Datum, EvalError> {
    if matches!(expr, Expression::ScalarFunction(_)) {
        if let Some(prepared) = prepare_expression(slot, expr, ctx, row.len())? {
            return prepared
                .evaluate_row(ctx, row)
                .map(|value| expr.apply_enum_set_as_int(value));
        }
    }
    expr.eval_unprepared(ctx, row)
}

fn evaluate_scalar_selection(
    expression: &Expression,
    slot: &mut Option<Option<PreparedExpression>>,
    ctx: &dyn Columns,
    input: &Chunk,
    selected: &[usize],
    mut emit: impl FnMut(Datum) -> Result<(), EvalError>,
) -> Result<(), EvalError> {
    if selected.is_empty() {
        return Ok(());
    }
    if matches!(expression, Expression::ScalarFunction(_)) {
        if let Some(prepared) = prepare_expression(slot, expression, ctx, input.num_cols())? {
            return prepared.evaluate_scalar_selection(ctx, input, selected, |value| {
                emit(expression.apply_enum_set_as_int(value))
            });
        }
    }
    for &row in selected {
        emit(expression.eval_unprepared(ctx, input.physical_row(row))?)?;
    }
    Ok(())
}

/// One batch of Go's `VectorizedFilterConsiderNull`.
///
/// The returned mask is indexed by physical rows. The supplied vectors
/// are output buffers (their previous contents are discarded); the input
/// chunk's selection vector is reapplied after evaluating physical rows.
/// As in Go, the vector path preserves NULL from equality rewritten from IN
/// until later conjuncts reject the row; ordinary NULL rejects it immediately.
///
/// Predicates run filter-major and receive only the rows that still qualify.
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
        &mut empty_filter_programs(filters.len()),
        input,
        selected,
        nulls,
        !vec_enabled || !vectorizable(filters),
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
        &mut empty_filter_programs(filters.len()),
        input,
        selected,
        nulls,
        false,
    )
}

fn filter_physical_rows<C: Columns>(
    ctx: &C,
    vec_enabled: bool,
    filters: &[Expression],
    prepared: &mut [Option<Option<PreparedExpression>>],
    input: &Chunk,
    mut selected: Vec<bool>,
    mut nulls: Vec<bool>,
    row_based: bool,
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

    // Go falls back to rowBasedFilter when vectorization is disabled or any
    // filter is not vectorizable. Keep the same filter-major order and
    // three-valued truth handling in that branch.
    if row_based {
        let mut live = Vec::with_capacity(physical_rows);
        for (filter, slot) in filters.iter().zip(prepared) {
            let int_type = filter
                .static_type()
                .is_some_and(|ty| ty.eval_type() == tidb_datatype::EvalType::Int);
            live.clear();
            live.extend(
                selected
                    .iter()
                    .enumerate()
                    .filter_map(|(index, &keep)| keep.then_some(index)),
            );
            let mut physical = live.iter().copied();
            evaluate_scalar_selection(filter, slot, ctx, input, &live, |value| {
                let row = physical.next().expect("one result per selected row");
                let truth = crate::truthy_of(&value)?;
                if truth.is_none() && int_type {
                    nulls[row] = true;
                }
                selected[row] = truth == Some(true);
                Ok(())
            })?;
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
    for (filter, slot) in filters.iter().zip(prepared) {
        if sel.is_empty() {
            break;
        }
        let column_wise = if !vec_enabled {
            false
        } else {
            match filter {
                // Go `Constant.VecEval*` reads parameters once per nonempty
                // batch; a deferred expression keeps row evaluation.
                Expression::Constant(constant) if constant.deferred_expr.is_none() => {
                    let code = truth_code(&constant.eval_in(ctx)?)?;
                    is_zero.clear();
                    is_zero.resize(sel.len(), code);
                    true
                }
                Expression::CorrelatedColumn(column) => {
                    let code = truth_code(&column.eval())?;
                    is_zero.clear();
                    is_zero.resize(sel.len(), code);
                    true
                }
                Expression::ScalarFunction(_) => {
                    match prepare_expression(slot, filter, ctx, input.num_cols())? {
                        Some(prepared) => {
                            is_zero.clear();
                            prepared.evaluate_rows(ctx, input, &sel, true, |datum| {
                                is_zero.push(truth_code(&datum)?);
                                Ok(())
                            })?;
                            true
                        }
                        None => false,
                    }
                }
                Expression::Column(_) | Expression::Constant(_) => false,
            }
        };
        if !column_wise {
            is_zero.clear();
            evaluate_scalar_selection(filter, slot, ctx, input, &sel, |value| {
                is_zero.push(truth_code(&value)?);
                Ok(())
            })?;
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

/// Immutable projection expressions and logical column mapping. Actual input
/// column ownership is discovered separately by each execution's suite.
pub struct EvaluatorProgram {
    calculated_output_indexes: Vec<usize>,
    calculated: Vec<Expression>,
    vectorizable: bool,
    parallelizable: bool,
    column_mapping: HashMap<usize, Vec<usize>>,
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
        let parallelizable = vectorizable && !calculated.iter().any(has_mutable_effects);
        Self {
            calculated_output_indexes,
            calculated,
            vectorizable,
            parallelizable,
            column_mapping,
        }
    }
    /// Independent batches may be evaluated by separate workers.
    pub fn parallelizable(&self) -> bool {
        self.parallelizable
    }
}

/// Go `EvaluatorSuite`: executes a projection program with an execution-local
/// column ownership cache. Calculated expressions finish before owner moves,
/// so an evaluation error cannot leave the input chunk half-consumed.
pub struct EvaluatorSuite {
    program: Arc<EvaluatorProgram>,
    prepared: RefCell<Vec<Option<Option<PreparedExpression>>>>,
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
            prepared: RefCell::new(program.calculated.iter().map(|_| None).collect()),
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
        let rows = input.num_rows();
        let program = &self.program;
        let mut prepared = self.prepared.borrow_mut();
        if program.vectorizable {
            for (position, (output_index, expression)) in program
                .calculated_output_indexes
                .iter()
                .zip(&program.calculated)
                .enumerate()
            {
                if rows != 0 && ctx.enable_vectorized_expression() {
                    if let Some(source) = crate::rpn::batch_source(expression, ctx)? {
                        if let Some(shared) = prepare_expression(
                            &mut prepared[position],
                            source,
                            ctx,
                            input.num_cols(),
                        )? {
                            shared.append_projection(ctx, input, output, *output_index)?;
                            continue;
                        }
                    }
                }
                if let Expression::Constant(constant) = expression {
                    // Go Constant.VecEval* broadcasts only non-deferred
                    // constants. Deferred expressions still consume rows;
                    // parameter values are read anew for every chunk.
                    if constant.deferred_expr.is_none() {
                        if rows != 0 {
                            let value = constant.eval_in(ctx)?;
                            for _ in 0..rows {
                                output.append_datum(*output_index, &value);
                            }
                        }
                        continue;
                    }
                }
                for row_index in 0..rows {
                    let value = evaluate_prepared_row(
                        expression,
                        &mut prepared[position],
                        ctx,
                        input.get_row(row_index),
                    )?;
                    output.append_datum(*output_index, &value);
                }
            }
        } else {
            for row_index in 0..rows {
                for (position, (output_index, expression)) in program
                    .calculated_output_indexes
                    .iter()
                    .zip(&program.calculated)
                    .enumerate()
                {
                    let value = evaluate_prepared_row(
                        expression,
                        &mut prepared[position],
                        ctx,
                        input.get_row(row_index),
                    )?;
                    output.append_datum(*output_index, &value);
                }
            }
        }

        if let Some(helper) = &self.column_swap_helper {
            helper
                .swap_columns(input, output)
                .map_err(EvaluatorError::Chunk)?;
        }
        Ok(())
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

    #[test]
    fn projection_uses_one_seeded_if_stream_for_its_execution() {
        let real = FieldType::new(FieldTypeCode::Double);
        let rand = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("rand"),
            real.clone(),
            vec![int_const(7)],
        ));
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            real.clone(),
            vec![input_column(0), rand, int_const(0)],
        ));
        let program = Arc::new(EvaluatorProgram::new(vec![expression], false));
        for _ in 0..2 {
            let suite = EvaluatorSuite::from_program(Arc::clone(&program));
            for (flags, expected) in [
                (
                    vec![None, Some(1), Some(0)],
                    vec![0.0, 0.9065021936842261, 0.0],
                ),
                (vec![Some(0), Some(-1)], vec![0.0, 0.37600881361962185]),
            ] {
                let mut input = Chunk::new(&[long()], 3, 3);
                for flag in flags {
                    match flag {
                        Some(flag) => input.append_int64(0, flag),
                        None => input.append_null(0),
                    }
                }
                let mut output = Chunk::new(std::slice::from_ref(&real), 3, 3);
                suite.run(&NoColumns, &mut input, &mut output).unwrap();
                assert!(matches!(&suite.prepared.borrow()[0], Some(Some(_))));
                assert_eq!(
                    (0..output.num_rows())
                        .map(|row| output.get_row(row).get_float64(0))
                        .collect::<Vec<_>>(),
                    expected,
                );
            }
        }
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
    fn shared_projection_preserves_user_variable_order_and_null_assignment() {
        struct Variables(RefCell<Datum>);
        impl Columns for Variables {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }

            fn get_uservar(&self, name: &str) -> Option<Datum> {
                assert_eq!(name, "running");
                Some(self.0.borrow().clone())
            }

            fn set_uservar(&self, name: &str, value: Datum) {
                assert_eq!(name, "running");
                self.0.replace(value);
            }
        }

        let get = scalar("getvar_int", vec![string_const("running")]);
        let sum = scalar("plus", vec![get.clone(), input_column(0)]);
        let set = scalar("setvar", vec![string_const("running"), sum]);
        let suite = EvaluatorSuite::new(vec![set, get], false);
        let context = Variables(RefCell::new(Datum::Int(0)));
        assert!(!suite.vectorizable());
        for (batch, want) in [
            (
                vec![Datum::Int(2), Datum::Null],
                vec![(Datum::Int(2), Datum::Int(2)), (Datum::Null, Datum::Int(2))],
            ),
            (vec![Datum::Int(5)], vec![(Datum::Int(7), Datum::Int(7))]),
        ] {
            let mut input = Chunk::new(&[long()], batch.len(), batch.len());
            for value in batch {
                input.append_datum(0, &value);
            }
            let mut output = Chunk::new(&[long(), long()], input.num_rows(), input.num_rows());
            suite.run(&context, &mut input, &mut output).unwrap();
            let got = (0..output.num_rows())
                .map(|row| {
                    let row = output.get_row(row);
                    (row.get_datum(0, &long()), row.get_datum(1, &long()))
                })
                .collect::<Vec<_>>();
            assert_eq!(got, want);
            assert!(suite
                .prepared
                .borrow()
                .iter()
                .all(|slot| matches!(slot, Some(Some(_)))));
        }
    }

    #[test]
    fn shared_arithmetic_observes_scalar_and_batch_operand_demand() {
        let expression = scalar("minus", vec![input_column(0), parameter(long())]);
        let error = EvalError::Unsupported("unbound prepared parameter");
        let context = CountedParameter {
            value: Err(error.clone()),
            reads: Cell::new(0),
        };
        let mut input = Chunk::new(&[long()], 1, 1);
        input.append_null(0);
        let mut prepared = PreparedExpression::new(&expression, &context, 1)
            .unwrap()
            .expect("shared integer subtraction");
        assert_eq!(
            prepared.evaluate_row(&context, input.get_row(0)),
            Ok(Datum::Null)
        );
        assert_eq!(context.reads.get(), 0);
        assert_eq!(
            prepared.evaluate_rows(&context, &input, &[0], true, |_| Ok(())),
            Err(error)
        );
        assert_eq!(context.reads.get(), 1);
    }

    #[test]
    fn shared_projection_reads_bound_values_only_for_a_demanded_branch() {
        let expression = scalar("if", vec![input_column(0), parameter(long()), int_const(0)]);
        let program = Arc::new(EvaluatorProgram::new(vec![expression], false));
        for parameter in [Datum::Int(5), Datum::Null, Datum::Int(-2)] {
            let suite = EvaluatorSuite::from_program(Arc::clone(&program));
            let context = CountedParameter {
                value: Ok(parameter.clone()),
                reads: Cell::new(0),
            };
            let mut input = Chunk::new(&[long()], 3, 3);
            for flag in [1, 0, -1] {
                input.append_int64(0, flag);
            }
            let mut output = Chunk::new(&[long()], 3, 3);
            suite.run(&context, &mut input, &mut output).unwrap();
            assert!(matches!(&suite.prepared.borrow()[0], Some(Some(_))));
            assert_eq!(context.reads.get(), 1);
            let values = (0..output.num_rows())
                .map(|row| output.get_row(row).get_datum(0, &long()))
                .collect::<Vec<_>>();
            assert_eq!(values, [parameter.clone(), Datum::Int(0), parameter]);
        }

        let suite = EvaluatorSuite::from_program(program);
        let error = EvalError::Unsupported("unbound prepared parameter");
        let context = CountedParameter {
            value: Err(error.clone()),
            reads: Cell::new(0),
        };
        let mut input = Chunk::new(&[long()], 2, 2);
        let mut output = Chunk::new(&[long()], 2, 2);
        input.append_int64(0, 0);
        suite.run(&context, &mut input, &mut output).unwrap();
        assert_eq!(context.reads.get(), 0);
        input.append_int64(0, 1);
        assert_eq!(
            suite.run(&context, &mut input, &mut output),
            Err(error.into())
        );
        assert_eq!(context.reads.get(), 1);
    }

    #[test]
    fn shared_control_demands_session_values_only_on_selected_rows() {
        struct Session(Cell<u64>);
        impl Columns for Session {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn connection_id(&self) -> Option<u64> {
                let id = self.0.get();
                self.0.set(id + 1);
                Some(id)
            }
        }
        let mut unsigned = long();
        unsigned.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        let connection = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("connection_id"),
            unsigned,
            Vec::new(),
        ));
        let expression = scalar("if", vec![input_column(0), connection, int_const(0)]);
        let suite = EvaluatorSuite::new(vec![expression], false);
        let context = Session(Cell::new(7));
        let mut input = Chunk::new(&[long()], 3, 3);
        for flag in [0, 1, -1] {
            input.append_int64(0, flag);
        }
        let mut output = Chunk::new(&[long()], 3, 3);
        suite.run(&context, &mut input, &mut output).unwrap();
        assert!(matches!(&suite.prepared.borrow()[0], Some(Some(_))));
        assert_eq!(context.0.get(), 9);
        let values = (0..output.num_rows())
            .map(|row| output.get_row(row).get_datum(0, &long()))
            .collect::<Vec<_>>();
        assert_eq!(values, [Datum::Int(0), Datum::Int(7), Datum::Int(8)]);
    }

    #[test]
    fn shared_control_passes_evaluated_operands_to_session_operations() {
        struct Session(RefCell<Vec<u64>>);
        impl Columns for Session {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn set_last_insert_id(&self, value: u64) {
                self.0.borrow_mut().push(value);
            }
        }
        let value = scalar("plus", vec![input_column(1), int_const(1)]);
        let operation = scalar("last_insert_id", vec![value]);
        let expression = scalar("if", vec![input_column(0), operation, int_const(0)]);
        let suite = EvaluatorSuite::new(vec![expression], false);
        let context = Session(RefCell::new(Vec::new()));
        let mut input = Chunk::new(&[long(), long()], 4, 4);
        for (flag, value) in [(0, 7), (1, 9), (0, 11), (-1, 13)] {
            input.append_int64(0, flag);
            input.append_int64(1, value);
        }
        let mut output = Chunk::new(&[long()], 4, 4);
        suite.run(&context, &mut input, &mut output).unwrap();
        assert!(matches!(&suite.prepared.borrow()[0], Some(Some(_))));
        assert_eq!(*context.0.borrow(), [10, 14]);
        let values = (0..output.num_rows())
            .map(|row| output.get_row(row).get_datum(0, &long()))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            [Datum::Int(0), Datum::Int(10), Datum::Int(0), Datum::Int(14)]
        );
    }

    #[test]
    fn filter_execution_continues_seeded_demand_across_batch_sizes_and_reopens() {
        let flag = input_column(0);
        let seed = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("rand"),
            FieldType::new(FieldTypeCode::Double),
            vec![int_const(7)],
        ));
        let real_constant = |value| {
            Expression::Constant(Constant::new(
                Datum::Real(value),
                FieldType::new(FieldTypeCode::Double),
            ))
        };
        let conditional = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            FieldType::new(FieldTypeCode::Double),
            vec![flag, seed, real_constant(0.0)],
        ));
        let predicate = scalar("gt", vec![conditional, real_constant(0.5)]);
        let mut filters = FilterExecution::new(vec![predicate]);
        let flags = [0, 1, 0, 1, 1, 0, 1, 0];
        let expected_rng = crate::MysqlRng::new_with_seed(7);
        let expected = flags.map(|flag| flag != 0 && expected_rng.gen() > 0.5);
        for batch_size in [2, flags.len(), 1] {
            filters.reset();
            let mut actual = Vec::new();
            for batch in flags.chunks(batch_size) {
                let mut input = Chunk::new(&[long()], batch.len(), batch.len());
                for &flag in batch {
                    input.append_int64(0, flag);
                }
                let (selected, nulls) = filters
                    .filter(&NoColumns, true, &input, Vec::new(), Vec::new())
                    .unwrap();
                assert_eq!(nulls, vec![false; batch.len()]);
                actual.extend(selected);
            }
            let prepared = filters.prepared.borrow();
            assert!(prepared[0]
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .requires_execution_state());
            assert_eq!(actual, expected);
        }
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
    fn numeric_batch_selection_signedness_and_preflight() {
        use tidb_datatype::FieldTypeFlags;
        for name in ["plus", "minus", "mul"] {
            for left_unsigned in [false, true] {
                for right_unsigned in [false, true] {
                    let field = |unsigned| {
                        let mut field = long();
                        if unsigned {
                            field.add_flags(FieldTypeFlags::UNSIGNED);
                        }
                        field
                    };
                    let left_type = field(left_unsigned);
                    let right_type = field(right_unsigned);
                    let out_type = field(left_unsigned || right_unsigned);
                    let mut left = Column::new(1, left_type.clone());
                    left.index = 0;
                    let mut right = Column::new(2, right_type.clone());
                    right.index = 1;
                    let expression = Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new(name),
                        out_type.clone(),
                        vec![Expression::Column(left), Expression::Column(right)],
                    ));
                    let mut input = Chunk::new_with_capacity(&[left_type, right_type], 3);
                    for (left, right) in [(5, 2), (6, 3), (7, 4)] {
                        input.append_int64(0, left);
                        input.append_int64(1, right);
                    }
                    input.set_sel(Some(vec![2, 0]));
                    let mut output = Chunk::new_with_capacity(&[out_type.clone()], 2);
                    EvaluatorSuite::new(vec![expression], false)
                        .run(&crate::NoColumns, &mut input, &mut output)
                        .unwrap();
                    let expected = match name {
                        "plus" => [11, 7],
                        "minus" => [3, 3],
                        _ => [28, 10],
                    };
                    for (index, value) in expected.into_iter().enumerate() {
                        let expected = if left_unsigned || right_unsigned {
                            Datum::UInt(value)
                        } else {
                            Datum::Int(value as i64)
                        };
                        assert_eq!(output.get_row(index).get_datum(0, &out_type), expected);
                    }
                }
            }
        }
        let ctx = CountedParameter {
            value: Err(EvalError::Unsupported("unbound prepared parameter")),
            reads: Cell::new(0),
        };
        let abs = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("abs"),
            long(),
            vec![input_column(0)],
        ));
        let mut batch = Chunk::new_with_capacity(&[long()], 1);
        batch.append_int64(0, -7);
        assert_eq!(
            crate::try_evaluate_batch(&abs, &ctx, &batch).unwrap(),
            Some(vec![Datum::Int(7)])
        );
        let unsupported = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("row"),
            long(),
            vec![input_column(0)],
        ));
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![parameter(long()), unsupported],
        ));
        let mut input = Chunk::new_with_capacity(&[long()], 1);
        input.append_int64(0, 1);
        assert_eq!(
            crate::try_evaluate_batch(&expression, &ctx, &input).unwrap(),
            None
        );
        assert_eq!(
            ctx.reads.get(),
            0,
            "declining a shape must not evaluate its earlier operand"
        );
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![parameter(long()), input_column(0)],
        ));
        input.reset();
        assert_eq!(
            crate::try_evaluate_batch(&expression, &ctx, &input).unwrap(),
            Some(vec![])
        );
        assert_eq!(
            ctx.reads.get(),
            0,
            "empty batches must not evaluate parameters"
        );
    }

    #[test]
    fn numeric_batch_casts_unsigned_arguments_before_arithmetic() {
        use tidb_datatype::{Decimal, FieldTypeFlags};
        let mut unsigned = FieldType::new(FieldTypeCode::LongLong);
        unsigned.add_flags(FieldTypeFlags::UNSIGNED);
        let mut column = Column::new(1, unsigned.clone());
        column.index = 0;
        let mut input = Chunk::new_with_capacity(&[unsigned], 2);
        input.append_int64(0, -1); // UINT64_MAX, not signed -1.
        input.append_null(0);
        input.set_sel(Some(vec![1, 0]));
        for code in [FieldTypeCode::Double, FieldTypeCode::NewDecimal] {
            for name in ["plus", "minus", "mul"] {
                let field = FieldType::new(code);
                let one = if code == FieldTypeCode::Double {
                    Datum::Real(1.0)
                } else {
                    Datum::Decimal(Decimal::from_int(1))
                };
                let expression = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(name),
                    field.clone(),
                    vec![
                        Expression::Column(column.clone()),
                        Expression::Constant(Constant::new(one, field)),
                    ],
                ));
                let expected = if code == FieldTypeCode::Double {
                    Datum::Real(u64::MAX as f64)
                } else {
                    let value = match name {
                        "plus" => "18446744073709551616",
                        "minus" => "18446744073709551614",
                        _ => "18446744073709551615",
                    };
                    Datum::Decimal(Decimal::from_literal(value))
                };
                let batch = crate::try_evaluate_batch(&expression, &crate::NoColumns, &input)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    batch,
                    vec![Datum::Null, expected.clone()],
                    "{code:?}/{name}"
                );
                assert_eq!(
                    expression
                        .eval(&crate::NoColumns, input.get_row(1))
                        .unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn numeric_batch_does_not_suppress_nested_errors_on_null_rows() {
        struct Context(bool);
        impl Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn enable_vectorized_expression(&self) -> bool {
                self.0
            }
        }
        let nested = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("mul"),
            long(),
            vec![input_column(1), int_const(2)],
        ));
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![input_column(0), nested],
        ));
        for vectorized in [false, true] {
            let mut input = Chunk::new_with_capacity(&[long(), long()], 1);
            input.append_null(0);
            input.append_int64(1, i64::MAX);
            let mut output = Chunk::new_with_capacity(&[long()], 1);
            let result = EvaluatorSuite::new(vec![expression.clone()], false).run(
                &Context(vectorized),
                &mut input,
                &mut output,
            );
            if vectorized {
                assert!(result.is_err());
            } else {
                assert!(result.is_ok());
                assert!(output.get_row(0).is_null(0));
            }
        }
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
        let check = |expression: &Expression, input: &mut Chunk| {
            let field = expression.static_type().unwrap();
            let expected = (0..input.num_rows())
                .map(|row| expression.eval(&ctx, input.get_row(row)).unwrap())
                .collect::<Vec<_>>();
            let suite = EvaluatorSuite::new(vec![expression.clone()], false);
            let mut output =
                Chunk::new_with_capacity(std::slice::from_ref(field), input.num_rows());
            suite.run(&ctx, input, &mut output).unwrap();
            assert!(matches!(&suite.prepared.borrow()[0], Some(Some(_))));
            assert_eq!(output.num_rows(), expected.len());
            for (row, expected) in expected.into_iter().enumerate() {
                let actual = output.get_row(row).get_datum(0, field);
                assert_eq!(actual, expected);
                if let (Datum::Decimal(actual), Datum::Decimal(expected)) = (actual, expected) {
                    assert_eq!(actual.to_string(), expected.to_string());
                }
            }
        };
        for expression in &expressions {
            check(expression, &mut input);
            input.set_sel(Some(vec![1, 3, 6]));
            check(expression, &mut input);
            input.set_sel(None);
        }
        // Keep integer-to-decimal padding and a declared shape that can
        // clamp in the same compiler, across both zero and positive scales.
        for (flen, scale) in [(20, 0), (22, 2), (10, 0)] {
            let mut target = FieldType::new(FieldTypeCode::NewDecimal);
            target.set_flen(flen);
            target.set_decimal(scale);
            let product = typed(
                "mul",
                vec![
                    decimal_column(0, &price_type),
                    typed("cast_decimal", vec![input_column(2)], &target),
                ],
                &result_type,
            );
            check(&product, &mut input);
        }
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
            let (selected, nulls) = vectorized_filter_consider_null(
                &ctx,
                true,
                &filters,
                &input,
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
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
