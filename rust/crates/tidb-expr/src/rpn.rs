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
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{cell::RefCell, sync::Arc};

use prost::Message;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{
    Datum, EvalType as NativeEvalType, FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone,
};
use tidb_proto::tipb::{Expr as WireExpr, ExprType};
use tidb_query_common::error::{ErrorInner, EvaluateError};
use tidb_query_datatype::codec::{batch::LazyBatchColumnVec, data_type::BATCH_MAX_SIZE};
use tidb_query_datatype::{
    codec::mysql::Tz,
    expr::{EvalConfig, EvalContext, Flag, SqlMode},
    EvalType, FieldTypeAccessor,
};
use tidb_query_expr::RpnExpression;
use tipb::{Expr as QueryExpr, ExprType as QueryExprType, ScalarFuncSig};

mod engine;
pub(crate) use engine::StatementRand;
use engine::{resolve_rpn_function, scalar_func_sig, RpnArgTypes};

mod binding;
mod rows;
mod signatures;

use tidb_exprctx::context::StatementSeededRandom;

use crate::{
    context::{BlockEncryptionMode, Columns, EvalError},
    expression::Expression,
    scalar_function::{PreparedContextInput, PreparedLocalBuiltin, ScalarFunction},
};

struct DatumRowScratch {
    chunk: Chunk,
}

fn fill_datum_row<'a>(chunk: &mut Chunk, mut values: impl Iterator<Item = &'a Datum>) {
    chunk.reset();
    for index in 0..chunk.num_cols() {
        chunk.append_datum(index, values.next().unwrap_or(&Datum::Null));
    }
    assert!(
        values.next().is_none(),
        "row has more values than declared fields"
    );
}

/// One native execution owns the prepared expression, its context and state.
pub(crate) struct PreparedExpression {
    program: RpnExpression,
    batch_program: Option<RpnExpression>,
    columns: Vec<FieldType>,
    bindings: Vec<InputBinding>,
    host_calls: Vec<HostCall>,
    diagnostics: Vec<ScalarFunction>,
    schema: Vec<tipb::FieldType>,
    column_paths: Vec<Vec<String>>,
    seed_keys: Vec<usize>,
    statement_seeds: Option<Box<[rows::StatementSeedSlot]>>,
    execution_seeds: Box<[StatementSeededRandom]>,
    row_ordered: bool,
    scalar_effects: bool,
    output: FieldType,
    context: EvalContext,
    datum_row: Option<Box<DatumRowScratch>>,
    block_encryption_mode: BlockEncryptionMode,
}

impl PreparedExpression {
    pub(crate) fn new(
        expression: &Expression,
        ctx: &dyn Columns,
        column_count: usize,
    ) -> Result<Option<Self>, EvalError> {
        Self::prepare(expression, ctx, column_count, false)
    }

    fn prepare(
        expression: &Expression,
        ctx: &dyn Columns,
        column_count: usize,
        use_statement_seed: bool,
    ) -> Result<Option<Self>, EvalError> {
        Self::prepare_with_source(expression, ctx, column_count, use_statement_seed, true, false)
    }

    fn prepare_with_source(
        expression: &Expression,
        ctx: &dyn Columns,
        column_count: usize,
        use_statement_seed: bool,
        physical_input: bool,
        typed_batch: bool,
    ) -> Result<Option<Self>, EvalError> {
        let Some(columns) = native_schema(expression, column_count) else {
            return Ok(None);
        };
        let Some(output) = expression.static_type().cloned() else {
            return Ok(None);
        };
        let mut context = new_context(ctx, None);
        let Some(compiled) = compile(
            expression,
            &columns,
            &mut context,
            ctx,
            physical_input,
            use_statement_seed,
            typed_batch,
        )
        .map_err(engine_error)?
        else {
            return Ok(None);
        };
        let Some(schema) = columns
            .iter()
            .chain(compiled.bindings.iter().map(|binding| &binding.field))
            .map(rows::query_field_type)
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let scalar_effects = crate::evaluator::has_scalar_effects(expression);
        let row_ordered = scalar_effects
            || compiled
                .host_calls
                .iter()
                .any(|call| match &call.operation {
                    HostOperation::LocalBuiltin(_)
                    | HostOperation::DeferredValue(_)
                    | HostOperation::SqlCast(_)
                    | HostOperation::NumericCast(_)
                    | HostOperation::JsonCast { .. }
                    | HostOperation::TemporalFsp(_) => false,
                    _ => true,
                });
        Ok(Some(Self {
            program: compiled.program,
            batch_program: compiled.batch_program,
            columns,
            bindings: compiled.bindings,
            host_calls: compiled.host_calls,
            diagnostics: compiled.diagnostics,
            schema,
            column_paths: Vec::new(),
            statement_seeds: use_statement_seed.then(|| {
                (0..compiled.seed_keys.len())
                    .map(|_| Default::default())
                    .collect()
            }),
            execution_seeds: (0..compiled.seed_keys.len())
                .map(|_| Default::default())
                .collect(),
            seed_keys: compiled.seed_keys,
            row_ordered,
            scalar_effects,
            output,
            context,
            datum_row: None,
            block_encryption_mode: ctx.block_encryption_mode(),
        }))
    }

    #[cfg(test)]
    pub(crate) fn requires_execution_state(&self) -> bool {
        !self.seed_keys.is_empty()
    }

    pub(crate) fn append_projection(
        &mut self,
        ctx: &dyn Columns,
        chunk: &Chunk,
        destination: &mut Chunk,
        output_index: usize,
    ) -> Result<(), EvalError> {
        let selected: Vec<_> = (0..chunk.num_rows()).collect();
        if destination.sel().is_some()
            || self.output.is_hybrid()
            || self.output.code() == FieldTypeCode::Float
        {
            return self.evaluate_rows(ctx, chunk, &selected, false, |datum| {
                destination.append_datum(output_index, &datum);
                Ok(())
            });
        }

        // Use the same result column sizing as native VecEval.
        let target = self.output.eval_type();
        let mut column = destination.column_mut(output_index);
        crate::chunk_executor::allocate(target, selected.len(), &mut column);
        let mut row = 0;
        self.evaluate_rows(ctx, chunk, &selected, false, |datum| {
            crate::scalar_function::vec_eval::put(&mut column, row, target, &datum)?;
            row += 1;
            Ok(())
        })
    }

    pub(crate) fn try_eval_typed_batch(
        expression: &Expression,
        ctx: &dyn Columns,
        chunk: &Chunk,
        mut emit: impl FnMut(Datum) -> Result<(), EvalError>,
    ) -> Result<bool, EvalError> {
        let Some(mut program) =
            Self::prepare_with_source(expression, ctx, chunk.num_cols(), false, true, true)?
        else {
            return Ok(false);
        };
        let selected = (0..chunk.num_rows()).collect::<Vec<_>>();
        program.evaluate(ctx, chunk, &selected, rows::ReadMode::TypedBatch, &mut emit)?;
        Ok(true)
    }

    pub(crate) fn evaluate_rows(
        &mut self,
        ctx: &dyn Columns,
        chunk: &Chunk,
        selected: &[usize],
        physical: bool,
        emit: impl FnMut(Datum) -> Result<(), EvalError>,
    ) -> Result<(), EvalError> {
        self.evaluate(
            ctx,
            chunk,
            selected,
            if physical {
                rows::ReadMode::PhysicalBatch
            } else {
                rows::ReadMode::LogicalBatch
            },
            emit,
        )
    }

    pub(crate) fn evaluate_scalar_selection(
        &mut self,
        ctx: &dyn Columns,
        chunk: &Chunk,
        selected: &[usize],
        emit: impl FnMut(Datum) -> Result<(), EvalError>,
    ) -> Result<(), EvalError> {
        self.evaluate(ctx, chunk, selected, rows::ReadMode::Scalar, emit)
    }

    pub(crate) fn evaluate_row(
        &mut self,
        ctx: &dyn Columns,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<Datum, EvalError> {
        let empty;
        let chunk = if let Some(chunk) = row.chunk() {
            chunk
        } else {
            empty = Chunk::new_empty_chunk(&[]);
            &empty
        };
        let mut result = Datum::Null;
        self.evaluate(ctx, chunk, &[row.idx()], rows::ReadMode::Scalar, |value| {
            result = value;
            Ok(())
        })?;
        Ok(result)
    }

    #[cfg(test)]
    fn append_quiet_int_presence(
        &mut self,
        ctx: &dyn Columns,
        chunk: &Chunk,
        output: &mut Vec<bool>,
    ) -> bool {
        use tidb_query_expr::RpnExpressionNode::*;

        if self.row_ordered
            || !self.seed_keys.is_empty()
            || !self.bindings.is_empty()
            || !self.host_calls.is_empty()
        {
            return false;
        }
        let int_only = self.program.as_ref().iter().all(|node| match node {
            FnCall {
                field_type,
                func_meta,
                ..
            } => {
                func_meta.name != "TiDB host"
                    && EvalType::try_from(field_type.as_accessor().tp()).ok() == Some(EvalType::Int)
            }
            Constant { value, .. } => value.eval_type() == EvalType::Int,
            ColumnRef { offset } => {
                *offset == 0
                    || self.schema.get(offset - 1).is_some_and(|field| {
                        EvalType::try_from(field.as_accessor().tp())
                            .is_ok_and(|kind| kind == EvalType::Int)
                    })
            }
            _ => false,
        });
        if !int_only {
            return false;
        }
        let selected = (0..chunk.num_rows()).collect::<Vec<_>>();
        let output_start = output.len();
        let result = self.evaluate(
            ctx,
            chunk,
            &selected,
            rows::ReadMode::QuietIntBatch,
            |value| {
                output.push(!value.is_null());
                Ok(())
            },
        );
        if result.is_ok() {
            return true;
        }
        output.truncate(output_start);
        false
    }

    fn evaluate(
        &mut self,
        ctx: &dyn Columns,
        chunk: &Chunk,
        selected: &[usize],
        mode: rows::ReadMode,
        mut emit: impl FnMut(Datum) -> Result<(), EvalError>,
    ) -> Result<(), EvalError> {
        let input = rows::NativeRows {
            chunk,
            fields: &self.columns,
            column_paths: &self.column_paths,
            bindings: &self.bindings,
            host_calls: &self.host_calls,
            diagnostics: &self.diagnostics,
            seed_keys: &self.seed_keys,
            statement_seeds: self.statement_seeds.as_deref(),
            execution_seeds: &self.execution_seeds,
            binding_error: RefCell::new(None),
            decimals: RefCell::default(),
            context: ctx,
            block_encryption_mode: self.block_encryption_mode,
            mode: rows::ReadMode::PhysicalBatch,
            scalar_operands: !matches!(mode, rows::ReadMode::TypedBatch)
                && (self.scalar_effects || matches!(mode, rows::ReadMode::Scalar)),
            typed_batch: matches!(mode, rows::ReadMode::TypedBatch),
        };
        // Finish side effects in a row before another row can draw or fail.
        let batch_size = match mode {
            rows::ReadMode::TypedBatch => selected.len().max(1),
            rows::ReadMode::Scalar => 1,
            _ if self.row_ordered => 1,
            _ => BATCH_MAX_SIZE,
        };
        use tidb_query_datatype::codec::data_type::VectorValue;
        let selected_physical;
        let selected = if matches!(
            mode,
            rows::ReadMode::LogicalBatch | rows::ReadMode::QuietIntBatch | rows::ReadMode::TypedBatch
        ) && chunk.sel().is_some()
        {
            selected_physical = selected
                .iter()
                .map(|&index| chunk.get_row(index).idx())
                .collect::<Vec<_>>();
            &selected_physical
        } else {
            selected
        };
        use std::borrow::Cow;
        if selected.is_empty() {
            return Ok(());
        }
        let ascending = selected.windows(2).all(|pair| pair[0] < pair[1]);
        let physical_rows = if ascending {
            Cow::Borrowed(selected)
        } else {
            let mut rows = selected.to_vec();
            rows.sort_unstable();
            rows.dedup();
            Cow::Owned(rows)
        };
        let logical_rows: Cow<'_, [usize]> = if selected.len() == 1 {
            Cow::Borrowed(&[0])
        } else if ascending {
            if selected[0] == 0 && selected[selected.len() - 1] == selected.len() - 1 {
                Cow::Borrowed(selected)
            } else {
                Cow::Owned((0..selected.len()).collect())
            }
        } else {
            Cow::Owned(
                selected
                    .iter()
                    .map(|index| {
                        physical_rows
                            .binary_search(index)
                            .expect("selected physical row")
                    })
                    .collect(),
            )
        };
        // Most SQL batches are contiguous. Sparse selections and scalar rows
        // still get a compact column index, regardless of their source offset.
        let row_ids = physical_rows
            .iter()
            .map(|&row| Some(row as i64))
            .collect::<Vec<_>>();
        let mut physical = vec![VectorValue::Int(row_ids.into())];
        let mut schema = vec![engine::index_field()];
        if self.column_paths.is_empty() {
            for (offset, (field, declaration)) in self.columns.iter().zip(&self.schema).enumerate()
            {
                let column = (offset < chunk.num_cols()).then(|| chunk.column(offset));
                let vector = match (engine::borrowable_field(field, declaration), column) {
                    (Some(EvalType::Int), Some(column)) => VectorValue::Int(
                        physical_rows
                            .iter()
                            .map(|&row| (!column.is_null(row)).then(|| column.get_int64(row)))
                            .collect::<Vec<_>>()
                            .into(),
                    ),
                    (Some(EvalType::Bytes), Some(column)) => {
                        use tidb_query_datatype::codec::data_type::{ChunkedVec, ChunkedVecBytes};
                        let mut values = ChunkedVecBytes::with_capacity(physical_rows.len());
                        for &row in physical_rows.iter() {
                            if column.is_null(row) {
                                values.push_ref(None);
                            } else {
                                values.push_ref(Some(column.get_bytes(row).as_ref()));
                            }
                        }
                        VectorValue::Bytes(values)
                    }
                    _ => VectorValue::Int(Vec::new().into()),
                };
                physical.push(vector);
                schema.push(declaration.clone());
            }
        }
        let physical = LazyBatchColumnVec::from(physical);
        let program = if input.scalar_operands {
            &self.program
        } else {
            self.batch_program.as_ref().unwrap_or(&self.program)
        };
        for rows in logical_rows.chunks(batch_size) {
            let result =
                engine::evaluate(program, &mut self.context, &schema, &input, &physical, rows);
            if matches!(mode, rows::ReadMode::QuietIntBatch)
                && self.context.warnings.warning_cnt != 0
            {
                self.context.warnings.warning_cnt = 0;
                self.context.warnings.warnings.clear();
                return Err(EvalError::Unsupported("batch produced diagnostics"));
            }
            forward_query_warnings(ctx, &mut self.context);
            let values = result.map_err(|error| {
                input
                    .binding_error
                    .take()
                    .unwrap_or_else(|| query_error(error))
            })?;
            for index in 0..rows.len() {
                let datum =
                    rows::from_query_value(values.get_logical_scalar_ref(index), &self.output)
                        .map_err(query_error)?;
                emit(datum)?;
            }
        }
        Ok(())
    }
}

/// A typed protobuf expression and its state for one local coprocessor request.
pub struct WireExpressionExecution(PreparedExpression);

impl std::fmt::Debug for WireExpressionExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireExpressionExecution")
            .field("instruction_count", &self.0.program.len())
            .finish()
    }
}

impl WireExpressionExecution {
    /// Compile a concrete wire declaration through the storage signature table.
    pub fn new(
        expr: &WireExpr,
        ctx: &dyn Columns,
        columns: &[FieldType],
        request_flags: u64,
    ) -> Result<Option<Self>, EvalError> {
        let mut expr = expr.clone();
        if bind_wire_fields(&mut expr, columns).is_none() {
            return Ok(None);
        }
        let output = crate::distsql_builtin::pb_type_to_field_type(
            expr.field_type.as_ref().expect("bound wire result"),
        );
        let mut context = new_context(ctx, Some(request_flags));
        let tree = engine::decode(&expr.encode_to_vec()).map_err(query_error)?;
        let Some(schema) = columns
            .iter()
            .map(rows::query_field_type)
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let program =
            engine::build_wire(tree, &mut context, &schema, columns).map_err(query_error)?;
        Ok(Some(Self(PreparedExpression {
            program,
            batch_program: None,
            columns: columns.to_vec(),
            bindings: Vec::new(),
            host_calls: Vec::new(),
            diagnostics: Vec::new(),
            schema,
            column_paths: Vec::new(),
            seed_keys: Vec::new(),
            statement_seeds: None,
            execution_seeds: Box::new([]),
            row_ordered: false,
            scalar_effects: false,
            output,
            context,
            datum_row: None,
            block_encryption_mode: Default::default(),
        })))
    }

    /// Evaluate a decoded scan row with the request's physical column types.
    pub fn evaluate_datums(
        &mut self,
        context: &dyn Columns,
        row: &[Datum],
    ) -> Result<Datum, EvalError> {
        if row.len() != self.0.columns.len() {
            return Err(EvalError::Unsupported(
                "coprocessor row width differs from schema",
            ));
        }
        let mut scratch = self.0.datum_row.take().unwrap_or_else(|| {
            Box::new(DatumRowScratch {
                chunk: Chunk::new_chunk_with_capacity(&self.0.columns, 1),
            })
        });
        fill_datum_row(&mut scratch.chunk, row.iter());
        let value = self.evaluate_row(context, scratch.chunk.get_row(0));
        self.0.datum_row = Some(scratch);
        value
    }

    /// Evaluate a physical scan row in the existing request.
    pub fn evaluate_row(
        &mut self,
        ctx: &dyn Columns,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<Datum, EvalError> {
        self.0.evaluate_row(ctx, row)
    }
}

fn bind_wire_fields(expr: &mut WireExpr, columns: &[FieldType]) -> Option<()> {
    if expr.tp() == ExprType::ColumnRef {
        let (_, offset) = tidb_codec::decode_int(expr.val()).ok()?;
        let column = columns.get(usize::try_from(offset).ok()?)?;
        expr.field_type = Some(binding::field_type_to_pb(column)?);
        return Some(());
    }
    for child in &mut expr.children {
        bind_wire_fields(child, columns)?;
    }
    if expr.tp() == ExprType::ScalarFunc && !wire_cast_preserves_sql_contract(expr) {
        return None;
    }
    if expr
        .field_type
        .as_ref()
        .is_none_or(|field| field.tp.is_none())
    {
        let code = match expr.tp() {
            ExprType::ScalarFunc => {
                let partial = engine::decode(&expr.encode_to_vec()).ok()?;
                let meta = resolve_rpn_function(
                    partial.get_sig(),
                    partial.get_field_type(),
                    partial.get_children(),
                )
                .ok()?;
                // Historical scan predicates omitted the result declaration.
                // Their bare non-integer leaves read as SQL NULL in an
                // integer slot. Leave typed requests subject to SDK validation.
                if let RpnArgTypes::Fixed(types) = meta.argument_types {
                    for ((wire, argument), required) in expr
                        .children
                        .iter_mut()
                        .zip(partial.get_children())
                        .zip(types)
                    {
                        if *required == EvalType::Int
                            && eval_type(argument) != Some(EvalType::Int)
                            && matches!(
                                wire.tp(),
                                ExprType::ColumnRef
                                    | ExprType::String
                                    | ExprType::Bytes
                                    | ExprType::Float32
                                    | ExprType::Float64
                            )
                        {
                            *wire = WireExpr {
                                tp: Some(ExprType::Null as i32),
                                field_type: native_field_declaration(&FieldType::new(
                                    FieldTypeCode::LongLong,
                                )),
                                ..Default::default()
                            };
                        }
                    }
                }
                native_type(meta.return_type)?.1
            }
            ExprType::Null => FieldTypeCode::Null,
            ExprType::Int64 | ExprType::Uint64 => FieldTypeCode::LongLong,
            ExprType::Float32 => FieldTypeCode::Float,
            ExprType::Float64 => FieldTypeCode::Double,
            ExprType::String | ExprType::Bytes => FieldTypeCode::VarString,
            _ => return None,
        };
        let mut field = FieldType::new(code);
        if expr.tp() == ExprType::Uint64 {
            field.add_flags(FieldTypeFlags::UNSIGNED);
        }
        let declared = native_field_declaration(&field)?;
        if let Some(partial) = &mut expr.field_type {
            partial.tp = declared.tp;
            partial.flen = partial.flen.or(declared.flen);
            partial.decimal = partial.decimal.or(declared.decimal);
        } else {
            expr.field_type = Some(declared);
        }
    }
    Some(())
}

fn wire_cast_preserves_sql_contract(expr: &WireExpr) -> bool {
    use tidb_proto::tipb::ScalarFuncSig;

    let source = expr.children.first().and_then(|child| child.field_type.as_ref());
    match expr.sig() {
        // The typed request kernels retain SQL diagnostics and row order
        // when numeric conversions produce warnings.
        ScalarFuncSig::CastStringAsReal | ScalarFuncSig::CastJsonAsReal => false,
        // YEAR converts to a partial date that is valid for comparison.
        ScalarFuncSig::CastIntAsTime => source.is_none_or(|field| {
            field.tp != Some(i32::from(FieldTypeCode::Year.mysql_type()))
        }),
        _ => true,
    }
}

/// Go's typed Constant.VecEval forwards a deferred expression of the
/// requested evaluation domain. Parameter markers continue to bind anew.
pub(crate) fn batch_source<'a>(
    expression: &'a Expression,
    ctx: &dyn Columns,
) -> Result<Option<&'a Expression>, EvalError> {
    let mut source = expression;
    while let Expression::Constant(constant) = source {
        if constant.param_marker.is_some() {
            return Ok(None);
        }
        let Some(child) = constant.deferred_expr.as_deref() else {
            return Ok(None);
        };
        source = child;
    }
    if !matches!(source, Expression::ScalarFunction(_)) {
        return Ok(None);
    }
    if let Expression::Constant(constant) = expression {
        let target = constant.get_type(ctx)?.eval_type();
        if source
            .static_type()
            .is_none_or(|field| field.eval_type() != target)
        {
            return Ok(None);
        }
    }
    Ok(Some(source))
}

/// Compile a bound scalar on first demand from the public row API.
pub(crate) fn try_evaluate_direct_row(
    expression: &Expression,
    ctx: &dyn Columns,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Option<Datum>, EvalError> {
    if !matches!(expression, Expression::ScalarFunction(_)) {
        return Ok(None);
    }
    let Some(mut execution) = PreparedExpression::prepare(expression, ctx, row.len(), true)? else {
        return Ok(None);
    };
    #[cfg(test)]
    DIRECT_ROW_EXECUTIONS.with(|count| count.set(count.get() + 1));
    execution.evaluate_row(ctx, row).map(Some)
}

#[cfg(test)]
thread_local! {
    static DIRECT_ROW_EXECUTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn try_evaluate_row(
    expression: &Expression,
    ctx: &dyn Columns,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Option<Datum>, EvalError> {
    let Some(mut execution) = PreparedExpression::prepare(expression, ctx, row.len(), true)? else {
        return Ok(None);
    };
    execution.evaluate_row(ctx, row).map(Some)
}

fn statement_rand_source(argument: &Expression) -> Option<&Expression> {
    // The SQL binder may wrap a statement parameter in an implicit integer
    // cast. Keep its original datum so the first-demand path converts it once.
    if let Expression::ScalarFunction(cast) = argument {
        if cast.is_cast() && !cast.cast_in_union() {
            let [source] = cast.args.as_slice() else {
                return None;
            };
            if matches!(source, Expression::Constant(seed) if is_statement_seed(seed)) {
                return Some(source);
            }
        }
    }
    matches!(argument, Expression::Constant(seed) if is_statement_seed(seed)).then_some(argument)
}

// Some deferred values are built with ordinary row dependencies. Inspect
// the retained tree before treating its value as stable within a statement.
fn is_statement_seed(seed: &crate::constant::Constant) -> bool {
    if seed.param_marker.is_some() {
        return true;
    }
    let Some(deferred) = seed.deferred_expr.as_deref() else {
        return true;
    };
    let mut pending = vec![deferred];
    while let Some(expression) = pending.pop() {
        match expression {
            Expression::Constant(constant) => {
                if constant.param_marker.is_none() {
                    pending.extend(constant.deferred_expr.as_deref());
                }
            }
            Expression::ScalarFunction(function) => {
                let name = function.func_name.lowercase();
                if crate::expr_util::is_unfoldable_function(name)
                    || crate::expr_util::is_mutable_effects_function(name)
                {
                    return false;
                }
                pending.extend(&function.args);
            }
            Expression::Column(_)
            | Expression::CorrelatedColumn(_)
            | Expression::ScalarSubQuery(_) => return false,
        }
    }
    true
}

fn native_schema(expression: &Expression, count: usize) -> Option<Vec<FieldType>> {
    let mut fields = vec![None; count];
    let mut pending = vec![expression];
    while let Some(expression) = pending.pop() {
        match expression {
            Expression::ScalarFunction(call) => pending.extend(&call.args),
            Expression::Column(column) => {
                let field = column.get_static_type()?;
                if matches!(field.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
                    && field.elems().is_empty()
                {
                    return None;
                }
                let slot = fields.get_mut(usize::try_from(column.index).ok()?)?;
                if slot.as_ref().is_some_and(|existing| existing != field) {
                    return None;
                }
                *slot = Some(field.clone());
            }
            Expression::Constant(constant) if constant.param_marker.is_none() => {
                pending.extend(constant.deferred_expr.as_deref());
            }
            Expression::Constant(_)
            | Expression::CorrelatedColumn(_)
            | Expression::ScalarSubQuery(_) => {}
        }
    }
    Some(
        fields
            .into_iter()
            .map(|field| field.unwrap_or_else(|| FieldType::new(FieldTypeCode::Null)))
            .collect(),
    )
}

fn new_context(ctx: &dyn Columns, wire_flags: Option<u64>) -> EvalContext {
    use crate::context::ErrorLevel;
    let mut flag = Flag::ENABLE_SHORT_CIRCUIT_EXPRESSION;
    flag.set(
        Flag::IGNORE_TRUNCATE,
        ctx.truncate_level() == ErrorLevel::Ignore,
    );
    flag.set(
        Flag::TRUNCATE_AS_WARNING,
        ctx.truncate_level() == ErrorLevel::Warn,
    );
    let division = ctx.division_by_zero_level();
    if let Some(statement) = ctx.expression_statement_flags() {
        let policy_flags = Flag::IN_INSERT_STMT
            | Flag::IN_UPDATE_OR_DELETE_STMT
            | Flag::IN_SELECT_STMT
            | Flag::IN_LOAD_DATA_STMT
            | Flag::OVERFLOW_AS_WARNING;
        flag |= Flag::from_bits_truncate(statement) & policy_flags;
    } else {
        flag.set(Flag::IN_SELECT_STMT, division == ErrorLevel::Warn);
        flag.set(Flag::IN_UPDATE_OR_DELETE_STMT, division != ErrorLevel::Warn);
    }
    if wire_flags.is_none() {
        flag.set(
            Flag::DIVIDED_BY_ZERO_AS_WARNING,
            division == ErrorLevel::Warn,
        );
        if division != ErrorLevel::Warn {
            flag.insert(Flag::IN_UPDATE_OR_DELETE_STMT);
        }
    }
    let date = ctx.date_modes();
    let mut sql_mode = SqlMode::empty();
    sql_mode.set(
        SqlMode::STRICT_TRANS_TABLES,
        ctx.strict_sql_mode() || division == ErrorLevel::Error,
    );
    sql_mode.set(SqlMode::NO_ZERO_IN_DATE, date.no_zero_in_date);
    sql_mode.set(SqlMode::NO_ZERO_DATE, date.no_zero_date);
    sql_mode.set(SqlMode::INVALID_DATES, date.allow_invalid_dates);
    sql_mode.set(
        SqlMode::ERROR_FOR_DIVISION_BY_ZERO,
        division != ErrorLevel::Ignore,
    );
    let flag = wire_flags.map_or(flag, |wire| {
        let mut flag = Flag::from_bits_truncate(wire) | Flag::ENABLE_SHORT_CIRCUIT_EXPRESSION;
        if division != ErrorLevel::Warn {
            flag.insert(Flag::IN_UPDATE_OR_DELETE_STMT);
        }
        flag
    });
    let mut config = EvalConfig::from_flag(flag);
    if wire_flags.is_none() {
        // Hand every retained warning to TiDB's statement warning buffer.
        config.set_max_warning_cnt(usize::from(u16::MAX));
    }
    config.sql_mode = sql_mode;
    config.tz = match ctx.time_zone() {
        SessionTimeZone::Fixed { offset_secs, .. } => {
            Tz::from_offset(i64::from(offset_secs)).unwrap_or_else(Tz::utc)
        }
        SessionTimeZone::Named(tz) => Tz::from_tz_name(tz.name()).unwrap_or_else(Tz::utc),
        SessionTimeZone::Local => Tz::local(),
    };
    config.div_precision_increment = u8::try_from(ctx.div_precision_increment()).unwrap_or(30);
    EvalContext::new(Arc::new(config))
}

fn forward_query_warnings(ctx: &dyn Columns, query_context: &mut EvalContext) {
    query_context.warnings.warning_cnt = 0;
    for warning in query_context.warnings.warnings.drain(..) {
        ctx.append_warning(
            u16::try_from(warning.get_code()).unwrap_or(1105),
            warning
                .get_msg()
                .strip_prefix("evaluation failed: ")
                .unwrap_or_else(|| warning.get_msg()),
        );
    }
}

pub(crate) fn evaluate_value_kernel<T>(
    ctx: &dyn Columns,
    value: impl FnOnce(&mut EvalContext) -> tidb_query_common::Result<T>,
) -> Result<T, EvalError> {
    let mut query_context = new_context(ctx, None);
    let result = value(&mut query_context);
    forward_query_warnings(ctx, &mut query_context);
    result.map_err(query_error)
}

pub(crate) fn query_error(error: tidb_query_common::Error) -> EvalError {
    match *error.0 {
        ErrorInner::Evaluate(error)
            if error.code() == 3154 && error.to_string().contains("json_contains_path") =>
        {
            EvalError::Json(crate::context::JsonError::BadOneOrAllArg {
                function: "json_contains_path",
            })
        }
        ErrorInner::Evaluate(error) if error.code() == 3165 => {
            EvalError::Json(crate::context::JsonError::InvalidPathArrayCell)
        }
        ErrorInner::Evaluate(error) if error.code() == 1301 => {
            EvalError::AllowedPacketOverflowed(error.to_string())
        }
        ErrorInner::Evaluate(error) if error.code() == 1365 => EvalError::DivisionByZero,
        ErrorInner::Evaluate(error) if error.code() == 1210 => {
            EvalError::IncorrectArguments(error.to_string())
        }
        ErrorInner::Evaluate(error)
            if error.code() == 1690 && error.to_string() == "%s value is out of range in '%s'" =>
        {
            EvalError::Conversion(tidb_datatype::types_errors::ERR_OVERFLOW.clone())
        }
        ErrorInner::Evaluate(error) if error.code() == 8029 => {
            EvalError::Conversion(tidb_datatype::types_errors::ERR_BAD_NUMBER.clone())
        }
        ErrorInner::Evaluate(error) if error.code() == 1265 => {
            EvalError::Conversion(tidb_datatype::types_errors::ERR_TRUNCATED.clone())
        }
        ErrorInner::Evaluate(error) if error.code() == 1292 => {
            let message = error.to_string();
            if message.starts_with("Truncated incorrect BINARY value:") {
                EvalError::Conversion(
                    tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL.generate(message),
                )
            } else {
                EvalError::TruncatedWrongValue(message)
            }
        }
        ErrorInner::Evaluate(error) if error.code() == 1690 => {
            let message = error.to_string();
            let outside_range = message
                .split_once(" value is out of range in '")
                .and_then(|(kind, expression)| Some((kind, expression.strip_suffix('\'')?)));
            let data_type = outside_range.and_then(|(kind, _)| match kind {
                "BIGINT" => Some("BIGINT"),
                "BIGINT UNSIGNED" => Some("BIGINT UNSIGNED"),
                "DOUBLE" | "REAL" => Some("DOUBLE"),
                "DECIMAL" | "Decimal" => Some("DECIMAL"),
                _ => None,
            });
            match (data_type, outside_range) {
                (Some(value), Some((_, expression))) => EvalError::DataOutOfRange {
                    value,
                    expression: expression.to_owned(),
                },
                _ => EvalError::SharedEvaluation {
                    code: 1690,
                    message,
                },
            }
        }
        ErrorInner::Evaluate(error) => EvalError::SharedEvaluation {
            code: u16::try_from(error.code())
                .ok()
                .filter(|&code| code != 10000)
                .unwrap_or(1105),
            message: error.to_string(),
        },
        ErrorInner::Storage(error) => engine_error(error.to_string()),
    }
}

fn engine_error(message: String) -> EvalError {
    EvalError::SharedEvaluation {
        code: 1105,
        message,
    }
}

/// Native operations whose state belongs to the SQL session. Arguments
/// are already evaluated by the caller's RPN program.
struct HostCall {
    output: tipb::FieldType,
    arguments: Vec<FieldType>,
    operation: HostOperation,
}

enum HostOperation {
    LoadFile,
    LocalBuiltin(Box<PreparedLocalBuiltin>),
    InvalidArity(&'static str),
    BenchmarkCount { body_error: Option<EvalError> },
    DeferredValue(FieldType),
    CapturedRandom(Arc<crate::MysqlRng>),
    ClockDuration { utc: bool },
    DateArithmetic { subtract: bool, result: FieldType },
    SqlCast(tidb_ast::CastType),
    NumericCast(NativeEvalType),
    JsonCast { parse: bool },
    TemporalFsp(i64),
    RejectVectorCast,
    RejectVectorJsonCast,
    PrepareJsonSchema(Arc<crate::builtin_ext::json::JsonSchemaExecution>),
    ValidateJsonSchema(Arc<crate::builtin_ext::json::JsonSchemaExecution>),
    AdvisoryLockName,
}

impl HostOperation {
    fn propagates_null_before_next_operand(&self) -> bool {
        match self {
            Self::ValidateJsonSchema(_) => true,
            Self::LocalBuiltin(function) => {
                matches!(
                    function.name(),
                    "aes_encrypt"
                        | "aes_decrypt"
                        | "convert_using"
                        | "like"
                        | "ilike"
                        | "regexp"
                        | "regexp_like"
                        | "json_set"
                        | "json_insert"
                        | "json_replace"
                        | "json_merge"
                        | "greatest"
                        | "least"
                )
            }
            _ => false,
        }
    }
}

enum InputSource {
    Expression(Expression),
    HybridInteger(Expression),
    Context(Box<PreparedContextInput>),
}

struct InputBinding {
    source: InputSource,
    field: FieldType,
}

struct CompiledExpression {
    program: RpnExpression,
    batch_program: Option<RpnExpression>,
    bindings: Vec<InputBinding>,
    host_calls: Vec<HostCall>,
    diagnostics: Vec<ScalarFunction>,
    seed_keys: Vec<usize>,
}

struct LoweringEnvironment<'a> {
    input_types: &'a [FieldType],
    host: &'a dyn Columns,
    direct_row: bool,
    typed_batch: bool,
}

/// Lower the already resolved native expression through the SDK compiler.
fn compile(
    expression: &Expression,
    input_types: &[FieldType],
    ctx: &mut EvalContext,
    host: &dyn Columns,
    physical_input: bool,
    direct_row: bool,
    typed_batch: bool,
) -> Result<Option<CompiledExpression>, String> {
    let environment = LoweringEnvironment {
        input_types,
        host,
        direct_row,
        typed_batch,
    };
    let mut bindings = Vec::new();
    let mut host_calls = Vec::new();
    let mut diagnostic_calls = Vec::new();
    let mut seed_keys = Vec::new();
    let Some(tree) = lower_declared(
        expression,
        &environment,
        &mut bindings,
        &mut host_calls,
        &mut diagnostic_calls,
        &mut seed_keys,
    ) else {
        return Ok(None);
    };
    let physical_fields = if physical_input { input_types } else { &[] };
    let count = input_types.len() + bindings.len();
    let batch_program = engine::needs_scalar_guard(&tree, physical_fields)
        .then(|| engine::build(tree.clone(), ctx, count, physical_fields, false))
        .transpose()
        .map_err(|err| err.to_string())?;
    engine::build(tree, ctx, count, physical_fields, true)
        .map(|program| {
            Some(CompiledExpression {
                program,
                batch_program,
                bindings,
                host_calls,
                seed_keys,
                diagnostics: diagnostic_calls,
            })
        })
        .map_err(|err| err.to_string())
}

// A native-only tag identifies the source diagnostic. These arithmetic
// signatures have no wire metadata. The wire compiler never interprets this tag.
const NATIVE_DIAGNOSTIC_TAG: &[u8] = b"source";
const NULL_HOST_TAG: &[u8] = b"null";
const PATH_FIRST_HOST_TAG: &[u8] = b"paths";
const DATE_ARITHMETIC_TAG: &[u8] = b"date";
const REPEAT_DECLARATION: &[u8] = b"repeat";
const LAST_INSERT_ID_DECLARATION: &[u8] = b"last_insert_id";
const EXECUTION_SEED_DECLARATION: &[u8] = b"seed";

fn lower_leaf(expression: &Expression, input_types: &[FieldType]) -> Option<QueryExpr> {
    if let Expression::Column(column) = expression {
        let offset = usize::try_from(column.index).ok()?;
        let field = column
            .ret_type
            .as_ref()
            .or_else(|| input_types.get(offset))?;
        input_types.get(offset)?;
        return column_input(offset, field);
    }
    let Expression::Constant(_) = expression else {
        return None;
    };
    let zone = crate::NoColumns.time_zone();
    let converter = crate::expr_to_pb::PbConverter::new(&crate::NoColumns, false, &zone);
    let leaf = converter.expr_to_pb(expression)?;
    let mut declaration = QueryExpr::new();
    declaration.set_tp(engine::expr_type(leaf.tp() as i32)?);
    let carrier = match leaf.tp() {
        ExprType::Int64 | ExprType::Uint64 => Some((FieldTypeCode::LongLong, EvalType::Int)),
        ExprType::Float32 | ExprType::Float64 => Some((FieldTypeCode::Double, EvalType::Real)),
        _ => None,
    };
    declaration.set_field_type(rows::query_field_from_wire(leaf.field_type?));
    // Direct constructors can declare an operand type that differs from its
    // saved Datum. The instruction must decode the literal's actual carrier.
    if let Some((code, expected)) = carrier {
        if eval_type(&declaration) != Some(expected) {
            declaration
                .mut_field_type()
                .set_tp(i32::from(code.mysql_type()));
        }
    }
    declaration.set_val(leaf.val.unwrap_or_default());
    Some(declaration)
}

fn lower_declared(
    expression: &Expression,
    environment: &LoweringEnvironment<'_>,
    bindings: &mut Vec<InputBinding>,
    host_calls: &mut Vec<HostCall>,
    diagnostic_calls: &mut Vec<ScalarFunction>,
    seed_keys: &mut Vec<usize>,
) -> Option<QueryExpr> {
    let base_columns = environment.input_types.len();
    if matches!(expression, Expression::ScalarFunction(_)) {
        if let Some(constant) = expression.binary_literal_source() {
            let literal = Expression::Constant(constant.clone());
            return lower_leaf(&literal, environment.input_types);
        }
    }
    match expression {
        Expression::Constant(constant)
            if constant.param_marker.is_some()
                || matches!(
                    constant.literal_value(),
                    Some(Datum::Duration(_) | Datum::Time(_))
                ) =>
        {
            return bound_input(expression, base_columns + bindings.len(), bindings);
        }
        Expression::Constant(constant) if constant.deferred_expr.is_some() => {
            let child = constant.deferred_expr.as_deref()?;
            let output = expression.static_type()?.clone();
            let mut call = QueryExpr::new();
            call.set_tp(QueryExprType::ScalarFunc);
            call.set_field_type(rows::query_field_type(&output)?);
            call.mut_children().push(lower_declared(
                child,
                environment,
                bindings,
                host_calls,
                diagnostic_calls,
                seed_keys,
            )?);
            return bind_host_call(
                call,
                HostOperation::DeferredValue(output),
                vec![child.static_type()?.clone()],
                host_calls,
            );
        }
        Expression::CorrelatedColumn(_) => {
            return bound_input(expression, base_columns + bindings.len(), bindings);
        }
        _ => {}
    }
    if let Expression::Column(column) = expression {
        let offset = usize::try_from(column.index).ok()?;
        return column_input(offset, column.get_static_type()?);
    }
    let Expression::ScalarFunction(function) = expression else {
        return lower_leaf(expression, environment.input_types)
            .or_else(|| bound_input(expression, base_columns + bindings.len(), bindings));
    };
    if environment.direct_row && !binding::preserves_direct_row_contract(function) {
        return None;
    }
    let name = binding::local_name(function);
    let result_type = expression.static_type()?;
    let string_decimal_union = function.cast_in_union()
        && function.pb_code() == tidb_proto::tipb::ScalarFuncSig::CastStringAsDecimal;
    if function.is_cast()
        && ((function.cast_in_union() && !string_decimal_union)
            || result_type.code() == FieldTypeCode::Timestamp
            || function.args.first().and_then(Expression::static_type).is_some_and(|source| {
                source.is_hybrid() || source.eval_type() == NativeEvalType::VectorFloat32
            }))
    {
        // Preserve UNION's coercion and column materialization, temporal
        // assignment, and the SQL conversion of BIT, ENUM, SET and VECTOR.
        return None;
    }
    let typed_add_sub = crate::time_fn::add_sub::sign_of_signature(function.pb_code()).is_some();
    let date_arithmetic = function.date_arithmetic_subtract();
    if ((name == "addtime" || name == "subtime") && !typed_add_sub)
        || name == "validate_password_strength"
        || (matches!(name, "date_add" | "date_sub")
            || name.starts_with("date_add_")
            || name.starts_with("date_sub_"))
            && (!environment.typed_batch || date_arithmetic.is_none()
                || function.args.get(2)?.const_level() < crate::expression::ConstLevel::ONLY_IN_CONTEXT)
    {
        return None;
    }
    if name == "conv" && function.args.first().is_some_and(crate::scalar_function::conv_reads_bits) {
        // The SQL class reads the bit pattern before its implicit string cast.
        return None;
    }
    if name == "hex"
        && function.args.first().and_then(Expression::static_type).is_some_and(FieldType::is_hybrid)
    {
        // Read the member in its source charset before obtaining its bytes.
        return None;
    }
    let string_extremum = matches!(
        function.pb_code(),
        tidb_proto::tipb::ScalarFuncSig::GreatestString | tidb_proto::tipb::ScalarFuncSig::LeastString
    );
    if matches!(name, "greatest" | "least")
        && result_type.eval_type() == NativeEvalType::String
        && !string_extremum
    {
        return None;
    }
    if name == "values" && result_type.is_hybrid() {
        // A typed VALUES signature reads the original insert value using its
        // SQL domain, which need not match the declared column carrier.
        return None;
    }
    if function.args.iter().any(|argument| {
        matches!(argument, Expression::ScalarFunction(child)
            if child.func_name.lowercase() == "dayname")
    }) {
        // Native callers can ask DAYNAME for the weekday index. Its lowered
        // string result alone does not carry that implicit numeric interface.
        return None;
    }
    if matches!(name, "charset" | "collation" | "coercibility") {
        let [argument] = function.args.as_slice() else {
            return None;
        };
        let value = crate::collation_derive::info_metadata_value(name, argument)?;
        let constant = crate::constant::Constant::new(value, function.get_static_type()?.clone());
        return lower_leaf(&Expression::Constant(constant), environment.input_types);
    }
    if name == "name_const" && binding::function_signature(function).is_none() {
        if let [Expression::Constant(label), value] = function.args.as_slice() {
            if label.literal_value().is_some() && value.static_type() == expression.static_type() {
                return lower_declared(
                    value,
                    environment,
                    bindings,
                    host_calls,
                    diagnostic_calls,
                    seed_keys,
                );
            }
        }
    }
    let native_call = true;
    if native_call && matches!(name, "cast_signed" | "cast_unsigned") {
        if let [operand] = function.args.as_slice() {
            if let Some(constant) = operand.binary_literal_source() {
                return bind_value(
                    &Expression::Constant(constant.clone()),
                    base_columns + bindings.len(),
                    result_type.clone(),
                    true,
                    bindings,
                );
            }
        }
    }
    let signature = binding::function_signature(function);
    let lock = native_call
        .then(|| crate::scalar_function::advisory_lock_spec(name))
        .flatten();
    let trim_direction = match name {
        "ltrim_with" if native_call => Some(2_i64),
        "rtrim_with" if native_call => Some(3_i64),
        _ => None,
    };
    let candidates = match name {
        "json_merge_preserve" => registered_candidates("json_merge", function.args.len()),
        _ if trim_direction.is_some() => registered_candidates("trim", function.args.len() + 1),
        "json_member_of" => registered_candidates("json_memberof", function.args.len()),
        _ => registered_candidates(name, function.args.len()),
    };
    if signature.is_none()
        && candidates.is_none()
        && lock.is_none()
        && (function.args.is_empty()
            || matches!(name, "tidb_is_ddl_owner" | "values")
            || (matches!(
                name,
                "curtime"
                    | "current_time"
                    | "utc_time"
                    | "now"
                    | "current_timestamp"
                    | "localtime"
                    | "localtimestamp"
                    | "utc_timestamp"
            ) && matches!(function.args.as_slice(), [Expression::Constant(precision)]
                    if precision.literal_value().is_some())))
    {
        // Read session values, including the statement clock, on demand.
        return bound_input(expression, base_columns + bindings.len(), bindings);
    }
    let mut field_type = rows::query_field_type(result_type)?;
    let (charset, collation) = function.collation.charset_and_collation();
    if !matches!(name, "char_func" | "convert_using")
        && !result_type.is_binary_string()
        && !charset.is_empty()
        && !collation.is_empty()
    {
        field_type.set_charset(charset.to_owned());
        field_type.set_collate(tidb_datatype::collation_to_proto(collation));
    }
    let mut call = QueryExpr::new();
    call.set_field_type(field_type);
    if environment.typed_batch {
        if let Some(generator) = function.captured_random() {
            call.set_tp(QueryExprType::ScalarFunc);
            return bind_host_call(
                call,
                HostOperation::CapturedRandom(generator),
                Vec::new(),
                host_calls,
            );
        }
    }

    if native_call && function.rejects_vector_json_cast() {
        call.set_tp(QueryExprType::ScalarFunc);
        return bind_host_call(
            call,
            HostOperation::RejectVectorJsonCast,
            Vec::new(),
            host_calls,
        );
    }
    let convert_using_metadata = native_call
        && name == "convert_using"
        && (|| {
            let [_, Expression::Constant(target)] = function.args.as_slice() else {
                return false;
            };
            target.literal_value().is_some_and(|value| {
                value
                    .sql_string()
                    .is_ok_and(|name| name.eq_ignore_ascii_case(result_type.charset_name()))
            })
        })();
    let char_using_metadata = native_call
        && name == "char_func"
        && (|| {
            let [numbers @ .., Expression::Constant(charset)] = function.args.as_slice() else {
                return false;
            };
            if numbers.is_empty()
                || !tidb_charset::is_supported_encoding(result_type.charset_name())
            {
                return false;
            }
            let Some(charset) = charset.literal_value() else {
                return false;
            };
            (charset.is_null()
                || charset
                    .sql_string()
                    .is_ok_and(|name| name.eq_ignore_ascii_case(result_type.charset_name())))
                && numbers.iter().all(|arg| {
                    arg.static_type().is_some_and(|field| {
                        matches!(
                            field.eval_type(),
                            NativeEvalType::Int
                                | NativeEvalType::Real
                                | NativeEvalType::Decimal
                                | NativeEvalType::String
                        )
                    })
                })
        })();
    let aes = native_call && matches!(name, "aes_encrypt" | "aes_decrypt");
    let operand_count = if let Some((_, arity)) = lock {
        if function.args.len() == arity {
            arity
        } else {
            0
        }
    } else if (char_using_metadata || convert_using_metadata) && !needs_sql_value_kernel(name) {
        function.args.len() - 1
    } else if aes {
        crate::builtin_ext::crypto::aes_operand_count(
            environment.host.block_encryption_mode(),
            function.args.len(),
        )
        .unwrap_or(0)
    } else {
        function.args.len()
    };
    let weight_input = (native_call && name == "weight_string")
        .then(|| function.weight_string_input_required())
        .flatten();
    if weight_input == Some(false) {
        call.set_tp(QueryExprType::Null);
        return Some(call);
    }
    call.set_tp(QueryExprType::ScalarFunc);
    if let Some(signature) = signature {
        call.set_sig(scalar_func_sig(signature as i32)?);
    }
    if native_call && function.cast_in_union() && !string_decimal_union {
        engine::bind_cast_in_union(&mut call).ok()?;
    }
    let seed_source = if name == "rand" {
        function.args.first().and_then(statement_rand_source)
    } else {
        None
    };
    let numeric_domain = native_call
        .then(|| function.numeric_operand_domain())
        .flatten();
    let comparison_domain =
        if native_call && matches!(name, "eq" | "ne" | "nulleq" | "lt" | "le" | "gt" | "ge") {
            if function.args.iter().filter_map(Expression::static_type).any(FieldType::is_hybrid) {
                // Compare the SQL member name and its collation, including
                // original bytes for transient ENUM/SET columns.
                return None;
            }
            comparison_domain(&function.args)
        } else {
            None
        };
    let benchmark = native_call && name == "benchmark";
    let benchmark_body_error = if benchmark {
        let [_, body] = function.args.as_slice() else {
            return None;
        };
        crate::func::ensure_benchmark_eval_type(body.static_type()).err()
    } else {
        None
    };
    let vector_extremum = native_call && function.native_vector_extremum().is_some();
    let vector_operands =
        vector_extremum || native_call && function.native_vector_operator().is_some();
    for (index, arg) in function.args.iter().take(operand_count).enumerate() {
        if index == 1 && benchmark_body_error.is_some() {
            let mut body = QueryExpr::new();
            body.set_tp(QueryExprType::Null);
            body.set_field_type(rows::query_field_type(&FieldType::new(
                FieldTypeCode::Null,
            ))?);
            call.mut_children().push(body);
            continue;
        }
        let arg = if index == 0 {
            seed_source.unwrap_or(arg)
        } else {
            arg
        };
        let field = arg.static_type()?;
        let cast;
        let arg = if vector_operands && field.eval_type() != NativeEvalType::VectorFloat32 {
            cast = crate::wrap_cast::wrap_with_cast_as_vector_float32(
                &crate::wrap_cast::ColumnsBuildContext(environment.host),
                arg.clone(),
            )
            .ok()?;
            &cast
        } else {
            arg
        };
        let mut lowered = if numeric_domain.is_some()
            && matches!(field.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
            && !matches!(arg, Expression::ScalarFunction(_))
        {
            let field = FieldType::new(FieldTypeCode::LongLong)
                .with_added_flags(field.flags() & FieldTypeFlags::UNSIGNED);
            bind_value(arg, base_columns + bindings.len(), field, true, bindings)?
        } else {
            lower_declared(
                arg,
                environment,
                bindings,
                host_calls,
                diagnostic_calls,
                seed_keys,
            )?
        };
        if native_call
            && is_binary_literal(arg)
            && (numeric_domain.is_some()
                || comparison_domain.is_some_and(|kind| kind != NativeEvalType::String)
                || matches!(name, "oct" | "abs")
                || arg.static_type()?.eval_type() == NativeEvalType::Int)
        {
            let unsigned = numeric_domain.is_none_or(|kind| kind != NativeEvalType::Int)
                || arg.static_type()?.is_unsigned();
            let constant = arg.binary_literal_source()?;
            let literal = constant.literal_value()?;
            let Datum::BinaryLiteral(literal) = literal else {
                return None;
            };
            let value = literal.to_int();
            let ty = FieldType::new(FieldTypeCode::LongLong).with_added_flags(if unsigned {
                FieldTypeFlags::UNSIGNED
            } else {
                0
            });
            if value.is_truncated() {
                lowered = bind_value(
                    &Expression::Constant(constant.clone()),
                    base_columns + bindings.len(),
                    ty,
                    true,
                    bindings,
                )?;
            } else {
                let datum = if unsigned {
                    Datum::UInt(value.value())
                } else {
                    Datum::Int(value.value() as i64)
                };
                let constant = Expression::Constant(crate::constant::Constant::new(datum, ty));
                lowered = lower_leaf(&constant, environment.input_types)?;
            }
        }
        let domain = numeric_domain.or(comparison_domain).or_else(|| {
            native_call
                .then(|| unary_operand_domain(function))
                .flatten()
        });
        if native_call
            && field.eval_type() == NativeEvalType::Decimal
            && matches!(
                name,
                "bitand" | "bitor" | "bitxor" | "leftshift" | "rightshift" | "bitneg"
            )
        {
            lowered = bind_host_operand(
                lowered,
                HostOperation::NumericCast(NativeEvalType::Int),
                field.clone(),
                &FieldType::new(FieldTypeCode::LongLong),
                host_calls,
            )?;
        }
        if let Some(required) = domain.map(query_eval_type) {
            let actual = eval_type(&lowered)?;
            if actual != required {
                if let Some((domain, result)) = domain
                    .filter(|domain| numeric_domain.is_some() || *domain == NativeEvalType::Real)
                    .filter(|_| {
                        matches!(
                            actual,
                            EvalType::Bytes
                                | EvalType::Json
                                | EvalType::DateTime
                                | EvalType::Duration
                        )
                    })
                    .and_then(|domain| arithmetic_cast_target(field, domain))
                {
                    lowered = bind_host_operand(
                        lowered,
                        HostOperation::NumericCast(domain),
                        field.clone(),
                        &result,
                        host_calls,
                    )?;
                } else {
                    lowered = make_cast(lowered, actual, required)?;
                }
                if is_binary_literal(arg) && !arg.static_type()?.is_unsigned() {
                    let field = lowered.mut_field_type();
                    field.set_flag(field.get_flag() & !FieldTypeFlags::UNSIGNED);
                }
            }
        }
        if native_call {
            let condition = match name {
                "if" => index == 0,
                "case" => index % 2 == 0 && index + 1 < function.args.len(),
                "and" | "or" | "xor" => true,
                _ => false,
            };
            if condition {
                if eval_type(&lowered) == Some(EvalType::Json) {
                    lowered = bind_host_operand(
                        lowered,
                        HostOperation::NumericCast(NativeEvalType::Real),
                        field.clone(),
                        &FieldType::new(FieldTypeCode::Double),
                        host_calls,
                    )?;
                }
                lowered = truth_operand(lowered)?;
            } else if matches!(name, "if" | "case" | "coalesce" | "ifnull") {
                lowered = bind_selection_result(lowered, result_type)?;
            }
        }
        call.mut_children().push(lowered);
    }
    if let Some(subtract) = date_arithmetic.filter(|_| environment.typed_batch) {
        let fields = function.args.iter()
            .map(|arg| arg.static_type().cloned())
            .collect::<Option<Vec<_>>>()?;
        return bind_host_call(
            call,
            HostOperation::DateArithmetic { subtract, result: result_type.clone() },
            fields,
            host_calls,
        );
    }
    use tidb_proto::tipb::ScalarFuncSig::{CurrentTime1Arg, UtcTimeWithArg};
    if environment.typed_batch && matches!(signature, Some(CurrentTime1Arg | UtcTimeWithArg)) {
        return bind_host_call(
            call,
            HostOperation::ClockDuration { utc: signature == Some(UtcTimeWithArg) },
            vec![function.args.first()?.static_type()?.clone()],
            host_calls,
        );
    }
    if environment.typed_batch
        && signature == Some(tidb_proto::tipb::ScalarFuncSig::LastInsertIdWithId)
    {
        call.set_sig(ScalarFuncSig::Unspecified);
        call.set_val(LAST_INSERT_ID_DECLARATION.to_vec());
        return Some(call);
    }
    if native_call && name == "json_schema_valid" {
        let [schema, document] = function.args.as_slice() else {
            return None;
        };
        let ready = FieldType::new(FieldTypeCode::LongLong);
        let validation = Arc::new(crate::builtin_ext::json::JsonSchemaExecution::new(
            schema.const_level() >= crate::expression::ConstLevel::ONLY_IN_CONTEXT,
        ));
        let schema = bind_host_operand(
            call.mut_children().remove(0),
            HostOperation::PrepareJsonSchema(Arc::clone(&validation)),
            schema.static_type()?.clone(),
            &ready,
            host_calls,
        )?;
        call.mut_children().insert(0, schema);
        return bind_host_call(
            call,
            HostOperation::ValidateJsonSchema(validation),
            vec![ready, document.static_type()?.clone()],
            host_calls,
        );
    }
    if let Some((name, arity)) = lock {
        let mut arguments = function.args[..operand_count]
            .iter()
            .map(|arg| arg.static_type().cloned())
            .collect::<Option<Vec<_>>>()?;
        if let Some(field) = arguments.first_mut() {
            let normalized = FieldType::new(FieldTypeCode::VarString);
            let validate_name = bind_host_operand(
                call.mut_children().remove(0),
                HostOperation::AdvisoryLockName,
                field.clone(),
                &normalized,
                host_calls,
            )?;
            *field = normalized;
            call.mut_children().insert(0, validate_name);
        }
        let operation = if arity != operand_count {
            HostOperation::InvalidArity(name)
        } else {
            HostOperation::LocalBuiltin(Box::new(PreparedLocalBuiltin::new(function)))
        };
        return bind_host_call(call, operation, arguments, host_calls);
    }
    if aes {
        if operand_count == 0 {
            let name = if name == "aes_encrypt" {
                "aes_encrypt"
            } else {
                "aes_decrypt"
            };
            return bind_host_call(
                call,
                HostOperation::InvalidArity(name),
                Vec::new(),
                host_calls,
            );
        }
        if operand_count == 2 && function.args.len() == 3 {
            // ECB warns about an ignored IV only when the data and key were
            // demanded successfully. The IV expression is never scheduled.
            let mut ignored = QueryExpr::new();
            ignored.set_tp(QueryExprType::Null);
            ignored.set_field_type(rows::query_field_type(&FieldType::new(
                FieldTypeCode::VarString,
            ))?);
            call.mut_children().push(ignored);
        }
    }
    if native_call && name == "interval" && function.args.len() >= 2 {
        call.set_sig(ScalarFuncSig::IntervalInt);
        return Some(call);
    }
    if benchmark {
        let [count, _] = function.args.as_slice() else {
            return None;
        };
        let count = bind_host_operand(
            call.mut_children().remove(0),
            HostOperation::BenchmarkCount {
                body_error: benchmark_body_error,
            },
            count.static_type()?.clone(),
            &FieldType::new(FieldTypeCode::LongLong),
            host_calls,
        )?;
        call.mut_children().insert(0, count);
        call.set_sig(ScalarFuncSig::Unspecified);
        call.set_val(REPEAT_DECLARATION.to_vec());
        return Some(call);
    }
    if let Some(direction) = trim_direction {
        let input = crate::constant::Constant::new(
            Datum::Int(direction),
            FieldType::new(FieldTypeCode::LongLong),
        );
        call.mut_children().push(lower_leaf(
            &Expression::Constant(input),
            environment.input_types,
        )?);
    }
    if let [argument] = function.args.as_slice() {
        let source = argument.static_type()?;
        if native_call && matches!(name, "cast_double" | "cast_float") {
            if let Some(literal) = argument
                .binary_literal_source()
                .and_then(|literal| literal.literal_value())
            {
                let Datum::BinaryLiteral(literal) = literal else {
                    return None;
                };
                let number = literal.to_int().value() as f64;
                return lower_leaf(
                    &Expression::Constant(crate::constant::Constant::new(
                        Datum::Real(number),
                        result_type.clone(),
                    )),
                    environment.input_types,
                );
            }
            if name == "cast_double"
                && matches!(
                    source.eval_type(),
                    NativeEvalType::String | NativeEvalType::Json
                )
            {
                return bind_host_call(
                    call,
                    HostOperation::NumericCast(NativeEvalType::Real),
                    vec![source.clone()],
                    host_calls,
                );
            }
        }
        if string_decimal_union
            || matches!(
                signature,
                Some(tidb_proto::tipb::ScalarFuncSig::CastJsonAsDuration | tidb_proto::tipb::ScalarFuncSig::CastDurationAsJson)
            )
        {
            return bind_host_call(
                call,
                HostOperation::LocalBuiltin(Box::new(PreparedLocalBuiltin::new(function))),
                vec![source.clone()],
                host_calls,
            );
        }
        if native_call
            && name == "cast_json"
            && (source.eval_type() == NativeEvalType::String
                || source.code() == FieldTypeCode::Year)
        {
            return bind_host_call(
                call,
                HostOperation::JsonCast {
                    parse: result_type.has_flag(FieldTypeFlags::PARSE_TO_JSON),
                },
                vec![source.clone()],
                host_calls,
            );
        }
        if let Some(target) = native_call
            .then(|| sql_cast_override(name, source, result_type))
            .flatten()
        {
            return bind_host_call(
                call,
                HostOperation::SqlCast(target),
                vec![source.clone()],
                host_calls,
            );
        }
    }
    let execution_seed = seed_source.is_some();
    let local_kernel = typed_add_sub || string_extremum || needs_sql_value_kernel(name)
        || (name == "minus"
            && native_call
            && environment.host.no_unsigned_subtraction()
            && result_type.eval_type() == NativeEvalType::Int
            && function
                .args
                .iter()
                .any(|arg| arg.static_type().is_some_and(FieldType::is_unsigned)))
        || local_typed_kernel(function)
        || matches!(name, "to_binary" | "from_binary");
    let selected = if execution_seed {
        let seed = call.mut_children().pop()?;
        let kind = eval_type(&seed)?;
        let key = function.rand_occurrence()?;
        let slot = seed_keys
            .iter()
            .position(|&existing| existing == key)
            .unwrap_or_else(|| {
                seed_keys.push(key);
                seed_keys.len() - 1
            });
        call.set_sig(ScalarFuncSig::Unspecified);
        call.set_val(
            [
                EXECUTION_SEED_DECLARATION,
                &u64::try_from(slot).ok()?.to_le_bytes(),
            ]
            .concat(),
        );
        call.mut_children().push(if kind == EvalType::Bytes {
            bind_host_operand(
                seed,
                HostOperation::SqlCast(tidb_ast::CastType::Signed),
                seed_source?.static_type()?.clone(),
                &FieldType::new(FieldTypeCode::LongLong),
                host_calls,
            )?
        } else if kind == EvalType::Int {
            seed
        } else {
            make_cast(seed, kind, EvalType::Int)?
        });
        Some(std::mem::take(&mut call))
    } else if native_call && local_kernel {
        None
    } else if signature.is_some() {
        // An already decoded native PB expression retains its selected
        // signature. Bind its operands through the same declaration rules.
        resolve_rpn_function(call.get_sig(), call.get_field_type(), call.get_children())
            .ok()
            .and_then(|meta| {
                if meta.validate(&call).is_ok() {
                    Some(std::mem::take(&mut call))
                } else {
                    let coerced = coerce_call(&call, meta.argument_types)?;
                    meta.validate(&coerced).ok()?;
                    Some(coerced)
                }
            })
    } else {
        candidates.and_then(|candidates| select_call(&mut call, function, candidates))
    };
    if let Some(mut call) = selected {
        if native_call && numeric_domain.is_some() {
            let slot = u64::try_from(diagnostic_calls.len()).ok()?;
            let mut metadata = NATIVE_DIAGNOSTIC_TAG.to_vec();
            metadata.extend(slot.to_le_bytes());
            call.set_val(metadata);
            diagnostic_calls.push(function.clone());
        }
        if native_call
            && name == "coalesce"
            && matches!(
                result_type.eval_type(),
                NativeEvalType::Duration | NativeEvalType::Datetime | NativeEvalType::Timestamp
            )
        {
            return bind_host_operand(
                call,
                HostOperation::TemporalFsp(result_type.decimal().max(0)),
                result_type.clone(),
                result_type,
                host_calls,
            );
        }
        return Some(call);
    }
    if function.args.is_empty() && !local_kernel {
        return bound_input(expression, base_columns + bindings.len(), bindings);
    }
    let (operation, arguments) = match function.args.as_slice() {
        [value] if native_call && name == "load_file" => {
            (HostOperation::LoadFile, vec![value.static_type()?.clone()])
        }
        [value] if native_call && name == "cast_vector" => (
            HostOperation::RejectVectorCast,
            vec![value.static_type()?.clone()],
        ),
        args if weight_input == Some(true)
            || local_kernel
            || vector_operands
            || function.accepts_eager_values() =>
        {
            (
                HostOperation::LocalBuiltin(Box::new(PreparedLocalBuiltin::new(function))),
                args.iter()
                    .map(|arg| arg.static_type().cloned())
                    .collect::<Option<Vec<_>>>()?,
            )
        }
        _ => return None,
    };
    bind_host_call(call, operation, arguments, host_calls)
}

// These SQL conversions carry the statement clock or retain TiDB's
// diagnostics and declared output widths.
fn sql_cast_override(
    name: &str,
    source: &FieldType,
    result: &FieldType,
) -> Option<tidb_ast::CastType> {
    use tidb_ast::CastType;
    let length = u32::try_from(result.flen()).ok();
    let fsp = u32::try_from(result.decimal()).ok();
    Some(match name {
        "cast_signed"
            if matches!(
                source.eval_type(),
                NativeEvalType::String | NativeEvalType::Real | NativeEvalType::Decimal
            ) =>
        {
            CastType::Signed
        }
        "cast_unsigned"
            if matches!(
                source.eval_type(),
                NativeEvalType::String
                    | NativeEvalType::Int
                    | NativeEvalType::Real
                    | NativeEvalType::Decimal
            ) =>
        {
            CastType::Unsigned
        }
        "cast_decimal" => CastType::Decimal {
            flen: length.unwrap_or(0),
            scale: fsp.unwrap_or(crate::cast::UNSPECIFIED_CAST_SCALE),
        },
        // The storage CAST renders the exact Decimal coefficient, which can
        // retain more fractional digits than the protocol result display.
        "cast_char" if source.eval_type() == NativeEvalType::Decimal => return None,
        "cast_char" => CastType::Char {
            len: length,
            charset: Some(if result.is_binary_string() {
                "BINARY".to_owned()
            } else {
                result.charset_name().to_owned()
            }),
        },
        "cast_binary" => CastType::Binary {
            len: length.filter(|_| result.code() == FieldTypeCode::String),
        },
        "cast_date" => CastType::Date,
        "cast_datetime" => CastType::DateTime { fsp },
        "cast_time" => CastType::Time { fsp },
        "cast_vector" => CastType::Vector { dimensions: length },
        "cast_float" => CastType::Float,
        _ => return None,
    })
}

fn local_typed_kernel(function: &ScalarFunction) -> bool {
    let name = function.func_name.lowercase();
    let source = function
        .args
        .first()
        .and_then(Expression::static_type)
        .map(FieldType::eval_type);
    let collation = function.derived_collation();
    matches!(
        (name, source),
        (
            "round" | "truncate",
            Some(NativeEvalType::Decimal | NativeEvalType::Int | NativeEvalType::Real)
        ) | (
            "hour" | "minute" | "second" | "microsecond",
            Some(
                NativeEvalType::String
                    | NativeEvalType::Int
                    | NativeEvalType::Decimal
                    | NativeEvalType::Real
            )
        )
    ) || matches!(name, "greatest" | "least")
        && (function.get_static_type().is_some_and(|field| {
            matches!(
                field.eval_type(),
                NativeEvalType::String | NativeEvalType::Datetime | NativeEvalType::Timestamp
            )
        }) || collation != tidb_charset::Collation::Binary)
}

// The baseline callbacks for these signatures omit TiDB's per-statement
// packet policy or use different character/NULL conventions.
fn needs_sql_value_kernel(name: &str) -> bool {
    binding::uses_native_real_math(name) || matches!(
        name,
        "aes_encrypt"
            | "aes_decrypt"
            | "char_func"
            | "convert_using"
            | "insert_func"
            | "lpad"
            | "rpad"
            | "space"
            | "to_base64"
            | "from_base64"
            | "ord"
            | "repeat"
            | "password"
            | "compress"
            | "uncompress"
            | "extract"
            | "like"
            | "ilike"
            | "regexp"
            | "regexp_like"
            | "json_overlaps"
            | "json_set"
            | "json_insert"
            | "json_replace"
            | "json_merge_patch"
            | "is_ipv4"
            | "is_ipv4_compat"
            | "is_ipv4_mapped"
            | "is_ipv6"
            | "conv"
            | "date"
            | "instr"
            | "uuid"
            | "uuid_timestamp"
            | "sysdate"
            | "str_to_date"
            | "timediff"
            | "convert_tz"
            | "json_extract"
            | "json_length"
            | "json_remove"
            | "json_contains"
            | "json_contains_path"
            | "json_type"
            | "json_pretty"
            | "json_storage_free"
            | "json_storage_size"
            | "json_quote"
            | "json_unquote"
            | "json_object"
            | "json_merge_preserve"
            | "json_merge"
            | "day"
            | "dayofmonth"
            | "weekday"
            | "weekofyear"
            | "utc_timestamp"
            | "week"
            | "year"
            | "yearweek"
            | "month"
            | "monthname"
            | "quarter"
            | "unix_timestamp"
            | "makedate"
            | "last_day"
            | "from_days"
            | "maketime"
            | "json_array_append"
            | "json_array_insert"
            | "json_search"
            | "find_in_set"
    )
}

pub(crate) fn json_modify_operand_order(argc: usize) -> impl Iterator<Item = usize> {
    std::iter::once(0).chain((1..argc).step_by(2)).chain((2..argc).step_by(2))
}

fn bind_host_operand(
    operand: QueryExpr,
    operation: HostOperation,
    input: FieldType,
    output: &FieldType,
    host_calls: &mut Vec<HostCall>,
) -> Option<QueryExpr> {
    let mut call = QueryExpr::new();
    call.set_tp(QueryExprType::ScalarFunc);
    call.set_field_type(rows::query_field_type(output)?);
    call.mut_children().push(operand);
    bind_host_call(call, operation, vec![input], host_calls)
}

fn bind_host_call(
    mut call: QueryExpr,
    operation: HostOperation,
    arguments: Vec<FieldType>,
    host_calls: &mut Vec<HostCall>,
) -> Option<QueryExpr> {
    let slot = u64::try_from(host_calls.len()).ok()?;
    host_calls.push(HostCall {
        output: call.get_field_type().clone(),
        arguments,
        operation,
    });
    call.set_sig(ScalarFuncSig::Unspecified);
    let mut metadata = Vec::new();
    let operation = &host_calls[slot as usize].operation;
    if matches!(operation, HostOperation::LocalBuiltin(function) if function.reads_json_paths_first()) {
        metadata.extend_from_slice(PATH_FIRST_HOST_TAG);
    } else if matches!(operation, HostOperation::DateArithmetic { .. }) {
        metadata.extend_from_slice(DATE_ARITHMETIC_TAG);
    } else if operation.propagates_null_before_next_operand() {
        metadata.extend_from_slice(NULL_HOST_TAG);
    }
    metadata.extend_from_slice(&slot.to_le_bytes());
    call.set_val(metadata);
    Some(call)
}

fn is_binary_literal(expression: &Expression) -> bool {
    expression.binary_literal_source().is_some()
}

// Some Go constructors all return the same type and derive an input domain
// before selecting the typed overload. Their output alone cannot break a tie.
fn unary_operand_domain(function: &ScalarFunction) -> Option<NativeEvalType> {
    use NativeEvalType::*;
    let [operand] = function.args.as_slice() else {
        return None;
    };
    let name = function.func_name.lowercase();
    let input = operand.static_type()?.eval_type();
    Some(match (name, input) {
        ("not", Datetime | Timestamp | Duration) => Int,
        ("not", String) | ("abs", String) => Real,
        (
            "istrue" | "isfalse" | "istrue_with_null",
            Datetime | Timestamp | Duration | Json | String,
        ) => Real,
        ("hex", Int | Real | Decimal) => Int,
        ("hex", String | Datetime | Timestamp | Duration | Json) => String,
        ("oct", _) if is_binary_literal(operand) => Int,
        ("oct", Int) => Int,
        ("oct", _) => String,
        _ => return None,
    })
}

fn arithmetic_cast_target(
    source: &FieldType,
    domain: NativeEvalType,
) -> Option<(NativeEvalType, FieldType)> {
    let field = match domain {
        NativeEvalType::Int if source.eval_type() != NativeEvalType::String => {
            FieldType::new(FieldTypeCode::LongLong)
        }
        NativeEvalType::Decimal => binding::decimal_cast_field_type(source),
        NativeEvalType::Real => crate::builtin_arithmetic::real_argument_type(source).into_owned(),
        _ => return None,
    };
    Some((domain, field))
}

fn comparison_domain(args: &[Expression]) -> Option<NativeEvalType> {
    use crate::builtin_compare::{get_accurate_cmp_type, CmpOperand};

    let [lhs, rhs] = args else { return None };
    fn operand(expr: &Expression) -> Option<CmpOperand<'_>> {
        Some(CmpOperand {
            field_type: expr.static_type()?,
            is_constant: matches!(expr, Expression::Constant(_)),
            is_column: matches!(expr, Expression::Column(_)),
        })
    }
    let left = operand(lhs)?;
    let right = operand(rhs)?;
    let type_for_numeric_literal = |expr: &Expression, other: CmpOperand<'_>| {
        is_binary_literal(expr) && other.field_type.eval_type() == NativeEvalType::Int
    };
    if type_for_numeric_literal(lhs, right) || type_for_numeric_literal(rhs, left) {
        return Some(NativeEvalType::Int);
    }
    Some(get_accurate_cmp_type(left, right))
}

fn query_eval_type(kind: NativeEvalType) -> EvalType {
    match kind {
        NativeEvalType::Int => EvalType::Int,
        NativeEvalType::Real => EvalType::Real,
        NativeEvalType::Decimal => EvalType::Decimal,
        NativeEvalType::String => EvalType::Bytes,
        NativeEvalType::Datetime | NativeEvalType::Timestamp => EvalType::DateTime,
        NativeEvalType::Duration => EvalType::Duration,
        NativeEvalType::Json => EvalType::Json,
        NativeEvalType::VectorFloat32 => EvalType::VectorFloat32,
    }
}

fn bound_input(
    expression: &Expression,
    offset: usize,
    bindings: &mut Vec<InputBinding>,
) -> Option<QueryExpr> {
    bind_value(
        expression,
        offset,
        expression.static_type()?.clone(),
        false,
        bindings,
    )
}

fn bind_value(
    expression: &Expression,
    offset: usize,
    field: FieldType,
    hybrid_integer: bool,
    bindings: &mut Vec<InputBinding>,
) -> Option<QueryExpr> {
    let column = column_input(offset, &field)?;
    let source = match expression {
        Expression::ScalarFunction(function) => {
            InputSource::Context(Box::new(PreparedContextInput::new(function)))
        }
        _ if hybrid_integer => InputSource::HybridInteger(expression.clone()),
        _ => InputSource::Expression(expression.clone()),
    };
    bindings.push(InputBinding { source, field });
    Some(column)
}

fn column_input(offset: usize, field: &FieldType) -> Option<QueryExpr> {
    let mut offset_bytes = Vec::new();
    tidb_codec::encode_int(&mut offset_bytes, i64::try_from(offset).ok()?);
    let mut column = QueryExpr::new();
    column.set_tp(QueryExprType::ColumnRef);
    column.set_field_type(rows::query_field_type(field)?);
    column.set_val(offset_bytes);
    Some(column)
}

fn native_field_declaration(field: &FieldType) -> Option<tidb_proto::tipb::FieldType> {
    if field.code() == FieldTypeCode::Set {
        // The native SET supplies the name and bits in memory. Both fit the
        // SDK's hybrid input carrier while wire SET decoding stays disabled.
        return binding::field_type_to_pb(&field.clone().with_code(FieldTypeCode::Enum));
    }
    let mut declaration = binding::field_type_to_pb(field)?;
    if field.code() == FieldTypeCode::String && field.is_binary_string() {
        declaration.flen = Some(field.flen().min(i64::from(i32::MAX)) as i32);
    }
    Some(declaration)
}

fn registered_candidates(name: &str, argc: usize) -> Option<&'static [ScalarFuncSig]> {
    let index = signatures::SIGNATURES
        .binary_search_by_key(&name, |&(name, _, _, _)| name)
        .ok()?;
    let (_, minimum, maximum, signatures) = signatures::SIGNATURES[index];
    (minimum <= argc && maximum.is_none_or(|maximum| argc <= maximum)).then_some(signatures)
}

fn select_call(
    call: &mut QueryExpr,
    function: &ScalarFunction,
    candidates: &[ScalarFuncSig],
) -> Option<QueryExpr> {
    let return_type = eval_type(call)?;
    let name = function.func_name.lowercase();
    let extremum = if matches!(name, "greatest" | "least") {
        Some(crate::rewriter::result_type::gl_signature(&function.args)?)
    } else {
        None
    };
    let field_domain = (name == "field").then(|| {
        let domains = function
            .args
            .iter()
            .filter_map(Expression::static_type)
            .map(FieldType::eval_type);
        if domains.clone().all(|kind| kind == NativeEvalType::String) {
            EvalType::Bytes
        } else if domains.clone().all(|kind| kind == NativeEvalType::Int) {
            EvalType::Int
        } else {
            EvalType::Real
        }
    });
    let mut exact = None;
    let mut converted = None;
    let mut converted_ambiguous = false;
    let date_result = function
        .get_static_type()
        .is_some_and(|field| field.code() == FieldTypeCode::Date);
    for &sig in candidates {
        if !matches_signedness(sig, call)
            || !matches_text_mode(sig, function)
            || (sig == ScalarFuncSig::StrToDateDate && !date_result)
            || (sig == ScalarFuncSig::StrToDateDatetime && date_result)
        {
            continue;
        }
        call.set_sig(sig);
        let Ok(meta) = resolve_rpn_function(sig, call.get_field_type(), call.get_children()) else {
            continue;
        };
        // No-read overloads must be selected from source declarations before
        // lowering children, as ADDTIME does above. A compatible carrier alone
        // cannot justify discarding a SQL operand.
        if meta.return_type != return_type || matches!(meta.argument_types, RpnArgTypes::Unused) {
            continue;
        }
        if let Some(expected) = field_domain.or(extremum.map(|mode| query_eval_type(mode.arg_type)))
        {
            if !matches!(meta.argument_types, RpnArgTypes::Uniform(actual) if actual == expected) {
                continue;
            }
        }
        if extremum.is_some_and(|mode| !matches_extremum(sig, mode)) {
            continue;
        }

        if meta.validate(call).is_ok() {
            if exact.replace(sig).is_some() {
                return None;
            }
        } else if let Some(candidate) = coerce_call(call, meta.argument_types) {
            if meta.validate(&candidate).is_ok() {
                converted_ambiguous |= converted.replace(candidate).is_some();
            }
        }
    }
    if let Some(sig) = exact {
        call.set_sig(sig);
        Some(std::mem::take(call))
    } else if converted_ambiguous {
        None
    } else {
        converted
    }
}

fn matches_extremum(signature: ScalarFuncSig, mode: crate::builtin_ext::GlSignature) -> bool {
    use crate::builtin_ext::GlCmpStringMode;
    use ScalarFuncSig::*;
    match signature {
        GreatestCmpStringAsDate | LeastCmpStringAsDate => {
            mode.cmp_string_mode == GlCmpStringMode::AsDate
        }
        GreatestCmpStringAsTime | LeastCmpStringAsTime => {
            mode.cmp_string_mode == GlCmpStringMode::AsDatetime
        }
        GreatestString | LeastString => mode.cmp_string_mode == GlCmpStringMode::Directly,
        GreatestDate | LeastDate => mode.ret_date,
        GreatestTime | LeastTime => !mode.ret_date,
        _ => true,
    }
}

#[derive(Clone, Copy)]
enum TextModeSource {
    First,
    FirstOrBit,
    FirstOrThird,
    Result,
    CallCollation,
}

fn matches_text_mode(signature: ScalarFuncSig, function: &ScalarFunction) -> bool {
    let Some(&(_, source, binary)) = signatures::TEXT_MODES
        .iter()
        .find(|&&(sig, _, _)| sig == signature)
    else {
        return true;
    };
    let argument = |index| function.args.get(index).and_then(Expression::static_type);
    let binary_argument = |index| argument(index).is_some_and(FieldType::is_binary_string);
    let actual = match source {
        TextModeSource::First => binary_argument(0),
        TextModeSource::FirstOrBit => {
            binary_argument(0)
                || argument(0).is_some_and(|field| field.code() == FieldTypeCode::Bit)
        }
        TextModeSource::FirstOrThird => binary_argument(0) || binary_argument(2),
        TextModeSource::Result => function
            .get_static_type()
            .is_some_and(FieldType::is_binary_string),
        TextModeSource::CallCollation => {
            let (_, collation) = function.collation.charset_and_collation();
            collation == "binary"
        }
    };
    binary == actual
}

fn matches_signedness(signature: ScalarFuncSig, call: &QueryExpr) -> bool {
    let unsigned = |expr: &QueryExpr| expr.get_field_type().as_accessor().is_unsigned();
    signatures::RESULT_SIGNEDNESS
        .iter()
        .find(|&&(sig, _)| sig == signature)
        .is_none_or(|&(_, expected)| unsigned(call) == expected)
        && signatures::ARGUMENT_SIGNEDNESS
            .iter()
            .find(|&&(sig, _)| sig == signature)
            .is_none_or(|&(_, expected)| {
                call.get_children()
                    .iter()
                    .map(unsigned)
                    .take(expected.len())
                    .eq(expected.iter().copied())
            })
}

fn eval_type(expr: &QueryExpr) -> Option<EvalType> {
    EvalType::try_from(expr.get_field_type().as_accessor().tp()).ok()
}

fn coerce_call(call: &QueryExpr, types: RpnArgTypes) -> Option<QueryExpr> {
    let args = call.get_children();
    let mut coerced = call.clone();
    for (index, arg) in args.iter().enumerate() {
        let mut json_value = false;
        let required = match types {
            RpnArgTypes::Fixed(types) if types.len() == args.len() => types[index],
            RpnArgTypes::Uniform(required) => required,
            RpnArgTypes::OptionalTail(types) => *types.get(index)?,
            RpnArgTypes::Repeated {
                prefix,
                group,
                prefix_json_values,
                json_values,
            } => {
                if let Some(&required) = prefix.get(index) {
                    json_value = prefix_json_values;
                    required
                } else {
                    json_value = json_values;
                    group[(index - prefix.len()) % group.len()]
                }
            }
            _ => return None,
        };
        let actual = eval_type(arg)?;
        if actual != required {
            // A document slot accepts JSON, non-binary text or SQL NULL.
            // JSON constructor values may instead lift other SQL carriers.
            if required == EvalType::Json && !json_value {
                let field = arg.get_field_type();
                let sql_null = field.get_tp() == i32::from(FieldTypeCode::Null.mysql_type());
                if !sql_null && (actual != EvalType::Bytes || field.get_charset() == "binary") {
                    return None;
                }
            }
            if call.get_sig() == ScalarFuncSig::JsonQuoteSig {
                return None;
            }
            let mut cast = make_cast(arg.clone(), actual, required)?;
            if json_value && required == EvalType::Json {
                let field = cast.mut_field_type();
                field.set_flag(field.get_flag() & !FieldTypeFlags::PARSE_TO_JSON);
            }
            coerced.mut_children()[index] = cast;
        }
    }
    Some(coerced)
}

// Go uses WrapWithIsTrue on control conditions, so a fractional value
// must be tested for zero without first rounding it to an integer.
fn truth_operand(mut arg: QueryExpr) -> Option<QueryExpr> {
    let kind = eval_type(&arg)?;
    let sig = match kind {
        EvalType::Int => return Some(arg),
        EvalType::Decimal => ScalarFuncSig::DecimalIsTrueWithNull,
        EvalType::Real => ScalarFuncSig::RealIsTrueWithNull,
        _ => {
            arg = make_cast(arg, kind, EvalType::Real)?;
            ScalarFuncSig::RealIsTrueWithNull
        }
    };
    unary_call(sig, &FieldType::new(FieldTypeCode::LongLong), arg)
}

fn bind_selection_result(mut arg: QueryExpr, result: &FieldType) -> Option<QueryExpr> {
    let field = rows::query_field_type(result)?;
    let expected = EvalType::try_from(field.as_accessor().tp()).ok()?;
    if arg.get_tp() == QueryExprType::Null {
        arg.set_field_type(field);
        return Some(arg);
    }
    let actual = eval_type(&arg)?;
    let target = result.eval_type();
    let (source, _) = native_type(actual)?;
    match target {
        NativeEvalType::Decimal if actual != EvalType::Decimal => {
            // Keep the merged control result scale for numeric widening.
            let sig = binding::cast_signature(source, target)??;
            unary_call(scalar_func_sig(sig as i32)?, result, arg)
        }
        NativeEvalType::Json if actual != EvalType::Json => {
            // Go's merged JSON declaration consumes text as a document and
            // promotes an integer branch, including predicates, as a number.
            let field = arg.mut_field_type();
            field.set_flag(field.get_flag() & !FieldTypeFlags::IS_BOOLEAN);
            let result = result
                .clone()
                .with_added_flags(if source == NativeEvalType::String {
                    FieldTypeFlags::PARSE_TO_JSON
                } else {
                    0
                });
            let sig = binding::cast_signature(source, target)??;
            unary_call(scalar_func_sig(sig as i32)?, &result, arg)
        }
        _ if actual == expected => Some(arg),
        _ => make_cast(arg, actual, expected),
    }
}

fn make_cast(arg: QueryExpr, from: EvalType, to: EvalType) -> Option<QueryExpr> {
    use binding::{decimal_cast_field_type, duration_cast_field_type, temporal_cast_field_type};

    let (from, _) = native_type(from)?;
    let (to, code) = native_type(to)?;
    // A native ENUM or SET and its string name share the SQL domain but
    // occupy different SDK carriers.
    let sig = binding::cast_signature(from, to)?
        .or_else(|| binding::explicit_cast_signature(from, to))?;
    let declaration = arg.get_field_type();
    let source = FieldType::new(FieldTypeCode::from_mysql_type(
        u8::try_from(declaration.get_tp()).ok()?,
    ))
    .with_flags(declaration.get_flag())
    .with_flen(i64::from(declaration.get_flen()))
    .with_decimal(i64::from(declaration.get_decimal()));
    let field = match to {
        NativeEvalType::Decimal => decimal_cast_field_type(&source),
        NativeEvalType::Real => crate::builtin_arithmetic::real_argument_type(&source).into_owned(),
        NativeEvalType::Int => FieldType::new(code)
            .with_flen(source.flen())
            .with_decimal(0)
            .with_charset_name("binary")
            .with_collation_name("binary")
            .with_added_flags(
                FieldTypeFlags::BINARY
                    | source.flags() & (FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL),
            ),
        NativeEvalType::Datetime | NativeEvalType::Timestamp => {
            temporal_cast_field_type(&source, FieldType::new(code))
        }
        NativeEvalType::Duration => duration_cast_field_type(&source),
        NativeEvalType::Json => FieldType::new(code)
            .with_flen(12_582_912)
            .with_charset_name("utf8mb4")
            .with_collation_name("utf8mb4_bin")
            .with_flags(
                FieldTypeFlags::BINARY
                    | if from == NativeEvalType::String {
                        FieldTypeFlags::PARSE_TO_JSON
                    } else {
                        0
                    },
            ),
        _ => FieldType::new(code),
    };
    unary_call(scalar_func_sig(sig as i32)?, &field, arg)
}

fn unary_call(sig: ScalarFuncSig, field: &FieldType, arg: QueryExpr) -> Option<QueryExpr> {
    let mut call = QueryExpr::new();
    call.set_tp(QueryExprType::ScalarFunc);
    call.set_field_type(rows::query_field_type(field)?);
    call.set_sig(sig);
    call.mut_children().push(arg);
    Some(call)
}

fn native_type(kind: EvalType) -> Option<(NativeEvalType, FieldTypeCode)> {
    Some(match kind {
        EvalType::Int => (NativeEvalType::Int, FieldTypeCode::LongLong),
        EvalType::Real => (NativeEvalType::Real, FieldTypeCode::Double),
        EvalType::Decimal => (NativeEvalType::Decimal, FieldTypeCode::NewDecimal),
        EvalType::Bytes => (NativeEvalType::String, FieldTypeCode::VarString),
        EvalType::DateTime => (NativeEvalType::Datetime, FieldTypeCode::Datetime),
        EvalType::Duration => (NativeEvalType::Duration, FieldTypeCode::Duration),
        EvalType::Json => (NativeEvalType::Json, FieldTypeCode::Json),
        EvalType::VectorFloat32 => (NativeEvalType::VectorFloat32, FieldTypeCode::VectorFloat32),
        EvalType::Enum => (NativeEvalType::String, FieldTypeCode::Enum),
        EvalType::Set => (NativeEvalType::String, FieldTypeCode::Set),
    })
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldTypeCode, MySqlDuration};
    use tidb_query_datatype::codec::{
        batch::LazyBatchColumnVec,
        data_type::{ScalarValueRef, VectorValue},
    };
    use tipb::FieldType as QueryFieldType;

    use super::*;
    use crate::{column::Column, scalar_function::ScalarFunction};

    fn build_function(
        name: CiString,
        result_type: FieldType,
        args: Vec<Expression>,
    ) -> ScalarFunction {
        let expression = crate::new_function::new_function_base(
            &crate::NoColumns,
            name.lowercase(),
            result_type,
            args,
        )
        .unwrap();
        let Expression::ScalarFunction(function) = expression else {
            panic!("{name:?} should be a scalar function");
        };
        function
    }

    #[test]
    fn public_row_eval_executes_the_bound_integer_program() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&integer), 1);
        input.append_int64(0, 7);
        let expression = Expression::ScalarFunction(build_function(
            CiString::new("plus"),
            integer.clone(),
            vec![
                Expression::Column(Column::new(0, integer.clone())),
                Expression::Constant(crate::constant::Constant::new(Datum::Int(2), integer)),
            ],
        ));
        DIRECT_ROW_EXECUTIONS.with(|count| count.set(0));
        assert_eq!(
            expression.eval(&crate::NoColumns, input.get_row(0)),
            Ok(Datum::Int(9))
        );
        DIRECT_ROW_EXECUTIONS.with(|count| assert_eq!(count.get(), 1));

        let literal = |value| {
            Expression::Constant(crate::constant::Constant::new(
                Datum::Int(value),
                FieldType::new(FieldTypeCode::LongLong),
            ))
        };
        let virtual_row = Expression::ScalarFunction(build_function(
            CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![literal(7), literal(2)],
        ));
        assert_eq!(
            crate::eval_expression_once(&virtual_row, &crate::NoColumns),
            Ok(Datum::Int(9))
        );
        DIRECT_ROW_EXECUTIONS.with(|count| assert_eq!(count.get(), 2));
    }

    #[test]
    fn quiet_integer_batch_preserves_the_scalar_overflow_contract() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&integer), 2);
        input.append_int64(0, 7);
        input.append_int64(0, i64::MAX);
        let increment = Expression::ScalarFunction(build_function(
            CiString::new("plus"),
            integer.clone(),
            vec![
                Expression::Column(Column::new(0, integer.clone())),
                Expression::Constant(crate::constant::Constant::new(Datum::Int(1), integer)),
            ],
        ));
        let mut prepared = PreparedExpression::prepare(&increment, &crate::NoColumns, 1, false)
            .unwrap()
            .unwrap();
        let mut presence = vec![false];
        assert!(!prepared.append_quiet_int_presence(&crate::NoColumns, &input, &mut presence));
        assert_eq!(presence, [false]);
        assert_eq!(
            prepared.evaluate_row(&crate::NoColumns, input.get_row(0)),
            Ok(Datum::Int(8))
        );
        assert!(matches!(
            prepared.evaluate_row(&crate::NoColumns, input.get_row(1)),
            Err(EvalError::DataOutOfRange { .. })
        ));

        input.reset();
        input.append_int64(0, 7);
        input.append_datum(0, &Datum::Null);
        assert!(prepared.append_quiet_int_presence(&crate::NoColumns, &input, &mut presence));
        assert_eq!(presence, [false, true, false]);
    }

    #[test]
    fn quiet_integer_batch_discards_only_speculative_warnings() {
        struct Warnings(RefCell<Vec<u16>>);

        impl Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }

            fn division_by_zero_level(&self) -> crate::context::ErrorLevel {
                crate::context::ErrorLevel::Warn
            }

            fn append_warning(&self, code: u16, _: &str) {
                self.0.borrow_mut().push(code);
            }
        }

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&integer), 2);
        input.append_int64(0, 2);
        input.append_int64(0, 0);
        let modulo = Expression::ScalarFunction(build_function(
            CiString::new("mod"),
            integer.clone(),
            vec![
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(7),
                    integer.clone(),
                )),
                Expression::Column(Column::new(0, integer)),
            ],
        ));
        let ctx = Warnings(RefCell::new(Vec::new()));
        let mut prepared = PreparedExpression::prepare(&modulo, &ctx, 1, false)
            .unwrap()
            .unwrap();
        let mut presence = Vec::new();
        assert!(!prepared.append_quiet_int_presence(&ctx, &input, &mut presence));
        assert!(presence.is_empty());
        assert!(ctx.0.borrow().is_empty());
        assert_eq!(
            prepared.evaluate_row(&ctx, input.get_row(0)),
            Ok(Datum::Int(1))
        );
        assert_eq!(
            prepared.evaluate_row(&ctx, input.get_row(1)),
            Ok(Datum::Null)
        );
        assert_eq!(*ctx.0.borrow(), [1365]);
    }

    #[test]
    fn a_retained_rand_parameter_is_read_and_converted_once_per_execution() {
        use std::cell::Cell;

        struct ParameterContext {
            value: Datum,
            reads: Cell<usize>,
            warnings: RefCell<Vec<u16>>,
        }

        impl Columns for ParameterContext {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }

            fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
                assert_eq!(order, 0);
                self.reads.set(self.reads.get() + 1);
                Ok(self.value.clone())
            }

            fn truncate_level(&self) -> crate::context::ErrorLevel {
                crate::context::ErrorLevel::Warn
            }

            fn append_warning(&self, code: u16, _: &str) {
                self.warnings.borrow_mut().push(code);
            }
        }

        let mut seed = crate::constant::Constant::new(
            Datum::new_string_datum("0"),
            FieldType::new(FieldTypeCode::VarString),
        );
        seed.param_marker = Some(crate::constant::ParamMarker { order: 0 });
        let rand = Expression::ScalarFunction(build_function(
            CiString::new("rand"),
            FieldType::new(FieldTypeCode::Double),
            vec![Expression::Constant(seed)],
        ));
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut chunk = Chunk::new(&[integer], 2, 2);
        for _ in 0..2 {
            chunk.append_int64(0, 1);
        }

        for (value, seed, warnings) in [
            ("7.9", 7, &[1292][..]),
            ("9007199254740993", 9_007_199_254_740_993, &[][..]),
        ] {
            let ctx = ParameterContext {
                value: Datum::new_string_datum(value),
                reads: Cell::new(0),
                warnings: RefCell::new(Vec::new()),
            };
            let mut execution = PreparedExpression::new(&rand, &ctx, 1).unwrap().unwrap();
            assert_eq!(ctx.reads.get(), 0);
            let expected = crate::MysqlRng::new_with_seed(seed);
            for row in 0..2 {
                assert_eq!(
                    execution.evaluate_row(&ctx, chunk.get_row(row)).unwrap(),
                    Datum::Real(expected.gen()),
                );
            }
            assert_eq!(ctx.reads.get(), 1, "{rand:?}");
            assert_eq!(ctx.warnings.borrow().as_slice(), warnings);
        }
    }

    #[test]
    fn direct_typed_rows_reuse_the_statement_stream_for_each_seed_call() {
        use std::collections::HashMap;

        struct RowContext(RefCell<HashMap<usize, tidb_exprctx::context::StatementSeededRandom>>);

        impl Columns for RowContext {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }

            fn rand_seeded_next(&self, _: usize, _: i64) -> Option<f64> {
                panic!("the typed row must use the program seed stream")
            }

            fn statement_seeded_random(
                &self,
                call: usize,
            ) -> Option<tidb_exprctx::context::StatementSeededRandom> {
                Some(self.0.borrow_mut().entry(call).or_default().clone())
            }
        }

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let real = FieldType::new(FieldTypeCode::Double);
        let rand = |seed| {
            Expression::ScalarFunction(build_function(
                CiString::new("rand"),
                real.clone(),
                vec![Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(seed),
                    integer.clone(),
                ))],
            ))
        };
        let flag = Expression::Column(Column::new(0, integer.clone()));
        let expression = Expression::ScalarFunction(build_function(
            CiString::new("if"),
            real.clone(),
            vec![flag, rand(7), rand(11)],
        ));
        let mut input = Chunk::new(&[integer], 2, 2);
        input.append_int64(0, 1);
        input.append_int64(0, 0);

        let context = RowContext(RefCell::new(HashMap::new()));
        let clone = expression.clone();
        let first = crate::MysqlRng::new_with_seed(7);
        let second = crate::MysqlRng::new_with_seed(11);
        for tree in [&expression, &clone] {
            for _ in 0..2 {
                assert_eq!(
                    try_evaluate_row(tree, &context, input.get_row(0)),
                    Ok(Some(Datum::Real(first.gen())))
                );
                assert_eq!(
                    try_evaluate_row(tree, &context, input.get_row(1)),
                    Ok(Some(Datum::Real(second.gen())))
                );
            }
        }
        assert_eq!(context.0.borrow().len(), 2);
    }

    #[test]
    fn generated_bindings_distinguish_aliases_from_configured_semantics() {
        assert_eq!(
            registered_candidates("ceil", 1),
            registered_candidates("ceiling", 1)
        );
        assert!(registered_candidates("ceil", 1)
            .unwrap()
            .contains(&ScalarFuncSig::CeilReal));
        assert!(registered_candidates("ceil", 2).is_none());
        let expected = [
            (
                "istrue",
                ScalarFuncSig::IntIsTrue,
                [Some(0), Some(0), Some(1)],
            ),
            (
                "isfalse",
                ScalarFuncSig::IntIsFalse,
                [Some(0), Some(1), Some(0)],
            ),
            (
                "istrue_with_null",
                ScalarFuncSig::IntIsTrueWithNull,
                [None, Some(0), Some(1)],
            ),
        ];
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut input = Chunk::new(std::slice::from_ref(&integer), 3, 3);
        input.append_null(0);
        input.append_int64(0, 0);
        input.append_int64(0, -2);
        for (name, sig, results) in expected {
            let candidates = registered_candidates(name, 1).unwrap();
            assert!(candidates.contains(&sig));
            assert!(!expected
                .iter()
                .any(|(_, other, _)| *other != sig && candidates.contains(other)));
            let expression = Expression::ScalarFunction(build_function(
                CiString::new(name),
                integer.clone(),
                vec![Expression::Column(Column::new(0, integer.clone()))],
            ));
            let mut program = PreparedExpression::new(&expression, &crate::NoColumns, 1)
                .unwrap()
                .unwrap();
            let mut values = Vec::new();
            program
                .evaluate_rows(&crate::NoColumns, &input, &[0, 1, 2], true, |value| {
                    values.push(value.as_int());
                    Ok(())
                })
                .unwrap();
            assert_eq!(values, results);
        }
        let compare = registered_candidates("eq", 2).unwrap();
        assert!(compare.contains(&ScalarFuncSig::EqInt));
        assert!(!compare.contains(&ScalarFuncSig::GtInt));
        let add = registered_candidates("date_add", 3).unwrap();
        let sub = registered_candidates("date_sub", 3).unwrap();
        assert!(add.contains(&ScalarFuncSig::AddDateDatetimeInt));
        assert!(sub.contains(&ScalarFuncSig::SubDateDatetimeInt));
        assert!(!add.contains(&ScalarFuncSig::SubDateDatetimeInt));
        assert!(!sub.contains(&ScalarFuncSig::AddDateDatetimeInt));
        assert!(registered_candidates("cast", 1)
            .unwrap()
            .contains(&ScalarFuncSig::CastIntAsReal));
    }

    #[test]
    fn selects_existing_kernels_outside_the_pushdown_catalog() {
        let real = FieldType::new(FieldTypeCode::Double);
        let mut input = Chunk::new(std::slice::from_ref(&real), 2, 2);
        input.append_float64(0, 0.0);
        input.append_null(0);
        let column = Expression::Column(Column::new(0, real.clone()));
        for (name, expected) in [("tan", 0.0), ("degrees", 0.0), ("sqrt", 0.0)] {
            let expression = Expression::ScalarFunction(build_function(
                CiString::new(name),
                real.clone(),
                vec![column.clone()],
            ));
            let mut program = PreparedExpression::new(&expression, &crate::NoColumns, 1)
                .unwrap()
                .unwrap();
            let mut values = Vec::new();
            program
                .evaluate_rows(&crate::NoColumns, &input, &[1, 0], true, |value| {
                    values.push(value);
                    Ok(())
                })
                .unwrap();
            assert_eq!(values, [Datum::Null, Datum::Real(expected)], "{name}");
        }
    }

    #[test]
    fn wide_json_column_compiles_with_its_native_declaration() {
        let json = FieldType::new(FieldTypeCode::Json).with_flen(i64::from(u32::MAX));
        let wire = binding::field_type_to_pb(&json).expect("Go narrows the width");
        assert_eq!(wire.flen(), -1);
        let mut chunk = Chunk::new(std::slice::from_ref(&json), 2, 2);
        chunk.append_json(0, &tidb_datatype::BinaryJSON::parse(r#"{"a":1}"#).unwrap());
        chunk.append_null(0);
        let expression = Expression::ScalarFunction(build_function(
            CiString::new("json_type"),
            FieldType::new(FieldTypeCode::VarString),
            vec![Expression::Column(Column::new(0, json))],
        ));
        let mut execution = PreparedExpression::new(&expression, &crate::NoColumns, 1)
            .unwrap()
            .expect("a stored JSON column enters shared evaluation");
        assert_eq!(
            execution
                .evaluate_row(&crate::NoColumns, chunk.get_row(0))
                .unwrap()
                .sql_string()
                .unwrap(),
            "OBJECT"
        );
        assert_eq!(
            execution
                .evaluate_row(&crate::NoColumns, chunk.get_row(1))
                .unwrap(),
            Datum::Null
        );
    }

    #[test]
    fn interval_evaluates_only_visited_boundaries_in_physical_row_order() {
        let integer =
            FieldType::new(FieldTypeCode::LongLong).with_added_flags(FieldTypeFlags::NOT_NULL);
        let literal = |value| {
            Expression::Constant(crate::constant::Constant::new(
                Datum::Int(value),
                integer.clone(),
            ))
        };
        let call = |name, args| {
            Expression::ScalarFunction(build_function(CiString::new(name), integer.clone(), args))
        };
        let mut chunk = Chunk::new(std::slice::from_ref(&integer), 2, 2);
        chunk.append_int64(0, 25);
        chunk.append_int64(0, 35);
        let expression = call(
            "interval",
            vec![
                Expression::Column(Column::new(0, integer.clone())),
                call("intdiv", vec![literal(1), literal(0)]),
                literal(20),
                literal(30),
            ],
        );
        let mut execution = PreparedExpression::new(&expression, &crate::NoColumns, 1)
            .unwrap()
            .expect("INTERVAL uses the shared search program");
        let mut output = Vec::new();
        execution
            .evaluate_rows(&crate::NoColumns, &chunk, &[1, 0, 1], true, |value| {
                output.push(value);
                Ok(())
            })
            .unwrap();
        assert_eq!(output, [Datum::Int(3), Datum::Int(2), Datum::Int(3)]);
    }

    #[test]
    fn fractional_control_conditions_test_zero_without_rounding() {
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(2)
            .with_decimal(1);
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let literal = |value| {
            Expression::Constant(crate::constant::Constant::new(
                Datum::Int(value),
                integer.clone(),
            ))
        };
        let mut chunk = Chunk::new(std::slice::from_ref(&decimal), 3, 3);
        chunk.append_datum(
            0,
            &Datum::Decimal(tidb_datatype::Decimal::from_literal("0.1")),
        );
        chunk.append_datum(
            0,
            &Datum::Decimal(tidb_datatype::Decimal::from_literal("0.0")),
        );
        chunk.append_null(0);
        for name in ["if", "case"] {
            let expression = Expression::ScalarFunction(build_function(
                CiString::new(name),
                integer.clone(),
                vec![
                    Expression::Column(Column::new(0, decimal.clone())),
                    literal(1),
                    literal(2),
                ],
            ));
            let mut prepared = PreparedExpression::new(&expression, &crate::NoColumns, 1)
                .unwrap()
                .unwrap();
            let mut values = Vec::new();
            prepared
                .evaluate_rows(&crate::NoColumns, &chunk, &[2, 0, 1], true, |value| {
                    values.push(value);
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                values,
                [Datum::Int(2), Datum::Int(1), Datum::Int(2)],
                "{name}"
            );
        }
    }

    #[test]
    fn compiles_nested_typed_time_with_existing_sdk_kernels() {
        let flag = FieldType::new(FieldTypeCode::LongLong);
        let time = FieldType::new(FieldTypeCode::Duration).with_decimal(3);
        let native_types = [flag, time.clone(), time.clone()];
        let column = |index: usize| {
            let mut column = Column::new(index as i64, native_types[index].clone());
            column.index = index as i64;
            Expression::Column(column)
        };
        let conditional = Expression::ScalarFunction(build_function(
            CiString::new("if"),
            time,
            vec![column(0), column(1), column(2)],
        ));
        let expression = Expression::ScalarFunction(build_function(
            CiString::new("hour"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![conditional],
        ));

        let mut ctx = EvalContext::default();
        let program = compile(
            &expression,
            &native_types,
            &mut ctx,
            &crate::NoColumns,
            false,
            false,
            false,
        )
        .unwrap()
        .unwrap();
        let mut chunk = Chunk::new(&native_types, 3, 3);
        let time =
            |hours: i64| MySqlDuration::from_nanoseconds(hours * 3_600_000_000_000, 3).unwrap();
        for (flag, left, right) in [
            (Some(1), 1, Some(4)),
            (Some(0), 2, Some(5)),
            (None, 3, None),
        ] {
            match flag {
                Some(flag) => chunk.append_int64(0, flag),
                None => chunk.append_null(0),
            }
            chunk.append_duration(1, time(left));
            match right {
                Some(right) => chunk.append_duration(2, time(right)),
                None => chunk.append_null(2),
            }
        }
        DIRECT_ROW_EXECUTIONS.with(|count| count.set(0));
        for (index, expected) in [Datum::Int(1), Datum::Int(5), Datum::Null]
            .into_iter()
            .enumerate()
        {
            assert_eq!(
                expression.eval(&crate::NoColumns, chunk.get_row(index)),
                Ok(expected)
            );
        }
        DIRECT_ROW_EXECUTIONS.with(|count| assert_eq!(count.get(), 3));

        let input = rows::NativeRows {
            chunk: &chunk,
            fields: &native_types,
            column_paths: &[],
            bindings: &program.bindings,
            host_calls: &program.host_calls,
            seed_keys: &program.seed_keys,
            statement_seeds: None,
            execution_seeds: &[],
            diagnostics: &[],
            binding_error: RefCell::new(None),
            decimals: RefCell::default(),
            context: &crate::NoColumns,
            block_encryption_mode: Default::default(),
            mode: rows::ReadMode::PhysicalBatch,
            scalar_operands: false,
            typed_batch: false,
        };
        let schema = [engine::index_field()];
        let physical = LazyBatchColumnVec::from(vec![
            tidb_query_datatype::codec::data_type::VectorValue::Int(
                vec![Some(0), Some(1), Some(2)].into(),
            ),
        ]);
        let values = engine::evaluate(
            &program.program,
            &mut ctx,
            &schema,
            &input,
            &physical,
            &[2, 0, 1],
        )
        .unwrap();
        let output = FieldType::new(FieldTypeCode::LongLong);
        let values = (0..3)
            .map(|index| {
                rows::from_query_value(values.get_logical_scalar_ref(index), &output).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            [
                tidb_datatype::Datum::Null,
                tidb_datatype::Datum::Int(1),
                tidb_datatype::Datum::Int(5),
            ]
        );
    }

    #[test]
    fn branch_converts_only_demanded_bit_rows() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let bit = FieldType::new(FieldTypeCode::Bit).with_flen(64);
        let field_types = [integer.clone(), bit.clone()];
        let column = |field: FieldType, offset: i64| {
            let mut column = Column::new(offset, field);
            column.index = offset;
            Expression::Column(column)
        };
        let expression = Expression::ScalarFunction(build_function(
            CiString::new("if"),
            integer.clone(),
            vec![
                column(integer.clone(), 0),
                Expression::Constant(crate::constant::Constant::new(Datum::Int(7), integer)),
                column(bit, 1),
            ],
        ));
        let mut chunk = Chunk::new_chunk_with_capacity(&field_types, 2);
        for flag in [1, 0] {
            chunk.append_int64(0, flag);
            chunk.append_datum(1, &Datum::Bit(vec![1; 9].into()));
        }
        let mut prepared = PreparedExpression::new(&expression, &crate::NoColumns, 2)
            .unwrap()
            .unwrap();
        assert_eq!(
            prepared
                .evaluate_row(&crate::NoColumns, chunk.get_row(0))
                .unwrap(),
            Datum::Int(7),
        );
        let error = prepared
            .evaluate_row(&crate::NoColumns, chunk.get_row(1))
            .unwrap_err();
        assert!(format!("{error:?}").contains("BIT column value exceeds 64 bits"));
        assert_eq!(
            prepared
                .evaluate_row(&crate::NoColumns, chunk.get_row(0))
                .unwrap(),
            Datum::Int(7),
        );
    }

    #[test]
    fn integer_projection_reuses_physical_columns_with_a_flat_upstream_program() {
        use tidb_query_expr::RpnExpressionNode::{ColumnRef, FnCall};
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let col = |index: i64| {
            let mut col = Column::new(index, ty.clone());
            col.index = index;
            Expression::Column(col)
        };
        let plus = |a, b| {
            Expression::ScalarFunction(build_function(
                CiString::new("plus"),
                ty.clone(),
                vec![a, b],
            ))
        };
        let fields = [ty.clone(), ty.clone()];
        let mut input = Chunk::new(&fields, 3, 3);
        for (dense, j) in [(Some(2), 3), (None, 5), (Some(7), 11)] {
            match dense {
                Some(value) => input.append_int64(0, value),
                None => input.append_null(0),
            };
            input.append_int64(1, j);
        }
        for (expression, offsets, expected) in [
            (
                plus(col(0), col(1)),
                vec![1, 2],
                vec![Datum::Int(18), Datum::Null, Datum::Int(5), Datum::Int(18)],
            ),
            (
                plus(plus(col(0), col(1)), col(1)),
                vec![1, 2, 2],
                vec![Datum::Int(29), Datum::Null, Datum::Int(8), Datum::Int(29)],
            ),
        ] {
            let mut prepared = PreparedExpression::new(&expression, &crate::NoColumns, 2)
                .unwrap()
                .unwrap();
            let nodes = prepared.program.as_ref();
            assert_eq!(
                nodes
                    .iter()
                    .filter_map(|node| match node {
                        ColumnRef { offset } => Some(*offset),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                offsets
            );
            assert_eq!(
                nodes
                    .iter()
                    .filter(|node| matches!(node, FnCall { args_len: 2, .. }))
                    .count(),
                offsets.len() - 1
            );
            assert_eq!(nodes.len(), offsets.len() * 2 - 1);
            let mut result = Vec::new();
            prepared
                .evaluate_rows(&crate::NoColumns, &input, &[2, 1, 0, 2], true, |value| {
                    result.push(value);
                    Ok(())
                })
                .unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn repeated_native_text_and_decimal_columns_keep_selected_row_order() {
        let int = FieldType::new(FieldTypeCode::LongLong);
        let text = FieldType::new(FieldTypeCode::VarString);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(8)
            .with_decimal(3);
        let column = |offset, field: &FieldType| {
            let mut column = Column::new(offset, field.clone());
            column.index = offset;
            Expression::Column(column)
        };
        let call = |name, result: &FieldType, args| {
            Expression::ScalarFunction(build_function(CiString::new(name), result.clone(), args))
        };
        let len = || call("length", &int, vec![column(0, &text)]);
        let byte_count = call("plus", &int, vec![len(), len()]);
        let total = call(
            "plus",
            &decimal,
            vec![column(1, &decimal), column(1, &decimal)],
        );
        let mut input = Chunk::new(&[text, decimal], 4, 4);
        for (text, decimal) in [
            (Some(b"yz".as_slice()), Some("1.250")),
            (Some(b"unselected".as_slice()), Some("999.999")),
            (None, None),
            (Some(b"abc".as_slice()), Some("-2.875")),
        ] {
            if let Some(text) = text {
                input.append_string(0, text);
            } else {
                input.append_null(0);
            }
            if let Some(decimal) = decimal {
                input.append_datum(
                    1,
                    &Datum::Decimal(tidb_datatype::Decimal::from_literal(decimal)),
                );
            } else {
                input.append_null(1);
            }
        }
        let evaluate = |expression| {
            let mut prepared = PreparedExpression::new(expression, &crate::NoColumns, 2)
                .unwrap()
                .unwrap();
            let mut result = Vec::new();
            prepared
                .evaluate_rows(&crate::NoColumns, &input, &[3, 2, 0, 3], true, |value| {
                    result.push(value);
                    Ok(())
                })
                .unwrap();
            result
        };
        assert_eq!(
            evaluate(&byte_count),
            [Datum::Int(6), Datum::Null, Datum::Int(4), Datum::Int(6)]
        );
        assert_eq!(
            evaluate(&total),
            [
                Datum::Decimal(tidb_datatype::Decimal::from_literal("-5.750")),
                Datum::Null,
                Datum::Decimal(tidb_datatype::Decimal::from_literal("2.500")),
                Datum::Decimal(tidb_datatype::Decimal::from_literal("-5.750")),
            ]
        );
    }

    #[test]
    fn nested_arithmetic_batches_stay_flat_and_scalar_null_skips_overflow() {
        use tidb_query_expr::RpnExpressionNode::{ColumnRef, FnCall};
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let column = |i: i64| {
            let mut column = Column::new(i, ty.clone());
            column.index = i;
            Expression::Column(column)
        };
        let call = |name, a, b| {
            Expression::ScalarFunction(build_function(CiString::new(name), ty.clone(), vec![a, b]))
        };
        let plus = |a, b| call("plus", a, b);
        let expression = plus(
            plus(
                plus(column(0), column(2)),
                call("minus", column(1), column(2)),
            ),
            plus(column(1), column(2)),
        );
        let fields = [ty.clone(), ty.clone(), ty.clone()];
        let mut input = Chunk::new(&fields, 1, 1);
        input.append_null(0);
        input.append_int64(1, i64::MAX);
        input.append_int64(2, 1);
        let mut prepared = PreparedExpression::new(&expression, &crate::NoColumns, 3)
            .unwrap()
            .unwrap();
        let nodes = prepared.batch_program.as_ref().unwrap().as_ref();
        assert_eq!(nodes.len(), 11);
        assert_eq!(
            nodes
                .iter()
                .filter(|node| matches!(node, ColumnRef { .. }))
                .count(),
            6
        );
        assert_eq!(
            nodes
                .iter()
                .filter(|node| matches!(node, FnCall { .. }))
                .count(),
            5
        );
        assert_eq!(
            prepared
                .evaluate_row(&crate::NoColumns, input.get_row(0))
                .unwrap(),
            Datum::Null
        );
        assert!(prepared
            .evaluate_rows(&crate::NoColumns, &input, &[0], true, |_| Ok(()))
            .is_err());
    }
}
