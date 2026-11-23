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

use std::any::Any;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, MySqlDuration};
use tidb_query_common::error::{Error, ErrorInner};
use tidb_query_datatype::codec::batch::LazyBatchColumnVec;
use tidb_query_datatype::codec::data_type::{
    Real, ScalarValue, ScalarValueRef, VectorValue, BATCH_MAX_SIZE,
};
use tidb_query_datatype::codec::mysql::Duration;
use tidb_query_datatype::expr::EvalContext;
use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tidb_query_expr::{
    RpnExpression, RpnExpressionBuilder, RpnExpressionNode, RpnFnCallExtra, RpnFnMeta,
    RpnStackNode, ShortCircuitFnMeta,
};
use tipb::{Expr, ExprType, FieldType as PbFieldType, ScalarFuncSig};

use crate::expression::Expression;
use crate::{Columns, EvalError, MysqlRng};

pub(crate) struct RandomScope {
    session: Option<Arc<MysqlRng>>,
    seeded: Option<Arc<crate::context::SeededRand>>,
}

impl RandomScope {
    pub(crate) fn new(ctx: &impl Columns) -> Self {
        Self {
            session: ctx.rand_session_handle(),
            seeded: ctx.rand_seeded_state(),
        }
    }

    pub(crate) fn is_same_scope(&self, other: &Self) -> bool {
        fn same<T>(a: &Option<Arc<T>>, b: &Option<Arc<T>>) -> bool {
            match (a, b) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            }
        }
        same(&self.session, &other.session) && same(&self.seeded, &other.seeded)
    }
}

pub(crate) struct Projection {
    expression: RpnExpression,
    columns: Vec<(usize, FieldType)>,
    schema: Vec<PbFieldType>,
    row_ordered: bool,
}

impl Projection {
    pub(crate) fn prepare(expression: &Expression, random: &RandomScope) -> Option<Self> {
        if !matches!(expression, Expression::ScalarFunction(_)) {
            return None;
        }
        let mut compiler = Compiler::default();
        let nodes = compiler.lower(expression, expression_type(expression)?, random)?;
        let schema = compiler
            .columns
            .iter()
            .map(|(_, field)| pb_type(field))
            .collect();
        Some(Self {
            expression: RpnExpression::from(nodes),
            columns: compiler.columns,
            schema,
            row_ordered: compiler.row_ordered,
        })
    }

    pub(crate) fn run(
        &self,
        ctx: &impl Columns,
        input: &Chunk,
        mut append: impl FnMut(Datum),
    ) -> Result<bool, EvalError> {
        let mut physical = Vec::with_capacity(self.columns.len());
        for ((index, field), pb_field) in self.columns.iter().zip(&self.schema) {
            if *index >= input.num_cols() {
                return Ok(false);
            }
            let eval_type =
                EvalType::try_from(pb_field.as_accessor().tp()).expect("typed RPN input");
            let mut column = VectorValue::with_capacity(input.num_rows(), eval_type);
            for row in 0..input.num_rows() {
                let Some(value) = rpn_value(input.get_row(row).get_datum(*index, field), eval_type)
                else {
                    return Ok(false);
                };
                push(&mut column, value.as_scalar_value_ref());
            }
            physical.push(column);
        }
        let physical = LazyBatchColumnVec::from(physical);
        let mut eval = EvalContext::default();
        let rows: Vec<_> = (0..input.num_rows()).collect();
        let batch_size = if self.row_ordered || !ctx.enable_vectorized_expression() {
            1
        } else {
            BATCH_MAX_SIZE
        };
        for rows in rows.chunks(batch_size) {
            let result =
                self.expression
                    .eval_decoded(&mut eval, &self.schema, &physical, rows, rows.len());
            for warning in eval.take_warnings().warnings {
                ctx.append_warning(
                    u16::try_from(warning.get_code()).unwrap_or(1105),
                    warning.get_msg(),
                );
            }
            let result = result.map_err(eval_error)?;
            for row in 0..rows.len() {
                append(native_value(result.get_logical_scalar_ref(row)));
            }
        }
        Ok(true)
    }
}

#[derive(Default)]
struct Compiler {
    columns: Vec<(usize, FieldType)>,
    row_ordered: bool,
}

impl Compiler {
    fn lower(
        &mut self,
        expression: &Expression,
        expected: &FieldType,
        random: &RandomScope,
    ) -> Option<Vec<RpnExpressionNode>> {
        let eval_type = eval_type(expected)?;
        let field_type = pb_type(expected);
        let declared = expression_type(expression)?;
        match expression {
            Expression::Constant(c) if c.deferred_expr.is_none() && c.param_marker.is_none() => {
                let value = rpn_value(c.value.clone(), eval_type)?;
                Some(vec![RpnExpressionNode::Constant { value, field_type }])
            }
            Expression::Column(column)
                if declared.code() == expected.code() && column.virtual_expr.is_none() =>
            {
                let index = usize::try_from(column.index).ok()?;
                let offset = self
                    .columns
                    .iter()
                    .position(|(i, ty)| *i == index && ty == declared)
                    .unwrap_or_else(|| {
                        self.columns.push((index, declared.clone()));
                        self.columns.len() - 1
                    });
                Some(vec![RpnExpressionNode::ColumnRef { offset }])
            }
            Expression::ScalarFunction(function) if declared.code() == expected.code() => {
                let args = function.get_args();
                let sig = match (function.func_name.lowercase(), args) {
                    ("if", [condition, yes, no]) => {
                        let condition_type = expression_type(condition)?;
                        if eval_type_for_expression(condition)? != EvalType::Int {
                            return None;
                        }
                        let args = [
                            RpnExpression::from(self.lower(condition, condition_type, random)?),
                            RpnExpression::from(self.lower(yes, expected, random)?),
                            RpnExpression::from(self.lower(no, expected, random)?),
                        ];
                        let sig = match eval_type {
                            EvalType::Int => ScalarFuncSig::IfInt,
                            EvalType::Real => ScalarFuncSig::IfReal,
                            EvalType::Duration => ScalarFuncSig::IfDuration,
                            _ => return None,
                        };
                        return Some(vec![RpnExpressionNode::ShortCircuitFnCall {
                            func_meta: ShortCircuitFnMeta {
                                sig,
                                fn_ptr: eval_if,
                            },
                            args: Box::new(args),
                            field_type,
                        }]);
                    }
                    ("rand", []) if eval_type == EvalType::Real => {
                        self.row_ordered = true;
                        return Some(vec![rand_node(
                            Arc::clone(random.session.as_ref()?),
                            field_type,
                        )]);
                    }
                    ("rand", [Expression::Constant(seed)])
                        if seed.deferred_expr.is_none()
                            && seed.param_marker.is_none()
                            && eval_type == EvalType::Real =>
                    {
                        self.row_ordered = true;
                        return Some(vec![rand_node(
                            random.seeded.as_ref()?.generator(
                                std::ptr::from_ref(function) as usize,
                                seed.value.as_int()?,
                            ),
                            field_type,
                        )]);
                    }
                    ("hour", [arg]) if eval_type_for_expression(arg)? == EvalType::Duration => {
                        ScalarFuncSig::Hour
                    }
                    ("exp", [arg]) if eval_type_for_expression(arg)? == EvalType::Real => {
                        self.row_ordered = true;
                        ScalarFuncSig::Exp
                    }
                    _ => return None,
                };
                let mut nodes = Vec::new();
                let mut pb_args = Vec::with_capacity(args.len());
                for arg in args {
                    let field = expression_type(arg)?;
                    nodes.extend(self.lower(arg, field, random)?);
                    let mut placeholder = Expr::new();
                    placeholder.set_tp(ExprType::Null);
                    placeholder.set_field_type(pb_type(field));
                    pb_args.push(placeholder);
                }
                let mut pb = Expr::new();
                pb.set_tp(ExprType::ScalarFunc);
                pb.set_sig(sig);
                pb.set_field_type(field_type);
                pb.set_children(pb_args.into());
                let root =
                    RpnExpressionBuilder::build_from_expr_tree(pb, &mut EvalContext::default(), 0)
                        .ok()?
                        .into_inner()
                        .pop()?;
                nodes.push(root);
                Some(nodes)
            }
            _ => None,
        }
    }
}

fn expression_type(expression: &Expression) -> Option<&FieldType> {
    match expression {
        Expression::Column(col) => col.ret_type.as_ref(),
        Expression::Constant(c) => c.ret_type.as_ref(),
        Expression::ScalarFunction(f) => f.ret_type.as_ref(),
        _ => None,
    }
}

fn eval_type_for_expression(expression: &Expression) -> Option<EvalType> {
    eval_type(expression_type(expression)?)
}

fn eval_type(field: &FieldType) -> Option<EvalType> {
    match field.code() {
        code if code.is_type_integer() && !field.is_unsigned() => Some(EvalType::Int),
        FieldTypeCode::Double => Some(EvalType::Real),
        FieldTypeCode::Duration => Some(EvalType::Duration),
        _ => None,
    }
}

fn pb_type(field: &FieldType) -> PbFieldType {
    let mut pb = PbFieldType::new();
    pb.set_tp(i32::from(field.code().mysql_type()));
    pb.set_flag(field.flags());
    pb.set_flen(i32::try_from(field.flen()).unwrap_or(-1));
    pb.set_decimal(i32::try_from(field.decimal()).unwrap_or(-1));
    pb
}

fn rpn_value(value: Datum, ty: EvalType) -> Option<ScalarValue> {
    match (ty, value) {
        (EvalType::Int, Datum::Null) => Some(ScalarValue::Int(None)),
        (EvalType::Int, Datum::Int(value)) => Some(ScalarValue::Int(Some(value))),
        (EvalType::Real, Datum::Null) => Some(ScalarValue::Real(None)),
        (EvalType::Real, Datum::Real(value)) => {
            Some(ScalarValue::Real(Some(Real::new(value).ok()?)))
        }
        (EvalType::Duration, Datum::Null) => Some(ScalarValue::Duration(None)),
        (EvalType::Duration, Datum::Duration(value)) => Some(ScalarValue::Duration(Some(
            Duration::from_nanos(value.nanoseconds(), i8::try_from(value.fsp()).ok()?).ok()?,
        ))),
        _ => None,
    }
}

fn push(vector: &mut VectorValue, value: ScalarValueRef<'_>) {
    match value {
        ScalarValueRef::Int(value) => vector.push_int(value.copied()),
        ScalarValueRef::Real(value) => vector.push_real(value.copied()),
        ScalarValueRef::Duration(value) => vector.push_duration(value.copied()),
        _ => unreachable!("typed RPN projection"),
    }
}

fn native_value(value: ScalarValueRef<'_>) -> Datum {
    match value {
        ScalarValueRef::Int(value) => value.copied().map_or(Datum::Null, Datum::Int),
        ScalarValueRef::Real(value) => {
            value.map_or(Datum::Null, |value| Datum::Real(value.into_inner()))
        }
        ScalarValueRef::Duration(value) => value.map_or(Datum::Null, |value| {
            Datum::Duration(MySqlDuration::from_raw_parts(
                value.to_nanos(),
                i64::from(value.fsp()),
            ))
        }),
        _ => unreachable!("typed RPN projection"),
    }
}

fn eval_if(
    ctx: &mut EvalContext,
    schema: &[PbFieldType],
    physical: &LazyBatchColumnVec,
    rows: &[usize],
    output_rows: usize,
    args: &[RpnExpression],
) -> tidb_query_common::Result<VectorValue> {
    let condition = args[0].eval_decoded(ctx, schema, physical, rows, output_rows)?;
    let mut selected = [Vec::new(), Vec::new()];
    for (position, row) in rows.iter().enumerate() {
        let ScalarValueRef::Int(value) = condition.get_logical_scalar_ref(position) else {
            unreachable!("typed RPN condition")
        };
        selected[usize::from(value.is_none_or(|value| *value == 0))].push(*row);
    }
    let yes = evaluate_branch(ctx, schema, physical, &selected[0], &args[1])?;
    let no = evaluate_branch(ctx, schema, physical, &selected[1], &args[2])?;
    let mut offsets = [0, 0];
    let mut result = VectorValue::with_capacity(
        output_rows,
        EvalType::try_from(args[1].ret_field_type(schema).as_accessor().tp())
            .expect("typed IF result"),
    );
    for position in 0..output_rows {
        let ScalarValueRef::Int(value) = condition.get_logical_scalar_ref(position) else {
            unreachable!("typed RPN condition")
        };
        let branch = usize::from(value.is_none_or(|value| *value == 0));
        let value = [&yes, &no][branch]
            .as_ref()
            .expect("selected branch")
            .get_logical_scalar_ref(offsets[branch]);
        push(&mut result, value);
        offsets[branch] += 1;
    }
    Ok(result)
}

fn evaluate_branch<'a>(
    ctx: &mut EvalContext,
    schema: &'a [PbFieldType],
    physical: &'a LazyBatchColumnVec,
    rows: &'a [usize],
    branch: &'a RpnExpression,
) -> tidb_query_common::Result<Option<RpnStackNode<'a>>> {
    if rows.is_empty() {
        return Ok(None);
    }
    branch
        .eval_decoded(ctx, schema, physical, rows, rows.len())
        .map(Some)
}

fn rand_node(rng: Arc<MysqlRng>, field_type: PbFieldType) -> RpnExpressionNode {
    RpnExpressionNode::FnCall {
        func_meta: RpnFnMeta {
            name: "tidb_rand",
            validator_ptr: |_| Ok(()),
            metadata_expr_ptr: |_| Ok(Box::new(())),
            fn_ptr: eval_rand,
        },
        args_len: 0,
        field_type,
        metadata: Box::new(rng),
    }
}

fn eval_rand(
    _: &mut EvalContext,
    rows: usize,
    _: &[RpnStackNode<'_>],
    _: &mut RpnFnCallExtra<'_>,
    metadata: &(dyn Any + Send),
) -> tidb_query_common::Result<VectorValue> {
    let rng = metadata
        .downcast_ref::<Arc<MysqlRng>>()
        .expect("RAND metadata");
    let mut result = VectorValue::with_capacity(rows, EvalType::Real);
    for _ in 0..rows {
        result.push_real(Real::new(rng.gen()).ok());
    }
    Ok(result)
}

fn eval_error(error: Error) -> EvalError {
    let (code, message) = match error.0.as_ref() {
        ErrorInner::Evaluate(error) => (error.code(), error.to_string()),
        ErrorInner::Storage(error) => (1105, error.to_string()),
    };
    EvalError::SharedEvaluation {
        code: u16::try_from(code).unwrap_or(1105),
        message,
    }
}
