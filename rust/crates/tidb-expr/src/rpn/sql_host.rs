// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use std::cell::RefCell;

use scoped_tls_hkt::scoped_thread_local;
use tidb_chunk::row::Row;
use tidb_query_datatype::{
    EvalType,
    codec::{
        batch::LazyBatchColumnVec,
        data_type::{ScalarValueRef, VectorValue},
    },
    expr::EvalContext,
};
use tidb_query_expr::{RpnExpression, RpnExpressionNode, ShortCircuitFnMeta};
use tipb::{FieldType, ScalarFuncSig};

use crate::{Columns, Datum, EvalError, expression::Expression, scalar_function::ScalarFunction};

type SqlResult = Result<Datum, EvalError>;
type ReadOperand<'a> = dyn Fn(&Expression, &dyn Columns, Row<'_>, bool) -> Option<SqlResult> + 'a;

struct Bindings<'a> {
    functions: &'a [&'a ScalarFunction],
    context: &'a dyn Columns,
    row: Row<'a>,
    result: RefCell<Option<SqlResult>>,
}

scoped_thread_local!(static BINDINGS: for<'a> &'a Bindings<'a>);
scoped_thread_local!(static READ_OPERAND: for<'a> &'a ReadOperand<'a>);

pub(crate) fn read_operand(
    expression: &Expression,
    context: &dyn Columns,
    row: Row<'_>,
    as_string: bool,
) -> Option<SqlResult> {
    if !READ_OPERAND.is_set() {
        return None;
    }
    READ_OPERAND.with(|read| read(expression, context, row, as_string))
}

pub(crate) fn evaluate(
    function: &ScalarFunction,
    context: &dyn Columns,
    row: Row<'_>,
) -> SqlResult {
    let mut functions = Vec::new();
    let program = compile(function, &mut functions);
    let bindings = Bindings {
        functions: &functions,
        context,
        row,
        result: RefCell::new(None),
    };
    BINDINGS.set(&bindings, || {
        evaluate_node(
            &program,
            &mut EvalContext::default(),
            &[],
            &LazyBatchColumnVec::empty(),
            &[0],
        )
    })
}

// SQL kernels can demand operands in a different order, skip them entirely,
// or re-evaluate them. Each scalar operand has its own RPN program.
fn compile<'a>(
    function: &'a ScalarFunction,
    functions: &mut Vec<&'a ScalarFunction>,
) -> RpnExpression {
    let slot = functions.len();
    functions.push(function);
    let mut args = vec![super::engine::integer_root(slot as i64)];
    args.extend(function.args.iter().map(|arg| match arg {
        Expression::ScalarFunction(child) => compile(child, functions),
        _ => RpnExpression::from(Vec::new()),
    }));
    RpnExpression::from(vec![RpnExpressionNode::ShortCircuitFnCall {
        func_meta: ShortCircuitFnMeta {
            sig: ScalarFuncSig::Unspecified,
            fn_ptr: call,
        },
        args: args.into_boxed_slice(),
        field_type: super::engine::index_field(),
    }])
}

fn evaluate_node(
    program: &RpnExpression,
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
) -> SqlResult {
    program
        .eval_decoded(ctx, schema, input, rows, 1)
        .map_err(|error| EvalError::SharedEvaluation {
            code: 1105,
            message: error.to_string(),
        })?;
    BINDINGS.with(|bindings| {
        bindings
            .result
            .borrow_mut()
            .take()
            .expect("SQL host result")
    })
}

fn call(
    ctx: &mut EvalContext,
    schema: &[FieldType],
    input: &LazyBatchColumnVec,
    rows: &[usize],
    count: usize,
    args: &[RpnExpression],
) -> tidb_query_common::Result<VectorValue> {
    let slot = args[0].eval_decoded(ctx, schema, input, rows, count)?;
    let ScalarValueRef::Int(Some(slot)) = slot.get_logical_scalar_ref(0) else {
        unreachable!("SQL host slot");
    };
    BINDINGS.with(|bindings| {
        let function = bindings.functions[*slot as usize];
        let ctx = RefCell::new(ctx);
        let read =
            |expression: &Expression, context: &dyn Columns, row: Row<'_>, as_string: bool| {
                let Expression::ScalarFunction(child) = expression else {
                    return None;
                };
                let index = function
                    .args
                    .iter()
                    .position(|arg| std::ptr::eq(arg, expression))?;
                if !as_string {
                    match super::try_evaluate_scalar_function(child, context, row) {
                        Ok(Some(value)) => return Some(Ok(value)),
                        Err(error) => return Some(Err(error)),
                        Ok(None) => {}
                    }
                }
                let child_bindings = Bindings {
                    functions: bindings.functions,
                    context,
                    row,
                    result: RefCell::new(None),
                };
                Some(BINDINGS.set(&child_bindings, || {
                    evaluate_node(&args[index + 1], &mut ctx.borrow_mut(), schema, input, rows)
                }))
            };
        let value = READ_OPERAND.set(&read, || {
            function.invoke_sql_kernel(bindings.context, bindings.row)
        });
        *bindings.result.borrow_mut() = Some(value);
    });
    // Keep native SQL datums and diagnostics in the invocation scope. The
    // scheduler's integer carrier works even for unrepresentable values.
    let mut output = VectorValue::with_capacity(1, EvalType::Int);
    output.push_int(Some(0));
    Ok(output)
}
