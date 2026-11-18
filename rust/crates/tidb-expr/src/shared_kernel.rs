// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Opt-in native adapter for the original TiKV RPN evaluator.
//!
//! shortcut: this opt-in subset covers numeric/control/state M1 probes.
//! Extend typed signature admission before enabling a default path; no native
//! evaluator fallback is available for unsupported expressions.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::Datum;
use tidb_query_datatype::{
    codec::{
        data_type::{ChunkRef, ScalarValue, ScalarValueRef, VectorValue},
        mysql::json::{Json, JsonType},
    },
    expr::EvalContext,
    EvalType, FieldTypeAccessor, FieldTypeFlag, FieldTypeTp,
};
use tidb_query_expr::{
    ExpressionScalar, ExpressionVector, InputColumns, InputResult, InvocationMode,
    NativeArithmetic, NativeDiagnosticPart, OutputOrder, OwnedRpnExecution, RpnExecutionRef,
    RpnExpression, RpnExpressionNode, RpnStackNode, RpnStackNodeVectorValue,
};

use crate::expression::Expression;

/// SQL diagnostics remain structured across the shared evaluator boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedEvaluationError {
    /// SQL error number, absent for adapter validation failures.
    pub code: Option<i32>,
    /// Diagnostic text without transport prefixes.
    pub message: String,
}

impl std::fmt::Display for SharedEvaluationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for SharedEvaluationError {}
impl From<String> for SharedEvaluationError {
    fn from(message: String) -> Self {
        Self {
            code: None,
            message,
        }
    }
}
impl From<&str> for SharedEvaluationError {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
impl From<tidb_query_expr::evaluation_error::Error> for SharedEvaluationError {
    fn from(error: tidb_query_expr::evaluation_error::Error) -> Self {
        use tidb_query_expr::evaluation_error::{ErrorInner, EvaluateError};
        match *error.0 {
            ErrorInner::Evaluate(EvaluateError::Other(message)) => message.into(),
            ErrorInner::Evaluate(error) => Self {
                code: Some(error.code()),
                message: error.to_string(),
            },
            ErrorInner::Storage(error) => error.to_string().into(),
        }
    }
}

/// A native expression compiled once into the same program used by protobuf.
#[derive(Debug)]
pub struct SharedExpression {
    program: RpnExpression,
    columns: Vec<(usize, tidb_datatype::FieldType)>,
    schema: Vec<tipb::FieldType>,
    result_type: Option<tidb_datatype::FieldType>,
    result_collation: tidb_datatype::Collation,
}

impl SharedExpression {
    /// Exact native declaration; protobuf execution types do not replace it.
    pub fn result_type(&self) -> Option<&tidb_datatype::FieldType> {
        self.result_type.as_ref()
    }

    /// Compile the admitted native expression subset without evaluating it.
    pub fn compile(expression: &Expression) -> Result<Self, String> {
        if matches!(expression, Expression::Constant(c) if matches!(c.literal_value(), Some(Datum::Bytes(_) | Datum::String(_))))
        {
            return Err(
                "byte literal identity is outside this checkpoint's output admission".into(),
            );
        }
        enum Visit<'a> {
            Enter(&'a Expression),
            DecimalOperand(&'a Expression),
            BigIntOperand(&'a Expression),
            ByteOperand(&'a Expression),
            JsonOperand(&'a Expression),
            JsonType,
            Md5,
            Date,
            RepeatCount(&'a crate::scalar_function::ScalarFunction),
            RepeatBody(usize),
            Variable {
                name: String,
                write: bool,
            },
            Rand {
                seeded: bool,
                constant_seed: bool,
            },
            NumericLeft(&'a crate::scalar_function::ScalarFunction),
            NumericRight(&'a crate::scalar_function::ScalarFunction, Option<usize>),
            ParseJson,
            JsonLength,
            RegexpLike(
                &'a crate::scalar_function::ScalarFunction,
                usize,
                Vec<usize>,
            ),
            IntCall(tidb_query_expr::RpnFnMeta, usize, Option<usize>),
            WireComparisonRight(tidb_query_expr::RpnFnMeta, &'a Expression),
            LogicalNext(&'a Expression, usize),
            LogicalEnd(usize),
            Conditional(&'a [Expression], tipb::FieldType),
            IfCondition(&'a Expression, &'a [Expression], tipb::FieldType),
            IfThen(&'a [Expression], tipb::FieldType, usize),
            Alternative(&'a [Expression], Vec<usize>, tipb::FieldType),
            IfElse(usize, usize),
        }
        let mut work = vec![Visit::Enter(expression)];
        let mut nodes = Vec::new();
        let mut columns = Vec::new();
        let batch_mixed_string_if = can_batch_mixed_string_if(expression);
        let mut row_fallback = false;
        let bytes_type = || tipb::FieldType::from(FieldTypeTp::VarString);
        let result_operand = |value, field_type: &tipb::FieldType| {
            if field_type.as_accessor().tp() == FieldTypeTp::VarString {
                Visit::ByteOperand(value)
            } else {
                Visit::Enter(value)
            }
        };
        while let Some(visit) = work.pop() {
            let mut truth_test = None;
            let mut byte_count = None;
            if let Visit::Enter(Expression::ScalarFunction(f)) = &visit {
                truth_test = native_int_truth_test(f);
                byte_count = native_byte_count(f);
                // Direct typed leaves never introduce per-row effects.
                let pure_call = (is_native_integer_comparison(f)
                    || truth_test.is_some()
                    || byte_count.is_some())
                    && f.args
                        .iter()
                        .all(|arg| matches!(arg, Expression::Constant(_) | Expression::Column(_)));
                row_fallback |= !batch_mixed_string_if
                    && !pure_call
                    && !matches!(f.func_name.lowercase(), "plus" | "minus" | "mul" | "div");
            }
            match visit {
                Visit::DecimalOperand(Expression::Constant(c)) if c.param_marker.is_some() => {
                    if c.deferred_expr.is_some() {
                        return Err("deferred parameter is outside shared admission".into());
                    }
                    let order = usize::try_from(c.param_marker.unwrap().order)
                        .map_err(|_| "invalid parameter order")?;
                    nodes.push(RpnExpressionNode::NativeParameterDecimal {
                        order,
                        field_type: FieldTypeTp::NewDecimal.into(),
                    });
                }
                Visit::DecimalOperand(expression) => work.push(Visit::Enter(expression)),
                Visit::JsonOperand(Expression::Constant(c)) => {
                    let value = match c.literal_value() {
                        Some(Datum::Null) => None,
                        Some(Datum::Json(json)) => Some(
                            native_binary_json(json.type_code(), json.value())
                                .map_err(|error| error.to_string())?,
                        ),
                        _ => return Err("JSON_TYPE requires an explicitly typed JSON value".into()),
                    };
                    nodes.push(RpnExpressionNode::Constant {
                        value: ScalarValue::Json(value),
                        field_type: FieldTypeTp::Json.into(),
                    });
                }
                Visit::ByteOperand(Expression::Constant(c))
                | Visit::BigIntOperand(Expression::Constant(c))
                | Visit::Enter(Expression::Constant(c)) => {
                    let unsigned = matches!(visit, Visit::BigIntOperand(_))
                        && c.ret_type.as_ref().is_some_and(|tp| tp.is_unsigned());
                    let datum = c
                        .literal_value()
                        .ok_or("shared kernel requires a literal constant")?;
                    if matches!(datum, Datum::Time(_))
                        || c.ret_type.as_ref().is_some_and(is_time_type)
                    {
                        let ty = c
                            .ret_type
                            .as_ref()
                            .filter(|t| is_time_type(t) && !t.is_array())
                            .ok_or("Time constant requires a time declaration")?;
                        let value = match datum {
                            Datum::Null => None,
                            Datum::Time(value) => Some(*value),
                            _ => return Err("time declaration does not match literal".into()),
                        };
                        nodes.push(RpnExpressionNode::ExpressionConstant {
                            value: ExpressionScalar::Time(value),
                            field_type: time_type(ty),
                        });
                        continue;
                    }
                    let extended = match datum {
                        Datum::Null => match c.ret_type.as_ref().map(|t| t.eval_type()) {
                            Some(tidb_datatype::EvalType::Real) => {
                                Some((ExpressionScalar::Real(None), FieldTypeTp::Double))
                            }
                            Some(tidb_datatype::EvalType::Decimal) => {
                                Some((ExpressionScalar::Decimal(None), FieldTypeTp::NewDecimal))
                            }
                            _ => None,
                        },
                        Datum::Real(v) => {
                            Some((ExpressionScalar::Real(Some(*v)), FieldTypeTp::Double))
                        }
                        Datum::Decimal(v) => Some((
                            ExpressionScalar::Decimal(Some(v.clone().into())),
                            FieldTypeTp::NewDecimal,
                        )),
                        _ => None,
                    };
                    if let Some((value, tp)) = extended {
                        nodes.push(RpnExpressionNode::ExpressionConstant {
                            value,
                            field_type: tp.into(),
                        });
                        continue;
                    }
                    let value = match datum {
                        Datum::Null
                            if c.ret_type
                                .as_ref()
                                .is_some_and(|t| t.eval_type() == tidb_datatype::EvalType::Int) =>
                        {
                            ScalarValue::Int(None)
                        }
                        Datum::Null => ScalarValue::Bytes(None),
                        Datum::Int(v) => ScalarValue::Int(Some(*v)),
                        Datum::UInt(v) if unsigned => ScalarValue::Int(Some(*v as i64)),
                        Datum::Bytes(v) => ScalarValue::Bytes(Some(v.clone())),
                        Datum::String(v) => ScalarValue::Bytes(Some(v.bytes().to_vec())),
                        Datum::BinaryLiteral(v) | Datum::Bit(v)
                            if matches!(visit, Visit::ByteOperand(_)) =>
                        {
                            ScalarValue::Bytes(Some(v.as_bytes().to_vec()))
                        }
                        _ => {
                            return Err("shared kernel does not yet admit this literal type".into())
                        }
                    };
                    let field_type = if matches!(value, ScalarValue::Int(_)) {
                        bigint_type(unsigned)
                    } else {
                        bytes_type()
                    };
                    nodes.push(RpnExpressionNode::Constant { value, field_type });
                }
                Visit::JsonOperand(Expression::Column(c))
                | Visit::ByteOperand(Expression::Column(c))
                | Visit::BigIntOperand(Expression::Column(c))
                | Visit::Enter(Expression::Column(c)) => {
                    let bigint_operand = matches!(visit, Visit::BigIntOperand(_));
                    let index = usize::try_from(c.index).map_err(|_| "negative column index")?;
                    let tp = c.ret_type.as_ref().ok_or("column requires a field type")?;
                    let admitted =
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    match tp.code() {
                        _ if matches!(visit, Visit::ByteOperand(_) | Visit::JsonOperand(_)) => {}
                        tidb_datatype::FieldTypeCode::VarString
                            if tp.collation_name() == admitted.collation_name() => {}
                        tidb_datatype::FieldTypeCode::Double if !tp.is_unsigned() => {}
                        tidb_datatype::FieldTypeCode::LongLong
                            if !tp.is_unsigned() || bigint_operand => {}
                        tidb_datatype::FieldTypeCode::NewDecimal
                        | tidb_datatype::FieldTypeCode::Date
                        | tidb_datatype::FieldTypeCode::Datetime
                        | tidb_datatype::FieldTypeCode::Timestamp => {}
                        _ => return Err("column type is outside shared input admission".into()),
                    };
                    if tp.is_array() {
                        return Err("array columns are not yet admitted".into());
                    }
                    let offset = match columns.iter().position(|(i, _)| *i == index) {
                        Some(i) => {
                            if columns[i].1 != *tp {
                                return Err("conflicting column types".into());
                            }
                            i
                        }
                        None => {
                            columns.push((index, tp.clone()));
                            columns.len() - 1
                        }
                    };
                    nodes.push(RpnExpressionNode::ColumnRef { offset });
                }
                Visit::BigIntOperand(expression) => work.push(Visit::Enter(expression)),
                Visit::Enter(Expression::ScalarFunction(f))
                    if f.func_name.lowercase() == "date" =>
                {
                    if f.args.len() != 1
                        || f.ret_type.as_ref().is_none_or(|t| {
                            t.code() != tidb_datatype::FieldTypeCode::Date || t.is_array()
                        })
                    {
                        return Err("DATE requires one argument and a Date result".into());
                    }
                    if f.args[0].static_type().is_some_and(|t| t.is_array()) {
                        return Err("DATE array conversion is outside shared admission".into());
                    }
                    work.push(Visit::Date);
                    if matches!(&f.args[0], Expression::Constant(c) if matches!(c.literal_value(), Some(Datum::Null)))
                    {
                        nodes.push(RpnExpressionNode::ExpressionConstant {
                            value: ExpressionScalar::Time(None),
                            field_type: FieldTypeTp::DateTime.into(),
                        });
                    } else {
                        if f.args[0]
                            .static_type()
                            .is_none_or(|t| !is_time_type(t) || t.is_array())
                        {
                            return Err("DATE time conversion is outside shared admission".into());
                        }
                        work.push(Visit::Enter(&f.args[0]));
                    }
                }
                Visit::Date => nodes.push(RpnExpressionNode::NativeDate {
                    field_type: FieldTypeTp::Date.into(),
                }),
                Visit::Enter(Expression::ScalarFunction(f))
                    if f.func_name.lowercase() == "benchmark" =>
                {
                    if f.args.len() != 2
                        || f.ret_type.as_ref().is_none_or(|t| {
                            t.code() != tidb_datatype::FieldTypeCode::LongLong
                                || t.is_unsigned()
                                || t.is_array()
                        })
                        || f.args[0].static_type().is_none_or(|t| {
                            t.code() != tidb_datatype::FieldTypeCode::LongLong
                                || t.is_unsigned()
                                || t.is_array()
                        })
                    {
                        return Err(
                            "BENCHMARK requires signed integer count/result in this checkpoint"
                                .into(),
                        );
                    }
                    work.push(Visit::RepeatCount(f));
                    work.push(Visit::Enter(&f.args[0]));
                }
                Visit::RepeatCount(f) => {
                    let supported_type = f.args[1]
                        .static_type()
                        .is_none_or(|t| t.eval_type() != tidb_datatype::EvalType::VectorFloat32);
                    let at = nodes.len();
                    nodes.push(RpnExpressionNode::Repeat {
                        end: 0,
                        supported_type,
                        field_type: FieldTypeTp::LongLong.into(),
                    });
                    work.push(Visit::RepeatBody(at));
                    if supported_type {
                        work.push(Visit::Enter(&f.args[1]));
                    }
                }
                Visit::RepeatBody(at) => {
                    nodes.push(RpnExpressionNode::RepeatEnd {
                        field_type: FieldTypeTp::LongLong.into(),
                    });
                    let end = nodes.len();
                    let RpnExpressionNode::Repeat { end: target, .. } = &mut nodes[at] else {
                        unreachable!()
                    };
                    *target = end;
                }
                Visit::Enter(Expression::ScalarFunction(f))
                    if matches!(f.func_name.lowercase(), "setvar" | "getvar_string") =>
                {
                    let write = f.func_name.lowercase() == "setvar";
                    let admitted = |t: &tidb_datatype::FieldType| {
                        t.code() == tidb_datatype::FieldTypeCode::VarString
                            && t.collation_name()
                                == tidb_datatype::FieldType::new(
                                    tidb_datatype::FieldTypeCode::VarString,
                                )
                                .collation_name()
                            && !t.is_array()
                    };
                    if f.args.len() != if write { 2 } else { 1 }
                        || f.ret_type.as_ref().is_none_or(|t| !admitted(t))
                        || f.args
                            .iter()
                            .any(|a| a.static_type().is_none_or(|t| !admitted(t)))
                    {
                        return Err(
                            "shared variables currently require default string types".into()
                        );
                    }
                    let Some(Expression::Constant(name)) = f.args.first() else {
                        return Err("variable name must be a literal".into());
                    };
                    let name = match name.literal_value() {
                        Some(Datum::String(v)) => std::str::from_utf8(v.bytes()),
                        Some(Datum::Bytes(v)) => std::str::from_utf8(v),
                        _ => return Err("variable name must be a string".into()),
                    }
                    .map_err(|e| e.to_string())?
                    .to_owned();
                    work.push(Visit::Variable { name, write });
                    if write {
                        if let Expression::Constant(value) = &f.args[1] {
                            // SETVAR preserves the Datum carrier, not just its bytes.
                            if !matches!(value.literal_value(), Some(Datum::Null))
                                && !matches!(value.literal_value(), Some(Datum::String(v)) if v.collation() == tidb_datatype::Collation::DEFAULT)
                            {
                                return Err("SETVAR literal carrier is outside default-string shared admission".into());
                            }
                        }
                        if matches!(&f.args[1], Expression::ScalarFunction(value)
                            if string_conditional_collation(value).is_some())
                        {
                            return Err(
                                "SETVAR conditional carrier is outside shared storage admission"
                                    .into(),
                            );
                        }
                        work.push(Visit::Enter(&f.args[1]));
                    }
                }
                Visit::Variable { name, write } => nodes.push(RpnExpressionNode::UserVariable {
                    name,
                    write,
                    field_type: bytes_type(),
                }),
                Visit::Enter(Expression::ScalarFunction(f))
                    if f.func_name.lowercase() == "rand" =>
                {
                    if f.args.len() > 1
                        || f.ret_type.as_ref().is_none_or(|t| {
                            t.code() != tidb_datatype::FieldTypeCode::Double || t.is_array()
                        })
                        || f.args.first().is_some_and(|a| {
                            a.static_type().is_none_or(|t| {
                                t.code() != tidb_datatype::FieldTypeCode::LongLong
                                    || t.is_unsigned()
                                    || t.is_array()
                            })
                        })
                    {
                        return Err(
                            "RAND currently admits signed integer seeds and DOUBLE output".into(),
                        );
                    }
                    work.push(Visit::Rand { seeded: !f.args.is_empty(), constant_seed: matches!(f.args.first(), Some(Expression::Constant(c)) if c.literal_value().is_some()) });
                    work.extend(f.args.iter().rev().map(Visit::Enter));
                }
                Visit::Rand {
                    seeded,
                    constant_seed,
                } => nodes.push(RpnExpressionNode::Rand {
                    seeded,
                    constant_seed,
                    field_type: FieldTypeTp::Double.into(),
                }),
                Visit::Enter(Expression::ScalarFunction(f))
                    if matches!(f.func_name.lowercase(), "plus" | "minus" | "mul" | "div") =>
                {
                    use tidb_datatype::FieldTypeCode as Code;
                    let name = f.func_name.lowercase();
                    let result = f.ret_type.as_ref().ok_or("arithmetic needs result type")?;
                    let admitted = |t: &tidb_datatype::FieldType| {
                        (t.code() == result.code()
                            || (result.code() == Code::Double
                                && t.code() == Code::VarString
                                && t.collation_name()
                                    == tidb_datatype::FieldType::new(Code::VarString)
                                        .collation_name()))
                            && !t.is_unsigned()
                            && !t.is_array()
                    };
                    if f.args.len() != 2
                        || !admitted(result)
                        || (result.code() == Code::NewDecimal
                            && f.numeric_operand_domain() != Some(tidb_datatype::EvalType::Decimal))
                        || !matches!(
                            result.code(),
                            Code::LongLong | Code::Double | Code::NewDecimal
                        )
                        || f.args
                            .iter()
                            .any(|a| a.static_type().is_none_or(|t| !admitted(t)))
                        || (result.code() == Code::LongLong
                            && (!matches!(name, "plus" | "minus" | "mul")
                                || (name == "minus" && f.pb_signature().is_some())))
                    {
                        return Err("arithmetic requires matching supported numeric types".into());
                    }
                    work.push(Visit::NumericLeft(f));
                    work.push(if result.code() == Code::NewDecimal {
                        Visit::DecimalOperand(&f.args[0])
                    } else {
                        Visit::Enter(&f.args[0])
                    });
                }
                Visit::NumericLeft(f) => {
                    if f.ret_type.as_ref().unwrap().eval_type() == tidb_datatype::EvalType::Real
                        && f.args[0].static_type().unwrap().eval_type()
                            == tidb_datatype::EvalType::String
                    {
                        nodes.push(RpnExpressionNode::NativeCastReal {
                            field_type: FieldTypeTp::Double.into(),
                        });
                    }
                    // Native REAL plus evaluates its RHS even for scalar NULL.
                    let guard = if f.func_name.lowercase() == "plus"
                        && f.ret_type.as_ref().unwrap().eval_type() == tidb_datatype::EvalType::Real
                    {
                        None
                    } else {
                        let at = nodes.len();
                        nodes.push(RpnExpressionNode::NullGuard {
                            target: 0,
                            field_type: control_type(f)?,
                        });
                        Some(at)
                    };
                    work.push(Visit::NumericRight(f, guard));
                    work.push(
                        if f.ret_type.as_ref().unwrap().eval_type()
                            == tidb_datatype::EvalType::Decimal
                        {
                            Visit::DecimalOperand(&f.args[1])
                        } else {
                            Visit::Enter(&f.args[1])
                        },
                    );
                }
                Visit::NumericRight(f, guard) => {
                    if f.ret_type.as_ref().unwrap().eval_type() == tidb_datatype::EvalType::Real
                        && f.args[1].static_type().unwrap().eval_type()
                            == tidb_datatype::EvalType::String
                    {
                        nodes.push(RpnExpressionNode::NativeCastReal {
                            field_type: FieldTypeTp::Double.into(),
                        });
                    }
                    use tidb_query_expr::impl_arithmetic::{
                        native_int_arithmetic_fn_meta, IntIntMinus, IntIntMultiply, IntIntPlus,
                    };
                    let (op, native_op) = match f.func_name.lowercase() {
                        "plus" => (NativeArithmetic::Add, tidb_ast::BinaryOp::Plus),
                        "minus" => (NativeArithmetic::Subtract, tidb_ast::BinaryOp::Minus),
                        "mul" => (NativeArithmetic::Multiply, tidb_ast::BinaryOp::Mul),
                        "div" => (NativeArithmetic::Divide, tidb_ast::BinaryOp::Div),
                        _ => unreachable!(),
                    };
                    let eval_type = f.ret_type.as_ref().unwrap().eval_type();
                    let real = eval_type == tidb_datatype::EvalType::Real;
                    let integer = eval_type == tidb_datatype::EvalType::Int;
                    let (class, refusal) = match eval_type {
                        tidb_datatype::EvalType::Real => (
                            "DOUBLE",
                            "DOUBLE overflow diagnostic requires execution-time operands",
                        ),
                        tidb_datatype::EvalType::Int => (
                            "BIGINT",
                            "integer overflow diagnostic requires execution-time operands",
                        ),
                        _ => (
                            "DECIMAL",
                            "DECIMAL overflow diagnostic requires execution-time operands",
                        ),
                    };
                    let needs_binding = numeric_diagnostic_needs_binding(&f.args).ok_or(refusal)?;
                    let (overflow, diagnostic) = if needs_binding {
                        if real || integer {
                            return Err(refusal.into());
                        }
                        (
                            format!("{class} value is out of range"),
                            decimal_diagnostic(f).ok_or(refusal)?,
                        )
                    } else {
                        let source = crate::scalar_function::arithmetic_overflow_expression(
                            f,
                            native_op,
                            real,
                            &crate::context::NoColumns,
                        );
                        let overflow = source.map_or_else(
                            || format!("{class} value is out of range"),
                            |source| format!("{class} value is out of range in '{source}'"),
                        );
                        (overflow, Vec::new())
                    };
                    if integer {
                        nodes.push(RpnExpressionNode::FnCall {
                            func_meta: match native_op {
                                tidb_ast::BinaryOp::Plus => {
                                    native_int_arithmetic_fn_meta::<IntIntPlus>()
                                }
                                tidb_ast::BinaryOp::Minus => {
                                    native_int_arithmetic_fn_meta::<IntIntMinus>()
                                }
                                tidb_ast::BinaryOp::Mul => {
                                    native_int_arithmetic_fn_meta::<IntIntMultiply>()
                                }
                                _ => unreachable!("only admitted signed integer operations"),
                            },
                            args_len: 2,
                            field_type: FieldTypeTp::LongLong.into(),
                            metadata: Box::new(overflow),
                        });
                    } else {
                        nodes.push(RpnExpressionNode::NativeArithmetic {
                            op,
                            overflow,
                            diagnostic,
                            field_type: control_type(f)?,
                        });
                    }
                    let end = nodes.len();
                    if let Some(guard) = guard {
                        let RpnExpressionNode::NullGuard { target, .. } = &mut nodes[guard] else {
                            unreachable!()
                        };
                        *target = end;
                    }
                }
                Visit::Enter(Expression::ScalarFunction(f)) if byte_count.is_some() => {
                    let [operand] = f.args.as_slice() else {
                        return Err("byte counts require one operand".into());
                    };
                    if !is_raw_string_operand(operand)
                        || !f.ret_type.as_ref().is_some_and(is_signed_bigint)
                    {
                        return Err(
                            "byte counts require a string leaf and signed BIGINT result".into()
                        );
                    }
                    work.push(Visit::IntCall(byte_count.unwrap(), 1, None));
                    work.push(Visit::ByteOperand(operand));
                }
                Visit::Enter(Expression::ScalarFunction(f)) if truth_test.is_some() => {
                    let (func_meta, invert) = truth_test.unwrap();
                    let [operand] = f.args.as_slice() else {
                        return Err("truth tests require one operand".into());
                    };
                    if bigint_operand_signedness(operand).is_none()
                        || !f.ret_type.as_ref().is_some_and(is_signed_bigint)
                    {
                        return Err(
                            "truth tests require a BIGINT operand and signed BIGINT result".into(),
                        );
                    }
                    if invert {
                        work.push(Visit::IntCall(
                            tidb_query_expr::impl_op::unary_not_int_fn_meta(),
                            1,
                            None,
                        ));
                    }
                    work.push(Visit::IntCall(func_meta, 1, None));
                    work.push(Visit::BigIntOperand(operand));
                }
                Visit::Enter(Expression::ScalarFunction(f))
                    if wire_integer_comparison(f).is_some()
                        || is_native_integer_comparison(f)
                        || matches!(f.func_name.lowercase(), "and" | "or") =>
                {
                    let [left, right] = f.args.as_slice() else {
                        return Err("logical operators and comparisons require two operands".into());
                    };
                    let name = f.func_name.lowercase();
                    let wire = wire_integer_comparison(f);
                    let logical = matches!(name, "and" | "or");
                    let [Some(lhs_unsigned), Some(rhs_unsigned)] =
                        [left, right].map(bigint_operand_signedness)
                    else {
                        return Err(
                            "logical operators and comparisons require matching BIGINT operands"
                                .into(),
                        );
                    };
                    if !f.ret_type.as_ref().is_some_and(is_signed_bigint)
                        || ((logical || wire.is_some()) && (lhs_unsigned || rhs_unsigned))
                    {
                        return Err(
                            "unsupported BIGINT declaration for logical operator or comparison"
                                .into(),
                        );
                    }
                    if let Some(name) = wire {
                        work.push(Visit::WireComparisonRight(
                            integer_comparison(name, false, false),
                            right,
                        ));
                    } else if logical {
                        let start = nodes.len();
                        nodes.push(RpnExpressionNode::LogicalStart {
                            and: name == "and",
                            end: 0,
                            preserve_mode: true,
                        });
                        work.push(Visit::LogicalNext(right, start));
                    } else {
                        work.push(Visit::IntCall(
                            integer_comparison(name, lhs_unsigned, rhs_unsigned),
                            2,
                            None,
                        ));
                        work.push(Visit::BigIntOperand(right));
                        work.push(Visit::BigIntOperand(left));
                        continue;
                    }
                    work.push(Visit::Enter(left));
                }
                Visit::WireComparisonRight(func_meta, right) => {
                    let guard = nodes.len();
                    nodes.push(RpnExpressionNode::NullGuard {
                        target: 0,
                        field_type: FieldTypeTp::LongLong.into(),
                    });
                    work.push(Visit::IntCall(func_meta, 2, Some(guard)));
                    work.push(Visit::Enter(right));
                }
                Visit::IntCall(func_meta, args_len, guard) => {
                    nodes.push(RpnExpressionNode::FnCall {
                        func_meta,
                        args_len,
                        field_type: FieldTypeTp::LongLong.into(),
                        metadata: Box::new(()),
                    });
                    let end = nodes.len();
                    if let Some(guard) = guard {
                        let RpnExpressionNode::NullGuard { target, .. } = &mut nodes[guard] else {
                            unreachable!()
                        };
                        *target = end;
                    }
                }
                Visit::LogicalNext(right, start) => {
                    nodes.push(RpnExpressionNode::LogicalNext);
                    work.push(Visit::LogicalEnd(start));
                    work.push(Visit::Enter(right));
                }
                Visit::LogicalEnd(start) => {
                    nodes.push(RpnExpressionNode::LogicalEnd {
                        field_type: FieldTypeTp::LongLong.into(),
                    });
                    let end = nodes.len();
                    let RpnExpressionNode::LogicalStart { end: target, .. } = &mut nodes[start]
                    else {
                        unreachable!()
                    };
                    *target = end;
                }
                Visit::Enter(Expression::ScalarFunction(f))
                    if matches!(f.func_name.lowercase(), "case" | "coalesce" | "ifnull") =>
                {
                    use tidb_datatype::FieldTypeCode;
                    let name = f.func_name.lowercase();
                    let declared = f
                        .ret_type
                        .as_ref()
                        .filter(|ty| {
                            matches!(
                                ty.code(),
                                FieldTypeCode::LongLong
                                    | FieldTypeCode::Double
                                    | FieldTypeCode::NewDecimal
                            ) && !ty.is_unsigned()
                                && !ty.is_array()
                                || is_raw_string_type(ty)
                        })
                        .ok_or("conditional result requires matching BIGINT, DOUBLE, DECIMAL or string")?;
                    let string_result = is_raw_string_type(declared);
                    if string_result && string_conditional_collation(f).is_none() {
                        return Err("string conditional requires matching string results".into());
                    }
                    if f.args.iter().enumerate().any(|(index, arg)| {
                        let condition =
                            name == "case" && index % 2 == 0 && index + 1 < f.args.len();
                        if condition {
                            bigint_operand_signedness(arg) != Some(false)
                        } else {
                            !string_result && !matches_conditional_value(arg, declared.code())
                        }
                    }) {
                        return Err("conditional arguments require matching typed values".into());
                    }
                    let field_type = control_type(f)?;
                    match (name, f.args.as_slice()) {
                        ("case", [_, _, ..]) => {
                            work.push(Visit::Conditional(&f.args, field_type));
                        }
                        ("ifnull", [first, last]) => {
                            let first = result_operand(first, &field_type);
                            work.push(Visit::Alternative(
                                std::slice::from_ref(last),
                                Vec::new(),
                                field_type,
                            ));
                            work.push(first);
                        }
                        ("coalesce", [first, rest @ ..]) => {
                            let first = result_operand(first, &field_type);
                            work.push(Visit::Alternative(rest, Vec::new(), field_type));
                            work.push(first);
                        }
                        _ => return Err("invalid conditional arity".into()),
                    }
                }
                Visit::Alternative(remaining, mut guards, field_type) => {
                    if let [next, rest @ ..] = remaining {
                        guards.push(nodes.len());
                        nodes.push(RpnExpressionNode::JumpIfNotNull {
                            target: 0,
                            field_type: field_type.clone(),
                        });
                        let next = result_operand(next, &field_type);
                        work.push(Visit::Alternative(rest, guards, field_type));
                        work.push(next);
                    } else {
                        let end = nodes.len();
                        for index in guards {
                            let RpnExpressionNode::JumpIfNotNull { target, .. } = &mut nodes[index]
                            else {
                                unreachable!()
                            };
                            *target = end;
                        }
                    }
                }
                Visit::Enter(Expression::ScalarFunction(f)) if f.func_name.lowercase() == "if" => {
                    if f.args.len() != 3 {
                        return Err("IF requires three arguments".into());
                    }
                    if f.args[0]
                        .static_type()
                        .is_none_or(|tp| tp.eval_type() != tidb_datatype::EvalType::Int)
                    {
                        return Err("IF requires an explicitly typed integer condition".into());
                    }
                    // shortcut: FLOAT/BIT and decorated result shapes need shared coercion;
                    // reject them until that instruction is implemented.
                    let declared = f.ret_type.as_ref().ok_or("IF requires a return type")?;
                    if is_raw_string_type(declared) {
                        if string_conditional_collation(f).is_none() {
                            return Err(
                                "string conditional requires matching string results".into()
                            );
                        }
                        work.push(Visit::Conditional(&f.args, control_type(f)?));
                        continue;
                    }
                    if !matches!(
                        declared.code(),
                        tidb_datatype::FieldTypeCode::LongLong
                            | tidb_datatype::FieldTypeCode::Double
                            | tidb_datatype::FieldTypeCode::NewDecimal
                            | tidb_datatype::FieldTypeCode::Date
                            | tidb_datatype::FieldTypeCode::Datetime
                            | tidb_datatype::FieldTypeCode::Timestamp
                    ) || declared.is_unsigned()
                        || declared.is_array()
                    {
                        return Err("IF result shape requires unimplemented shared coercion".into());
                    }
                    let result_type = f
                        .ret_type
                        .as_ref()
                        .ok_or("IF requires a return type")?
                        .eval_type();
                    if f.args[1..].iter().any(|arg| {
                        arg.static_type().is_none_or(|t| {
                            t.eval_type() != result_type
                                && !(is_time_type(t) && is_time_type(declared))
                        })
                    }) {
                        return Err(
                            "IF currently requires branches explicitly cast to its result type"
                                .into(),
                        );
                    }
                    work.push(Visit::Conditional(&f.args, control_type(f)?));
                }
                Visit::Conditional(args, field_type) => match args {
                    [condition, value, rest @ ..] => {
                        work.push(Visit::IfCondition(value, rest, field_type));
                        work.push(Visit::Enter(condition));
                    }
                    [value] => work.push(result_operand(value, &field_type)),
                    [] => nodes.push(match field_type.as_accessor().tp() {
                        FieldTypeTp::Double => RpnExpressionNode::ExpressionConstant {
                            value: ExpressionScalar::Real(None),
                            field_type,
                        },
                        FieldTypeTp::NewDecimal => RpnExpressionNode::ExpressionConstant {
                            value: ExpressionScalar::Decimal(None),
                            field_type,
                        },
                        FieldTypeTp::VarString => RpnExpressionNode::Constant {
                            value: ScalarValue::Bytes(None),
                            field_type,
                        },
                        _ => RpnExpressionNode::Constant {
                            value: ScalarValue::Int(None),
                            field_type,
                        },
                    }),
                },
                Visit::IfCondition(value, rest, field_type) => {
                    let at = nodes.len();
                    nodes.push(RpnExpressionNode::JumpIfFalse {
                        target: 0,
                        end: 0,
                        field_type: field_type.clone(),
                    });
                    let value = result_operand(value, &field_type);
                    work.push(Visit::IfThen(rest, field_type, at));
                    work.push(value);
                }
                Visit::IfThen(rest, field_type, branch) => {
                    let at = nodes.len();
                    nodes.push(RpnExpressionNode::Jump {
                        target: 0,
                        field_type: field_type.clone(),
                    });
                    let RpnExpressionNode::JumpIfFalse { target, .. } = &mut nodes[branch] else {
                        unreachable!()
                    };
                    *target = at + 1;
                    work.push(Visit::IfElse(at, branch));
                    work.push(Visit::Conditional(rest, field_type));
                }
                Visit::IfElse(jump, branch) => {
                    let end = nodes.len();
                    let RpnExpressionNode::Jump { target, .. } = &mut nodes[jump] else {
                        unreachable!()
                    };
                    *target = end;
                    let RpnExpressionNode::JumpIfFalse { end: merge, .. } = &mut nodes[branch]
                    else {
                        unreachable!()
                    };
                    *merge = end;
                }
                Visit::Enter(Expression::ScalarFunction(f))
                    if matches!(f.func_name.lowercase(), "json_length" | "regexp_like") =>
                {
                    let name = f.func_name.lowercase();
                    if f.ret_type.as_ref().is_none_or(|tp| {
                        tp.code() != tidb_datatype::FieldTypeCode::LongLong
                            || tp.is_unsigned()
                            || tp.is_array()
                    }) {
                        return Err("JSON_LENGTH/REGEXP_LIKE require signed integer output".into());
                    }
                    if (name == "json_length" && f.args.len() != 1)
                        || (name == "regexp_like" && !matches!(f.args.len(), 2 | 3))
                    {
                        return Err("unsupported JSON_LENGTH/REGEXP_LIKE arity".into());
                    }
                    if name == "regexp_like"
                        && tidb_datatype::is_ci_collation(f.derived_collation().name())
                    {
                        return Err(
                            "REGEXP_LIKE requires a case-sensitive effective collation".into()
                        );
                    }
                    let default_string =
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    if f.args.iter().any(|a| {
                        a.static_type().is_none_or(|tp| {
                            tp.code() != default_string.code()
                                || tp.collation_name() != default_string.collation_name()
                                || tp.is_array()
                        })
                    }) {
                        return Err("complex guard kernels currently require default UTF-8 string arguments".into());
                    }
                    if name == "json_length" {
                        work.push(Visit::JsonLength);
                        work.push(Visit::ParseJson);
                    } else {
                        work.push(Visit::RegexpLike(f, 0, Vec::new()));
                        work.push(Visit::Enter(&f.args[0]));
                        continue;
                    }
                    work.extend(f.args.iter().rev().map(Visit::Enter));
                }
                Visit::Enter(Expression::ScalarFunction(f))
                    if f.func_name.lowercase() == "json_type" && f.pb_signature().is_none() =>
                {
                    let [operand] = f.args.as_slice() else {
                        return Err("JSON_TYPE requires one operand".into());
                    };
                    if !f.ret_type.as_ref().is_some_and(is_raw_string_type)
                        || operand.static_type().is_none_or(|ty| {
                            ty.code() != tidb_datatype::FieldTypeCode::Json || ty.is_array()
                        })
                    {
                        return Err(
                            "JSON_TYPE requires a binary JSON operand and string result".into()
                        );
                    }
                    work.push(Visit::JsonType);
                    work.push(Visit::JsonOperand(operand));
                }
                Visit::JsonType => nodes.push(RpnExpressionNode::FnCall {
                    func_meta: tidb_query_expr::impl_json::native_json_type_fn_meta(),
                    args_len: 1,
                    field_type: bytes_type(),
                    metadata: Box::new(()),
                }),
                Visit::Enter(Expression::ScalarFunction(f)) if f.func_name.lowercase() == "md5" => {
                    if f.args.len() != 1 {
                        return Err("MD5 requires one argument".into());
                    }
                    if f.ret_type.as_ref().is_none_or(|tp| {
                        tp.eval_type() != tidb_datatype::EvalType::String || tp.is_array()
                    }) {
                        return Err("MD5 requires a string result declaration".into());
                    }
                    if f.args[0]
                        .static_type()
                        .is_none_or(|tp| tp.eval_type() != tidb_datatype::EvalType::String)
                    {
                        return Err(
                            "MD5 requires an explicitly typed byte argument in this checkpoint"
                                .into(),
                        );
                    }
                    let argument = &f.args[0];
                    work.push(Visit::Md5);
                    if f.pb_signature().is_none()
                        && matches!(argument, Expression::Column(_))
                        && is_raw_string_operand(argument)
                    {
                        work.push(Visit::ByteOperand(argument));
                    } else {
                        work.push(Visit::Enter(argument));
                    }
                }
                Visit::ByteOperand(value @ Expression::ScalarFunction(_)) => {
                    work.push(Visit::Enter(value));
                }
                Visit::Enter(_) | Visit::ByteOperand(_) | Visit::JsonOperand(_) => {
                    return Err("expression is outside shared kernel admission".into())
                }
                Visit::Md5 => nodes.push(RpnExpressionNode::FnCall {
                    func_meta: tidb_query_expr::impl_encryption::md5_fn_meta(),
                    args_len: 1,
                    field_type: bytes_type(),
                    metadata: Box::new(()),
                }),
                Visit::ParseJson => {
                    let mut field_type: tipb::FieldType = FieldTypeTp::Json.into();
                    field_type
                        .as_mut_accessor()
                        .set_flag(FieldTypeFlag::PARSE_TO_JSON);
                    nodes.push(RpnExpressionNode::FnCall {
                        func_meta: tidb_query_expr::impl_cast::parse_native_json_fn_meta(),
                        args_len: 1,
                        field_type,
                        metadata: Box::new(()),
                    });
                }
                Visit::JsonLength => nodes.push(RpnExpressionNode::FnCall {
                    func_meta: tidb_query_expr::impl_json::json_length_fn_meta(),
                    args_len: 1,
                    field_type: FieldTypeTp::LongLong.into(),
                    metadata: Box::new(()),
                }),
                // Native preparation does not parse a guarded constant pattern.
                Visit::RegexpLike(f, index, mut guards) => {
                    guards.push(nodes.len());
                    nodes.push(RpnExpressionNode::RegexpArgument {
                        target: 0,
                        count: index + 1,
                        field_type: FieldTypeTp::LongLong.into(),
                    });
                    if index + 1 < f.args.len() {
                        work.push(Visit::RegexpLike(f, index + 1, guards));
                        work.push(Visit::Enter(&f.args[index + 1]));
                    } else {
                        if f.args[1..].iter().all(|arg| matches!(arg,
                            Expression::Constant(c) if matches!(c.literal_value(), Some(Datum::Bytes(_) | Datum::String(_)))
                        )) {
                            nodes.push(RpnExpressionNode::NativeRegexpLike {
                                args_len: f.args.len(),
                                field_type: FieldTypeTp::LongLong.into(),
                            });
                        } else {
                        nodes.push(RpnExpressionNode::FnCall {
                            func_meta: tidb_query_expr::impl_regexp::native_regexp_like_fn_meta(),
                            args_len: f.args.len(),
                            field_type: FieldTypeTp::LongLong.into(),
                            metadata: Box::new(()),
                        });
                        }
                        let end = nodes.len();
                        for at in guards {
                            let RpnExpressionNode::RegexpArgument { target, .. } = &mut nodes[at]
                            else {
                                unreachable!()
                            };
                            *target = end;
                        }
                    }
                }
            }
        }
        let schema: Vec<tipb::FieldType> = columns
            .iter()
            .map(|(_, tp)| match tp.code() {
                tidb_datatype::FieldTypeCode::LongLong => bigint_type(tp.is_unsigned()),
                tidb_datatype::FieldTypeCode::Double => FieldTypeTp::Double.into(),
                tidb_datatype::FieldTypeCode::NewDecimal => FieldTypeTp::NewDecimal.into(),
                tidb_datatype::FieldTypeCode::Json => FieldTypeTp::Json.into(),
                _ if is_time_type(tp) => time_type(tp),
                _ => bytes_type(),
            })
            .collect();
        let temporary = RpnExpression::from(nodes);
        let exit = match expression {
            Expression::ScalarFunction(f) if f.func_name.lowercase() == "if" => control_type(f)?,
            _ => temporary.ret_field_type(&schema).clone(),
        };
        let mut nodes = temporary.into_inner();
        if row_fallback {
            for node in &mut nodes {
                match node {
                    RpnExpressionNode::JumpIfFalse { target, end, .. } => {
                        *target += 1;
                        *end += 1;
                    }
                    RpnExpressionNode::Jump { target, .. }
                    | RpnExpressionNode::JumpIfNotNull { target, .. }
                    | RpnExpressionNode::RegexpArgument { target, .. }
                    | RpnExpressionNode::NullGuard { target, .. } => *target += 1,
                    RpnExpressionNode::Repeat { end, .. }
                    | RpnExpressionNode::LogicalStart { end, .. } => *end += 1,
                    _ => {}
                }
            }
            nodes.insert(
                0,
                RpnExpressionNode::OrderedRows {
                    end: nodes.len() + 1,
                    field_type: exit.clone(),
                },
            );
        }
        let program = RpnExpression::finish(nodes, &schema, exit).map_err(|e| e.to_string())?;
        Ok(Self {
            program,
            columns,
            schema,
            result_type: expression.static_type().cloned(),
            result_collation: match expression {
                Expression::ScalarFunction(f) => {
                    string_conditional_collation(f).unwrap_or(tidb_datatype::Collation::DEFAULT)
                }
                _ => tidb_datatype::Collation::DEFAULT,
            },
        })
    }

    /// Execute a native chunk selection through the shared RPN loop.
    ///
    /// The selection uses physical row indices, retaining duplicates and order.
    /// Only columns referenced by the compiled program are adapted.
    pub fn evaluate(
        &self,
        input: &Chunk,
        rows: &[usize],
        ctx: &mut EvalContext,
    ) -> Result<Vec<Datum>, SharedEvaluationError> {
        self.evaluate_mode(input, rows, ctx, InvocationMode::Vector)
    }

    /// Explicit scalar invocation accepts exactly one selected row.
    pub fn evaluate_mode(
        &self,
        input: &Chunk,
        rows: &[usize],
        ctx: &mut EvalContext,
        mode: InvocationMode,
    ) -> Result<Vec<Datum>, SharedEvaluationError> {
        self.start_execution(&crate::context::NoColumns)
            .evaluate(input, rows, ctx, mode)
    }

    /// Start an independent execution bound to one effects context.
    pub fn start_execution<'p, 'e>(
        &'p self,
        effects: &'e dyn crate::context::Columns,
    ) -> SharedExpressionExecution<'p, 'e> {
        SharedExpressionExecution {
            expression: self,
            execution: tidb_query_expr::RpnExecution::new(
                &self.program,
                effects.supports_seeded_rand(),
            ),
            effects,
        }
    }
}

/// A frozen coprocessor comparison over two signed BIGINT leaf operands.
#[derive(Debug)]
pub struct SharedIntegerComparison(SharedExpression);

impl SharedIntegerComparison {
    /// Check the decoded request and bind column references to the scan schema.
    pub fn compile(
        expression: &Expression,
        input_types: &[tidb_datatype::FieldType],
    ) -> Option<Self> {
        let Expression::ScalarFunction(function) = expression else {
            return None;
        };
        wire_integer_comparison(function)?;
        if !function
            .args
            .iter()
            .all(|arg| matches!(arg, Expression::Column(_) | Expression::Constant(_)))
        {
            return None;
        }
        let program = SharedExpression::compile(expression).ok()?;
        if program
            .columns
            .iter()
            .any(|(index, field)| input_types.get(*index) != Some(field))
        {
            return None;
        }
        Some(Self(program))
    }

    /// Compare one scan row. Nonmatching row carriers retain the caller's original path.
    pub fn evaluate(&self, row: &[Datum]) -> Option<Result<Datum, SharedEvaluationError>> {
        struct Input([ScalarValue; 2]);
        impl InputColumns for Input {
            fn read<'a>(
                &'a self,
                schema: &'a [tipb::FieldType],
                offset: usize,
                _rows: &'a [usize],
                _ctx: &mut EvalContext,
            ) -> InputResult<'a> {
                Ok(RpnStackNode::Scalar {
                    value: &self.0[offset],
                    field_type: &schema[offset],
                })
            }
        }
        let mut values = [ScalarValue::Int(None), ScalarValue::Int(None)];
        for (value, (index, _)) in values.iter_mut().zip(&self.0.columns) {
            *value = match row.get(*index)? {
                Datum::Int(value) => ScalarValue::Int(Some(*value)),
                Datum::Null => ScalarValue::Int(None),
                _ => return None,
            };
        }
        let input = Input(values);
        Some(
            self.0
                .program
                .eval_input_mode(
                    &mut EvalContext::default(),
                    &self.0.schema,
                    &input,
                    &[0],
                    1,
                    InvocationMode::Scalar,
                )
                .map_err(SharedEvaluationError::from)
                .and_then(|value| match value.get_logical_scalar_ref(0) {
                    ScalarValueRef::Int(value) => {
                        Ok(value.map_or(Datum::Null, |value| Datum::Int(*value)))
                    }
                    _ => Err(SharedEvaluationError::from(
                        "expected integer comparison result",
                    )),
                }),
        )
    }
}

/// Mutable execution state, separate from the frozen expression.
pub struct SharedExpressionExecution<'p, 'e> {
    expression: &'p SharedExpression,
    execution: tidb_query_expr::RpnExecution<'p>,
    effects: &'e dyn crate::context::Columns,
}
impl SharedExpressionExecution<'_, '_> {
    /// Evaluate another batch in this execution, retaining execution state.
    pub fn evaluate(
        &mut self,
        input: &Chunk,
        rows: &[usize],
        ctx: &mut EvalContext,
        mode: InvocationMode,
    ) -> Result<Vec<Datum>, SharedEvaluationError> {
        if mode == InvocationMode::Scalar && rows.len() != 1 {
            return Err("scalar invocation requires one row".into());
        }
        if rows.iter().any(|r| *r >= input.physical_rows()) {
            return Err("selected row is outside the input chunk".into());
        }
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        if rows.len() > tidb_query_datatype::codec::data_type::BATCH_MAX_SIZE {
            return Err("selection exceeds the RPN batch limit".into());
        }
        let columns = NativeInput {
            chunk: input,
            columns: &self.expression.columns,
            effects: self.effects,
        };
        let result = self
            .execution
            .eval_input_mode(
                ctx,
                &self.expression.schema,
                &columns,
                rows,
                rows.len(),
                mode,
            )
            .map_err(SharedEvaluationError::from)?;
        native_output_with_collation(result, rows.len(), self.expression.result_collation)
            .map_err(SharedEvaluationError::from)
    }
}

/// A select list whose root order is executed inside the shared engine.
#[derive(Debug)]
pub struct SharedProjection {
    expressions: Vec<SharedExpression>,
    order: OutputOrder,
}
impl SharedProjection {
    /// Compile a frozen bound expression snapshot. Execute through an explicit
    /// execution object; no cross-EXECUTE cache or default integration is installed.
    pub fn compile(expressions: &[Expression]) -> Result<Self, String> {
        Ok(Self {
            expressions: expressions
                .iter()
                .map(SharedExpression::compile)
                .collect::<Result<_, _>>()?,
            order: if crate::evaluator::vectorizable(expressions) {
                OutputOrder::Columns
            } else {
                OutputOrder::Rows
            },
        })
    }
    /// Compile all predicates against the bound child schema, before reading rows.
    pub fn compile_selection(
        filters: &[Expression],
        input_types: &[tidb_datatype::FieldType],
    ) -> Result<Self, String> {
        if filters.is_empty()
            || !crate::evaluator::vectorizable(filters)
            || filters
                .iter()
                .any(|filter| !filter.static_type().is_some_and(is_signed_bigint))
        {
            return Err("shared selection requires vectorizable signed BIGINT predicates".into());
        }
        let program = Self::compile(filters)?;
        if program.expressions.iter().any(|expression| {
            expression
                .columns
                .iter()
                .any(|(column, field_type)| input_types.get(*column) != Some(field_type))
        }) {
            return Err("selection input declarations differ from the child schema".into());
        }
        Ok(program)
    }

    /// Largest number of selected rows supported in one child batch.
    pub const MAX_BATCH_SIZE: usize = tidb_query_datatype::codec::data_type::BATCH_MAX_SIZE;

    /// Validate the native output schema before opting a caller into this group.
    pub fn matches_output_types(&self, types: &[tidb_datatype::FieldType]) -> bool {
        self.expressions.len() == types.len()
            && self
                .expressions
                .iter()
                .zip(types)
                .all(|(expr, tp)| expr.result_type() == Some(tp))
    }

    /// Own this frozen program and effects when a caller cannot borrow them
    /// across its execution callbacks.
    pub fn into_owned_execution<C: crate::Columns>(self, effects: C) -> OwnedSharedProjection<C> {
        let supports_seeded_rand = effects.supports_seeded_rand();
        OwnedSharedProjection {
            roots: self
                .expressions
                .into_iter()
                .map(|expr| OwnedProjectionRoot {
                    execution: OwnedRpnExecution::new(expr.program, supports_seeded_rand),
                    columns: expr.columns,
                    schema: expr.schema,
                })
                .collect(),
            order: self.order,
            effects,
        }
    }

    /// Bind one execution to a fixed effects context; reuse it across chunks.
    pub fn start_execution<'p, 'e>(
        &'p self,
        effects: &'e dyn crate::context::Columns,
    ) -> SharedProjectionExecution<'p, 'e> {
        let supports_seeded_rand = effects.supports_seeded_rand();
        SharedProjectionExecution {
            projection: self,
            executions: self
                .expressions
                .iter()
                .map(|e| tidb_query_expr::RpnExecution::new(&e.program, supports_seeded_rand))
                .collect(),
            effects,
        }
    }
}

/// One projection execution. New objects start independent seeded streams.
pub struct SharedProjectionExecution<'p, 'e> {
    projection: &'p SharedProjection,
    executions: Vec<tidb_query_expr::RpnExecution<'p>>,
    effects: &'e dyn crate::context::Columns,
}
impl<'p> SharedProjectionExecution<'p, '_> {
    /// Return one typed native column per root.
    pub fn evaluate(
        &mut self,
        input: &Chunk,
        rows: &[usize],
        ctx: &mut EvalContext,
    ) -> Result<Vec<Vec<Datum>>, SharedEvaluationError> {
        let expressions = &self.projection.expressions;
        let order = self.projection.order;
        let effects = self.effects;
        with_projection_results(self.roots(), order, effects, input, rows, ctx, |results| {
            if rows.is_empty() {
                return Ok(expressions.iter().map(|_| Vec::new()).collect());
            }
            results
                .into_iter()
                .zip(expressions)
                .map(|(value, expr)| {
                    native_output_with_collation(value, rows.len(), expr.result_collation)
                        .map_err(SharedEvaluationError::from)
                })
                .collect()
        })
    }

    /// Writes the same evaluated roots without materializing Decimal text.
    /// Output must own its columns independently of all other live chunks.
    /// Detectable input/output and output/output aliases are rejected before
    /// effects. Evaluation/validation errors preserve output; effects remain.
    pub fn evaluate_into_chunk(
        &mut self,
        input: &Chunk,
        rows: &[usize],
        ctx: &mut EvalContext,
        output: &mut Chunk,
    ) -> Result<(), SharedEvaluationError> {
        let order = self.projection.order;
        let effects = self.effects;
        write_projection(self.roots(), order, effects, input, rows, ctx, output)
    }

    fn roots<'s>(&'s mut self) -> impl ExactSizeIterator<Item = ProjectionRoot<'p, 's>> {
        self.executions
            .iter_mut()
            .zip(&self.projection.expressions)
            .map(|(execution, expr)| ProjectionRoot {
                execution: execution.borrow_execution(),
                columns: &expr.columns,
                schema: &expr.schema,
            })
    }
}

/// Serial owner of one frozen projection and its statement effects.
pub struct OwnedSharedProjection<C> {
    roots: Vec<OwnedProjectionRoot>,
    order: OutputOrder,
    effects: C,
}

struct OwnedProjectionRoot {
    execution: OwnedRpnExecution,
    columns: Vec<(usize, tidb_datatype::FieldType)>,
    schema: Vec<tipb::FieldType>,
}

struct ProjectionRoot<'p, 's> {
    execution: RpnExecutionRef<'p, 's>,
    columns: &'p [(usize, tidb_datatype::FieldType)],
    schema: &'p [tipb::FieldType],
}

impl<C: crate::Columns> OwnedSharedProjection<C> {
    /// Clear the per-execution state while keeping the frozen program.
    pub fn reset(&mut self) {
        let supports_seeded_rand = self.effects.supports_seeded_rand();
        for root in &mut self.roots {
            root.execution.reset(supports_seeded_rand);
        }
    }

    /// Evaluate a compiled predicate group with the native filter's live-row rules.
    pub fn evaluate_selection_mask(
        &mut self,
        filters: &[Expression],
        input: &Chunk,
        selected: &mut Vec<bool>,
    ) -> Result<(), SharedEvaluationError> {
        let width = input.physical_rows();
        if width > SharedProjection::MAX_BATCH_SIZE || filters.len() != self.roots.len() {
            return Err("invalid shared selection input".into());
        }
        selected.clear();
        selected.resize(width, false);
        let mut nulls = vec![false; width];
        let mut ctx = EvalContext::default();
        crate::evaluator::select_live_rows(
            filters,
            selected,
            &mut nulls,
            |index, _, rows, truth| {
                let root = &mut self.roots[index];
                let columns = NativeInput {
                    chunk: input,
                    columns: &root.columns,
                    effects: &self.effects,
                };
                let result = root.execution.borrow_execution().eval_input_mode(
                    &mut ctx,
                    &root.schema,
                    &columns,
                    rows,
                    rows.len(),
                    InvocationMode::Vector,
                )?;
                truth.clear();
                for logical in 0..rows.len() {
                    let ScalarValueRef::Int(value) = result.get_logical_scalar_ref(logical) else {
                        return Err(SharedEvaluationError::from("expected integer predicate"));
                    };
                    truth.push(value.map_or(-1, |value| i8::from(*value != 0)));
                }
                Ok(())
            },
        )?;
        crate::evaluator::apply_input_selection(input, selected);
        Ok(())
    }

    /// Evaluate a child chunk's logical rows into an independently owned chunk.
    pub fn evaluate_into_chunk(
        &mut self,
        input: &Chunk,
        output: &mut Chunk,
    ) -> Result<(), SharedEvaluationError> {
        let contiguous;
        let rows = match input.sel() {
            Some(rows) => rows,
            None => {
                contiguous = (0..input.num_rows()).collect::<Vec<_>>();
                &contiguous
            }
        };
        let roots = self.roots.iter_mut().map(|root| ProjectionRoot {
            execution: root.execution.borrow_execution(),
            columns: &root.columns,
            schema: &root.schema,
        });
        write_projection(
            roots,
            self.order,
            &self.effects,
            input,
            rows,
            &mut EvalContext::default(),
            output,
        )
    }
}

fn write_projection<'p, 's>(
    roots: impl ExactSizeIterator<Item = ProjectionRoot<'p, 's>>,
    order: OutputOrder,
    effects: &dyn crate::Columns,
    input: &Chunk,
    rows: &[usize],
    ctx: &mut EvalContext,
    output: &mut Chunk,
) -> Result<(), SharedEvaluationError> {
    if output.num_cols() != roots.len() {
        return Err("output column count differs from projection".into());
    }
    for col in 0..output.num_cols() {
        if (0..input.num_cols()).any(|i| output.columns_share_identity(col, input, i))
            || (0..col).any(|i| output.columns_share_identity(col, output, i))
        {
            return Err("direct output requires independent columns".into());
        }
    }
    let roots: Vec<_> = roots.collect();
    for (col, root) in roots.iter().enumerate() {
        let expected = match root
            .execution
            .program()
            .ret_field_type(root.schema)
            .as_accessor()
            .tp()
        {
            FieldTypeTp::NewDecimal => tidb_chunk::column::MY_DECIMAL_STRUCT_SIZE,
            FieldTypeTp::Double
            | FieldTypeTp::LongLong
            | FieldTypeTp::Date
            | FieldTypeTp::DateTime
            | FieldTypeTp::Timestamp => 8,
            _ => -1,
        };
        if output.column(col).type_size() != expected {
            return Err("output column storage type mismatch".into());
        }
    }
    with_projection_results(roots, order, effects, input, rows, ctx, |results| {
        enum Root {
            Decimal(Vec<Option<tidb_datatype::DecimalArithmeticValue>>),
            Datum(Vec<Datum>),
        }
        let mut prepared = Vec::with_capacity(results.len());
        for result in results {
            let values = match result {
                RpnStackNode::ExpressionVector {
                    value: ExpressionVector::Decimal(values),
                    ..
                } => Some(values),
                RpnStackNode::ExpressionScalar { value, .. }
                    if matches!(value.as_ref(), ExpressionScalar::Decimal(_)) =>
                {
                    let ExpressionScalar::Decimal(value) = value.as_ref() else {
                        unreachable!()
                    };
                    Some(vec![value.clone(); rows.len()])
                }
                other => {
                    prepared.push(Root::Datum(native_output(other, rows.len())?));
                    None
                }
            };
            if let Some(values) = values {
                if values.len() != rows.len() || values.iter().flatten().any(|v| !v.is_valid()) {
                    return Err("invalid expression decimal result".into());
                }
                prepared.push(Root::Decimal(values));
            }
        }
        output.reset();
        for (col, values) in prepared.into_iter().enumerate() {
            match values {
                Root::Decimal(values) => {
                    for value in values {
                        match value {
                            Some(value) => {
                                output.append_my_decimal(col, &value.to_chunk_my_decimal_lossy())
                            }
                            None => output.append_null(col),
                        }
                    }
                }
                Root::Datum(values) => {
                    for value in values {
                        output.append_datum(col, &value);
                    }
                }
            }
        }
        output.set_num_virtual_rows(rows.len());
        Ok(())
    })
}

fn with_projection_results<'p, 's, T>(
    roots: impl IntoIterator<Item = ProjectionRoot<'p, 's>>,
    order: OutputOrder,
    effects: &dyn crate::Columns,
    input: &Chunk,
    rows: &[usize],
    ctx: &mut EvalContext,
    consume: impl FnOnce(Vec<RpnStackNode<'_>>) -> Result<T, SharedEvaluationError>,
) -> Result<T, SharedEvaluationError> {
    if rows.iter().any(|r| *r >= input.physical_rows())
        || rows.len() > tidb_query_datatype::codec::data_type::BATCH_MAX_SIZE
    {
        return Err("invalid group selection".into());
    }
    if rows.is_empty() {
        return consume(Vec::new());
    }
    let (inputs, executions): (Vec<_>, Vec<_>) = roots
        .into_iter()
        .map(|root| {
            (
                NativeInput {
                    chunk: input,
                    columns: root.columns,
                    effects,
                },
                (root.execution, root.schema),
            )
        })
        .unzip();
    let mut programs: Vec<_> = executions
        .into_iter()
        .zip(&inputs)
        .map(|((execution, schema), input)| (execution, schema, input as &dyn InputColumns))
        .collect();
    let results = RpnExecutionRef::eval_group(
        &mut programs,
        ctx,
        rows,
        order,
        if effects.enable_vectorized_expression() {
            InvocationMode::Vector
        } else {
            InvocationMode::Scalar
        },
    )
    .map_err(SharedEvaluationError::from)?;
    consume(results)
}

struct NativeInput<'a> {
    chunk: &'a Chunk,
    columns: &'a [(usize, tidb_datatype::FieldType)],
    effects: &'a dyn crate::context::Columns,
}

impl tidb_datatype::ConversionWarningAppender for NativeInput<'_> {
    fn append_conversion_warning(&self, warning: tidb_error::terror::TerrorError) {
        let warning = warning.to_sql_error();
        self.effects.append_warning(warning.code, &warning.message);
    }
}

impl InputColumns for NativeInput<'_> {
    fn parameter(&self, order: usize) -> tidb_query_expr::evaluation_error::Result<Datum> {
        self.effects.param_value(order).map_err(native_policy_error)
    }
    fn conversion_flags(&self) -> tidb_datatype::ConversionFlags {
        self.effects.type_flags()
    }
    fn conversion_location(&self) -> tidb_datatype::ConversionLocation {
        tidb_datatype::ConversionLocation::from_time_zone(&self.effects.time_zone())
    }
    fn conversion_warnings(&self) -> &dyn tidb_datatype::ConversionWarningAppender {
        self
    }
    fn div_precision_increment(&self) -> u32 {
        self.effects.div_precision_increment()
    }
    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.effects.date_modes()
    }

    fn handle_truncate(&self, message: &str) -> tidb_query_expr::evaluation_error::Result<()> {
        self.effects
            .handle_truncate(message)
            .map_err(native_policy_error)
    }
    fn handle_division_by_zero(&self) -> tidb_query_expr::evaluation_error::Result<()> {
        self.effects
            .handle_division_by_zero()
            .map_err(native_policy_error)
    }

    fn get_string_variable(
        &self,
        name: &str,
    ) -> tidb_query_expr::evaluation_error::Result<Option<Vec<u8>>> {
        match self.effects.get_uservar(name) {
            None | Some(Datum::Null) => Ok(None),
            // shortcut: extend the typed string carrier before admitting metadata
            // that cannot survive a byte-only read followed by SETVAR.
            Some(Datum::String(v)) if v.collation() == tidb_datatype::Collation::DEFAULT => {
                Ok(Some(v.bytes().to_vec()))
            }
            _ => Err(tidb_query_expr::evaluation_error::EvaluateError::Other(
                "session variable carrier is outside default-string shared admission".into(),
            )
            .into()),
        }
    }
    fn set_string_variable(
        &self,
        name: &str,
        value: &[u8],
    ) -> tidb_query_expr::evaluation_error::Result<()> {
        self.effects
            .set_uservar(name, Datum::new_string(value.to_vec()));
        Ok(())
    }

    fn rand_next(&self) -> Option<f64> {
        self.effects.rand_next()
    }

    fn read<'a>(
        &'a self,
        schema: &'a [tipb::FieldType],
        offset: usize,
        rows: &'a [usize],
        _ctx: &mut EvalContext,
    ) -> InputResult<'a> {
        use tidb_query_datatype::codec::Error;
        let (index, field_type) = self
            .columns
            .get(offset)
            .ok_or(Error::ColumnOffset(offset))?;
        let index = *index;
        let eval_type = EvalType::try_from(schema[offset].as_accessor().tp())
            .map_err(|error| Error::InvalidDataType(error.to_string()))?;
        if index >= self.chunk.num_cols() {
            return Err(Error::ColumnOffset(index).into());
        }
        if rows.iter().any(|row| *row >= self.chunk.physical_rows()) {
            return Err(Error::InvalidDataType("selected row is outside input".into()).into());
        }
        if eval_type == EvalType::Decimal {
            let column = self.chunk.column(index);
            let values = rows
                .iter()
                .map(|row| {
                    (!column.is_null(*row)).then(|| {
                        tidb_datatype::DecimalArithmeticValue::from_column(
                            &column.get_my_decimal(*row),
                            field_type,
                        )
                    })
                })
                .collect();
            return Ok(RpnStackNode::ExpressionVector {
                value: ExpressionVector::Decimal(values),
                field_type: &schema[offset],
            });
        }
        let column = self.chunk.column(index);
        if eval_type == EvalType::DateTime {
            return Ok(RpnStackNode::ExpressionVector {
                value: ExpressionVector::Time(
                    rows.iter()
                        .map(|row| (!column.is_null(*row)).then(|| column.get_time(*row)))
                        .collect(),
                ),
                field_type: &schema[offset],
            });
        }
        if eval_type == EvalType::Real {
            return Ok(RpnStackNode::ExpressionVector {
                value: ExpressionVector::Real(
                    rows.iter()
                        .map(|row| (!column.is_null(*row)).then(|| column.get_float64(*row)))
                        .collect(),
                ),
                field_type: &schema[offset],
            });
        }
        let mut values = VectorValue::with_capacity(rows.len(), eval_type);
        for row in rows {
            match eval_type {
                EvalType::Int => {
                    // BIGINT comparison interprets the same raw word using its declaration.
                    values.push_int((!column.is_null(*row)).then(|| column.get_int64(*row)))
                }
                EvalType::Bytes => values
                    .push_bytes((!column.is_null(*row)).then(|| column.get_bytes(*row).to_vec())),
                EvalType::Json => {
                    let value = if column.is_null(*row) {
                        None
                    } else {
                        let bytes = column.get_bytes(*row);
                        let (&tag, payload) =
                            bytes.split_first().ok_or_else(invalid_native_json)?;
                        Some(native_binary_json(tag, payload)?)
                    };
                    values.push_json(value);
                }
                _ => unreachable!("compiler checks input types"),
            }
        }
        Ok(RpnStackNode::Vector {
            value: RpnStackNodeVectorValue::Generated {
                physical_value: values,
            },
            field_type: (&schema[offset]).into(),
        })
    }
}

fn invalid_native_json() -> tidb_query_expr::evaluation_error::Error {
    let error = crate::JsonError::InvalidText;
    tidb_query_expr::evaluation_error::EvaluateError::Custom {
        code: i32::from(error.code()),
        msg: error.message(),
    }
    .into()
}

fn native_binary_json(tag: u8, payload: &[u8]) -> tidb_query_expr::evaluation_error::Result<Json> {
    let tag = JsonType::try_from(tag).map_err(|_| invalid_native_json())?;
    Ok(Json::new(tag, payload.to_vec()))
}

// Native overflow text can inspect a hidden column's source even when
// the value comes from a materialized chunk column.
fn numeric_diagnostic_needs_binding(args: &[Expression]) -> Option<bool> {
    let mut needs_binding = false;
    let mut pending: Vec<_> = args.iter().collect();
    while let Some(expression) = pending.pop() {
        match expression {
            Expression::Constant(c) if c.deferred_expr.is_some() => return None,
            Expression::Constant(c) if c.param_marker.is_some() => needs_binding = true,
            Expression::Column(c) if c.is_hidden => pending.extend(c.virtual_expr.as_deref()),
            Expression::ScalarFunction(f)
                if f.func_name.lowercase() == "cast_json"
                    || crate::scalar_function::named_arithmetic_symbol(f.func_name.lowercase())
                        .is_some() =>
            {
                pending.extend(f.get_args());
            }
            _ => {}
        }
    }
    Some(needs_binding)
}

fn decimal_diagnostic(
    function: &crate::scalar_function::ScalarFunction,
) -> Option<Vec<NativeDiagnosticPart>> {
    fn append(expression: &Expression, parts: &mut Vec<NativeDiagnosticPart>) -> Option<()> {
        match expression {
            Expression::Constant(c) if c.param_marker.is_some() && c.deferred_expr.is_none() => {
                parts.push(NativeDiagnosticPart::Parameter(
                    usize::try_from(c.param_marker?.order).ok()?,
                ));
            }
            Expression::ScalarFunction(f) => append_function(f, parts)?,
            Expression::Column(c) if c.is_hidden && c.virtual_expr.is_some() => {
                append(c.virtual_expr.as_deref()?, parts)?
            }
            other => parts.push(NativeDiagnosticPart::Text(
                crate::scalar_function::numeric_expression_text(other, false, &crate::NoColumns)?,
            )),
        }
        Some(())
    }
    fn append_function(
        f: &crate::scalar_function::ScalarFunction,
        parts: &mut Vec<NativeDiagnosticPart>,
    ) -> Option<()> {
        let symbol = match f.func_name.lowercase() {
            "plus" => "+",
            "minus" => "-",
            "mul" => "*",
            "div" => "/",
            _ => return None,
        };
        let [left, right] = f.args.as_slice() else {
            return None;
        };
        parts.push(NativeDiagnosticPart::Text("(".into()));
        append(left, parts)?;
        parts.push(NativeDiagnosticPart::Text(format!(" {symbol} ")));
        append(right, parts)?;
        parts.push(NativeDiagnosticPart::Text(")".into()));
        Some(())
    }
    let mut parts = vec![NativeDiagnosticPart::Text(
        "DECIMAL value is out of range in '".into(),
    )];
    append_function(function, &mut parts)?;
    parts.push(NativeDiagnosticPart::Text("'".into()));
    // Existing static diagnostics need no runtime template.
    parts
        .iter()
        .any(|p| matches!(p, NativeDiagnosticPart::Parameter(_)))
        .then_some(parts)
}

fn native_policy_error(error: crate::EvalError) -> tidb_query_expr::evaluation_error::Error {
    use tidb_query_expr::evaluation_error::EvaluateError;
    let (code, msg) = match error {
        crate::EvalError::TruncatedWrongValue(message) => (1292, message),
        crate::EvalError::DivisionByZero => (1365, "Division by 0".into()),
        crate::EvalError::Unsupported(message) => (1105, message.into()),
        other => {
            return EvaluateError::Other(format!("unsupported native policy diagnostic: {other:?}"))
                .into()
        }
    };
    EvaluateError::Custom { code, msg }.into()
}

fn native_byte_count(
    function: &crate::scalar_function::ScalarFunction,
) -> Option<tidb_query_expr::RpnFnMeta> {
    use tidb_query_expr::impl_string::{
        ascii_fn_meta, bit_length_fn_meta, char_length_fn_meta, length_fn_meta,
    };

    if function.pb_signature().is_some() {
        return None;
    }
    Some(match function.func_name.lowercase() {
        "length" | "octet_length" => length_fn_meta(),
        "ascii" => ascii_fn_meta(),
        "bit_length" => bit_length_fn_meta(),
        "char_length" | "character_length"
            if function
                .args
                .first()
                .and_then(Expression::static_type)
                .is_some_and(tidb_datatype::FieldType::is_binary_string) =>
        {
            char_length_fn_meta()
        }
        _ => return None,
    })
}

fn is_raw_string_type(ty: &tidb_datatype::FieldType) -> bool {
    ty.is_string() && ty.code() != tidb_datatype::FieldTypeCode::Unspecified && !ty.is_array()
}

fn is_raw_string_operand(expression: &Expression) -> bool {
    if !expression.static_type().is_some_and(is_raw_string_type) {
        return false;
    }
    match expression {
        Expression::Column(_) => true,
        Expression::Constant(c) => matches!(
            c.literal_value(),
            Some(
                Datum::Null
                    | Datum::String(_)
                    | Datum::Bytes(_)
                    | Datum::Bit(_)
                    | Datum::BinaryLiteral(_)
            )
        ),
        _ => false,
    }
}

fn native_int_truth_test(
    function: &crate::scalar_function::ScalarFunction,
) -> Option<(tidb_query_expr::RpnFnMeta, bool)> {
    use tidb_query_expr::impl_op::{
        int_is_false_fn_meta, int_is_true_fn_meta, is_null_fn_meta, unary_not_int_fn_meta,
        KeepNullOff, KeepNullOn,
    };

    if function.pb_signature().is_some() {
        return None;
    }
    let name = function.func_name.lowercase();
    Some(match name {
        "not" => (unary_not_int_fn_meta(), false),
        "isnull" | "isunknown" => (is_null_fn_meta::<i64>(), false),
        "istrue" => (int_is_true_fn_meta::<KeepNullOff>(), false),
        "istrue_with_null" => (int_is_true_fn_meta::<KeepNullOn>(), false),
        "isnottrue" => (int_is_true_fn_meta::<KeepNullOff>(), true),
        "isfalse" => (int_is_false_fn_meta::<KeepNullOff>(), false),
        "isnotfalse" => (int_is_false_fn_meta::<KeepNullOff>(), true),
        "isnotunknown" => (is_null_fn_meta::<i64>(), true),
        _ => return None,
    })
}

fn is_native_integer_comparison(function: &crate::scalar_function::ScalarFunction) -> bool {
    function.pb_signature().is_none()
        && matches!(
            function.func_name.lowercase(),
            "eq" | "ne" | "lt" | "le" | "gt" | "ge" | "nulleq"
        )
}

fn wire_integer_comparison(
    function: &crate::scalar_function::ScalarFunction,
) -> Option<&'static str> {
    use tidb_proto::tipb::ScalarFuncSig;
    match function.pb_signature()? {
        ScalarFuncSig::EqInt => Some("eq"),
        ScalarFuncSig::GtInt => Some("gt"),
        _ => None,
    }
}

fn integer_comparison(
    name: &str,
    lhs_unsigned: bool,
    rhs_unsigned: bool,
) -> tidb_query_expr::RpnFnMeta {
    use tidb_query_expr::impl_compare::{
        CmpOpEq, CmpOpGe, CmpOpGt, CmpOpLe, CmpOpLt, CmpOpNe, CmpOpNullEq,
    };
    use tidb_query_expr::integer_comparison_fn_meta;
    let select = match name {
        "eq" => integer_comparison_fn_meta::<CmpOpEq>,
        "ne" => integer_comparison_fn_meta::<CmpOpNe>,
        "lt" => integer_comparison_fn_meta::<CmpOpLt>,
        "le" => integer_comparison_fn_meta::<CmpOpLe>,
        "gt" => integer_comparison_fn_meta::<CmpOpGt>,
        "ge" => integer_comparison_fn_meta::<CmpOpGe>,
        "nulleq" => integer_comparison_fn_meta::<CmpOpNullEq>,
        _ => unreachable!(),
    };
    select(lhs_unsigned, rhs_unsigned)
}

fn bigint_operand_signedness(expression: &Expression) -> Option<bool> {
    let ty = expression.static_type()?;
    if ty.code() != tidb_datatype::FieldTypeCode::LongLong || ty.is_array() {
        return None;
    }
    let unsigned = ty.is_unsigned();
    match expression {
        Expression::Constant(c)
            if !matches!(
                (c.literal_value(), unsigned),
                (Some(Datum::Int(_)), false)
                    | (Some(Datum::UInt(_)), true)
                    | (Some(Datum::Null), _)
            ) =>
        {
            None
        }
        Expression::Constant(_) | Expression::Column(_) => Some(unsigned),
        _ => (!unsigned).then_some(false),
    }
}

fn bigint_type(unsigned: bool) -> tipb::FieldType {
    let mut field_type: tipb::FieldType = FieldTypeTp::LongLong.into();
    if unsigned {
        field_type
            .as_mut_accessor()
            .set_flag(FieldTypeFlag::UNSIGNED);
    }
    field_type
}

// MD5 over a raw column has no SQL data-dependent failure. JSON_TYPE
// reads and reports errors only within the chosen rows. Neither branch
// performs host effects, so the existing vector IF can group their reads.
fn can_batch_mixed_string_if(expression: &Expression) -> bool {
    let Expression::ScalarFunction(condition) = expression else {
        return false;
    };
    let [Expression::Column(flag), Expression::ScalarFunction(hash), Expression::ScalarFunction(kind)] =
        condition.args.as_slice()
    else {
        return false;
    };
    condition.func_name.lowercase() == "if"
        && [condition, hash, kind]
            .iter()
            .all(|f| f.pb_signature().is_none())
        && flag.ret_type.as_ref().is_some_and(is_signed_bigint)
        && hash.func_name.lowercase() == "md5"
        && kind.func_name.lowercase() == "json_type"
        && matches!(hash.args.as_slice(), [Expression::Column(_)])
        && matches!(kind.args.as_slice(), [Expression::Column(_)])
}

// Native conditional values retain the chosen branch's collation even when
// the SQL output declaration uses a different collation.
fn string_conditional_collation(
    f: &crate::scalar_function::ScalarFunction,
) -> Option<tidb_datatype::Collation> {
    let result = f.ret_type.as_ref().filter(|ty| is_raw_string_type(ty))?;
    let name = f.func_name.lowercase();
    if f.pb_signature().is_some() || !matches!(name, "if" | "case" | "ifnull" | "coalesce") {
        return None;
    }
    let mut common = None;
    for (index, arg) in f.args.iter().enumerate() {
        if (name == "if" && index == 0)
            || (name == "case" && index % 2 == 0 && index + 1 < f.args.len())
        {
            continue;
        }
        let ty = arg.static_type().filter(|ty| is_raw_string_type(ty))?;
        let collation = match arg {
            Expression::Column(_) => ty.collation(),
            Expression::Constant(c) => match c.literal_value()? {
                Datum::Null => continue,
                Datum::String(value) if value.collation() == ty.collation() => value.collation(),
                _ => return None,
            },
            Expression::ScalarFunction(value)
                if value.pb_signature().is_none()
                    && matches!(value.func_name.lowercase(), "md5" | "json_type") =>
            {
                // These native functions construct default-collation String datums.
                tidb_datatype::Collation::DEFAULT
            }
            _ => return None,
        };
        if common
            .replace(collation)
            .is_some_and(|previous| previous != collation)
        {
            return None;
        }
    }
    Some(common.unwrap_or(result.collation()))
}

fn matches_conditional_value(expression: &Expression, code: tidb_datatype::FieldTypeCode) -> bool {
    use tidb_datatype::FieldTypeCode;
    if expression
        .static_type()
        .is_none_or(|ty| ty.code() != code || ty.is_array() || ty.is_unsigned())
    {
        return false;
    }
    match expression {
        Expression::Constant(c) => matches!(
            (code, c.literal_value()),
            (_, Some(Datum::Null))
                | (FieldTypeCode::LongLong, Some(Datum::Int(_)))
                | (FieldTypeCode::Double, Some(Datum::Real(_)))
                | (FieldTypeCode::NewDecimal, Some(Datum::Decimal(_)))
        ),
        _ => true,
    }
}

fn is_signed_bigint(tp: &tidb_datatype::FieldType) -> bool {
    tp.code() == tidb_datatype::FieldTypeCode::LongLong && !tp.is_unsigned() && !tp.is_array()
}

fn is_time_type(tp: &tidb_datatype::FieldType) -> bool {
    use tidb_datatype::FieldTypeCode;
    matches!(
        tp.code(),
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
    )
}
fn time_type(tp: &tidb_datatype::FieldType) -> tipb::FieldType {
    use tidb_datatype::FieldTypeCode;
    match tp.code() {
        FieldTypeCode::Date => FieldTypeTp::Date,
        FieldTypeCode::Timestamp => FieldTypeTp::Timestamp,
        _ => FieldTypeTp::DateTime,
    }
    .into()
}

fn control_type(f: &crate::scalar_function::ScalarFunction) -> Result<tipb::FieldType, String> {
    use tidb_datatype::EvalType as NativeType;
    if let Some(tp) = f.ret_type.as_ref().filter(|tp| is_time_type(tp)) {
        return Ok(time_type(tp));
    }
    Ok(match f
        .ret_type
        .as_ref()
        .ok_or("IF requires a return type")?
        .eval_type()
    {
        NativeType::Int => FieldTypeTp::LongLong,
        NativeType::Real => FieldTypeTp::Double,
        NativeType::Decimal => FieldTypeTp::NewDecimal,
        NativeType::String => FieldTypeTp::VarString,
        _ => return Err("IF result type is outside shared admission".into()),
    }
    .into())
}

fn native_output(result: RpnStackNode<'_>, rows: usize) -> Result<Vec<Datum>, String> {
    native_output_with_collation(result, rows, tidb_datatype::Collation::DEFAULT)
}

fn native_output_with_collation(
    result: RpnStackNode<'_>,
    rows: usize,
    collation: tidb_datatype::Collation,
) -> Result<Vec<Datum>, String> {
    let convert = |v: Option<&[u8]>| {
        v.map_or(Datum::Null, |v| {
            Datum::new_collation_string(v.to_vec(), collation)
        })
    };
    match result {
        RpnStackNode::ExpressionVector { value, .. } => match value {
            ExpressionVector::Time(values) => Ok(values
                .into_iter()
                .map(|v| v.map_or(Datum::Null, Datum::Time))
                .collect()),
            ExpressionVector::Real(values) => Ok(values
                .into_iter()
                .map(|v| v.map_or(Datum::Null, Datum::Real))
                .collect()),
            ExpressionVector::Decimal(values) => Ok(values
                .into_iter()
                .map(|v| v.map_or(Datum::Null, |v| Datum::Decimal(v.into_decimal())))
                .collect()),
        },
        RpnStackNode::ExpressionScalar { value, .. } => {
            let datum = match value.as_ref() {
                ExpressionScalar::Time(v) => v.map_or(Datum::Null, Datum::Time),
                ExpressionScalar::Real(v) => v.map_or(Datum::Null, Datum::Real),
                ExpressionScalar::Decimal(v) => v
                    .clone()
                    .map_or(Datum::Null, |v| Datum::Decimal(v.into_decimal())),
            };
            Ok(vec![datum; rows])
        }
        RpnStackNode::Scalar {
            value: ScalarValue::Int(v),
            ..
        } => Ok(vec![v.map_or(Datum::Null, Datum::Int); rows]),
        RpnStackNode::Scalar {
            value: ScalarValue::Bytes(v),
            ..
        } => Ok((0..rows).map(|_| convert(v.as_deref())).collect()),
        RpnStackNode::Vector { value, .. } => {
            let values = value.as_ref();
            if let VectorValue::Int(values) = values {
                return Ok((0..rows)
                    .map(|r| {
                        values
                            .get_option_ref(value.logical_rows()[r])
                            .copied()
                            .map_or(Datum::Null, Datum::Int)
                    })
                    .collect());
            }
            let VectorValue::Bytes(values) = values else {
                return Err("expected byte or integer result".into());
            };
            Ok((0..rows)
                .map(|r| convert(values.get_option_ref(value.logical_rows()[r])))
                .collect())
        }
        _ => Err("expected byte result".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{column::Column, constant::Constant, scalar_function::ScalarFunction};
    use tidb_ast::CiString;
    use tidb_datatype::FieldType;
    use tidb_query_datatype::codec::batch::LazyBatchColumnVec;

    fn native_type() -> FieldType {
        FieldType::new(tidb_datatype::FieldTypeCode::VarString)
    }
    fn md5(arg: Expression) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("md5"),
            native_type(),
            vec![arg],
        ))
    }

    struct DateEffects {
        modes: tidb_datatype::DateModes,
        session: Session,
        ignore: bool,
    }
    impl crate::Columns for DateEffects {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn date_modes(&self) -> tidb_datatype::DateModes {
            self.modes
        }
        fn rand_next(&self) -> Option<f64> {
            self.session.rand_next()
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            if self.ignore {
                crate::ErrorLevel::Ignore
            } else {
                self.session.truncate_level()
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.session.append_warning(code, message);
        }
        fn enable_vectorized_expression(&self) -> bool {
            !self.session.vectorized_off
        }
    }

    #[test]
    fn shared_kernel_date_original_native_policy() {
        use tidb_datatype::{CoreTime, DateModes, FieldTypeCode, Time, TimeType};
        let ty = FieldType::new(FieldTypeCode::Date);
        let mut input = Chunk::new_empty(&[]);
        input.set_num_virtual_rows(1);
        for no_zero_date in [false, true] {
            for no_zero_in_date in [false, true] {
                let effects = DateEffects {
                    modes: DateModes {
                        no_zero_date,
                        no_zero_in_date,
                        ..DateModes::TIDB_DEFAULT_SQL_MODE
                    },
                    session: Session::default(),
                    ignore: false,
                };
                for (y, m, d, h, tag) in [
                    (0, 0, 0, 0, 0),
                    (0, 0, 0, 1, 0),
                    (2024, 0, 3, 0, 0),
                    (2024, 2, 3, 12, 14),
                    (2024, 2, 3, 12, 15),
                ] {
                    let core = CoreTime::from_date(y, m, d, h, 0, 0, 0);
                    let time = Time::from_go_raw_like_go(core.raw() | tag);
                    let expr = Expression::ScalarFunction(ScalarFunction::new(
                        CiString::new("date"),
                        ty.clone(),
                        vec![Expression::Constant(Constant::new(
                            Datum::Time(time),
                            FieldType::new(FieldTypeCode::Datetime),
                        ))],
                    ));
                    let actual = if tag == 15 {
                        crate::time_fn::date(&[Datum::Time(time)], &effects).unwrap()
                    } else {
                        expr.eval(&effects, input.physical_row(0)).unwrap()
                    };
                    let rejected = if time.is_zero() {
                        no_zero_date
                    } else {
                        time.invalid_zero() && no_zero_in_date
                    };
                    let expected = if rejected {
                        Datum::Null
                    } else {
                        Datum::Time(
                            Time::new(CoreTime::from_date(y, m, d, 0, 0, 0, 0), TimeType::Date, 0)
                                .unwrap(),
                        )
                    };
                    assert_eq!(actual, expected);
                }
            }
        }
    }

    fn date_call(arg: Expression) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("date"),
            FieldType::new(tidb_datatype::FieldTypeCode::Date),
            vec![arg],
        ))
    }
    fn time_column(index: i32, ty: &FieldType) -> Expression {
        let mut column = Column::new(i64::from(index), ty.clone());
        column.index = i64::from(index);
        Expression::Column(column)
    }

    #[test]
    fn shared_kernel_date_constants_modes_and_diagnostics() {
        use tidb_datatype::{CoreTime, DateModes, FieldTypeCode, Time, TimeType};
        let source = FieldType::new(FieldTypeCode::Datetime).with_decimal(3);
        let result = FieldType::new(FieldTypeCode::Date);
        let mut input = Chunk::new_empty(&[]);
        input.set_num_virtual_rows(3);
        for scalar in [false, true] {
            for strict in [false, true] {
                for ignore in [false, true] {
                    for no_zero_date in [false, true] {
                        for no_zero_in_date in [false, true] {
                            for (y, m, d, h, tag) in [
                                (0, 0, 0, 0, 0),
                                (0, 0, 0, 1, 0),
                                (2024, 0, 3, 0, 0),
                                (2024, 2, 31, 12, 0),
                                (2024, 2, 3, 12, 6),
                                (2024, 2, 3, 12, 13),
                                (2024, 2, 3, 12, 14),
                                (2024, 2, 3, 12, 15),
                            ] {
                                let time = Time::from_go_raw_like_go(
                                    CoreTime::from_date(y, m, d, h, 4, 5, 123456).raw() | tag,
                                );
                                // Include a genuinely all-zero value separately from zero calendar with clock.
                                for value in [
                                    time,
                                    Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap(),
                                ] {
                                    let expr =
                                        date_call(numeric_literal(Datum::Time(value), &source));
                                    let effects = DateEffects {
                                        modes: DateModes {
                                            no_zero_date,
                                            no_zero_in_date,
                                            ..DateModes::TIDB_DEFAULT_SQL_MODE
                                        },
                                        session: Session {
                                            strict,
                                            vectorized_off: scalar,
                                            ..Session::default()
                                        },
                                        ignore,
                                    };
                                    let rejected = if value.is_zero() {
                                        no_zero_date
                                    } else {
                                        value.invalid_zero() && no_zero_in_date
                                    };
                                    let expected = if rejected {
                                        Datum::Null
                                    } else {
                                        Datum::Time(
                                            Time::new(
                                                CoreTime::from_date(
                                                    value.core_time().year() as u16,
                                                    value.core_time().month(),
                                                    value.core_time().day(),
                                                    0,
                                                    0,
                                                    0,
                                                    0,
                                                ),
                                                TimeType::Date,
                                                0,
                                            )
                                            .unwrap(),
                                        )
                                    };
                                    let projection = SharedProjection::compile(&[expr]).unwrap();
                                    for direct in [false, true] {
                                        effects.session.warnings.borrow_mut().clear();
                                        let mut execution = projection.start_execution(&effects);
                                        let mut output =
                                            Chunk::new(std::slice::from_ref(&result), 1, 3);
                                        output.append_null(0);
                                        let actual = if direct {
                                            execution
                                                .evaluate_into_chunk(
                                                    &input,
                                                    &[2, 0, 2],
                                                    &mut EvalContext::default(),
                                                    &mut output,
                                                )
                                                .map(|_| {
                                                    vec![(0..3)
                                                        .map(|i| {
                                                            output
                                                                .physical_row(i)
                                                                .get_datum(0, &result)
                                                        })
                                                        .collect::<Vec<_>>()]
                                                })
                                        } else {
                                            execution.evaluate(
                                                &input,
                                                &[2, 0, 2],
                                                &mut EvalContext::default(),
                                            )
                                        };
                                        if rejected && strict && !ignore {
                                            assert_eq!(actual.unwrap_err().code, Some(1292));
                                            assert_eq!(output.num_rows(), 1);
                                        } else {
                                            assert_eq!(
                                                actual.unwrap(),
                                                vec![vec![expected.clone(); 3]]
                                            );
                                        }
                                        let warnings = effects.session.warnings.borrow();
                                        assert_eq!(
                                            warnings.len(),
                                            if rejected && !strict && !ignore { 3 } else { 0 }
                                        );
                                        if !warnings.is_empty() {
                                            assert_eq!(
                                                warnings[0],
                                                (
                                                    1292,
                                                    format!("Incorrect datetime value: '{value}'")
                                                )
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn shared_kernel_time_columns_if_and_outputs() {
        use tidb_datatype::{CoreTime, FieldTypeCode, Time, TimeType};
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let datetime = FieldType::new(FieldTypeCode::Datetime).with_decimal(3);
        let timestamp = FieldType::new(FieldTypeCode::Timestamp).with_decimal(6);
        let date = FieldType::new(FieldTypeCode::Date);
        let types = vec![integer.clone(), datetime.clone(), timestamp.clone()];
        let choose = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            datetime.clone(),
            vec![
                time_column(0, &integer),
                time_column(1, &datetime),
                time_column(2, &timestamp),
            ],
        ));
        let dead = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            date.clone(),
            vec![
                numeric_literal(Datum::Int(0), &integer),
                date_call(time_column(99, &datetime)),
                numeric_literal(Datum::Null, &date),
            ],
        ));
        let roots = vec![
            time_column(1, &datetime),
            choose.clone(),
            date_call(choose),
            dead,
        ];
        let out_types = [
            datetime.clone(),
            datetime.clone(),
            date.clone(),
            date.clone(),
        ];
        let projection = SharedProjection::compile(&roots).unwrap();
        assert_eq!(projection.expressions[0].result_type(), Some(&datetime));
        for scalar in [false, true] {
            let effects = Session {
                vectorized_off: scalar,
                ..Session::default()
            };
            let mut execution = projection.start_execution(&effects);
            for day in [3, 4] {
                let mut input = Chunk::new(&types, 3, 3);
                for i in 0..3 {
                    input.append_int64(0, if i == 0 { 0 } else { 1 });
                    input.append_datum(
                        1,
                        &if i == 1 {
                            Datum::Null
                        } else {
                            Datum::Time(Time::from_go_raw_like_go(
                                CoreTime::from_date(2024, 2, day, 12, 34, 56, 123456).raw() | 14,
                            ))
                        },
                    );
                    input.append_datum(
                        2,
                        &Datum::Time(
                            Time::new(
                                CoreTime::from_date(2024, 2, day, 5, 6, 7, 123456),
                                TimeType::Timestamp,
                                6,
                            )
                            .unwrap(),
                        ),
                    );
                }
                for rows in [vec![2, 0, 2], vec![1], vec![]] {
                    let expected: Vec<Vec<_>> = roots
                        .iter()
                        .map(|expr| {
                            rows.iter()
                                .map(|r| expr.eval(&effects, input.physical_row(*r)).unwrap())
                                .collect()
                        })
                        .collect();
                    assert_eq!(
                        execution
                            .evaluate(&input, &rows, &mut EvalContext::default())
                            .unwrap(),
                        expected
                    );
                    let mut output = Chunk::new(&out_types, 1, 3);
                    execution
                        .evaluate_into_chunk(
                            &input,
                            &rows,
                            &mut EvalContext::default(),
                            &mut output,
                        )
                        .unwrap();
                    for (c, ty) in out_types.iter().enumerate() {
                        assert_eq!(
                            (0..rows.len())
                                .map(|i| output.physical_row(i).get_datum(c, ty))
                                .collect::<Vec<_>>(),
                            expected[c]
                        );
                    }
                }
            }
            assert!(effects.warnings.borrow().is_empty());
        }
    }

    #[test]
    fn shared_kernel_date_error_keeps_output_effects_and_execution() {
        use tidb_datatype::{CoreTime, DateModes, FieldTypeCode, Time, TimeType};
        let source = FieldType::new(FieldTypeCode::Datetime);
        let date = FieldType::new(FieldTypeCode::Date);
        let real = FieldType::new(FieldTypeCode::Double);
        let rand = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("rand"),
            real.clone(),
            vec![],
        ));
        let projection =
            SharedProjection::compile(&[rand, date_call(time_column(0, &source))]).unwrap();
        for scalar in [false, true] {
            let effects = DateEffects {
                modes: DateModes::TIDB_DEFAULT_SQL_MODE,
                session: Session {
                    strict: true,
                    vectorized_off: scalar,
                    ..Session::default()
                },
                ignore: false,
            };
            let mut execution = projection.start_execution(&effects);
            let mut bad = Chunk::new(std::slice::from_ref(&source), 1, 1);
            bad.append_time(
                0,
                Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap(),
            );
            let mut output = Chunk::new(&[real.clone(), date.clone()], 1, 1);
            output.append_float64(0, 99.0);
            output.append_null(1);
            let error = execution
                .evaluate_into_chunk(&bad, &[0], &mut EvalContext::default(), &mut output)
                .unwrap_err();
            assert_eq!(error.code, Some(1292));
            assert_eq!(effects.session.draws.get(), 1);
            assert_eq!(output.column(0).get_float64(0), 99.0);
            assert!(output.column(1).is_null(0));
            let mut good = Chunk::new(std::slice::from_ref(&source), 1, 1);
            good.append_time(
                0,
                Time::new(
                    CoreTime::from_date(2024, 2, 3, 4, 5, 6, 0),
                    TimeType::DateTime,
                    0,
                )
                .unwrap(),
            );
            execution
                .evaluate_into_chunk(&good, &[0], &mut EvalContext::default(), &mut output)
                .unwrap();
            assert_eq!(effects.session.draws.get(), 2);
            assert_eq!(output.column(0).get_float64(0), 0.25);
            assert_eq!(output.column(1).get_time(0).core_time().hour(), 0);
        }
    }

    #[test]
    fn shared_kernel_time_admission_and_chunk_limit() {
        use tidb_datatype::{CoreTime, FieldTypeCode, Time};
        let time = FieldType::new(FieldTypeCode::Datetime);
        for datum in [
            Datum::Int(1),
            Datum::Real(1.0),
            Datum::new_string(b"2024-02-03".to_vec()),
        ] {
            assert!(SharedExpression::compile(&date_call(numeric_literal(datum, &time))).is_err());
        }
        for ty in [
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::Duration),
            native_type(),
        ] {
            assert!(SharedExpression::compile(&date_call(time_column(0, &ty))).is_err());
        }
        let raw = Time::from_go_raw_like_go(
            CoreTime::from_date(2024, 2, 3, 12, 34, 56, 123456).raw() | 15,
        );
        let expr = numeric_literal(Datum::Time(raw), &time);
        let mut input = Chunk::new_empty(&[]);
        input.set_num_virtual_rows(1);
        assert_eq!(
            SharedExpression::compile(&expr)
                .unwrap()
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap(),
            vec![Datum::Time(raw)]
        );
        let mut chunk = Chunk::new(std::slice::from_ref(&time), 1, 1);
        chunk.append_time(0, raw);
        assert_eq!(raw.fsp(), 7);
        let decoded = chunk.column(0).get_time(0);
        assert_eq!(decoded.fsp(), 6);
        assert_eq!(decoded.kind(), raw.kind());
        assert_eq!(decoded.core_time(), raw.core_time());
        let column = time_column(0, &time);
        assert_eq!(
            SharedExpression::compile(&column)
                .unwrap()
                .evaluate(&chunk, &[0], &mut EvalContext::default())
                .unwrap(),
            vec![Datum::Time(decoded)]
        );
        let projection = SharedProjection::compile(&[column]).unwrap();
        let mut output = Chunk::new(std::slice::from_ref(&time), 1, 1);
        projection
            .start_execution(&crate::NoColumns)
            .evaluate_into_chunk(&chunk, &[0], &mut EvalContext::default(), &mut output)
            .unwrap();
        assert_eq!(output.column(0).get_time(0).fsp(), 6);

        assert!(SharedExpression::compile(&date_call(numeric_literal(
            Datum::Null,
            &FieldType::new(FieldTypeCode::Null)
        )))
        .is_ok());
    }

    #[test]
    fn shared_kernel_date_metadata_and_lazy_admission() {
        use tidb_datatype::{CoreTime, FieldTypeCode, Time, TimeType};
        let ty = FieldType::new(FieldTypeCode::Datetime)
            .with_raw_flags(1u64 << 52)
            .with_flen(128)
            .with_decimal(3);
        let value = Datum::Time(
            Time::new(
                CoreTime::from_date(2024, 2, 3, 12, 34, 56, 123456),
                TimeType::Timestamp,
                6,
            )
            .unwrap(),
        );
        let compiled = SharedExpression::compile(&numeric_literal(value.clone(), &ty)).unwrap();
        assert_eq!(compiled.result_type(), Some(&ty));
        let column = SharedExpression::compile(&time_column(0, &ty)).unwrap();
        assert_eq!(column.columns[0].1, ty);
        let mut marker = Constant::new(value.clone(), ty.clone());
        marker.param_marker = Some(crate::constant::ParamMarker { order: 0 });
        assert!(SharedExpression::compile(&date_call(Expression::Constant(marker))).is_err());
        let mut deferred = Constant::new(value, ty.clone());
        deferred.deferred_expr = Some(Box::new(time_column(99, &ty)));
        assert!(SharedExpression::compile(&date_call(Expression::Constant(deferred))).is_err());
        let array = ty.clone().with_array(true);
        assert!(SharedExpression::compile(&date_call(time_column(0, &array))).is_err());
        assert!(
            SharedExpression::compile(&date_call(numeric_literal(Datum::Null, &array))).is_err()
        );
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let choose = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            ty.clone(),
            vec![
                numeric_literal(Datum::Int(0), &integer),
                time_column(0, &ty),
                time_column(0, &ty.clone().with_decimal(6)),
            ],
        ));
        assert!(SharedExpression::compile(&choose).is_err());
        let invalid = date_call(numeric_literal(
            Datum::Time(Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap()),
            &ty,
        ));
        let date = FieldType::new(FieldTypeCode::Date);
        let dead = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            date.clone(),
            vec![
                numeric_literal(Datum::Int(0), &integer),
                invalid,
                numeric_literal(Datum::Null, &date),
            ],
        ));
        let session = Session {
            strict: true,
            ..Session::default()
        };
        let mut input = Chunk::new_empty(&[]);
        input.set_num_virtual_rows(2);
        assert_eq!(
            SharedProjection::compile(&[dead])
                .unwrap()
                .start_execution(&session)
                .evaluate(&input, &[1, 0], &mut EvalContext::default())
                .unwrap(),
            vec![vec![Datum::Null; 2]]
        );
        assert!(session.warnings.borrow().is_empty());
    }

    #[test]
    fn shared_kernel_mixed_string_if_reads_only_the_chosen_column() {
        use tidb_datatype::{BinaryJSON, Collation, FieldTypeCode};

        let int = FieldType::new(FieldTypeCode::LongLong);
        let text = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        let json = FieldType::new(FieldTypeCode::Json);
        let result = native_type();
        let mut input = Chunk::new(&[int.clone(), text.clone(), json.clone()], 8, 8);
        for row in 0..8 {
            input.append_int64(0, i64::from(row == 7));
            input.append_bytes(1, b"abc");
            input.append_json(
                2,
                &if row == 2 {
                    BinaryJSON::from_encoded_parts(0x04, [0])
                } else {
                    BinaryJSON::from_encoded_parts(0xff, [])
                },
            );
        }
        let column = |index, ty| {
            let mut c = Column::new(index, ty);
            c.index = index;
            Expression::Column(c)
        };
        let json_type = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("json_type"),
            result.clone(),
            vec![column(2, json)],
        ));
        let expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            result.clone(),
            vec![column(0, int), md5(column(1, text)), json_type],
        ));
        let ctx = crate::NoColumns;
        let rows = [7, 2, 7];
        let digest = Datum::new_string(b"900150983cd24fb0d6963f7d28e17f72");
        let expected = [digest.clone(), Datum::new_string(b"NULL"), digest.clone()];
        assert_eq!(
            rows.map(|row| expr.eval(&ctx, input.physical_row(row)).unwrap()),
            expected
        );
        let projection = SharedProjection::compile(std::slice::from_ref(&expr)).unwrap();
        let mut execution = projection.start_execution(&ctx);
        let mut output = Chunk::new(std::slice::from_ref(&result), 3, 3);
        let actual = |chunk: &Chunk| {
            (0..chunk.num_rows())
                .map(|row| chunk.get_row(row).get_datum(0, &result))
                .collect::<Vec<_>>()
        };
        execution
            .evaluate_into_chunk(&input, &rows, &mut EvalContext::default(), &mut output)
            .unwrap();
        assert_eq!(actual(&output), expected);
        let invalid = crate::JsonError::InvalidText;
        assert_eq!(
            expr.eval(&ctx, input.physical_row(1)),
            Err(crate::EvalError::Json(invalid.clone()))
        );
        let error = execution
            .evaluate_into_chunk(&input, &[7, 1], &mut EvalContext::default(), &mut output)
            .unwrap_err();
        assert_eq!(error.code, Some(i32::from(invalid.code())));
        assert_eq!(error.message, invalid.message());
        assert_eq!(actual(&output), expected);

        let compiled = SharedExpression::compile(&expr).unwrap();
        assert!(!matches!(
            compiled.program[0],
            RpnExpressionNode::OrderedRows { .. }
        ));
        let Expression::ScalarFunction(mut compound) = expr else {
            unreachable!();
        };
        compound.args[0] = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("benchmark"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![
                numeric_literal(Datum::Int(1), &FieldType::new(FieldTypeCode::LongLong)),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("rand"),
                    FieldType::new(FieldTypeCode::Double),
                    vec![],
                )),
            ],
        ));
        let ordered = SharedExpression::compile(&Expression::ScalarFunction(compound)).unwrap();
        assert!(matches!(
            ordered.program[0],
            RpnExpressionNode::OrderedRows { .. }
        ));
        assert_eq!(
            compiled
                .evaluate_mode(
                    &input,
                    &[7],
                    &mut EvalContext::default(),
                    InvocationMode::Scalar
                )
                .unwrap(),
            [digest],
        );
    }

    #[test]
    fn shared_kernel_json_type_keeps_binary_type_codes() {
        use tidb_datatype::{BinaryJSON, FieldTypeCode};

        let json = FieldType::new(FieldTypeCode::Json);
        let cases: &[(u8, &[u8], &str)] = &[
            (0x01, &[], "OBJECT"),
            (0x03, &[], "ARRAY"),
            (0x04, &[0], "NULL"),
            (0x04, &[1], "BOOLEAN"),
            (0x04, &[0, 1], "BOOLEAN"),
            (0x09, &0i64.to_le_bytes(), "INTEGER"),
            (0x0a, &u64::MAX.to_le_bytes(), "UNSIGNED INTEGER"),
            (0x0b, &1.5f64.to_le_bytes(), "DOUBLE"),
            (0x0c, &[1, 0xff], "STRING"),
            (0x0d, &[0xfc, 1, 0xff], "BLOB"),
            (0x0d, &[0x10, 1, 0x80], "BIT"),
            (0x0d, &[0x01, 0], "OPAQUE"),
            (0x0e, &[0; 8], "DATE"),
            (0x0f, &[0; 8], "DATETIME"),
            (0x10, &[0; 8], "DATETIME"),
            (0x11, &[0; 12], "TIME"),
        ];
        let mut input = Chunk::new(std::slice::from_ref(&json), 32, 32);
        input.append_null(0);
        for &(tag, value, _) in cases {
            input.append_json(0, &BinaryJSON::from_encoded_parts(tag, value));
        }
        let mut column = Column::new(1, json.clone());
        column.index = 0;
        let result_type = native_type();
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("json_type"),
            result_type.clone(),
            vec![Expression::Column(column)],
        ));
        let mut rows: Vec<_> = (0..input.num_rows()).rev().collect();
        rows.push(3);
        let original = |row| expression.eval(&crate::NoColumns, input.physical_row(row));
        let expected = rows
            .iter()
            .map(|&row| {
                if row == 0 {
                    Datum::Null
                } else {
                    Datum::new_string(cases[row - 1].2)
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows.iter()
                .map(|&row| original(row).unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        let program = SharedExpression::compile(&expression).unwrap();
        assert_eq!(program.result_type(), Some(&result_type));
        assert_eq!(
            program
                .evaluate(&input, &rows, &mut EvalContext::default())
                .unwrap(),
            expected
        );

        let literal = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("json_type"),
            result_type,
            vec![numeric_literal(
                Datum::Json(BinaryJSON::from_encoded_parts(0x10, [0; 8])),
                &json,
            )],
        ));
        assert_eq!(
            SharedExpression::compile(&literal)
                .unwrap()
                .evaluate_mode(
                    &input,
                    &[0],
                    &mut EvalContext::default(),
                    InvocationMode::Scalar
                )
                .unwrap(),
            [literal
                .eval(&crate::NoColumns, input.physical_row(0))
                .unwrap()],
        );

        let invalid = crate::EvalError::Json(crate::JsonError::InvalidText);
        for (tag, value) in [(0xff, &[][..]), (0x0d, &[0xfc, 3, 0xff][..])] {
            let row = input.num_rows();
            input.append_json(0, &BinaryJSON::from_encoded_parts(tag, value));
            assert_eq!(
                expression.eval(&crate::NoColumns, input.physical_row(row)),
                Err(invalid.clone())
            );
            let error = program
                .evaluate(&input, &[row], &mut EvalContext::default())
                .unwrap_err();
            assert_eq!(error.code, Some(3140));
            assert_eq!(error.message, crate::JsonError::InvalidText.message());
        }
    }

    #[test]
    fn shared_kernel_string_conditionals_keep_byte_identity_and_null() {
        use tidb_datatype::{Collation, FieldTypeCode};

        let int = FieldType::new(FieldTypeCode::LongLong);
        let ctx = crate::NoColumns;
        let rows = [1, 0, 2, 1];
        for collation in [Collation::Binary, Collation::Utf8Mb4Bin] {
            let ty = FieldType::new(FieldTypeCode::Varchar).with_collation(collation);
            let mut input = Chunk::new(&[int.clone(), ty.clone(), ty.clone()], 3, 3);
            input.append_int64(0, 1);
            input.append_int64(0, 0);
            input.append_null(0);
            input.append_null(1);
            input.append_bytes(1, &[0xff, 0x00]);
            input.append_null(1);
            input.append_bytes(2, &[0x80]);
            input.append_bytes(2, b"");
            input.append_null(2);
            let column = |index, ty: &FieldType| {
                let mut c = Column::new(index, ty.clone());
                c.index = index;
                Expression::Column(c)
            };
            let flag = || column(0, &int);
            let lhs = || column(1, &ty);
            let rhs = || column(2, &ty);
            let conditional = |name, args| {
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(name),
                    ty.clone(),
                    args,
                ))
            };
            let roots = [
                conditional("if", vec![flag(), lhs(), rhs()]),
                conditional("case", vec![flag(), lhs()]),
                conditional("ifnull", vec![lhs(), rhs()]),
                conditional("coalesce", vec![lhs(), rhs()]),
            ];
            let expected = roots
                .each_ref()
                .map(|root| rows.map(|row| root.eval(&ctx, input.physical_row(row)).unwrap()));
            assert_eq!(
                expected[0],
                [
                    Datum::new_collation_string(b"", collation),
                    Datum::Null,
                    Datum::Null,
                    Datum::new_collation_string(b"", collation)
                ]
            );
            assert_eq!(
                expected[2][0],
                Datum::new_collation_string([0xff, 0x00], collation)
            );
            assert_eq!(
                expected[2][1],
                Datum::new_collation_string([0x80], collation)
            );
            let projection = SharedProjection::compile(&roots).unwrap();
            let mut execution = projection.start_execution(&ctx);
            let actual = execution
                .evaluate(&input, &rows, &mut EvalContext::default())
                .unwrap();
            assert_eq!(actual, expected.map(|column| column.to_vec()));
            let mut output = Chunk::new(&[ty.clone(), ty.clone(), ty.clone(), ty.clone()], 4, 4);
            execution
                .evaluate_into_chunk(&input, &rows, &mut EvalContext::default(), &mut output)
                .unwrap();
            for (column, values) in actual.iter().enumerate() {
                assert_eq!(
                    (0..rows.len())
                        .map(|row| output.get_row(row).get_datum(column, &ty))
                        .collect::<Vec<_>>(),
                    *values
                );
            }

            let other_collation = if collation == Collation::Binary {
                Collation::Utf8Mb4Bin
            } else {
                Collation::Binary
            };
            let declared = ty.clone().with_collation(other_collation);
            let case = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("case"),
                declared.clone(),
                vec![flag(), rhs()],
            ));
            let program = SharedExpression::compile(&case).unwrap();
            assert_eq!(program.result_type(), Some(&declared));
            let expected = Datum::new_collation_string([0x80], collation);
            assert_eq!(case.eval(&ctx, input.physical_row(0)).unwrap(), expected);
            assert_eq!(
                program
                    .evaluate_mode(
                        &input,
                        &[0],
                        &mut EvalContext::default(),
                        InvocationMode::Scalar
                    )
                    .unwrap(),
                [expected],
            );

            let different = Expression::Constant(Constant::new(
                Datum::new_collation_string(b"x", other_collation),
                declared,
            ));
            assert!(
                SharedExpression::compile(&conditional("ifnull", vec![lhs(), different])).is_err()
            );
            let bytes = Expression::Constant(Constant::new(Datum::new_bytes([0xff]), ty.clone()));
            assert!(SharedExpression::compile(&conditional("ifnull", vec![bytes, rhs()])).is_err());

            if collation == Collation::Binary {
                let native_string = native_type();
                let name = numeric_literal(Datum::new_string(b"test"), &native_string);
                let case = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("case"),
                    native_string.clone(),
                    vec![flag(), rhs()],
                ));
                let set = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("setvar"),
                    native_string,
                    vec![name, case],
                ));
                let session = Session::default();
                set.eval(&session, input.physical_row(0)).unwrap();
                assert_eq!(
                    session.vars.borrow().get("test"),
                    Some(&Datum::new_collation_string([0x80], Collation::Binary)),
                );
                assert!(SharedExpression::compile(&set).is_err());
            }
        }
    }

    #[test]
    fn shared_kernel_byte_counts_read_raw_binary_columns() {
        use tidb_datatype::{Collation, FieldTypeCode};
        let int = FieldType::new(FieldTypeCode::LongLong);
        let bytes = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        let mut input = Chunk::new(std::slice::from_ref(&bytes), 4, 4);
        input.append_bytes(0, &[0xff, 0x00]);
        input.append_bytes(0, b"");
        input.append_null(0);
        input.append_bytes(0, "界".as_bytes());
        let mut column = Column::new(1, bytes);
        column.index = 0;
        let rows = [3, 0, 2, 1, 0];
        let ctx = crate::NoColumns;
        for (name, expected) in [
            ("length", [Some(3), Some(2), None, Some(0), Some(2)]),
            ("octet_length", [Some(3), Some(2), None, Some(0), Some(2)]),
            ("bit_length", [Some(24), Some(16), None, Some(0), Some(16)]),
            ("ascii", [Some(231), Some(255), None, Some(0), Some(255)]),
            ("char_length", [Some(3), Some(2), None, Some(0), Some(2)]),
            (
                "character_length",
                [Some(3), Some(2), None, Some(0), Some(2)],
            ),
        ] {
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                int.clone(),
                vec![Expression::Column(column.clone())],
            ));
            let expected = expected.map(|value| value.map_or(Datum::Null, Datum::Int));
            let original = rows.map(|row| expression.eval(&ctx, input.physical_row(row)).unwrap());
            assert_eq!(original, expected, "{name}");
            let plan = SharedExpression::compile(&expression).unwrap();
            let mut execution = plan.start_execution(&ctx);
            assert_eq!(
                execution
                    .evaluate(
                        &input,
                        &rows,
                        &mut EvalContext::default(),
                        InvocationMode::Vector
                    )
                    .unwrap(),
                expected,
                "{name}"
            );
            let literal = Expression::Constant(Constant::new(
                Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![0xff, 0x00])),
                column.ret_type.clone().unwrap(),
            ));
            let scalar = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                int.clone(),
                vec![literal],
            ));
            assert_eq!(
                scalar.eval(&ctx, input.physical_row(0)).unwrap(),
                expected[1]
            );
            assert_eq!(
                SharedExpression::compile(&scalar)
                    .unwrap()
                    .evaluate_mode(
                        &input,
                        &[0],
                        &mut EvalContext::default(),
                        InvocationMode::Scalar
                    )
                    .unwrap(),
                [expected[1].clone()]
            );
        }
    }

    #[test]
    fn shared_kernel_bigint_truth_tests_keep_null_and_unsigned_bits() {
        use tidb_datatype::FieldTypeCode;
        let int = FieldType::new(FieldTypeCode::LongLong);
        let ctx = crate::NoColumns;
        let rows = [3, 0, 1, 2, 3];
        let cases = [
            ("not", [None, Some(1), Some(0), Some(0)]),
            ("isnull", [Some(1), Some(0), Some(0), Some(0)]),
            ("istrue", [Some(0), Some(0), Some(1), Some(1)]),
            ("istrue_with_null", [None, Some(0), Some(1), Some(1)]),
            ("isnottrue", [Some(1), Some(1), Some(0), Some(0)]),
            ("isfalse", [Some(0), Some(1), Some(0), Some(0)]),
            ("isnotfalse", [Some(1), Some(0), Some(1), Some(1)]),
            ("isunknown", [Some(1), Some(0), Some(0), Some(0)]),
            ("isnotunknown", [Some(0), Some(1), Some(1), Some(1)]),
        ];
        for unsigned in [false, true] {
            let ty = int.clone().with_unsigned(unsigned);
            let mut input = Chunk::new(std::slice::from_ref(&ty), 4, 4);
            input.append_null(0);
            for value in [0, i64::MIN, -1] {
                input.append_int64(0, value);
            }
            let mut column = Column::new(1, ty);
            column.index = 0;
            for (name, expected) in cases {
                let expression = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(name),
                    int.clone(),
                    vec![Expression::Column(column.clone())],
                ));
                let expected = rows.map(|row| expected[row].map_or(Datum::Null, Datum::Int));
                let original =
                    rows.map(|row| expression.eval(&ctx, input.physical_row(row)).unwrap());
                assert_eq!(original, expected, "{name} unsigned={unsigned}");
                let projection = SharedProjection::compile(&[expression]).unwrap();
                let mut execution = projection.start_execution(&ctx);
                assert_eq!(
                    execution
                        .evaluate(&input, &rows, &mut EvalContext::default())
                        .unwrap(),
                    [expected.to_vec()],
                    "{name} unsigned={unsigned}",
                );
            }
        }
    }

    #[test]
    fn shared_kernel_native_if_allows_diagnostic_reordering() {
        use tidb_datatype::FieldTypeCode;
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let text = |value: &str| {
            Expression::Constant(Constant::new(
                Datum::new_string(value.as_bytes().to_vec()),
                native_type(),
            ))
        };
        let mut c = Column::new(1, integer.clone());
        c.index = 0;
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            integer.clone(),
            vec![
                Expression::Column(c),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("json_length"),
                    integer.clone(),
                    vec![text("{")],
                )),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("regexp_like"),
                    integer.clone(),
                    vec![text("a"), text("[")],
                )),
            ],
        ));
        let mut input = Chunk::new(&[integer], 2, 2);
        input.append_int64(0, 0);
        input.append_int64(0, 1);
        assert!(matches!(
            expression.eval(&crate::context::NoColumns, input.physical_row(0)),
            Err(crate::EvalError::Unsupported(
                "invalid regular expression pattern"
            ))
        ));
        assert!(matches!(
            expression.eval(&crate::context::NoColumns, input.physical_row(1)),
            Err(crate::EvalError::Json(crate::JsonError::InvalidText))
        ));
        let actual = SharedExpression::compile(&expression)
            .unwrap()
            .evaluate(&input, &[0, 1], &mut EvalContext::default())
            .unwrap_err();
        // The user permits diagnostic ordering differences between reachable errors.
        let message = actual.to_string().to_lowercase();
        assert!(
            message.contains("regular expression") || message.contains("json"),
            "unexpected error: {actual}"
        );
    }

    #[derive(Default)]
    struct Session {
        strict: bool,
        warnings: std::cell::RefCell<Vec<(u16, String)>>,
        vectorized_off: bool,
        draws: std::cell::Cell<u32>,
        seeds: std::cell::RefCell<std::collections::HashMap<usize, crate::MysqlRng>>,
        vars: std::cell::RefCell<std::collections::HashMap<String, Datum>>,
    }
    impl crate::context::Columns for Session {
        fn truncate_level(&self) -> crate::ErrorLevel {
            if self.strict {
                crate::ErrorLevel::Error
            } else {
                crate::ErrorLevel::Warn
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }

        fn enable_vectorized_expression(&self) -> bool {
            !self.vectorized_off
        }
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn rand_next(&self) -> Option<f64> {
            self.draws.set(self.draws.get() + 1);
            Some(f64::from(self.draws.get()) / 8.0)
        }
        fn supports_seeded_rand(&self) -> bool {
            true
        }
        fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
            Some(
                self.seeds
                    .borrow_mut()
                    .entry(key)
                    .or_insert_with(|| crate::MysqlRng::new_with_seed(seed))
                    .gen(),
            )
        }
        fn get_uservar(&self, name: &str) -> Option<Datum> {
            self.vars.borrow().get(&name.to_ascii_lowercase()).cloned()
        }
        fn set_uservar(&self, name: &str, value: Datum) {
            self.vars
                .borrow_mut()
                .insert(name.to_ascii_lowercase(), value);
        }
    }

    #[test]
    fn shared_kernel_wire_comparison_skips_right_on_left_null() {
        use tidb_datatype::FieldTypeCode;
        use tidb_proto::tipb as wire;

        struct Input {
            left: ScalarValue,
            right: ScalarValue,
            reads: std::cell::RefCell<Vec<usize>>,
        }
        impl InputColumns for Input {
            fn read<'a>(
                &'a self,
                schema: &'a [tipb::FieldType],
                offset: usize,
                _rows: &'a [usize],
                _ctx: &mut EvalContext,
            ) -> InputResult<'a> {
                self.reads.borrow_mut().push(offset);
                Ok(RpnStackNode::Scalar {
                    value: if offset == 0 { &self.left } else { &self.right },
                    field_type: &schema[offset],
                })
            }
        }

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let schema = [integer.clone(), integer.clone()];
        let column = |index| {
            let mut val = Vec::new();
            tidb_codec::encode_int(&mut val, index);
            wire::Expr {
                tp: Some(wire::ExprType::ColumnRef as i32),
                val: Some(val),
                ..Default::default()
            }
        };
        for (sig, truth) in [
            (wire::ScalarFuncSig::EqInt, 0),
            (wire::ScalarFuncSig::GtInt, 1),
        ] {
            let node = wire::Expr {
                tp: Some(wire::ExprType::ScalarFunc as i32),
                sig: Some(sig as i32),
                field_type: Some(wire::FieldType {
                    tp: Some(8),
                    ..Default::default()
                }),
                children: vec![column(0), column(1)],
                ..Default::default()
            };
            let expression = crate::distsql_builtin::pb_to_expr(&node, &schema).unwrap();
            let plan = SharedIntegerComparison::compile(&expression, &schema).unwrap();
            assert!(matches!(
                plan.0.program[0],
                RpnExpressionNode::OrderedRows { .. }
            ));
            for (left, wanted_reads, expected) in [
                (None, &[0][..], Datum::Null),
                (Some(3), &[0, 1][..], Datum::Int(truth)),
            ] {
                let input = Input {
                    left: ScalarValue::Int(left),
                    right: ScalarValue::Int(Some(2)),
                    reads: Default::default(),
                };
                let output = plan
                    .0
                    .program
                    .eval_input_mode(
                        &mut EvalContext::default(),
                        &plan.0.schema,
                        &input,
                        &[0],
                        1,
                        InvocationMode::Scalar,
                    )
                    .unwrap();
                assert_eq!(native_output(output, 1).unwrap(), [expected.clone()]);
                assert_eq!(*input.reads.borrow(), wanted_reads);
                let row = [left.map_or(Datum::Null, Datum::Int), Datum::Int(2)];
                assert_eq!(plan.evaluate(&row).unwrap().unwrap(), expected);
                eprintln!("{sig:?}: left={left:?}, reads={wanted_reads:?}, result={expected:?}");
            }
            assert!(plan.evaluate(&[Datum::UInt(3), Datum::Int(2)]).is_none());
            assert!(SharedIntegerComparison::compile(
                &expression,
                &[integer.clone().with_unsigned(true), integer.clone()],
            )
            .is_none());
            assert!(SharedIntegerComparison::compile(&expression, &[]).is_none());
        }
    }

    #[test]
    fn shared_kernel_selection_retains_native_null_and_physical_demand() {
        use tidb_datatype::FieldTypeCode;

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let types = [integer.clone(), integer.clone()];
        let mut input = Chunk::new(&types, 5, 5);
        for (left, right) in [
            (Some(0), 0),
            (None, 1),
            (Some(1), 0),
            (Some(1), 1),
            (Some(1), 1),
        ] {
            match left {
                Some(value) => input.append_int64(0, value),
                None => input.append_null(0),
            }
            input.append_int64(1, right);
        }
        input.set_sel(Some(vec![3, 3, 0]));
        let function = |name, args| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                integer.clone(),
                args,
            ))
        };
        let literal =
            |value| Expression::Constant(Constant::new(Datum::Int(value), integer.clone()));
        for (in_operand, expected_draws) in [(false, 6), (true, 8)] {
            let mut left = Column::new(1, integer.clone());
            left.index = 0;
            left.in_operand = in_operand;
            let mut right = Column::new(2, integer.clone());
            right.index = 1;
            let filters = [
                function("eq", vec![Expression::Column(left), literal(1)]),
                function(
                    "gt",
                    vec![
                        Expression::Column(right),
                        function(
                            "benchmark",
                            vec![
                                literal(2),
                                Expression::ScalarFunction(ScalarFunction::new(
                                    CiString::new("rand"),
                                    FieldType::new(FieldTypeCode::Double),
                                    vec![],
                                )),
                            ],
                        ),
                    ],
                ),
            ];
            let native = Session::default();
            let (expected, _) = crate::evaluator::vectorized_filter_consider_null(
                &native,
                true,
                &filters,
                &input,
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
            let program = SharedProjection::compile_selection(&filters, &types).unwrap();
            assert!(matches!(
                program.expressions[1].program[0],
                RpnExpressionNode::OrderedRows { .. }
            ));
            let mut shared = program.into_owned_execution(Session::default());
            let mut actual = Vec::new();
            shared
                .evaluate_selection_mask(&filters, &input, &mut actual)
                .unwrap();
            eprintln!(
                "from IN={in_operand}, physical mask={actual:?}, draws={}",
                shared.effects.draws.get()
            );
            assert_eq!(expected, [false, false, false, true, false]);
            assert_eq!(actual, expected);
            assert_eq!(native.draws.get(), expected_draws);
            assert_eq!(shared.effects.draws.get(), expected_draws);
        }
    }

    #[test]
    fn shared_kernel_native_signed_comparisons_match_original() {
        use tidb_datatype::FieldTypeCode;

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut input = Chunk::new(&[integer.clone(), integer.clone()], 5, 5);
        for (left, right) in [
            (Some(i64::MIN), Some(i64::MAX)),
            (Some(-1), Some(-1)),
            (Some(i64::MAX), Some(i64::MIN)),
            (None, Some(0)),
            (None, None),
        ] {
            for (index, value) in [left, right].into_iter().enumerate() {
                match value {
                    Some(value) => input.append_int64(index, value),
                    None => input.append_null(index),
                }
            }
        }
        let rows = [4, 0, 3, 1, 2, 0];
        for name in ["eq", "ne", "lt", "le", "gt", "ge", "nulleq"] {
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                integer.clone(),
                (0..2)
                    .map(|index| {
                        let mut col = Column::new(index + 1, integer.clone());
                        col.index = index;
                        Expression::Column(col)
                    })
                    .collect(),
            ));
            let expected = rows
                .iter()
                .map(|row| {
                    expression
                        .eval(&crate::context::NoColumns, input.physical_row(*row))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            eprintln!("{name}: rows={rows:?}, expected={expected:?}");
            let program = SharedExpression::compile(&expression).unwrap();
            assert_eq!(
                program
                    .evaluate(&input, &rows, &mut EvalContext::default())
                    .unwrap(),
                expected,
            );
        }
    }

    #[test]
    fn shared_kernel_native_mixed_integer_comparison_limits() {
        use tidb_datatype::FieldTypeCode;

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let signed = [
            Datum::Int(i64::MIN),
            Datum::Int(-1),
            Datum::Int(0),
            Datum::Int(i64::MAX),
            Datum::Null,
        ];
        let unsigned = [
            Datum::UInt(0),
            Datum::UInt(i64::MAX as u64),
            Datum::UInt(i64::MAX as u64 + 1),
            Datum::UInt(u64::MAX),
            Datum::Null,
        ];
        let field = |unsigned| integer.clone().with_unsigned(unsigned);
        let values = |is_unsigned| if is_unsigned { &unsigned } else { &signed };
        for (left_unsigned, right_unsigned) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let types = [field(left_unsigned), field(right_unsigned)];
            let mut input = Chunk::new(&types, 25, 25);
            for left in values(left_unsigned) {
                for right in values(right_unsigned) {
                    for (index, datum) in [left, right].into_iter().enumerate() {
                        match datum {
                            Datum::Int(v) => input.append_int64(index, *v),
                            Datum::UInt(v) => input.append_uint64(index, *v),
                            Datum::Null => input.append_null(index),
                            _ => unreachable!(),
                        }
                    }
                }
            }
            let mut rows = vec![24, 0, 24];
            rows.extend((0..input.num_rows()).rev());
            for name in ["eq", "ne", "lt", "le", "gt", "ge", "nulleq"] {
                let expression = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(name),
                    integer.clone(),
                    types
                        .iter()
                        .enumerate()
                        .map(|(index, tp)| {
                            let mut column = Column::new(index as i64 + 1, tp.clone());
                            column.index = index as i64;
                            Expression::Column(column)
                        })
                        .collect(),
                ));
                let expected = rows
                    .iter()
                    .map(|row| {
                        expression
                            .eval(&crate::context::NoColumns, input.physical_row(*row))
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                let program = SharedExpression::compile(&expression).unwrap_or_else(|error| {
                    panic!("{name}, unsigned={left_unsigned}/{right_unsigned}: {error}")
                });
                assert!(matches!(
                    program.program[0],
                    RpnExpressionNode::ColumnRef { .. }
                ));
                assert_eq!(
                    program
                        .evaluate(&input, &rows, &mut EvalContext::default())
                        .unwrap(),
                    expected,
                );
            }
            eprintln!(
                "unsigned={left_unsigned}/{right_unsigned}, {} values per operator",
                rows.len()
            );
        }
        let literal = |datum, ty| Expression::Constant(Constant::new(datum, ty));
        for (value, expected) in [
            (Datum::UInt(u64::MAX), Datum::Int(1)),
            (Datum::Null, Datum::Null),
        ] {
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("gt"),
                integer.clone(),
                vec![
                    literal(value, field(true)),
                    literal(Datum::Int(i64::MAX), integer.clone()),
                ],
            ));
            let mut input = Chunk::new(&[], 1, 1);
            input.set_num_virtual_rows(1);
            assert_eq!(
                SharedExpression::compile(&expression)
                    .unwrap()
                    .evaluate(&input, &[0], &mut EvalContext::default())
                    .unwrap(),
                [expected],
            );
        }

        let mismatched = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            integer.clone(),
            vec![
                literal(Datum::Int(-1), field(true)),
                literal(Datum::Int(1), integer.clone()),
            ],
        ));
        assert!(SharedExpression::compile(&mismatched).is_err());
        assert!(SharedExpression::compile(&literal(Datum::UInt(u64::MAX), field(true))).is_err());
    }

    #[test]
    fn shared_kernel_typed_numeric_conditionals_keep_selected_representation() {
        use tidb_datatype::{Decimal, FieldTypeCode};

        let int = FieldType::new(FieldTypeCode::LongLong);
        let real = FieldType::new(FieldTypeCode::Double);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal);
        let literal =
            |value, tp: &FieldType| Expression::Constant(Constant::new(value, tp.clone()));
        let call = |name, tp: &FieldType, arguments| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                tp.clone(),
                arguments,
            ))
        };
        let mut input = Chunk::new(&[], 3, 3);
        input.set_num_virtual_rows(3);
        let rows = [2, 0, 2, 1];
        let hidden = Decimal::from_int(8)
            .true_div(&Decimal::from_int(7), 7)
            .unwrap();
        let wide =
            Decimal::from_literal(&format!("1{}", "0".repeat(81))).with_declared_shape(82, 0);
        let nan = f64::from_bits(0x7ff8000000000042);
        for (tp, value, other) in [
            (&real, Datum::Real(nan), Datum::Real(-0.0)),
            (&decimal, Datum::Decimal(hidden), Datum::Decimal(wide)),
        ] {
            let null = || literal(Datum::Null, tp);
            let broken = call(
                "if",
                tp,
                vec![
                    call(
                        "plus",
                        &int,
                        vec![
                            literal(Datum::Int(i64::MAX), &int),
                            literal(Datum::Int(1), &int),
                        ],
                    ),
                    literal(other.clone(), tp),
                    null(),
                ],
            );
            for expression in [
                call(
                    "coalesce",
                    tp,
                    vec![null(), literal(value.clone(), tp), broken.clone()],
                ),
                call("ifnull", tp, vec![null(), literal(other.clone(), tp)]),
                call(
                    "case",
                    tp,
                    vec![
                        literal(Datum::Int(0), &int),
                        broken,
                        literal(Datum::Int(-1), &int),
                        literal(other.clone(), tp),
                    ],
                ),
                call(
                    "case",
                    tp,
                    vec![literal(Datum::Null, &int), literal(value.clone(), tp)],
                ),
                call("coalesce", tp, vec![null(), null()]),
            ] {
                let native = expression
                    .eval(&crate::context::NoColumns, input.physical_row(0))
                    .unwrap();
                let shared = SharedExpression::compile(&expression).unwrap();
                assert_eq!(shared.result_type(), Some(tp));
                for actual in shared
                    .evaluate(&input, &rows, &mut EvalContext::default())
                    .unwrap()
                {
                    match (&actual, &native) {
                        (Datum::Real(a), Datum::Real(b)) => assert_eq!(a.to_bits(), b.to_bits()),
                        (Datum::Decimal(_), Datum::Decimal(_)) => {
                            assert_same_decimal(&actual, &native)
                        }
                        (Datum::Null, Datum::Null) => {}
                        _ => panic!("native {native:?}, shared {actual:?}"),
                    }
                }
                eprintln!(
                    "conditional declaration={:?}, native value={native:?}",
                    tp.code()
                );
            }
        }
    }

    #[test]
    fn shared_kernel_native_conditionals_skip_errors_and_draws() {
        use tidb_datatype::FieldTypeCode;

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let literal = |value| Expression::Constant(Constant::new(value, integer.clone()));
        let call = |name, args| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                integer.clone(),
                args,
            ))
        };
        let mut column = Column::new(1, integer.clone());
        column.index = 0;
        let column = Expression::Column(column);
        let mut input = Chunk::new(&[integer.clone()], 3, 3);
        input.append_int64(0, -1);
        input.append_int64(0, 0);
        input.append_null(0);
        let rows = [2, 0, 1, 2];
        let repeat = call(
            "benchmark",
            vec![
                literal(Datum::Int(2)),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("rand"),
                    FieldType::new(FieldTypeCode::Double),
                    vec![],
                )),
            ],
        );
        let overflow = call(
            "plus",
            vec![literal(Datum::Int(i64::MAX)), literal(Datum::Int(1))],
        );
        for (expression, draws) in [
            (
                call(
                    "coalesce",
                    vec![column.clone(), repeat.clone(), overflow.clone()],
                ),
                4,
            ),
            (
                call(
                    "case",
                    vec![
                        column.clone(),
                        literal(Datum::Int(7)),
                        literal(Datum::Null),
                        overflow.clone(),
                        repeat,
                    ],
                ),
                6,
            ),
        ] {
            let original = Session::default();
            let session = Session::default();
            let wanted = rows
                .iter()
                .map(|row| {
                    expression
                        .eval(&original, input.physical_row(*row))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(original.draws.get(), draws);
            let program = SharedExpression::compile(&expression).unwrap();
            let mut exec = program.start_execution(&session);
            let mut ctx = EvalContext::default();
            let mut actual = exec
                .evaluate(&input, &rows[..2], &mut ctx, InvocationMode::Vector)
                .unwrap();
            actual.extend(
                exec.evaluate(&input, &rows[2..], &mut ctx, InvocationMode::Vector)
                    .unwrap(),
            );
            assert_eq!(actual, wanted);
            assert_eq!(session.draws.get(), draws);
            eprintln!("conditional values={actual:?}, draws={draws}");
        }

        for expression in [
            call("coalesce", vec![literal(Datum::Null), literal(Datum::Null)]),
            call(
                "case",
                vec![
                    literal(Datum::Int(1)),
                    literal(Datum::Null),
                    overflow.clone(),
                    literal(Datum::Int(7)),
                ],
            ),
        ] {
            let native = expression
                .eval(&crate::context::NoColumns, input.physical_row(0))
                .unwrap();
            assert_eq!(native, Datum::Null);
            assert_eq!(
                SharedExpression::compile(&expression)
                    .unwrap()
                    .evaluate(&input, &rows, &mut EvalContext::default())
                    .unwrap(),
                vec![native; rows.len()],
            );
        }

        let expression = call("ifnull", vec![column, overflow]);
        let native = crate::context::NoColumns;
        let program = SharedExpression::compile(&expression).unwrap();
        let mut ctx = EvalContext::default();
        for row in [1, 0, 1] {
            let expected = expression.eval(&native, input.physical_row(row)).unwrap();
            assert_eq!(
                program
                    .evaluate_mode(&input, &[row], &mut ctx, InvocationMode::Scalar)
                    .unwrap(),
                [expected]
            );
        }
        let expected = expression.eval(&native, input.physical_row(2)).unwrap_err();
        let actual = program.evaluate(&input, &[1, 2], &mut ctx).unwrap_err();
        assert_native_error(actual, expected);
    }

    #[test]
    fn shared_kernel_native_logical_values_and_admission() {
        use tidb_datatype::FieldTypeCode;

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let column = |index| {
            let mut column = Column::new(index + 1, integer.clone());
            column.index = index;
            Expression::Column(column)
        };
        let logic = |name, lhs, rhs| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                crate::builtin_op::infer_op_type(name).unwrap(),
                vec![lhs, rhs],
            ))
        };
        let expression = logic("and", logic("or", column(0), column(1)), column(2));
        let mut input = Chunk::new(&[integer.clone(), integer.clone(), integer.clone()], 4, 4);
        for row in [
            [Some(-2), Some(0), Some(-3)],
            [Some(0), None, Some(-1)],
            [None, Some(0), Some(0)],
            [Some(0), Some(2), None],
        ] {
            for (column, value) in row.into_iter().enumerate() {
                match value {
                    Some(value) => input.append_int64(column, value),
                    None => input.append_null(column),
                }
            }
        }
        let rows = [3, 0, 2, 1, 0];
        let expected = rows
            .iter()
            .map(|row| {
                expression
                    .eval(&crate::context::NoColumns, input.physical_row(*row))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            expected,
            [
                Datum::Null,
                Datum::Int(1),
                Datum::Int(0),
                Datum::Null,
                Datum::Int(1)
            ]
        );
        eprintln!("native rows={rows:?}, expected={expected:?}");
        let program = SharedExpression::compile(&expression).unwrap();
        assert_eq!(program.result_type(), expression.static_type());
        assert_eq!(
            program
                .evaluate(&input, &rows, &mut EvalContext::default())
                .unwrap(),
            expected,
        );

        for bad in [
            Constant::new(Datum::Int(1), integer.clone().with_unsigned(true)),
            Constant::new(Datum::Null, integer.clone().with_array(true)),
            Constant::new(Datum::Real(1.0), integer.clone()),
            Constant::new(Datum::Null, FieldType::new(FieldTypeCode::Null)),
        ] {
            assert!(
                SharedExpression::compile(&logic("or", column(0), Expression::Constant(bad)))
                    .is_err()
            );
        }
        for result in [
            integer.clone().with_unsigned(true),
            integer.clone().with_array(true),
            FieldType::new(FieldTypeCode::Double),
        ] {
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("or"),
                result,
                vec![column(0), column(1)],
            ));
            assert!(SharedExpression::compile(&expression).is_err());
        }
    }

    #[test]
    fn shared_kernel_native_logical_delays_repeat_and_errors() {
        use tidb_datatype::FieldTypeCode;

        let integer = FieldType::new(FieldTypeCode::LongLong);
        let logic = |name, lhs, rhs| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                crate::builtin_op::infer_op_type(name).unwrap(),
                vec![lhs, rhs],
            ))
        };
        let literal = |datum| Expression::Constant(Constant::new(datum, integer.clone()));
        let benchmark = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("benchmark"),
            integer.clone(),
            vec![
                literal(Datum::Int(2)),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("rand"),
                    FieldType::new(FieldTypeCode::Double),
                    vec![],
                )),
            ],
        ));
        let mut condition = Column::new(1, integer.clone());
        condition.index = 0;
        let mut input = Chunk::new(&[integer.clone()], 3, 3);
        input.append_int64(0, -1);
        input.append_int64(0, 0);
        input.append_null(0);
        let rows = [2, 0, 1, 0, 2];
        for (name, draws) in [("or", 6), ("and", 8)] {
            let expression = logic(
                name,
                Expression::Column(condition.clone()),
                benchmark.clone(),
            );
            let baseline = Session::default();
            let expected = rows
                .iter()
                .map(|row| {
                    expression
                        .eval(&baseline, input.physical_row(*row))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(baseline.draws.get(), draws);
            eprintln!("native {name}: values={expected:?}, draws={draws}");
            let program = SharedExpression::compile(&expression).unwrap();
            let session = Session::default();
            let mut execution = program.start_execution(&session);
            let mut ctx = EvalContext::default();
            let mut actual = execution
                .evaluate(&input, &rows[..2], &mut ctx, InvocationMode::Vector)
                .unwrap();
            actual.extend(
                execution
                    .evaluate(&input, &rows[2..], &mut ctx, InvocationMode::Vector)
                    .unwrap(),
            );
            assert_eq!(actual, expected);
            assert_eq!(session.draws.get(), draws);
            let scalar = Session::default();
            let mut execution = program.start_execution(&scalar);
            for (&row, expected) in rows.iter().zip(expected) {
                assert_eq!(
                    execution
                        .evaluate(&input, &[row], &mut ctx, InvocationMode::Scalar)
                        .unwrap(),
                    [expected],
                );
            }
            assert_eq!(scalar.draws.get(), draws);
        }

        let overflow = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            integer.clone(),
            vec![literal(Datum::Int(i64::MAX)), literal(Datum::Int(1))],
        ));
        for (name, dead, live) in [("and", 0, -1), ("or", -1, 0)] {
            for (left, expected) in [
                (
                    Datum::Int(dead),
                    Some(Datum::Int(if dead == 0 { 0 } else { 1 })),
                ),
                (Datum::Int(live), None),
                (Datum::Null, None),
            ] {
                let expression = logic(name, literal(left), overflow.clone());
                let native = expression.eval(&crate::context::NoColumns, input.physical_row(0));
                let actual = SharedExpression::compile(&expression).unwrap().evaluate(
                    &input,
                    &rows,
                    &mut EvalContext::default(),
                );
                if let Some(expected) = expected {
                    assert_eq!(native.unwrap(), expected);
                    assert_eq!(actual.unwrap(), vec![expected; rows.len()]);
                } else {
                    let Err(crate::EvalError::DataOutOfRange { value, expression }) = native else {
                        panic!("expected original overflow, got {native:?}");
                    };
                    let actual = actual.unwrap_err();
                    assert_eq!(actual.code, Some(1690));
                    assert_eq!(
                        actual.message,
                        format!("{value} value is out of range in '{expression}'")
                    );
                    eprintln!("{name} live error: {actual:?}");
                }
            }
        }
    }

    #[test]
    fn shared_kernel_benchmark_reexecutes_child_and_preserves_type_demand() {
        use tidb_datatype::FieldTypeCode;
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let constant = |value| Expression::Constant(Constant::new(value, integer.clone()));
        let rand = || {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("rand"),
                FieldType::new(FieldTypeCode::Double),
                vec![],
            ))
        };
        let bench = |count, body| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("benchmark"),
                integer.clone(),
                vec![count, body],
            ))
        };
        let mut input = Chunk::new(&[], 2, 2);
        input.set_num_virtual_rows(2);
        for (count, draws) in [
            (Datum::Null, 0),
            (Datum::Int(-1), 0),
            (Datum::Int(0), 0),
            (Datum::Int(3), 9),
        ] {
            let expression = bench(constant(count), rand());
            let baseline = Session::default();
            let session = Session::default();
            let expected: Vec<_> = [1, 0, 1]
                .iter()
                .map(|r| expression.eval(&baseline, input.physical_row(*r)).unwrap())
                .collect();
            assert_eq!(baseline.draws.get(), draws);
            let actual = SharedExpression::compile(&expression)
                .unwrap()
                .start_execution(&session)
                .evaluate(
                    &input,
                    &[1, 0, 1],
                    &mut EvalContext::default(),
                    InvocationMode::Vector,
                )
                .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(session.draws.get(), draws);
        }
        let expression = bench(
            constant(Datum::Int(2)),
            bench(constant(Datum::Int(3)), rand()),
        );
        let session = Session::default();
        assert_eq!(
            SharedExpression::compile(&expression)
                .unwrap()
                .start_execution(&session)
                .evaluate(
                    &input,
                    &[1, 0],
                    &mut EvalContext::default(),
                    InvocationMode::Vector
                )
                .unwrap(),
            vec![Datum::Int(0); 2]
        );
        assert_eq!(session.draws.get(), 12);
        for count in [Datum::Null, Datum::Int(-1), Datum::Int(0), Datum::Int(1)] {
            let expression = bench(
                constant(count.clone()),
                Expression::Constant(Constant::new(
                    Datum::Null,
                    FieldType::new(FieldTypeCode::VectorFloat32),
                )),
            );
            let native = expression.eval(&crate::context::NoColumns, input.physical_row(0));
            let actual = SharedExpression::compile(&expression).unwrap().evaluate(
                &input,
                &[0],
                &mut EvalContext::default(),
            );
            if matches!(count, Datum::Null | Datum::Int(-1)) {
                assert_eq!(native.unwrap(), Datum::Null);
                assert_eq!(actual.unwrap(), vec![Datum::Null]);
            } else {
                assert_eq!(
                    native.unwrap_err(),
                    crate::EvalError::Unsupported("VectorFloat32 is not supported for BENCHMARK()")
                );
                let error = actual.unwrap_err();
                assert_eq!(error.code, Some(1105));
                assert_eq!(
                    error.message,
                    "VectorFloat32 is not supported for BENCHMARK()"
                );
            }
        }
        let guarded = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            integer.clone(),
            vec![
                constant(Datum::Int(0)),
                bench(constant(Datum::Int(3)), rand()),
                constant(Datum::Int(7)),
            ],
        ));
        let session = Session::default();
        assert_eq!(
            SharedExpression::compile(&guarded)
                .unwrap()
                .start_execution(&session)
                .evaluate(
                    &input,
                    &[1, 0, 1],
                    &mut EvalContext::default(),
                    InvocationMode::Vector
                )
                .unwrap(),
            vec![Datum::Int(7); 3]
        );
        assert_eq!(session.draws.get(), 0);
        let bad = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("json_length"),
            integer.clone(),
            vec![Expression::Constant(Constant::new(
                Datum::new_string(b"{".to_vec()),
                native_type(),
            ))],
        ));
        assert_eq!(
            SharedExpression::compile(&bench(constant(Datum::Int(0)), bad.clone()))
                .unwrap()
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap(),
            vec![Datum::Int(0)]
        );
        assert_eq!(
            SharedExpression::compile(&bench(constant(Datum::Int(1)), bad))
                .unwrap()
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap_err()
                .code,
            Some(3140)
        );
    }

    #[test]
    fn shared_kernel_independent_executions_reset_seeded_state() {
        let real = FieldType::new(tidb_datatype::FieldTypeCode::Double);
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("rand"),
            real,
            vec![numeric_literal(Datum::Int(7), &integer)],
        ));
        let program = SharedExpression::compile(&expression).unwrap();
        let effects = Session::default();
        let mut input = Chunk::new(&[], 2, 2);
        input.set_num_virtual_rows(2);
        // A new execution on the same frozen program/effects resets the seed;
        // replacing EvalContext alone did not do so before explicit ownership.
        let first = program
            .start_execution(&effects)
            .evaluate(
                &input,
                &[0, 1],
                &mut EvalContext::default(),
                InvocationMode::Vector,
            )
            .unwrap();
        let second = program
            .start_execution(&effects)
            .evaluate(
                &input,
                &[0, 1],
                &mut EvalContext::default(),
                InvocationMode::Vector,
            )
            .unwrap();
        eprintln!("independent execution witness: first={first:?}, second={second:?}");
        assert_eq!(first, second, "new execution must restart a constant seed");
    }

    #[test]
    fn shared_kernel_execution_interleaves_batches_and_outputs() {
        eprintln!(
            "execution sizes: SDK={}, expression={}, projection={}",
            std::mem::size_of::<tidb_query_expr::RpnExecution>(),
            std::mem::size_of::<SharedExpressionExecution>(),
            std::mem::size_of::<SharedProjectionExecution>()
        );
        use tidb_datatype::FieldTypeCode;
        let real = FieldType::new(FieldTypeCode::Double);
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let rand = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("rand"),
            real.clone(),
            vec![numeric_literal(Datum::Int(7), &integer)],
        ));
        let projection = SharedProjection::compile(&[rand.clone(), rand.clone()]).unwrap();
        // Two same-seed sites inside ONE RPN program need distinct local slots.
        let sum = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            real.clone(),
            vec![rand.clone(), rand.clone()],
        ));
        let sum = SharedExpression::compile(&sum).unwrap();
        let effects = Session::default();
        let mut execution = sum.start_execution(&effects);
        let oracle = crate::MysqlRng::new_with_seed(7);
        for mode in [InvocationMode::Scalar, InvocationMode::Vector] {
            let mut input = Chunk::new_empty(&[]);
            input.set_num_virtual_rows(1);
            assert_eq!(
                execution
                    .evaluate(&input, &[0], &mut EvalContext::default(), mode)
                    .unwrap(),
                vec![Datum::Real(oracle.gen() * 2.0)]
            );
        }

        for scalar in [false, true] {
            let effects = Session {
                vectorized_off: scalar,
                ..Session::default()
            };
            let mut a = projection.start_execution(&effects);
            let mut b = projection.start_execution(&effects);
            let oracle = crate::MysqlRng::new_with_seed(7);
            let mut first = Vec::new();
            for (batch, rows) in [&[0, 1][..], &[2, 0, 2][..]].into_iter().enumerate() {
                // Each iteration really replaces the input allocation.
                let mut input = Chunk::new_empty(&[]);
                input.set_num_virtual_rows(3);
                let expected: Vec<_> = rows.iter().map(|_| Datum::Real(oracle.gen())).collect();
                if batch == 0 {
                    first = expected.clone();
                }
                assert_eq!(
                    a.evaluate(&input, &[], &mut EvalContext::default())
                        .unwrap(),
                    vec![vec![], vec![]]
                );
                assert_eq!(
                    a.evaluate(&input, rows, &mut EvalContext::default())
                        .unwrap(),
                    vec![expected.clone(), expected.clone()]
                );
                let mut output = Chunk::new(&[real.clone(), real.clone()], 3, 3);
                b.evaluate_into_chunk(&input, rows, &mut EvalContext::default(), &mut output)
                    .unwrap();
                for col in 0..2 {
                    for (row, value) in expected.iter().enumerate() {
                        assert_eq!(output.physical_row(row).get_datum(col, &real), *value);
                    }
                }
            }
            let mut input = Chunk::new_empty(&[]);
            input.set_num_virtual_rows(2);
            assert_eq!(
                projection
                    .start_execution(&effects)
                    .evaluate(&input, &[0, 1], &mut EvalContext::default())
                    .unwrap(),
                vec![first.clone(), first]
            );
            assert_eq!(effects.draws.get(), 0, "capability must not probe RAND()");
            assert!(
                effects.seeds.borrow().is_empty(),
                "shared state stays out of native map"
            );
            assert_eq!(
                SharedExpression::compile(&rand)
                    .unwrap()
                    .evaluate(&input, &[0, 1], &mut EvalContext::default())
                    .unwrap(),
                vec![Datum::Real(crate::MysqlRng::new_with_seed(7).gen()); 2]
            );
        }
    }

    #[test]
    fn shared_kernel_execution_dynamic_null_and_dead_seed_demand() {
        use tidb_datatype::FieldTypeCode;
        let real = FieldType::new(FieldTypeCode::Double);
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut seed = Column::new(1, integer.clone());
        seed.index = 0;
        let mut condition = Column::new(2, integer.clone());
        condition.index = 1;
        let rand = |seed| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("rand"),
                real.clone(),
                vec![seed],
            ))
        };
        let dynamic = rand(Expression::Column(seed));
        let guarded = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            real.clone(),
            vec![
                Expression::Column(condition),
                rand(numeric_literal(Datum::Null, &integer)),
                numeric_literal(Datum::Real(-1.0), &real),
            ],
        ));
        let projection = SharedProjection::compile(&[dynamic, guarded]).unwrap();
        for scalar in [false, true] {
            let session = Session {
                vectorized_off: scalar,
                ..Session::default()
            };
            let mut execution = projection.start_execution(&session);
            let seeded = crate::MysqlRng::new_with_seed(0);
            for _ in 0..2 {
                let mut input = Chunk::new(&[integer.clone(), integer.clone()], 3, 3);
                for (seed, condition) in [(Datum::Null, 0), (Datum::Int(7), 1), (Datum::Int(9), 1)]
                {
                    input.append_datum(0, &seed);
                    input.append_int64(1, condition);
                }
                let result = execution
                    .evaluate(&input, &[0, 2, 1, 1], &mut EvalContext::default())
                    .unwrap();
                assert_eq!(
                    result[0],
                    [0, 9, 7, 7]
                        .map(|seed| Datum::Real(crate::MysqlRng::new_with_seed(seed).gen()))
                );
                assert_eq!(
                    result[1],
                    vec![
                        Datum::Real(-1.0),
                        Datum::Real(seeded.gen()),
                        Datum::Real(seeded.gen()),
                        Datum::Real(seeded.gen())
                    ]
                );
            }
            assert_eq!(session.draws.get(), 0);
        }
    }

    #[test]
    fn shared_kernel_execution_retains_error_effects_and_recovers() {
        use tidb_datatype::FieldTypeCode;
        let real = FieldType::new(FieldTypeCode::Double);
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let text = native_type();
        let rand = |args| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("rand"),
                real.clone(),
                args,
            ))
        };
        let mut col = Column::new(1, text.clone());
        col.index = 0;
        let json = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("json_length"),
            integer.clone(),
            vec![Expression::Column(col)],
        ));
        let projection = SharedProjection::compile(&[
            rand(vec![numeric_literal(Datum::Int(7), &integer)]),
            rand(vec![]),
            json,
        ])
        .unwrap();
        for scalar in [false, true] {
            for direct in [false, true] {
                let effects = Session {
                    vectorized_off: scalar,
                    ..Session::default()
                };
                let mut execution = projection.start_execution(&effects);
                let mut bad = Chunk::new(std::slice::from_ref(&text), 1, 1);
                bad.append_datum(0, &Datum::new_string(b"{".to_vec()));
                let mut output = Chunk::new(&[real.clone(), real.clone(), integer.clone()], 1, 1);
                output.append_datum(0, &Datum::Real(99.0));
                output.append_datum(1, &Datum::Real(98.0));
                output.append_datum(2, &Datum::Int(97));
                let error = if direct {
                    execution
                        .evaluate_into_chunk(&bad, &[0], &mut EvalContext::default(), &mut output)
                        .unwrap_err()
                } else {
                    execution
                        .evaluate(&bad, &[0], &mut EvalContext::default())
                        .unwrap_err()
                };
                assert_eq!(error.code, Some(3140));
                assert_eq!(effects.draws.get(), 1);
                assert_eq!(
                    output.physical_row(0).get_datum(0, &real),
                    Datum::Real(99.0)
                );
                let mut good = Chunk::new(std::slice::from_ref(&text), 1, 1);
                good.append_datum(0, &Datum::new_string(b"[]".to_vec()));
                let oracle = crate::MysqlRng::new_with_seed(7);
                oracle.gen();
                let expected = vec![
                    vec![Datum::Real(oracle.gen())],
                    vec![Datum::Real(0.25)],
                    vec![Datum::Int(0)],
                ];
                if direct {
                    execution
                        .evaluate_into_chunk(&good, &[0], &mut EvalContext::default(), &mut output)
                        .unwrap();
                    for (col, ty) in [&real, &real, &integer].into_iter().enumerate() {
                        assert_eq!(output.physical_row(0).get_datum(col, ty), expected[col][0]);
                    }
                } else {
                    assert_eq!(
                        execution
                            .evaluate(&good, &[0], &mut EvalContext::default())
                            .unwrap(),
                        expected
                    );
                }
                assert_eq!(effects.draws.get(), 2);
            }
        }
    }

    #[test]
    fn shared_kernel_rand_state_and_projection_order() {
        let real = FieldType::new(tidb_datatype::FieldTypeCode::Double);
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let rand = |args| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("rand"),
                real.clone(),
                args,
            ))
        };
        let roots = vec![rand(vec![]), rand(vec![])];
        let mut input = Chunk::new(&[], 2, 2);
        input.set_num_virtual_rows(2);
        let native_session = Session::default();
        let mut output = Chunk::new(&[real.clone(), real.clone()], 2, 2);
        crate::evaluator::EvaluatorSuite::new(roots.clone(), false)
            .run(&native_session, &mut input, &mut output)
            .unwrap();
        let expected = vec![
            vec![Datum::Real(0.125), Datum::Real(0.25)],
            vec![Datum::Real(0.375), Datum::Real(0.5)],
        ];
        for (c, values) in expected.iter().enumerate() {
            for (r, value) in values.iter().enumerate() {
                assert_eq!(Datum::Real(output.column(c).get_float64(r)), *value);
            }
        }
        let projection = SharedProjection::compile(&roots).unwrap();
        assert_eq!(
            projection
                .start_execution(&Session::default())
                .evaluate(&input, &[0, 1], &mut EvalContext::default())
                .unwrap(),
            expected
        );
        let seed = || Expression::Constant(Constant::new(Datum::Int(7), integer.clone()));
        let a = rand(vec![seed()]);
        let b = rand(vec![seed()]);
        let shared_a = SharedExpression::compile(&a).unwrap();
        let shared_b = SharedExpression::compile(&b).unwrap();
        let baseline = Session::default();
        let session = Session::default();
        let mut execution_a = shared_a.start_execution(&session);
        let mut execution_b = shared_b.start_execution(&session);
        for (source, which, rows) in [
            (&a, 0, &[1, 0][..]),
            (&b, 1, &[0][..]),
            (&a, 0, &[1, 1][..]),
        ] {
            let execution = if which == 0 {
                &mut execution_a
            } else {
                &mut execution_b
            };
            let values: Vec<_> = rows
                .iter()
                .map(|r| source.eval(&baseline, input.physical_row(*r)).unwrap())
                .collect();
            assert_eq!(
                execution
                    .evaluate(
                        &input,
                        rows,
                        &mut EvalContext::default(),
                        InvocationMode::Vector
                    )
                    .unwrap(),
                values
            );
        }
        assert!(session.seeds.borrow().is_empty());
        assert_eq!(baseline.seeds.borrow().len(), 2);
        let fresh = SharedExpression::compile(&rand(vec![seed()])).unwrap();
        assert_eq!(
            fresh
                .start_execution(&session)
                .evaluate(
                    &input,
                    &[0],
                    &mut EvalContext::default(),
                    InvocationMode::Scalar
                )
                .unwrap(),
            vec![Datum::Real(crate::MysqlRng::new_with_seed(7).gen())]
        );
        let mut seed_column = Column::new(1, integer.clone());
        seed_column.index = 0;
        let dynamic = rand(vec![Expression::Column(seed_column)]);
        let mut input = Chunk::new(&[integer], 2, 2);
        input.append_int64(0, 7);
        input.append_int64(0, 7);
        let dynamic_session = Session::default();
        let values = SharedExpression::compile(&dynamic)
            .unwrap()
            .start_execution(&dynamic_session)
            .evaluate(
                &input,
                &[1, 0, 1],
                &mut EvalContext::default(),
                InvocationMode::Vector,
            )
            .unwrap();
        assert_eq!(
            values,
            vec![
                dynamic
                    .eval(&dynamic_session, input.physical_row(0))
                    .unwrap();
                3
            ]
        );
        assert!(dynamic_session.seeds.borrow().is_empty());
    }

    fn numeric_literal(value: Datum, ty: &FieldType) -> Expression {
        Expression::Constant(Constant::new(value, ty.clone()))
    }
    fn numeric_binary(name: &str, ty: &FieldType, left: Datum, right: Datum) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new(name),
            ty.clone(),
            vec![numeric_literal(left, ty), numeric_literal(right, ty)],
        ))
    }
    fn assert_same_decimal(actual: &Datum, expected: &Datum) {
        let (Datum::Decimal(a), Datum::Decimal(b)) = (actual, expected) else {
            panic!("decimal results required");
        };
        assert_eq!(a.is_negative(), b.is_negative());
        assert_eq!(a.coefficient_digits(), b.coefficient_digits());
        assert_eq!(a.storage_scale(), b.storage_scale());
        assert_eq!(a.scale(), b.scale());
        assert_eq!(a.declared_shape(), b.declared_shape());
        assert_eq!(a.to_string(), b.to_string());
    }
    fn assert_native_error(actual: SharedEvaluationError, expected: crate::EvalError) {
        match expected {
            crate::EvalError::DataOutOfRange { value, expression } => {
                assert_eq!(actual.code, Some(1690));
                assert_eq!(
                    actual.message,
                    format!("{value} value is out of range in '{expression}'")
                );
            }
            crate::EvalError::TruncatedWrongValue(message) => {
                assert_eq!(actual.code, Some(1292));
                assert_eq!(actual.message, message);
            }
            crate::EvalError::Conversion(error) => {
                let error = error.to_sql_error();
                assert_eq!(actual.code, Some(i32::from(error.code)));
                assert_eq!(actual.message, error.message);
            }
            crate::EvalError::Unsupported(message) => {
                assert_eq!(actual.code, Some(1105));
                assert_eq!(actual.message, message);
            }
            other => panic!("unexpected original error {other:?}"),
        }
    }

    fn regexp_expr(
        subject: Expression,
        pattern: Expression,
        flags: Option<Expression>,
    ) -> Expression {
        let mut args = vec![subject, pattern];
        args.extend(flags);
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("regexp_like"),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            args,
        ))
    }

    fn regexp_column(index: usize) -> Expression {
        let mut col = Column::new(index as i64 + 1, native_type());
        col.index = index as i64;
        Expression::Column(col)
    }

    #[test]
    fn shared_kernel_regexp_rejects_case_insensitive_result_collation() {
        let mut input = Chunk::new(&[native_type()], 1, 1);
        input.append_datum(0, &Datum::new_string(b"^a$"));
        let text = |v: &[u8]| numeric_literal(Datum::new_string(v), &native_type());
        for (kind, pattern) in [("literal", text(b"^a$")), ("dynamic", regexp_column(0))] {
            let mut expression = regexp_expr(text(b"A"), pattern, None);
            let Expression::ScalarFunction(f) = &mut expression else {
                unreachable!()
            };
            f.ret_type
                .as_mut()
                .unwrap()
                .set_collation_name("utf8mb4_general_ci");
            let native = expression
                .eval(&crate::context::NoColumns, input.physical_row(0))
                .unwrap();
            assert_eq!(native, Datum::Int(1));
            assert_eq!(
                SharedExpression::compile(&expression).unwrap_err(),
                "REGEXP_LIKE requires a case-sensitive effective collation",
                "{kind} pattern"
            );
            let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
            let one = numeric_literal(Datum::Int(1), &integer);
            let parent = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                integer,
                vec![one.clone(), expression, one.clone()],
            ));
            assert_eq!(
                SharedProjection::compile(&[one, parent]).unwrap_err(),
                "REGEXP_LIKE requires a case-sensitive effective collation"
            );
        }
    }

    #[test]
    fn shared_kernel_regexp_respects_rightmost_case_flag() {
        let text = |v: &[u8]| numeric_literal(Datum::new_string(v), &native_type());
        let mut input = Chunk::new(&[native_type()], 1, 1);
        input.append_datum(0, &Datum::new_string(b"^a$"));
        for pattern in [text(b"^a$"), regexp_column(0)] {
            for (flags, matched) in [(b"ci", 1), (b"ic", 0)] {
                let expression = regexp_expr(text(b"A"), pattern.clone(), Some(text(flags)));
                let expected = Datum::Int(matched);
                assert_eq!(
                    expression
                        .eval(&crate::context::NoColumns, input.physical_row(0))
                        .unwrap(),
                    expected
                );
                assert_eq!(
                    SharedExpression::compile(&expression)
                        .unwrap()
                        .evaluate(&input, &[0], &mut EvalContext::default())
                        .unwrap(),
                    vec![expected]
                );
            }
        }
    }

    #[test]
    fn shared_kernel_regexp_literal_admission_and_dynamic_batches() {
        let text = |v| numeric_literal(Datum::new_string(v), &native_type());
        for (pattern, flags, cached) in [
            (text(b"a".to_vec()), None, true),
            (text(b"a".to_vec()), Some(text(b"i".to_vec())), true),
            (regexp_column(1), Some(text(b"i".to_vec())), false),
            (text(b"a".to_vec()), Some(regexp_column(2)), false),
        ] {
            let expression = regexp_expr(regexp_column(0), pattern, flags);
            let compiled = SharedExpression::compile(&expression).unwrap();
            assert_eq!(
                compiled
                    .program
                    .as_ref()
                    .iter()
                    .any(|n| matches!(n, RpnExpressionNode::NativeRegexpLike { .. })),
                cached
            );
            let mut execution = compiled.start_execution(&crate::context::NoColumns);
            for (subject, pattern, flags) in
                [("Abc", "a", "i"), ("abc", "z", "c"), ("ABC", "A", "c")]
            {
                let mut input = Chunk::new(&[native_type(), native_type(), native_type()], 3, 3);
                for _ in 0..3 {
                    for (i, v) in [subject, pattern, flags].into_iter().enumerate() {
                        input.append_datum(i, &Datum::new_string(v.as_bytes()));
                    }
                }
                let expected = expression
                    .eval(&crate::context::NoColumns, input.physical_row(0))
                    .unwrap();
                assert_eq!(
                    execution
                        .evaluate(
                            &input,
                            &[2, 0, 2],
                            &mut EvalContext::default(),
                            InvocationMode::Vector
                        )
                        .unwrap(),
                    vec![expected; 3]
                );
            }
        }
    }

    #[test]
    fn shared_kernel_regexp_cached_error_guards_and_output_recovery() {
        use crate::context::Columns;
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let text = |v: &[u8]| numeric_literal(Datum::new_string(v), &native_type());
        let write = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("setvar"),
            native_type(),
            vec![text(b"v"), regexp_column(0)],
        ));
        let expr = regexp_expr(write, text(b"["), None);
        let projection = SharedProjection::compile(&[expr.clone()]).unwrap();
        for scalar in [false, true] {
            for direct in [false, true] {
                let effects = Session {
                    vectorized_off: scalar,
                    ..Session::default()
                };
                let mut execution = projection.start_execution(&effects);
                let mut output = Chunk::new(std::slice::from_ref(&integer), 1, 1);
                output.append_int64(0, 97);
                for value in [
                    Some(b"first".to_vec()),
                    None,
                    Some(vec![0xff]),
                    Some(b"again".to_vec()),
                    None,
                ] {
                    let mut input = Chunk::new(&[native_type()], 1, 1);
                    input.append_datum(
                        0,
                        &value.clone().map(Datum::new_string).unwrap_or(Datum::Null),
                    );
                    let prior = output.physical_row(0).get_datum(0, &integer);
                    let oracle = Session::default();
                    let expected = expr.eval(&oracle, input.physical_row(0));
                    let actual = if direct {
                        execution
                            .evaluate_into_chunk(
                                &input,
                                &[0],
                                &mut EvalContext::default(),
                                &mut output,
                            )
                            .map(|()| vec![vec![output.physical_row(0).get_datum(0, &integer)]])
                    } else {
                        execution.evaluate(&input, &[0], &mut EvalContext::default())
                    };
                    match expected {
                        Ok(value) => assert_eq!(actual.unwrap(), vec![vec![value]]),
                        Err(error) => {
                            assert_native_error(actual.unwrap_err(), error);
                            assert_eq!(output.physical_row(0).get_datum(0, &integer), prior);
                        }
                    }
                    if value.is_some() {
                        assert_eq!(effects.get_uservar("v"), oracle.get_uservar("v"));
                    }
                }
            }
        }
    }

    #[test]
    fn shared_kernel_regexp_empty_dead_and_cached_subject_effects() {
        use crate::context::Columns;
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let text = |v: &[u8]| numeric_literal(Datum::new_string(v), &native_type());
        let write = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("setvar"),
            native_type(),
            vec![text(b"v"), regexp_column(0)],
        ));
        let regexp = regexp_expr(write, text(b"a"), None);
        let mut condition = Column::new(2, integer.clone());
        condition.index = 1;
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            integer.clone(),
            vec![
                Expression::Column(condition),
                regexp,
                numeric_literal(Datum::Int(7), &integer),
            ],
        ));
        let projection = SharedProjection::compile(&[expression.clone(), expression]).unwrap();
        let effects = Session::default();
        let mut execution = projection.start_execution(&effects);
        for (subject, condition, expected) in [("first", 0, 7), ("abc", 1, 1), ("zzz", 1, 0)] {
            let mut input = Chunk::new(&[native_type(), integer.clone()], 3, 3);
            for _ in 0..3 {
                input.append_datum(0, &Datum::new_string(subject.as_bytes()));
                input.append_int64(1, condition);
            }
            assert_eq!(
                execution
                    .evaluate(&input, &[], &mut EvalContext::default())
                    .unwrap(),
                vec![vec![], vec![]]
            );
            assert_eq!(
                execution
                    .evaluate(&input, &[2, 0, 2], &mut EvalContext::default())
                    .unwrap(),
                vec![vec![Datum::Int(expected); 3]; 2]
            );
            if condition == 0 {
                assert_eq!(effects.get_uservar("v"), None);
            } else {
                assert_eq!(
                    effects.get_uservar("v"),
                    Some(Datum::new_string(subject.as_bytes()))
                );
            }
        }
    }

    #[test]
    fn shared_kernel_regexp_arguments_stop_on_null_and_invalid_utf8() {
        use crate::context::Columns;
        let text = |v| numeric_literal(v, &native_type());
        for first in [Datum::Null, Datum::new_string(vec![0xff])] {
            let write = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("setvar"),
                native_type(),
                vec![text(Datum::new_string(b"v")), text(Datum::new_string(b"["))],
            ));
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("regexp_like"),
                FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                vec![text(first.clone()), write],
            ));
            let original = Session::default();
            let session = Session::default();
            let mut input = Chunk::new(&[], 1, 1);
            input.set_num_virtual_rows(1);
            let expected = expression.eval(&original, input.physical_row(0));
            assert_eq!(original.get_uservar("v"), None);
            let actual = SharedExpression::compile(&expression)
                .unwrap()
                .start_execution(&session)
                .evaluate(
                    &input,
                    &[0],
                    &mut EvalContext::default(),
                    InvocationMode::Vector,
                );
            assert_eq!(session.get_uservar("v"), None);
            if first == Datum::Null {
                assert_eq!(expected, Ok(Datum::Null));
                assert_eq!(actual.unwrap(), vec![Datum::Null]);
            } else {
                assert_eq!(
                    expected,
                    Err(crate::EvalError::Unsupported(
                        "invalid UTF-8 REGEXP_LIKE argument"
                    ))
                );
                assert_eq!(actual.unwrap_err().code, Some(1105));
            }
        }
    }

    #[test]
    fn shared_kernel_regexp_and_rand_diagnostic_identity() {
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let text = |v: Vec<u8>| numeric_literal(Datum::new_string(v), &native_type());
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        for (subject, pattern, flags) in [
            (b"abc".to_vec(), b"".to_vec(), b"".to_vec()),
            (b"abc".to_vec(), b"[".to_vec(), b"".to_vec()),
            (b"abc".to_vec(), b"a".to_vec(), b"n".to_vec()),
            (vec![0xff], b"a".to_vec(), vec![]),
            (b"abc".to_vec(), vec![0xff], vec![]),
            (b"abc".to_vec(), b"a".to_vec(), vec![0xff]),
        ] {
            let expr = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("regexp_like"),
                integer.clone(),
                vec![text(subject), text(pattern), text(flags)],
            ));
            let expected = expr
                .eval(&crate::context::NoColumns, input.physical_row(0))
                .unwrap_err();
            let actual = SharedExpression::compile(&expr)
                .unwrap()
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap_err();
            let crate::EvalError::Unsupported(message) = expected else {
                panic!("unexpected {expected:?}");
            };
            assert_eq!(
                actual,
                SharedEvaluationError {
                    code: Some(1105),
                    message: message.into()
                }
            );
        }
        let expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("rand"),
            FieldType::new(tidb_datatype::FieldTypeCode::Double),
            vec![],
        ));
        assert_eq!(
            expr.eval(&crate::context::NoColumns, input.physical_row(0)),
            Err(crate::EvalError::Unsupported("RAND requires a session"))
        );
        assert_eq!(
            SharedExpression::compile(&expr)
                .unwrap()
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap_err(),
            SharedEvaluationError {
                code: Some(1105),
                message: "RAND requires a session".into()
            }
        );
    }

    #[test]
    fn shared_kernel_numeric_warning_prefix_matches_actual_invocation() {
        let real = FieldType::new(tidb_datatype::FieldTypeCode::Double);
        let mut s = Column::new(1, native_type());
        s.index = 0;
        let mut x = Column::new(2, real.clone());
        x.index = 1;
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            real.clone(),
            vec![
                Expression::Column(s),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("mul"),
                    real.clone(),
                    vec![
                        Expression::Column(x),
                        numeric_literal(Datum::Real(2.0), &real),
                    ],
                )),
            ],
        ));
        let shared = SharedProjection::compile(&[expression.clone()]).unwrap();
        for vectorized_off in [true, false] {
            let original = Session {
                vectorized_off,
                ..Session::default()
            };
            let session = Session {
                vectorized_off,
                ..Session::default()
            };
            let mut input = Chunk::new(&[native_type(), real.clone()], 2, 2);
            input.append_string(0, "1x");
            input.append_float64(1, f64::MAX);
            input.append_string(0, "2y");
            input.append_float64(1, 1.0);
            let mut output = Chunk::new(&[real.clone()], 2, 2);
            let expected = crate::evaluator::EvaluatorSuite::new(vec![expression.clone()], false)
                .run(&original, &mut input, &mut output)
                .unwrap_err();
            let actual = shared
                .start_execution(&session)
                .evaluate(&input, &[0, 1], &mut EvalContext::default())
                .unwrap_err();
            let crate::evaluator::EvaluatorError::Eval(expected) = expected else {
                panic!("expected evaluation failure");
            };
            assert_native_error(actual, expected);
            let expected_warnings = if vectorized_off { 1 } else { 2 };
            assert_eq!(original.warnings.borrow().len(), expected_warnings);
            assert_eq!(*session.warnings.borrow(), *original.warnings.borrow());
        }
        let mut input = Chunk::new(&[native_type(), real.clone()], 8, 8);
        for row in 0..8 {
            input.append_string(0, if row == 2 { "2y" } else { "1x" });
            input.append_float64(1, 1.0);
        }
        let session = Session::default();
        assert_eq!(
            shared
                .start_execution(&session)
                .evaluate(&input, &[7, 2, 7], &mut EvalContext::default())
                .unwrap(),
            vec![vec![Datum::Real(3.0), Datum::Real(4.0), Datum::Real(3.0)]]
        );
        assert_eq!(session.warnings.borrow().len(), 3);
    }

    #[test]
    fn shared_kernel_real_arithmetic_checks_only_demanded_values() {
        let real = FieldType::new(tidb_datatype::FieldTypeCode::Double);
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        for (a, b) in [(f64::from_bits(0x7ff8000000000001), 1.0), (1e308, 1e308)] {
            for flag in [0, 1] {
                let arithmetic = numeric_binary("plus", &real, Datum::Real(a), Datum::Real(b));
                let expression = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("if"),
                    real.clone(),
                    vec![
                        numeric_literal(Datum::Int(flag), &integer),
                        arithmetic,
                        numeric_literal(Datum::Real(7.0), &real),
                    ],
                ));
                let baseline = expression.eval(&crate::context::NoColumns, input.physical_row(0));
                let shared = SharedExpression::compile(&expression).unwrap();
                for mode in [InvocationMode::Scalar, InvocationMode::Vector] {
                    let actual =
                        shared.evaluate_mode(&input, &[0], &mut EvalContext::default(), mode);
                    if flag == 0 {
                        assert_eq!(baseline, Ok(Datum::Real(7.0)));
                        assert_eq!(actual.unwrap(), vec![Datum::Real(7.0)]);
                    } else {
                        assert_native_error(actual.unwrap_err(), baseline.clone().unwrap_err());
                    }
                }
            }
        }
    }

    #[test]
    fn shared_kernel_decimal_multiply_preserves_hidden_precision() {
        use tidb_datatype::Decimal;
        let decimal = FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal);
        let literal = |n| numeric_literal(Datum::Decimal(Decimal::from_int(n)), &decimal);
        let binary = |name, left, right| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                decimal.clone(),
                vec![left, right],
            ))
        };
        let expressions: Vec<_> = [1, -1, 0, i64::MAX]
            .into_iter()
            .flat_map(|factor| {
                let quotient = binary("div", literal(8), literal(7));
                let product = binary("mul", quotient, literal(factor));
                let final_value = binary("div", product.clone(), literal(100));
                [product, final_value]
            })
            .collect();
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        let mut differences = Vec::new();
        for expression in expressions {
            let expected = expression
                .eval(&Session::default(), input.physical_row(0))
                .unwrap();
            for mode in [InvocationMode::Scalar, InvocationMode::Vector] {
                let actual = SharedExpression::compile(&expression)
                    .unwrap()
                    .start_execution(&Session::default())
                    .evaluate(&input, &[0], &mut EvalContext::default(), mode)
                    .unwrap()
                    .remove(0);
                let parts = |value: &Datum| {
                    let Datum::Decimal(v) = value else {
                        panic!("decimal required")
                    };
                    (
                        v.is_negative(),
                        v.coefficient_digits().to_string(),
                        v.storage_scale(),
                        v.scale(),
                        v.declared_shape(),
                        v.to_string(),
                    )
                };
                if parts(&actual) != parts(&expected) {
                    differences.push((parts(&actual), parts(&expected)));
                }
            }
        }
        assert!(
            differences.is_empty(),
            "actual versus original Decimal parts: {differences:?}"
        );
    }

    #[test]
    fn shared_kernel_decimal_arithmetic_boundary_differential() {
        use tidb_datatype::Decimal;
        let decimal = FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal);
        let mut input = Chunk::new(&[], 3, 3);
        input.set_num_virtual_rows(3);
        let mut seed = 193u64;
        let mut value = |width: usize, scale: usize, negative: bool| {
            let digits: String = (0..width)
                .map(|_| {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    char::from(b'0' + ((seed >> 32) % 10) as u8)
                })
                .collect();
            let split = width.saturating_sub(scale);
            Decimal::from_signed_literal(&format!(
                "{}{}.{}",
                if negative { "-" } else { "" },
                &digits[..split],
                &digits[split..]
            ))
        };
        for case in 0..48 {
            let width = [1, 8, 9, 10, 18, 39, 40, 72, 81, 82][case % 10];
            let scale = [0, 1, 9, 30, 40][case % 5].min(width);
            let a = value(width, scale, case % 2 == 0);
            let b = value(
                width,
                [0, 1, 9, 30, 40][(case / 5) % 5].min(width),
                case % 3 == 0,
            );
            for op in ["plus", "minus", "mul", "div"] {
                let expression = numeric_binary(
                    op,
                    &decimal,
                    Datum::Decimal(a.clone()),
                    Datum::Decimal(b.clone()),
                );
                let baseline = Session::default();
                let context = Session::default();
                let expected = expression.eval(&baseline, input.physical_row(0));
                let actual = SharedExpression::compile(&expression)
                    .unwrap()
                    .start_execution(&context)
                    .evaluate(
                        &input,
                        &[2, 0, 2],
                        &mut EvalContext::default(),
                        InvocationMode::Vector,
                    );
                match expected {
                    Ok(expected) => {
                        for actual in actual.unwrap() {
                            if expected == Datum::Null {
                                assert_eq!(actual, Datum::Null);
                            } else {
                                assert_same_decimal(&actual, &expected);
                            }
                        }
                        assert_eq!(
                            *context.warnings.borrow(),
                            (0..3)
                                .flat_map(|_| baseline.warnings.borrow().clone())
                                .collect::<Vec<_>>()
                        );
                    }
                    Err(error) => assert_native_error(actual.unwrap_err(), error),
                }
            }
        }
    }

    #[test]
    fn shared_kernel_string_real_cast_matches_original_prefix_policy() {
        let real = FieldType::new(tidb_datatype::FieldTypeCode::Double);
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        for bytes in [
            b"".to_vec(),
            b"1e".to_vec(),
            b"1e+".to_vec(),
            b"1e999x".to_vec(),
            b"-1e999".to_vec(),
            b"1\0suffix".to_vec(),
            b"\0".to_vec(),
            b".2x".to_vec(),
            b"--2".to_vec(),
            [b"12".as_slice(), &[0xff]].concat(),
            "  ".as_bytes().to_vec(),
            "\u{2003}1x\u{2003}".as_bytes().to_vec(),
            format!("1{}", "é".repeat(80)).into_bytes(),
        ] {
            let expr = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                real.clone(),
                vec![
                    numeric_literal(Datum::new_string(bytes), &native_type()),
                    numeric_literal(Datum::Real(0.0), &real),
                ],
            ));
            for strict in [false, true] {
                let baseline = Session {
                    strict,
                    ..Session::default()
                };
                let context = Session {
                    strict,
                    ..Session::default()
                };
                let expected = expr.eval(&baseline, input.physical_row(0));
                let actual = SharedExpression::compile(&expr)
                    .unwrap()
                    .start_execution(&context)
                    .evaluate(
                        &input,
                        &[0],
                        &mut EvalContext::default(),
                        InvocationMode::Vector,
                    );
                match expected {
                    Ok(value) => assert_eq!(actual.unwrap(), vec![value]),
                    Err(error) => assert_native_error(actual.unwrap_err(), error),
                }
                assert_eq!(*baseline.warnings.borrow(), *context.warnings.borrow());
            }
        }
    }

    #[test]
    fn shared_kernel_decimal_columns_rebind_chunks_and_contexts() {
        use tidb_datatype::{Decimal, FieldTypeCode};
        let integer = FieldType::new(FieldTypeCode::LongLong);
        for scale in [-1, 4] {
            let decimal = FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(65)
                .with_decimal(scale)
                .with_added_raw_flags(1u64 << 40);
            let column = |index, ty: &FieldType| {
                let mut c = Column::new(index + 1, ty.clone());
                c.index = index;
                Expression::Column(c)
            };
            let arithmetic = |op| {
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new(op),
                    decimal.clone(),
                    vec![column(0, &decimal), column(1, &decimal)],
                ))
            };
            let expressions = vec![
                column(0, &decimal),
                arithmetic("plus"),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("if"),
                    decimal.clone(),
                    vec![column(2, &integer), arithmetic("mul"), column(0, &decimal)],
                )),
            ];
            let projection = SharedProjection::compile(&expressions).unwrap();
            let types = [decimal.clone(), decimal.clone(), integer.clone()];
            let mut ctx = EvalContext::default();
            // Reuse this exact projection and RPN context with new physical chunks.
            for turn in 0..3 {
                let mut input = Chunk::new(&types, 7, 7);
                for (a, b, flag) in [
                    ("-12.3400", "2.50", Some(1)),
                    (
                        "1234567890123456789012345678901234567890.125",
                        "-2",
                        Some(0),
                    ),
                    ("0.000", "3", None),
                    ("7.25", "0", Some(1)),
                    ("NULL", "2", Some(1)),
                    ("123456789012345678901234", "1", Some(1)),
                    ("1234567890123456789012345", "1", Some(1)),
                ] {
                    for (i, text) in [a, b].into_iter().enumerate() {
                        let value = if text == "NULL" {
                            Datum::Null
                        } else {
                            let value = Decimal::from_signed_literal(text);
                            Datum::Decimal(if turn == 1 { value.negate() } else { value })
                        };
                        input.append_datum(i, &value);
                    }
                    input.append_datum(2, &flag.map_or(Datum::Null, Datum::Int));
                }
                // The explicit selection is physical, independent of Chunk.sel.
                input.set_sel(Some(vec![1, 0]));
                for vectorized_off in [false, true] {
                    let original = Session {
                        vectorized_off,
                        ..Session::default()
                    };
                    let session = Session {
                        vectorized_off,
                        ..Session::default()
                    };
                    let mut execution = projection.start_execution(&session);
                    let rows = [4, 1, 0, 2, 3, 6, 5, 1, 0];
                    let actual = execution.evaluate(&input, &rows, &mut ctx).unwrap();
                    let mut direct = Chunk::new(
                        &vec![decimal.clone(); expressions.len()],
                        rows.len(),
                        rows.len(),
                    );
                    execution
                        .evaluate_into_chunk(&input, &rows, &mut ctx, &mut direct)
                        .unwrap();
                    for (root, expression) in expressions.iter().enumerate() {
                        let mut expected_chunk =
                            Chunk::new(std::slice::from_ref(&decimal), rows.len(), rows.len());
                        let mut actual_chunk =
                            Chunk::new(std::slice::from_ref(&decimal), rows.len(), rows.len());
                        for (r, row) in rows.iter().enumerate() {
                            let expected = expression
                                .eval(&original, input.physical_row(*row))
                                .unwrap();
                            if expected == Datum::Null {
                                assert_eq!(actual[root][r], Datum::Null);
                            } else {
                                assert_same_decimal(&actual[root][r], &expected);
                            }
                            expected_chunk.append_datum(0, &expected);
                            actual_chunk.append_datum(0, &actual[root][r]);
                        }
                        for r in 0..rows.len() {
                            let a = actual_chunk.physical_row(r).get_datum(0, &decimal);
                            let e = expected_chunk.physical_row(r).get_datum(0, &decimal);
                            let d = direct.physical_row(r).get_datum(root, &decimal);
                            if e == Datum::Null {
                                assert_eq!(d, e);
                            } else {
                                assert_same_decimal(&d, &e);
                            }
                            if e == Datum::Null {
                                assert_eq!(a, e);
                            } else {
                                assert_same_decimal(&a, &e);
                            }
                        }
                    }
                    let one = execution.evaluate(&input, &[0], &mut ctx).unwrap();
                    for root in 0..expressions.len() {
                        assert_same_decimal(&one[root][0], &actual[root][2]);
                    }
                    assert_eq!(*session.warnings.borrow(), *original.warnings.borrow());
                    assert_eq!(
                        execution.evaluate(&input, &[], &mut ctx).unwrap(),
                        vec![Vec::<Datum>::new(); 3]
                    );
                    execution
                        .evaluate_into_chunk(&input, &[], &mut ctx, &mut direct)
                        .unwrap();
                    assert_eq!(direct.num_rows(), 0);
                    assert!(execution.evaluate(&input, &[7], &mut ctx).is_err());
                    assert!(execution
                        .evaluate(&Chunk::new(&[], 0, 0), &[0], &mut ctx)
                        .is_err());
                    assert_eq!(execution.evaluate(&input, &rows, &mut ctx).unwrap(), actual);
                }
                let expected = expressions[0]
                    .eval(&Session::default(), input.physical_row(0))
                    .unwrap();
                let retained = projection
                    .start_execution(&Session::default())
                    .evaluate(&input, &[0], &mut ctx)
                    .unwrap();
                drop(input);
                assert_same_decimal(&retained[0][0], &expected);
            }
        }
    }

    #[test]
    fn shared_kernel_parameter_review_static_source() {
        use crate::constant::ParamMarker;
        use tidb_datatype::{Decimal, FieldTypeCode};
        let decimal = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(-1);
        let arithmetic = |source| {
            let mut marker = Constant::new(Datum::Null, source);
            marker.param_marker = Some(ParamMarker { order: 0 });
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("mul"),
                decimal.clone(),
                vec![
                    Expression::Constant(marker),
                    numeric_literal(Datum::Decimal(Decimal::from_int(2)), &decimal),
                ],
            ))
        };
        let expression = arithmetic(FieldType::new(FieldTypeCode::VarString));
        let compiled = SharedProjection::compile(std::slice::from_ref(&expression));
        for scalar in [false, true] {
            let mut native = ParameterProbe::new(Some(Datum::new_string(b"2.5junk".to_vec())), 4);
            native.session.vectorized_off = scalar;
            let mut input = Chunk::new_empty(&[]);
            input.set_num_virtual_rows(3);
            if scalar {
                for row in 0..3 {
                    expression.eval(&native, input.get_row(row)).unwrap();
                }
            } else {
                let mut output = Chunk::new_empty(std::slice::from_ref(&decimal));
                crate::evaluator::EvaluatorSuite::new(vec![expression.clone()], false)
                    .run(&native, &mut input, &mut output)
                    .unwrap();
            }
            assert_eq!(native.session.warnings.borrow().len(), 3);
            assert!(native
                .session
                .warnings
                .borrow()
                .iter()
                .all(|w| w.0 == if scalar { 1292 } else { 1265 }));
            // Preserve the before-fix source-entry mismatch if admission regresses.
            if let Ok(shared) = &compiled {
                let mut current =
                    ParameterProbe::new(Some(Datum::new_string(b"2.5junk".to_vec())), 4);
                current.session.vectorized_off = scalar;
                shared
                    .start_execution(&current)
                    .evaluate(&input, &[0, 1, 2], &mut EvalContext::default())
                    .unwrap();
                eprintln!("static VarString scalar={scalar}: native warnings={:?}, shared warnings={:?}, shared reads={}", native.session.warnings.borrow(), current.session.warnings.borrow(), current.reads.get());
            }
        }
        assert!(
            compiled.is_err(),
            "static String parameter must retain original admission boundary"
        );
        for source in [
            decimal.clone().with_unsigned(true),
            decimal.clone().with_array(true),
            FieldType::new(FieldTypeCode::LongLong),
        ] {
            assert!(SharedProjection::compile(&[arithmetic(source)]).is_err());
        }
        assert!(SharedProjection::compile(&[arithmetic(decimal.clone())]).is_ok());
    }

    #[test]
    fn shared_kernel_integer_diagnostic_declines_lazy_hidden_sources() {
        use crate::constant::ParamMarker;
        use tidb_datatype::FieldTypeCode;

        let ty = FieldType::new(FieldTypeCode::LongLong);
        let literal = |value| numeric_literal(Datum::Int(value), &ty);
        let plus = |left, right| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                ty.clone(),
                vec![left, right],
            ))
        };
        let hidden_column = |source| {
            let mut column = Column::new(1, ty.clone());
            column.index = 0;
            column.is_hidden = true;
            column.virtual_expr = Some(Box::new(source));
            Expression::Column(column)
        };
        let mut input = Chunk::new_empty(std::slice::from_ref(&ty));
        input.append_int64(0, i64::MAX);
        let mut parameter = Constant::new(Datum::Int(99), ty.clone());
        parameter.param_marker = Some(ParamMarker { order: 0 });
        let mut deferred = Constant::new(Datum::Int(99), ty.clone());
        deferred.deferred_expr = Some(Box::new(literal(19)));
        for (kind, source, shown, reads) in [
            ("parameter", parameter, 17, 1),
            ("deferred", deferred, 19, 0),
        ] {
            let source = plus(Expression::Constant(source), literal(2));
            let expression = plus(hidden_column(source), literal(1));
            let context = ParameterProbe::new(Some(Datum::Int(17)), 4);
            let native = expression.eval(&context, input.get_row(0)).unwrap_err();
            assert_eq!(
                native,
                crate::EvalError::DataOutOfRange {
                    value: "BIGINT",
                    expression: format!("(({shown} + 2) + 1)"),
                }
            );
            let expressions = [plus(literal(2), literal(3)), expression];
            assert_eq!(
                SharedProjection::compile(&expressions).unwrap_err(),
                "integer overflow diagnostic requires execution-time operands",
                "{kind}"
            );
            assert_eq!(context.reads.get(), reads);
        }

        let expression = plus(hidden_column(plus(literal(5), literal(2))), literal(1));
        let error = SharedProjection::compile(&[expression])
            .unwrap()
            .start_execution(&crate::NoColumns)
            .evaluate(&input, &[0], &mut EvalContext::default())
            .unwrap_err();
        assert_eq!(error.code, Some(1690));
        assert_eq!(
            error.message,
            "BIGINT value is out of range in '((5 + 2) + 1)'"
        );
    }

    #[test]
    fn shared_kernel_declines_unsupported_numeric_diagnostic_sources() {
        use crate::constant::ParamMarker;
        use tidb_datatype::{Decimal, FieldTypeCode};

        let real = FieldType::new(FieldTypeCode::Double);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(-1);
        let mut parameter = Constant::new(Datum::Real(99.0), real.clone());
        parameter.param_marker = Some(ParamMarker { order: 0 });
        let mut deferred = Constant::new(Datum::Decimal(Decimal::from_int(99)), decimal.clone());
        deferred.deferred_expr = Some(Box::new(numeric_literal(
            Datum::Decimal(Decimal::from_int(19)),
            &decimal,
        )));
        for (ty, source, stored, multiplier, expected) in [
            (
                real,
                parameter,
                Datum::Real(f64::MAX),
                Datum::Real(2.0),
                "(17 * 2)",
            ),
            (
                decimal,
                deferred,
                Datum::Decimal(Decimal::from_int(100)),
                Datum::Decimal(Decimal::from_literal(&"9".repeat(80))),
                "(19 * ",
            ),
        ] {
            let mut column = Column::new(1, ty.clone());
            column.index = 0;
            column.is_hidden = true;
            column.virtual_expr = Some(Box::new(Expression::Constant(source)));
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("mul"),
                ty.clone(),
                vec![Expression::Column(column), numeric_literal(multiplier, &ty)],
            ));
            let mut input = Chunk::new_empty(std::slice::from_ref(&ty));
            input.append_datum(0, &stored);
            let context = ParameterProbe::new(Some(Datum::Int(17)), 4);
            let error = expression.eval(&context, input.get_row(0)).unwrap_err();
            let crate::EvalError::DataOutOfRange {
                expression: text, ..
            } = &error
            else {
                panic!("unexpected native diagnostic: {error:?}");
            };
            assert!(text.starts_with(expected), "{text}");
            let roots = [numeric_literal(stored, &ty), expression];
            assert_eq!(
                SharedProjection::compile(&roots).unwrap_err(),
                format!(
                    "{} overflow diagnostic requires execution-time operands",
                    if ty.code() == FieldTypeCode::Double {
                        "DOUBLE"
                    } else {
                        "DECIMAL"
                    }
                )
            );
            assert_eq!(
                context.reads.get(),
                usize::from(ty.code() == FieldTypeCode::Double)
            );
        }
    }

    #[test]
    fn shared_kernel_parameter_review_hidden_diagnostic() {
        use crate::constant::ParamMarker;
        use tidb_datatype::{Decimal, FieldTypeCode};
        let ty = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(-1);
        let mut marker = Constant::new(Datum::Null, ty.clone());
        marker.param_marker = Some(ParamMarker { order: 0 });
        let mut column = Column::new(1, ty.clone());
        column.index = 0;
        column.is_hidden = true;
        column.virtual_expr = Some(Box::new(Expression::Constant(marker)));
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("mul"),
            ty.clone(),
            vec![
                Expression::Column(column),
                numeric_literal(Datum::Decimal(Decimal::from_literal(&"9".repeat(80))), &ty),
            ],
        ));
        let shared = SharedProjection::compile(std::slice::from_ref(&expression)).unwrap();
        let suite = crate::evaluator::EvaluatorSuite::new(vec![expression.clone()], false);
        for (vectorized_off, binding) in [
            (false, None),
            (true, None),
            (false, Some(17)),
            (true, Some(23)),
        ] {
            let binding = binding.map(Datum::Int);
            let mut native = ParameterProbe::new(binding.clone(), 4);
            native.session.vectorized_off = vectorized_off;
            let mut current = ParameterProbe::new(binding.clone(), 4);
            current.session.vectorized_off = vectorized_off;
            let mut input = Chunk::new_empty(std::slice::from_ref(&ty));
            input.append_datum(0, &Datum::Decimal(Decimal::from_int(100)));
            let mut output = Chunk::new_empty(std::slice::from_ref(&ty));
            let expected = if vectorized_off {
                expression.eval(&native, input.get_row(0)).unwrap_err()
            } else {
                let crate::evaluator::EvaluatorError::Eval(error) =
                    suite.run(&native, &mut input, &mut output).unwrap_err()
                else {
                    panic!("expected native overflow");
                };
                error
            };
            let actual = shared
                .start_execution(&current)
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap_err();
            eprintln!("hidden diagnostic: native={expected:?}, shared={actual:?}");
            assert_eq!(current.reads.get(), 1);
            if binding.is_some() {
                assert_native_error(actual, expected);
            } else {
                assert_eq!(expected, crate::EvalError::DecimalOverflow);
                assert_eq!(actual.code, Some(1690));
                assert_eq!(actual.message, "DECIMAL value is out of range");
            }
        }
    }

    #[test]
    fn shared_kernel_typed_decimal_parameter_vector_and_if() {
        use crate::constant::ParamMarker;
        use tidb_datatype::{Decimal, FieldTypeCode};
        let ty = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(-1);
        let mut marker = Constant::new(Datum::Decimal(Decimal::from_int(99)), ty.clone());
        marker.param_marker = Some(ParamMarker { order: 0 });
        let arithmetic = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("div"),
            ty.clone(),
            vec![
                Expression::Constant(marker),
                numeric_literal(Datum::Decimal(Decimal::from_int(2)), &ty),
            ],
        ));
        let conditional = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            ty.clone(),
            vec![
                numeric_literal(Datum::Int(1), &FieldType::new(FieldTypeCode::LongLong)),
                arithmetic.clone(),
                numeric_literal(Datum::Decimal(Decimal::from_int(0)), &ty),
            ],
        ));
        for (expression, warning_count) in [(arithmetic, 1), (conditional, 3)] {
            let shared = SharedProjection::compile(std::slice::from_ref(&expression)).unwrap();
            let suite = crate::evaluator::EvaluatorSuite::new(vec![expression.clone()], false);
            for (vectorized_off, rows) in [(false, 1), (false, 3), (true, 3)] {
                let activations = if vectorized_off || warning_count == 3 {
                    rows
                } else {
                    1
                };
                for strict in [false, true] {
                    for value in [
                        Datum::Int(7),
                        Datum::UInt(u64::MAX),
                        Datum::Real(1.25),
                        Datum::Float32(1.25),
                        Datum::Decimal(Decimal::from_scaled_i128(125, 2)),
                        Datum::Null,
                        Datum::new_string(b"2.5junk".to_vec()),
                    ] {
                        let mut expected_ctx = ParameterProbe::new(Some(value.clone()), 4);
                        expected_ctx.session.strict = strict;
                        expected_ctx.session.vectorized_off = vectorized_off;
                        let mut actual_ctx = ParameterProbe::new(Some(value.clone()), 4);
                        actual_ctx.session.strict = strict;
                        actual_ctx.session.vectorized_off = vectorized_off;
                        let mut input = Chunk::new_empty(&[]);
                        input.set_num_virtual_rows(rows);
                        let mut output = Chunk::new_empty(std::slice::from_ref(&ty));
                        let expected = suite.run(&expected_ctx, &mut input, &mut output);
                        let actual = shared.start_execution(&actual_ctx).evaluate(
                            &input,
                            &(0..rows).collect::<Vec<_>>(),
                            &mut EvalContext::default(),
                        );
                        if strict && matches!(value, Datum::String(_)) {
                            let crate::evaluator::EvaluatorError::Eval(
                                crate::EvalError::Conversion(expected),
                            ) = expected.unwrap_err()
                            else {
                                panic!("expected original conversion error")
                            };
                            let expected = expected.to_sql_error();
                            let error = actual.unwrap_err();
                            assert_eq!(error.code, Some(i32::from(expected.code)));
                            assert_eq!(error.message, expected.message);
                        } else {
                            expected.unwrap();
                            let actual = actual.unwrap();
                            let scalar = expression
                                .eval(
                                    &ParameterProbe::new(Some(value.clone()), 4),
                                    input.get_row(0),
                                )
                                .unwrap();
                            let mut encoded = Chunk::new_empty(std::slice::from_ref(&ty));
                            for value in &actual[0] {
                                encoded.append_datum(0, value);
                            }
                            for (i, value) in actual[0].iter().enumerate() {
                                let expected = output.get_row(i).get_datum(0, &ty);
                                if expected.is_null() {
                                    assert!(value.is_null());
                                } else {
                                    assert_same_decimal(value, &scalar);
                                    assert_same_decimal(
                                        &encoded.get_row(i).get_datum(0, &ty),
                                        &expected,
                                    );
                                }
                            }
                            let expected_count = if matches!(value, Datum::String(_)) {
                                activations
                            } else {
                                0
                            };
                            assert_eq!(
                                expected_ctx.session.warnings.borrow().len(),
                                expected_count
                            );
                        }
                        assert_eq!(
                            actual_ctx.reads.get(),
                            if strict && matches!(value, Datum::String(_)) {
                                1
                            } else {
                                activations
                            }
                        );
                        assert_eq!(
                            *actual_ctx.session.warnings.borrow(),
                            *expected_ctx.session.warnings.borrow()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn shared_kernel_typed_decimal_parameter_demand_and_current_diagnostics() {
        use crate::constant::ParamMarker;
        use tidb_datatype::{Decimal, FieldTypeCode};
        let ty = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(-1);
        let literal = |n| numeric_literal(Datum::Decimal(Decimal::from_int(n)), &ty);
        let mut c = Constant::new(Datum::Decimal(Decimal::from_int(99)), ty.clone());
        c.param_marker = Some(ParamMarker { order: 0 });
        let parameter = Expression::Constant(c.clone());
        let binary = |name, a, b| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                ty.clone(),
                vec![a, b],
            ))
        };
        let choose = |condition, branch| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                ty.clone(),
                vec![
                    numeric_literal(
                        Datum::Int(condition),
                        &FieldType::new(FieldTypeCode::LongLong),
                    ),
                    branch,
                    literal(0),
                ],
            ))
        };
        assert!(SharedProjection::compile(std::slice::from_ref(&parameter)).is_err());
        assert!(SharedProjection::compile(&[choose(1, parameter.clone())]).is_err());
        let mut unresolved = c.clone();
        unresolved.ret_type = None;
        assert!(SharedProjection::compile(&[binary(
            "plus",
            Expression::Constant(unresolved),
            literal(0)
        )])
        .is_err());
        c.deferred_expr = Some(Box::new(literal(2)));
        assert!(
            SharedProjection::compile(&[binary("plus", Expression::Constant(c), literal(0))])
                .is_err()
        );
        let sum = binary("plus", parameter.clone(), literal(0));
        let mut column = Column::new(1, ty.clone());
        column.index = 0;
        column.orig_name = "t.amount".into();
        let product = binary("mul", sum.clone(), Expression::Column(column.clone()));
        let generic = binary("mul", choose(1, sum.clone()), Expression::Column(column));
        let unbound = ParameterProbe::new(None, 4);
        let dead = SharedProjection::compile(&[choose(0, product.clone())]).unwrap();
        let plain = SharedProjection::compile(std::slice::from_ref(&product)).unwrap();
        assert_eq!(unbound.reads.get(), 0);
        let empty = Chunk::new_empty(std::slice::from_ref(&ty));
        assert!(plain
            .start_execution(&unbound)
            .evaluate(&empty, &[], &mut EvalContext::default())
            .unwrap()[0]
            .is_empty());
        let mut input = Chunk::new_empty(std::slice::from_ref(&ty));
        for _ in 0..3 {
            input.append_datum(0, &Datum::Decimal(Decimal::from_int(100)));
        }
        dead.start_execution(&unbound)
            .evaluate(&input, &[2, 0, 2], &mut EvalContext::default())
            .unwrap();
        assert_eq!(unbound.reads.get(), 0);
        let error = plain
            .start_execution(&unbound)
            .evaluate(&input, &[0], &mut EvalContext::default())
            .unwrap_err();
        assert_eq!(error.code, Some(1105));
        assert_eq!(error.message, "unbound prepared parameter");
        for (expression, detailed) in [(product, true), (generic, false)] {
            let shared = SharedProjection::compile(std::slice::from_ref(&expression)).unwrap();
            let suite = crate::evaluator::EvaluatorSuite::new(vec![expression.clone()], false);
            for value in [
                Datum::Decimal(Decimal::from_literal(&"9".repeat(80))),
                Datum::new_string(format!("{}junk", "9".repeat(80)).into_bytes()),
            ] {
                let native = ParameterProbe::new(Some(value.clone()), 4);
                let current = ParameterProbe::new(Some(value), 4);
                let mut output = Chunk::new_empty(std::slice::from_ref(&ty));
                let crate::evaluator::EvaluatorError::Eval(expected) =
                    suite.run(&native, &mut input, &mut output).unwrap_err()
                else {
                    panic!("expected numeric error")
                };
                let got = shared
                    .start_execution(&current)
                    .evaluate(&input, &[0, 1, 2], &mut EvalContext::default())
                    .unwrap_err();
                if detailed {
                    assert_native_error(got, expected);
                } else {
                    assert_eq!(expected, crate::EvalError::DecimalOverflow);
                    assert_eq!(got.code, Some(1690));
                    assert_eq!(got.message, "DECIMAL value is out of range");
                }
                assert_eq!(
                    *native.session.warnings.borrow(),
                    *current.session.warnings.borrow()
                );
            }
            let mut current = ParameterProbe::new(Some(Datum::Int(2)), 8);
            let old = shared
                .start_execution(&current)
                .evaluate(&input, &[2, 0, 2], &mut EvalContext::default())
                .unwrap();
            // Both contexts and physical input can change; returned values own their data.
            current.value = Some(Datum::Null);
            let nulls = shared
                .start_execution(&current)
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap();
            assert!(nulls[0][0].is_null());
            drop(current);
            for value in &old[0] {
                assert_same_decimal(value, &Datum::Decimal(Decimal::from_int(200)));
            }
        }
    }

    #[test]
    fn shared_kernel_shared_typed_decimal_primitive_padding() {
        use tidb_datatype::{Decimal, FieldTypeCode};
        let ty = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(3);
        let mut input = Chunk::new_empty(&[]);
        input.set_num_virtual_rows(1);
        let value = Constant::new(Datum::Decimal(Decimal::from_int(7)), ty.clone())
            .eval_typed_as_on_row(
                &crate::NoColumns,
                input.get_row(0),
                tidb_datatype::EvalType::Decimal,
            )
            .unwrap();
        assert_same_decimal(&value, &Datum::Decimal(Decimal::from_literal("7.000")));
        let wide = Constant::new(Datum::Decimal(Decimal::from_literal(&"9".repeat(81))), ty);
        assert!(matches!(
            wide.eval_typed_as_on_row(
                &crate::NoColumns,
                input.get_row(0),
                tidb_datatype::EvalType::Decimal
            ),
            Err(crate::EvalError::Conversion(_))
        ));
    }

    #[test]
    fn shared_kernel_typed_decimal_parameter_runtime_carriers() {
        use crate::constant::ParamMarker;
        use tidb_datatype::{Decimal, FieldTypeCode};
        let ty = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(-1);
        let mut c = Constant::new(Datum::Decimal(Decimal::from_int(99)), ty.clone());
        c.param_marker = Some(ParamMarker { order: 0 });
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            ty.clone(),
            vec![
                Expression::Constant(c),
                numeric_literal(Datum::Decimal(Decimal::from_int(0)), &ty),
            ],
        ));
        let program = SharedExpression::compile(&expression).unwrap();
        let mut input = Chunk::new_empty(&[]);
        input.set_num_virtual_rows(1);
        for value in [
            Datum::MinNotNull,
            Datum::MaxValue,
            Datum::new_bytes(vec![255, 0]),
            Datum::Real(f64::NAN),
            Datum::Real(f64::INFINITY),
            Datum::new_string(vec![255, 0]),
            Datum::new_string(b"bad".to_vec()),
        ] {
            for mode in [InvocationMode::Scalar, InvocationMode::Vector] {
                let native = ParameterProbe::new(Some(value.clone()), 4);
                let shared = ParameterProbe::new(Some(value.clone()), 4);
                let expected = expression.eval(&native, input.get_row(0));
                let actual = program.start_execution(&shared).evaluate(
                    &input,
                    &[0],
                    &mut EvalContext::default(),
                    mode,
                );
                match expected {
                    Ok(expected) => {
                        let actual = actual.unwrap();
                        if expected.is_null() {
                            assert!(actual[0].is_null());
                        } else {
                            assert_same_decimal(&actual[0], &expected);
                        }
                    }
                    Err(expected) => assert_native_error(actual.unwrap_err(), expected),
                }
                assert_eq!(
                    *native.session.warnings.borrow(),
                    *shared.session.warnings.borrow()
                );
            }
        }
    }

    // Synthetic column-slot fixture, distinct from the product parameter instruction.
    struct ParameterProbe {
        value: Option<Datum>,
        reads: std::cell::Cell<usize>,
        precision: u32,
        session: Session,
    }
    impl ParameterProbe {
        fn new(value: Option<Datum>, precision: u32) -> Self {
            Self {
                value,
                reads: std::cell::Cell::new(0),
                precision,
                session: Session::default(),
            }
        }
    }
    impl crate::context::Columns for ParameterProbe {
        fn enable_vectorized_expression(&self) -> bool {
            !self.session.vectorized_off
        }
        fn truncate_level(&self) -> crate::ErrorLevel {
            crate::context::Columns::truncate_level(&self.session)
        }
        fn append_warning(&self, code: u16, message: &str) {
            crate::context::Columns::append_warning(&self.session, code, message)
        }
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, order: usize) -> Result<Datum, crate::EvalError> {
            assert_eq!(order, 0);
            self.reads.set(self.reads.get() + 1);
            self.value
                .clone()
                .ok_or(crate::EvalError::Unsupported("unbound prepared parameter"))
        }
        fn div_precision_increment(&self) -> u32 {
            self.precision
        }
    }
    impl InputColumns for ParameterProbe {
        fn div_precision_increment(&self) -> u32 {
            self.precision
        }
        fn read<'a>(
            &'a self,
            schema: &'a [tipb::FieldType],
            offset: usize,
            rows: &'a [usize],
            _: &mut EvalContext,
        ) -> InputResult<'a> {
            let value =
                crate::context::Columns::param_value(self, offset).map_err(native_policy_error)?;
            let value = match value {
                Datum::Decimal(value) => Some(value),
                Datum::Null => None,
                _ => {
                    return Err(tidb_query_expr::evaluation_error::EvaluateError::Other(
                        "prototype slot accepts Decimal/NULL only".into(),
                    )
                    .into())
                }
            };
            Ok(RpnStackNode::ExpressionVector {
                value: ExpressionVector::Decimal(vec![value.map(Into::into); rows.len()]),
                field_type: &schema[offset],
            })
        }
    }
    fn parameter_probe_eval(
        program: &SharedExpression,
        input: &ParameterProbe,
        rows: &[usize],
        mode: InvocationMode,
    ) -> Result<Vec<Datum>, SharedEvaluationError> {
        // Match the native adapter boundary: the RPN engine requires a nonempty batch.
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let result = program
            .program
            .eval_input_mode(
                &mut EvalContext::default(),
                &program.schema,
                input,
                rows,
                rows.len(),
                mode,
            )
            .map_err(SharedEvaluationError::from)?;
        native_output(result, rows.len()).map_err(SharedEvaluationError::from)
    }

    #[test]
    fn shared_kernel_parameter_contract_native_forms_and_rejection() {
        use crate::constant::ParamMarker;
        use tidb_datatype::{Decimal, EvalType, FieldTypeCode};
        let mut empty = Chunk::new_empty(&[]);
        empty.set_num_virtual_rows(1);
        let ty = FieldType::new(FieldTypeCode::NewDecimal).with_decimal(3);
        let mut marker = Constant::new(Datum::Decimal(Decimal::from_int(99)), ty.clone());
        marker.param_marker = Some(ParamMarker { order: 0 });
        let unbound = ParameterProbe::new(None, 4);
        for deferred in [false, true] {
            let mut constant = marker.clone();
            if deferred {
                // A deliberately unbound deferred expression must not run when a marker exists.
                let mut other = marker.clone();
                other.param_marker = Some(ParamMarker { order: 1 });
                constant.deferred_expr = Some(Box::new(Expression::Constant(other)));
            }
            assert!(SharedExpression::compile(&Expression::Constant(constant.clone())).is_err());
            assert_eq!(unbound.reads.get(), 0);
            for value in [
                Datum::Int(7),
                Datum::Null,
                Datum::new_string(b"2.5".to_vec()),
            ] {
                let ctx = ParameterProbe::new(Some(value.clone()), 4);
                assert_eq!(
                    constant.get_type(&ctx).unwrap().eval_type(),
                    tidb_datatype::infer_param_type_from_datum(&value).eval_type()
                );
                let raw = constant.eval_in(&ctx).unwrap();
                let typed = constant
                    .eval_typed_as_on_row(&ctx, empty.get_row(0), EvalType::Decimal)
                    .unwrap();
                if value.is_null() {
                    assert!(raw.is_null() && typed.is_null());
                } else {
                    let expected = if matches!(value, Datum::Int(_)) {
                        "7"
                    } else {
                        "2.5"
                    };
                    assert_same_decimal(&typed, &Datum::Decimal(Decimal::from_literal(expected)));
                    if deferred {
                        assert!(matches!(raw, Datum::Decimal(_)));
                    } else {
                        assert_eq!(raw, value);
                    }
                }
                assert_eq!(constant.value, Datum::Decimal(Decimal::from_int(99)));
            }
        }
    }

    #[test]
    fn shared_kernel_parameter_contract_demanded_slot_and_missing_diagnostics() {
        use crate::constant::ParamMarker;
        use tidb_datatype::{Decimal, FieldTypeCode};
        let ty = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(-1);
        let literal = |n| numeric_literal(Datum::Decimal(Decimal::from_int(n)), &ty);
        let mut marker = Constant::new(Datum::Decimal(Decimal::from_int(99)), ty.clone());
        marker.param_marker = Some(ParamMarker { order: 0 });
        let parameter = Expression::Constant(marker);
        let mut column = Column::new(1, ty.clone());
        column.index = 0;
        let slot = Expression::Column(column);
        let arithmetic = |op, arg, right| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(op),
                ty.clone(),
                vec![arg, literal(right)],
            ))
        };
        let choose = |condition, branch| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                ty.clone(),
                vec![
                    numeric_literal(
                        Datum::Int(condition),
                        &FieldType::new(FieldTypeCode::LongLong),
                    ),
                    branch,
                    literal(9),
                ],
            ))
        };
        let unbound = ParameterProbe::new(None, 4);
        let native = choose(1, arithmetic("div", parameter.clone(), 3));
        assert!(SharedExpression::compile(&native).is_ok());
        let program =
            SharedExpression::compile(&choose(1, arithmetic("div", slot.clone(), 3))).unwrap();
        assert_eq!(unbound.reads.get(), 0);
        let mut retained = Vec::new();
        for precision in [4, 8] {
            for value in [
                Datum::Decimal(Decimal::from_int(1)),
                Datum::Null,
                Datum::Decimal(Decimal::from_scaled_i128(125, 2)),
            ] {
                let ctx = ParameterProbe::new(Some(value), precision);
                let expected = crate::eval_expression_once(&native, &ctx).unwrap();
                for (mode, rows) in [
                    (InvocationMode::Scalar, vec![0]),
                    (InvocationMode::Vector, vec![0]),
                    (InvocationMode::Vector, vec![2, 0, 2]),
                ] {
                    let got = parameter_probe_eval(&program, &ctx, &rows, mode).unwrap();
                    for value in &got {
                        if expected.is_null() {
                            assert!(value.is_null());
                        } else {
                            assert_same_decimal(value, &expected);
                        }
                    }
                    retained.push((got, expected.clone()));
                }
            }
        }
        let dead_native = choose(0, arithmetic("div", parameter.clone(), 3));
        let dead =
            SharedExpression::compile(&choose(0, arithmetic("div", slot.clone(), 3))).unwrap();
        assert_same_decimal(
            &crate::eval_expression_once(&dead_native, &unbound).unwrap(),
            &Datum::Decimal(Decimal::from_int(9)),
        );
        parameter_probe_eval(&dead, &unbound, &[0, 1], InvocationMode::Vector).unwrap();
        assert!(
            parameter_probe_eval(&program, &unbound, &[], InvocationMode::Vector)
                .unwrap()
                .is_empty()
        );
        parameter_probe_eval(&dead, &unbound, &[0], InvocationMode::Scalar).unwrap();
        let mut empty = Chunk::new_empty(&[]);
        let mut output = Chunk::new_empty(std::slice::from_ref(&ty));
        crate::evaluator::EvaluatorSuite::new(vec![native.clone()], false)
            .run(&unbound, &mut empty, &mut output)
            .unwrap();
        assert!(program
            .start_execution(&unbound)
            .evaluate(
                &empty,
                &[],
                &mut EvalContext::default(),
                InvocationMode::Vector
            )
            .unwrap()
            .is_empty());
        assert_eq!(unbound.reads.get(), 0);
        assert_eq!(
            crate::eval_expression_once(&native, &unbound).unwrap_err(),
            crate::EvalError::Unsupported("unbound prepared parameter")
        );
        let error =
            parameter_probe_eval(&program, &unbound, &[0], InvocationMode::Scalar).unwrap_err();
        assert_eq!(error.code, Some(1105));
        assert_eq!(error.message, "unbound prepared parameter");
        let integer = ParameterProbe::new(Some(Datum::Int(7)), 4);
        assert!(crate::eval_expression_once(&native, &integer).is_ok());
        assert!(parameter_probe_eval(&program, &integer, &[0], InvocationMode::Scalar).is_err());
        let overflow_native = arithmetic("mul", parameter, 10);
        let overflow = SharedExpression::compile(&arithmetic("mul", slot, 10)).unwrap();
        let huge = ParameterProbe::new(
            Some(Datum::Decimal(Decimal::from_literal(&"9".repeat(81)))),
            4,
        );
        let expected = crate::eval_expression_once(&overflow_native, &huge).unwrap_err();
        let got = parameter_probe_eval(&overflow, &huge, &[0], InvocationMode::Scalar).unwrap_err();
        let crate::EvalError::DataOutOfRange { expression, .. } = expected else {
            panic!("expected overflow")
        };
        assert!(expression.contains(&"9".repeat(81)));
        assert_eq!(got.code, Some(1690));
        assert!(
            !got.message.contains(&"9".repeat(81)),
            "frozen slot text cannot report the current parameter"
        );
        let fresh = ParameterProbe::new(Some(Datum::Decimal(Decimal::from_int(2))), 8);
        assert_same_decimal(
            &parameter_probe_eval(&overflow, &fresh, &[0], InvocationMode::Scalar).unwrap()[0],
            &Datum::Decimal(Decimal::from_int(20)),
        );
        assert!(parameter_probe_eval(&program, &fresh, &[0], InvocationMode::Scalar).is_ok());
        for (values, expected) in retained {
            for value in values {
                if expected.is_null() {
                    assert!(value.is_null());
                } else {
                    assert_same_decimal(&value, &expected);
                }
            }
        }
    }

    #[test]
    fn shared_kernel_decimal_column_metadata_and_error_recovery() {
        use tidb_datatype::{Decimal, FieldTypeCode};
        struct Policy(Session, u32);
        impl crate::context::Columns for Policy {
            fn div_precision_increment(&self) -> u32 {
                self.1
            }
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn division_by_zero_level(&self) -> crate::ErrorLevel {
                if self.0.strict {
                    crate::ErrorLevel::Error
                } else {
                    crate::ErrorLevel::Warn
                }
            }
            fn enable_vectorized_expression(&self) -> bool {
                !self.0.vectorized_off
            }
            fn append_warning(&self, code: u16, text: &str) {
                self.0.warnings.borrow_mut().push((code, text.into()));
            }
        }
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(-1)
            .with_added_raw_flags(1u64 << 40)
            .with_charset_name("binary")
            .with_collation_name("binary");
        let col = |index, ty: &FieldType| {
            let mut c = Column::new(index + 1, ty.clone());
            c.index = index;
            Expression::Column(c)
        };
        let unsigned_col = col(0, &decimal.clone().with_unsigned(true));
        let unsigned = SharedExpression::compile(&unsigned_col).unwrap();
        assert_eq!(unsigned.columns[0].1, decimal.clone().with_unsigned(true));
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("div"),
            decimal.clone(),
            vec![col(0, &decimal), col(1, &decimal)],
        ));
        let arithmetic = |op, left, right| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(op),
                decimal.clone(),
                vec![left, right],
            ))
        };
        let expression = arithmetic(
            "div",
            arithmetic(
                "mul",
                expression,
                numeric_literal(Datum::Decimal(Decimal::from_int(1)), &decimal),
            ),
            numeric_literal(Datum::Decimal(Decimal::from_int(100)), &decimal),
        );
        let shared = SharedProjection::compile(std::slice::from_ref(&expression)).unwrap();
        assert!(SharedExpression::compile(&col(0, &decimal.clone().with_array(true))).is_err());
        assert_eq!(
            shared.expressions[0].columns,
            vec![(0, decimal.clone()), (1, decimal.clone())]
        );
        // Conflicting declarations of the same physical cell must not coalesce.
        for other in [
            decimal.clone().with_decimal(2),
            decimal.clone().with_flen(40),
            decimal.clone().with_raw_flags(0),
            decimal.clone().with_charset_name("utf8mb4"),
        ] {
            let conflict = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("plus"),
                decimal.clone(),
                vec![col(0, &decimal), col(0, &other)],
            ));
            assert_eq!(
                SharedExpression::compile(&conflict).unwrap_err(),
                "conflicting column types"
            );
        }
        for flag in [0, 1] {
            let lazy = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                decimal.clone(),
                vec![
                    numeric_literal(Datum::Int(flag), &FieldType::new(FieldTypeCode::LongLong)),
                    col(7, &decimal),
                    numeric_literal(Datum::Decimal(Decimal::from_int(9)), &decimal),
                ],
            ));
            let mut empty = Chunk::new(&[], 1, 1);
            empty.set_num_virtual_rows(1);
            let actual = SharedExpression::compile(&lazy).unwrap().evaluate(
                &empty,
                &[0],
                &mut EvalContext::default(),
            );
            if flag == 0 {
                assert_same_decimal(
                    &actual.unwrap()[0],
                    &lazy
                        .eval(&Session::default(), empty.physical_row(0))
                        .unwrap(),
                );
            } else {
                assert!(actual.is_err());
            }
        }
        let mut ctx = EvalContext::default();
        for vectorized_off in [false, true] {
            for (strict, divisor, precision) in
                [(true, 0, 4), (false, 0, 4), (true, 7, 4), (false, 7, 8)]
            {
                let policy = Policy(
                    Session {
                        strict,
                        vectorized_off,
                        ..Session::default()
                    },
                    precision,
                );
                let original = Policy(
                    Session {
                        strict,
                        vectorized_off,
                        ..Session::default()
                    },
                    precision,
                );
                let mut input = Chunk::new(&[decimal.clone(), decimal.clone()], 3, 3);
                for n in [8, -3, 9] {
                    input.append_datum(0, &Datum::Decimal(Decimal::from_int(n)));
                    input.append_datum(1, &Datum::Decimal(Decimal::from_int(divisor)));
                }
                let unsigned_value = unsigned.evaluate(&input, &[0], &mut ctx).unwrap();
                assert_same_decimal(
                    &unsigned_value[0],
                    &unsigned_col.eval(&original, input.physical_row(0)).unwrap(),
                );
                let rows = [2, 0, 2];
                let expected = rows
                    .iter()
                    .map(|r| expression.eval(&original, input.physical_row(*r)))
                    .collect::<Result<Vec<_>, _>>();
                let actual = shared
                    .start_execution(&policy)
                    .evaluate(&input, &rows, &mut ctx);
                match expected {
                    Err(crate::EvalError::DivisionByZero) => {
                        let error = actual.unwrap_err();
                        assert_eq!(error.code, Some(1365));
                        assert_eq!(error.message, "Division by 0");
                    }
                    Ok(expected) => {
                        let actual = actual.unwrap();
                        if divisor == 7 && precision == 4 {
                            let Datum::Decimal(v) = &actual[0][1] else {
                                unreachable!()
                            };
                            assert_eq!(v.to_string(), "0.01142857");
                        }
                        for (a, e) in actual[0].iter().zip(&expected) {
                            if e == &Datum::Null {
                                assert_eq!(a, e);
                            } else {
                                assert_same_decimal(a, e);
                            }
                        }
                    }
                    other => panic!("unexpected result {other:?}"),
                }
                assert_eq!(*policy.0.warnings.borrow(), *original.0.warnings.borrow());
            }
            ctx = EvalContext::default();
        }
    }

    #[test]
    fn shared_kernel_full_real_columns_and_wide_null_demand() {
        let real = FieldType::new(tidb_datatype::FieldTypeCode::Double);
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let mut input = Chunk::new(&[real.clone()], 8, 8);
        for row in 0..8 {
            if row == 2 {
                input.append_null(0);
            } else {
                input.append_float64(0, f64::from_bits(0xfff8000000000042));
            }
        }
        let mut column = Column::new(1, real.clone());
        column.index = 0;
        let expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            real.clone(),
            vec![
                numeric_literal(Datum::Int(1), &integer),
                Expression::Column(column),
                numeric_literal(Datum::Real(0.0), &real),
            ],
        ));
        let result = SharedExpression::compile(&expr)
            .unwrap()
            .evaluate(&input, &[7, 2, 7], &mut EvalContext::default())
            .unwrap();
        assert_eq!(result[1], Datum::Null);
        for i in [0, 2] {
            let Datum::Real(value) = result[i] else {
                panic!("full Real required");
            };
            assert_eq!(value.to_bits(), 0xfff8000000000042);
        }
        for op in ["plus", "minus", "mul", "div"] {
            let expr = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(op),
                real.clone(),
                vec![
                    numeric_literal(Datum::Null, &real),
                    numeric_binary("mul", &real, Datum::Real(f64::MAX), Datum::Real(2.0)),
                ],
            ));
            let baseline = expr.eval(&crate::context::NoColumns, input.physical_row(0));
            let compiled = SharedExpression::compile(&expr).unwrap();
            let scalar = compiled.evaluate_mode(
                &input,
                &[0],
                &mut EvalContext::default(),
                InvocationMode::Scalar,
            );
            if op == "plus" {
                assert_native_error(scalar.unwrap_err(), baseline.unwrap_err());
            } else {
                assert_eq!(baseline, Ok(Datum::Null));
                assert_eq!(scalar.unwrap(), vec![Datum::Null]);
            }
            assert_eq!(
                compiled
                    .evaluate_mode(
                        &input,
                        &[0],
                        &mut EvalContext::default(),
                        InvocationMode::Vector
                    )
                    .unwrap_err()
                    .code,
                Some(1690)
            );
        }
    }

    #[test]
    fn shared_kernel_decimal_arithmetic_exact_parts_and_policy() {
        use tidb_datatype::Decimal;
        let decimal = FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal);
        let mut input = Chunk::new(&[], 8, 8);
        input.set_num_virtual_rows(8);
        let d = |s: &str| Datum::Decimal(Decimal::from_signed_literal(s));
        let p = format!("81{}", "0".repeat(79));
        let d8 = Decimal::from_int(8)
            .true_div(&Decimal::from_int(7), 7)
            .unwrap();
        let d9 = Decimal::from_int(9)
            .true_div(&Decimal::from_int(7), 7)
            .unwrap();
        let cases = vec![
            ("plus", d(&format!("{}.5", "9".repeat(40))), d("0.25")),
            (
                "plus",
                Datum::Decimal(Decimal::from_literal("1.50").with_declared_shape(50, 2)),
                d("0.00"),
            ),
            ("plus", Datum::Decimal(d8), Datum::Decimal(d9)),
            (
                "minus",
                d(&format!("1{}", "0".repeat(81))),
                d(&format!("1{}", "0".repeat(81))),
            ),
            ("plus", d(&p), d(&p)),
            ("minus", d(&p), d(&format!("-{p}"))),
            ("mul", d(&p), d(&p)),
            ("div", d(&p), d("0.1")),
            ("plus", d(&format!("999999999{}", "0".repeat(72))), d("1")),
            ("mul", d("1.234567890123456789"), d("9.876543210987654321")),
            (
                "div",
                d("10000000000000000000.000000000000000000000000000000"),
                d("3.000000000000000000000000000000"),
            ),
            ("div", Datum::Null, d("3.000000000000000000000000000000")),
        ];
        for (case, (op, left, right)) in cases.into_iter().enumerate() {
            let expression = numeric_binary(op, &decimal, left, right);
            let program = SharedExpression::compile(&expression).unwrap();
            for strict in [false, true] {
                for mode in [InvocationMode::Scalar, InvocationMode::Vector] {
                    let original = Session {
                        strict,
                        ..Session::default()
                    };
                    let session = Session {
                        strict,
                        ..Session::default()
                    };
                    let rows = if mode == InvocationMode::Scalar {
                        &[7][..]
                    } else {
                        &[7, 2, 7][..]
                    };
                    let baseline = expression.eval(&original, input.physical_row(7));
                    let actual = program.start_execution(&session).evaluate(
                        &input,
                        rows,
                        &mut EvalContext::default(),
                        mode,
                    );
                    match baseline {
                        Ok(expected) => {
                            let values = actual.unwrap();
                            for value in values {
                                if expected == Datum::Null {
                                    assert_eq!(value, Datum::Null);
                                } else {
                                    assert_same_decimal(&value, &expected);
                                }
                                if case == 0 {
                                    assert_eq!(value, d(&format!("{}.75", "9".repeat(40))));
                                }
                                if case == 2 {
                                    let Datum::Decimal(v) = value else {
                                        unreachable!()
                                    };
                                    assert_eq!(v.div_round(2, 14).to_string(), "1.21428571350000");
                                }
                            }
                            let expected_warnings = (0..rows.len())
                                .flat_map(|_| original.warnings.borrow().clone())
                                .collect::<Vec<_>>();
                            assert_eq!(
                                *session.warnings.borrow(),
                                expected_warnings,
                                "case {case}"
                            );
                        }
                        Err(error) => assert_native_error(actual.unwrap_err(), error),
                    }
                }
            }
        }
    }

    #[test]
    fn shared_kernel_projection_session_vector_switch() {
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let constant = |v| Expression::Constant(Constant::new(v, integer.clone()));
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            integer.clone(),
            vec![
                constant(Datum::Null),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("mul"),
                    integer.clone(),
                    vec![constant(Datum::Int(i64::MAX)), constant(Datum::Int(2))],
                )),
            ],
        ));
        for vectorized_off in [false, true] {
            let context = Session {
                vectorized_off,
                ..Session::default()
            };
            let mut input = Chunk::new(&[], 2, 2);
            input.set_num_virtual_rows(2);
            let mut output = Chunk::new(&[integer.clone()], 2, 2);
            let original = crate::evaluator::EvaluatorSuite::new(vec![expression.clone()], false)
                .run(&context, &mut input, &mut output);
            assert_eq!(original.is_ok(), vectorized_off);
            if vectorized_off {
                assert!(output.get_row(0).is_null(0));
            }
            let actual = SharedProjection::compile(&[expression.clone()])
                .unwrap()
                .start_execution(&context)
                .evaluate(&input, &[0, 1], &mut EvalContext::default());
            assert_eq!(actual.is_ok(), vectorized_off, "{actual:?}");
            if vectorized_off {
                assert_eq!(actual.unwrap(), vec![vec![Datum::Null; 2]]);
            }
        }
        // Disabling root vectorization must preserve column-major RAND traversal.
        let rand = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("rand"),
            FieldType::new(tidb_datatype::FieldTypeCode::Double),
            vec![],
        ));
        let mut input = Chunk::new(&[], 2, 2);
        input.set_num_virtual_rows(2);
        assert_eq!(
            SharedProjection::compile(&[rand.clone(), rand])
                .unwrap()
                .start_execution(&Session {
                    vectorized_off: true,
                    ..Session::default()
                })
                .evaluate(&input, &[0, 1], &mut EvalContext::default())
                .unwrap(),
            vec![
                vec![Datum::Real(0.125), Datum::Real(0.25)],
                vec![Datum::Real(0.375), Datum::Real(0.5)]
            ]
        );
    }

    #[test]
    fn shared_kernel_variable_storage_admission() {
        use crate::context::Columns;
        let text = |s: &str| {
            Expression::Constant(Constant::new(
                Datum::new_string(s.as_bytes()),
                native_type(),
            ))
        };
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("setvar"),
            native_type(),
            vec![
                text("copy"),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("getvar_string"),
                    native_type(),
                    vec![text("v")],
                )),
            ],
        ));
        let shared = SharedProjection::compile(&[expression.clone()]).unwrap();
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        for value in [
            Datum::new_string(b"abc"),
            Datum::new_bytes(b"abc"),
            Datum::new_collation_string(b"abc", tidb_datatype::Collation::Binary),
        ] {
            let baseline = Session::default();
            baseline.set_uservar("v", value.clone());
            expression.eval(&baseline, input.physical_row(0)).unwrap();
            assert_eq!(baseline.get_uservar("copy"), Some(value.clone()));
            let session = Session::default();
            session.set_uservar("v", value.clone());
            let result = shared.start_execution(&session).evaluate(
                &input,
                &[0],
                &mut EvalContext::default(),
            );
            if value == Datum::new_string(b"abc") {
                result.unwrap();
                assert_eq!(session.get_uservar("copy"), Some(value));
            } else {
                assert!(
                    result.is_err(),
                    "unsupported stored carrier must be rejected: {value:?}"
                );
                assert_eq!(session.get_uservar("copy"), None);
            }
        }
    }

    #[test]
    fn shared_kernel_setvar_literal_storage_admission() {
        use crate::context::Columns;
        let set = |name: &str, value| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("setvar"),
                native_type(),
                vec![
                    numeric_literal(Datum::new_string(name.as_bytes()), &native_type()),
                    value,
                ],
            ))
        };
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        let mut wrongly_admitted = Vec::new();
        for value in [
            Datum::new_string(b"abc"),
            Datum::Null,
            Datum::new_bytes(b"abc"),
            Datum::new_collation_string(b"abc", tidb_datatype::Collation::Binary),
        ] {
            for nested in [false, true] {
                let literal = numeric_literal(value.clone(), &native_type());
                let expression = set(
                    "copy",
                    if nested {
                        set("inner", literal)
                    } else {
                        literal
                    },
                );
                let baseline = Session::default();
                expression.eval(&baseline, input.physical_row(0)).unwrap();
                let expected = if value.is_null() {
                    None
                } else {
                    Some(value.clone())
                };
                assert_eq!(baseline.get_uservar("copy"), expected);
                if nested {
                    assert_eq!(baseline.get_uservar("inner"), expected);
                }
                let conditional = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("if"),
                    native_type(),
                    vec![
                        numeric_literal(
                            Datum::Int(1),
                            &FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                        ),
                        numeric_literal(value.clone(), &native_type()),
                        numeric_literal(Datum::Null, &native_type()),
                    ],
                ));
                assert!(SharedProjection::compile(&[set("copy", conditional)]).is_err());
                let compiled = SharedProjection::compile(&[expression]);
                if matches!(&value, Datum::Null) || value == Datum::new_string(b"abc") {
                    let session = Session::default();
                    compiled
                        .unwrap()
                        .start_execution(&session)
                        .evaluate(&input, &[0], &mut EvalContext::default())
                        .unwrap();
                    assert_eq!(session.get_uservar("copy"), expected);
                    if nested {
                        assert_eq!(session.get_uservar("inner"), expected);
                    }
                } else if compiled.is_ok() {
                    wrongly_admitted.push((value.clone(), nested));
                }
            }
        }
        assert!(
            wrongly_admitted.is_empty(),
            "carrier-losing SETVAR literals admitted: {wrongly_admitted:?}"
        );
    }

    #[test]
    fn shared_kernel_variables_follow_rows_and_skip_null_assignment() {
        let text = |s: &str| {
            Expression::Constant(Constant::new(
                Datum::new_string(s.as_bytes().to_vec()),
                native_type(),
            ))
        };
        let mut column = Column::new(1, native_type());
        column.index = 0;
        let roots = vec![
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("setvar"),
                native_type(),
                vec![text("v"), Expression::Column(column)],
            )),
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("getvar_string"),
                native_type(),
                vec![text("v")],
            )),
        ];
        let mut input = Chunk::new(&[native_type()], 3, 3);
        input.append_string(0, "a");
        input.append_null(0);
        input.append_string(0, "b");
        let mut output = Chunk::new(&[native_type(), native_type()], 3, 3);
        let suite = crate::evaluator::EvaluatorSuite::new(roots.clone(), false);
        assert!(!suite.vectorizable());
        suite
            .run(&Session::default(), &mut input, &mut output)
            .unwrap();
        let expected = vec![
            vec![
                Datum::new_string(b"a".to_vec()),
                Datum::Null,
                Datum::new_string(b"b".to_vec()),
            ],
            vec![
                Datum::new_string(b"a".to_vec()),
                Datum::new_string(b"a".to_vec()),
                Datum::new_string(b"b".to_vec()),
            ],
        ];
        for (c, values) in expected.iter().enumerate() {
            for (r, value) in values.iter().enumerate() {
                let actual = if output.column(c).is_null(r) {
                    Datum::Null
                } else {
                    Datum::new_string(output.column(c).get_bytes(r).to_vec())
                };
                assert_eq!(actual, *value);
            }
        }
        let shared = SharedProjection::compile(&roots).unwrap();
        assert_eq!(
            shared
                .start_execution(&Session::default())
                .evaluate(&input, &[0, 1, 2], &mut EvalContext::default())
                .unwrap(),
            expected
        );
        let expected = vec![
            vec![
                Datum::new_string(b"b".to_vec()),
                Datum::new_string(b"a".to_vec()),
                Datum::new_string(b"b".to_vec())
            ];
            2
        ];
        assert_eq!(
            shared
                .start_execution(&Session::default())
                .evaluate(&input, &[2, 0, 2], &mut EvalContext::default())
                .unwrap(),
            expected
        );
    }

    #[test]
    fn shared_kernel_json_utf8_and_root_null_demand() {
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let json = |bytes| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("json_length"),
                integer.clone(),
                vec![Expression::Constant(Constant::new(
                    Datum::new_string(bytes),
                    native_type(),
                ))],
            ))
        };
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        let invalid = json(vec![0xff]);
        assert_eq!(
            invalid.eval(&crate::context::NoColumns, input.physical_row(0)),
            Err(crate::EvalError::Unsupported("invalid UTF-8 string datum"))
        );
        let error = SharedExpression::compile(&invalid)
            .unwrap()
            .evaluate(&input, &[0], &mut EvalContext::default())
            .unwrap_err();
        assert_eq!(error.code, Some(1105));
        assert_eq!(error.message, "invalid UTF-8 string datum");
        let root = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            integer.clone(),
            vec![
                Expression::Constant(Constant::new(Datum::Null, integer.clone())),
                json(b"{".to_vec()),
            ],
        ));
        assert_eq!(
            crate::scalar_function::try_eval_numeric_batch(
                &root,
                &crate::context::NoColumns,
                &input
            )
            .unwrap(),
            None
        );
        assert_eq!(
            root.eval(&crate::context::NoColumns, input.physical_row(0))
                .unwrap(),
            Datum::Null
        );
        assert_eq!(
            SharedExpression::compile(&root)
                .unwrap()
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap(),
            vec![Datum::Null]
        );
    }

    #[test]
    fn shared_kernel_json_error_identity() {
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let mut input = Chunk::new(&[integer.clone()], 1, 1);
        input.append_int64(0, 0);
        for (text, expected) in [
            ("", crate::JsonError::EmptyText),
            (" \t\n", crate::JsonError::EmptyText),
            ("{", crate::JsonError::InvalidText),
            ("{}x", crate::JsonError::InvalidText),
        ] {
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("json_length"),
                integer.clone(),
                vec![Expression::Constant(Constant::new(
                    Datum::new_string(text.as_bytes().to_vec()),
                    native_type(),
                ))],
            ));
            assert_eq!(
                expression.eval(&crate::context::NoColumns, input.physical_row(0)),
                Err(crate::EvalError::Json(expected.clone()))
            );
            let error = SharedExpression::compile(&expression)
                .unwrap()
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap_err();
            assert_eq!(error.code, Some(i32::from(expected.code())));
            assert_eq!(error.to_string(), expected.message());
        }
    }

    #[test]
    fn shared_kernel_scalar_and_width_one_vector_preserve_null_demand() {
        use tidb_datatype::FieldTypeCode;
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut left = Column::new(1, integer.clone());
        left.index = 0;
        let mut right = Column::new(2, integer.clone());
        right.index = 1;
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            integer.clone(),
            vec![
                Expression::Column(left),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("mul"),
                    integer.clone(),
                    vec![
                        Expression::Column(right),
                        Expression::Constant(Constant::new(Datum::Int(2), integer.clone())),
                    ],
                )),
            ],
        ));
        let mut input = Chunk::new(&[integer.clone(), integer], 1, 1);
        input.append_null(0);
        input.append_int64(1, i64::MAX);
        assert_eq!(
            expression
                .eval(&crate::context::NoColumns, input.physical_row(0))
                .unwrap(),
            Datum::Null
        );
        assert!(crate::scalar_function::try_eval_numeric_batch(
            &expression,
            &crate::context::NoColumns,
            &input
        )
        .is_err());
        let program = SharedExpression::compile(&expression).unwrap();
        assert_eq!(
            program
                .evaluate_mode(
                    &input,
                    &[0],
                    &mut EvalContext::default(),
                    InvocationMode::Scalar
                )
                .unwrap(),
            vec![Datum::Null]
        );
        assert!(program
            .evaluate_mode(
                &input,
                &[0],
                &mut EvalContext::default(),
                InvocationMode::Vector
            )
            .is_err());
    }

    #[test]
    fn shared_kernel_if_children_keep_scalar_null_demand() {
        let integer = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let constant = |v| Expression::Constant(Constant::new(v, integer.clone()));
        let call = |name, args| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                integer.clone(),
                args,
            ))
        };
        let expression = call(
            "if",
            vec![
                constant(Datum::Int(1)),
                call(
                    "plus",
                    vec![
                        constant(Datum::Null),
                        call(
                            "mul",
                            vec![constant(Datum::Int(i64::MAX)), constant(Datum::Int(2))],
                        ),
                    ],
                ),
                constant(Datum::Int(0)),
            ],
        );
        let mut input = Chunk::new(&[], 2, 2);
        input.set_num_virtual_rows(2);
        assert_eq!(
            crate::scalar_function::try_eval_numeric_batch(
                &expression,
                &crate::context::NoColumns,
                &input
            )
            .unwrap(),
            None
        );
        assert_eq!(
            expression
                .eval(&crate::context::NoColumns, input.physical_row(0))
                .unwrap(),
            Datum::Null
        );
        let outer = call(
            "plus",
            vec![
                expression.clone(),
                call(
                    "mul",
                    vec![constant(Datum::Int(i64::MAX)), constant(Datum::Int(2))],
                ),
            ],
        );
        assert_eq!(
            crate::scalar_function::try_eval_numeric_batch(
                &outer,
                &crate::context::NoColumns,
                &input
            )
            .unwrap(),
            None
        );
        assert_eq!(
            outer
                .eval(&crate::context::NoColumns, input.physical_row(0))
                .unwrap(),
            Datum::Null
        );
        assert_eq!(
            SharedExpression::compile(&outer)
                .unwrap()
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap(),
            vec![Datum::Null]
        );
        let program = SharedExpression::compile(&expression).unwrap();
        for rows in [&[0][..], &[1, 0, 1][..]] {
            assert_eq!(
                program
                    .evaluate(&input, rows, &mut EvalContext::default())
                    .unwrap(),
                vec![Datum::Null; rows.len()]
            );
        }
    }

    #[test]
    fn shared_kernel_if_guards_real_json_and_constant_regexp_errors() {
        use tidb_datatype::FieldTypeCode;
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let string = |value: &str| {
            Expression::Constant(Constant::new(
                Datum::new_string(value.as_bytes().to_vec()),
                native_type(),
            ))
        };
        let mut input = Chunk::new(&[], 2, 2);
        input.set_num_virtual_rows(2);
        for bad in [
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("json_length"),
                integer.clone(),
                vec![string("{")],
            )),
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("regexp_like"),
                integer.clone(),
                vec![string("a"), string("[")],
            )),
        ] {
            for condition in [0, 1] {
                let expression = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("if"),
                    integer.clone(),
                    vec![
                        Expression::Constant(Constant::new(Datum::Int(condition), integer.clone())),
                        bad.clone(),
                        Expression::Constant(Constant::new(Datum::Int(7), integer.clone())),
                    ],
                ));
                let expected = expression.eval(&crate::context::NoColumns, input.physical_row(0));
                let program = SharedExpression::compile(&expression)
                    .expect("guarded preparation must succeed");
                let actual = program.evaluate(&input, &[1, 0, 1], &mut EvalContext::default());
                if condition == 0 {
                    assert_eq!(expected.unwrap(), Datum::Int(7));
                    assert_eq!(actual.unwrap(), vec![Datum::Int(7); 3]);
                } else {
                    assert!(expected.is_err());
                    assert!(actual.is_err());
                }
            }
        }
        for (name, args, expected) in [
            ("json_length", vec![string("[1,2]")], 2),
            ("regexp_like", vec![string("Ab"), string("^a")], 0),
            (
                "regexp_like",
                vec![string("Ab"), string("^a"), string("i")],
                1,
            ),
        ] {
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new(name),
                integer.clone(),
                args,
            ));
            assert_eq!(
                expression
                    .eval(&crate::context::NoColumns, input.physical_row(0))
                    .unwrap(),
                Datum::Int(expected)
            );
            assert_eq!(
                SharedExpression::compile(&expression)
                    .unwrap()
                    .evaluate(&input, &[1, 0, 1], &mut EvalContext::default())
                    .unwrap(),
                vec![Datum::Int(expected); 3]
            );
        }
        let mut condition = Column::new(1, integer.clone());
        condition.index = 0;
        let mut json = Column::new(2, native_type());
        json.index = 1;
        let expression = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("if"),
            integer.clone(),
            vec![
                Expression::Column(condition),
                Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("json_length"),
                    integer.clone(),
                    vec![Expression::Column(json)],
                )),
                Expression::Constant(Constant::new(Datum::Int(7), integer.clone())),
            ],
        ));
        let mut input = Chunk::new(&[integer.clone(), native_type()], 4, 4);
        for (condition, json) in [(0, "{"), (1, "[1,2]"), (0, "{"), (1, "null")] {
            input.append_int64(0, condition);
            input.append_bytes(1, json.as_bytes());
        }
        let rows = [3, 0, 1, 0];
        let expected = rows
            .iter()
            .map(|r| {
                expression
                    .eval(&crate::context::NoColumns, input.physical_row(*r))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            expected,
            vec![Datum::Int(1), Datum::Int(7), Datum::Int(2), Datum::Int(7)]
        );
        assert_eq!(
            SharedExpression::compile(&expression)
                .unwrap()
                .evaluate(&input, &rows, &mut EvalContext::default())
                .unwrap(),
            expected
        );
        let invalid = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("md5"),
            integer,
            vec![string("a")],
        ));
        assert!(SharedExpression::compile(&invalid).is_err());
    }

    #[test]
    fn shared_kernel_dynamic_if_merges_occurrences_and_nested_wide_values() {
        use tidb_datatype::{Decimal, FieldTypeCode};
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut c = Column::new(1, integer.clone());
        c.index = 0;
        let mut input = Chunk::new(&[integer.clone()], 8, 8);
        for i in 0..8 {
            if i == 0 {
                input.append_null(0);
            } else {
                input.append_int64(0, i % 2);
            }
        }
        let if_expr = |tp: FieldType, yes: Expression, no: Expression| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                tp,
                vec![Expression::Column(c.clone()), yes, no],
            ))
        };
        let literal =
            |value: Datum, tp: &FieldType| Expression::Constant(Constant::new(value, tp.clone()));
        let nested = if_expr(
            integer.clone(),
            if_expr(
                integer.clone(),
                literal(Datum::Int(7), &integer),
                literal(Datum::Int(8), &integer),
            ),
            literal(Datum::Int(9), &integer),
        );
        let program = SharedExpression::compile(&nested).unwrap();
        for rows in [&[7, 2, 7, 0][..], &[2, 0][..], &[7, 7][..]] {
            let expected = rows
                .iter()
                .map(|r| {
                    nested
                        .eval(&crate::context::NoColumns, input.physical_row(*r))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                program
                    .evaluate(&input, rows, &mut EvalContext::default())
                    .unwrap(),
                expected
            );
        }
        let real = FieldType::new(FieldTypeCode::Double);
        let bits = 0x7ff8000000000042;
        let expression = if_expr(
            real.clone(),
            if_expr(
                real.clone(),
                literal(Datum::Real(f64::from_bits(bits)), &real),
                literal(Datum::Null, &real),
            ),
            literal(Datum::Real(-0.0), &real),
        );
        let actual = SharedExpression::compile(&expression)
            .unwrap()
            .evaluate(&input, &[7, 2, 7, 0], &mut EvalContext::default())
            .unwrap();
        for (value, expected) in
            actual
                .iter()
                .zip([bits, (-0.0f64).to_bits(), bits, (-0.0f64).to_bits()])
        {
            let Datum::Real(value) = value else {
                panic!("expected real")
            };
            assert_eq!(value.to_bits(), expected);
        }
        let decimal = FieldType::new(FieldTypeCode::NewDecimal);
        let hidden = Decimal::from_int(8)
            .true_div(&Decimal::from_int(7), 7)
            .unwrap();
        let wide =
            Decimal::from_literal(&format!("1{}", "0".repeat(81))).with_declared_shape(82, 0);
        let expression = if_expr(
            decimal.clone(),
            literal(Datum::Decimal(hidden.clone()), &decimal),
            literal(Datum::Decimal(wide.clone()), &decimal),
        );
        let actual = SharedExpression::compile(&expression)
            .unwrap()
            .evaluate(&input, &[7, 2, 7, 0], &mut EvalContext::default())
            .unwrap();
        for (value, expected) in actual.iter().zip([&hidden, &wide, &hidden, &wide]) {
            let Datum::Decimal(value) = value else {
                panic!("expected decimal")
            };
            assert_eq!(value.coefficient_digits(), expected.coefficient_digits());
            assert_eq!(value.storage_scale(), expected.storage_scale());
            assert_eq!(value.scale(), expected.scale());
            assert_eq!(value.declared_shape(), expected.declared_shape());
        }
    }

    #[test]
    fn shared_kernel_nested_partial_selections_and_deep_control() {
        use tidb_datatype::FieldTypeCode;
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let column = |index| {
            let mut c = Column::new(index + 1, integer.clone());
            c.index = index;
            Expression::Column(c)
        };
        let constant =
            |value| Expression::Constant(Constant::new(Datum::Int(value), integer.clone()));
        let condition = |c, yes, no| {
            Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                integer.clone(),
                vec![c, yes, no],
            ))
        };
        let expression = condition(
            column(0),
            condition(column(1), constant(7), constant(8)),
            constant(9),
        );
        let mut input = Chunk::new(&[integer.clone(), integer], 4, 4);
        for (a, b) in [(1, 0), (1, 1), (0, 0), (0, 1)] {
            input.append_int64(0, a);
            input.append_int64(1, b);
        }
        let rows = [1, 0, 1, 3];
        assert_eq!(
            SharedExpression::compile(&expression)
                .unwrap()
                .evaluate(&input, &rows, &mut EvalContext::default())
                .unwrap(),
            vec![Datum::Int(7), Datum::Int(8), Datum::Int(7), Datum::Int(9)]
        );
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut deep = Expression::Constant(Constant::new(Datum::Int(7), integer.clone()));
        for _ in 0..512 {
            deep = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                integer.clone(),
                vec![
                    Expression::Constant(Constant::new(Datum::Int(1), integer.clone())),
                    deep,
                    Expression::Constant(Constant::new(Datum::Int(9), integer.clone())),
                ],
            ));
        }
        let program = SharedExpression::compile(&deep).unwrap();
        assert_eq!(
            program
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap(),
            vec![Datum::Int(7)]
        );
        drop(program);
    }

    #[test]
    fn shared_kernel_finish_rejects_invalid_control_and_freezes_valid_program() {
        let int = || RpnExpressionNode::Constant {
            value: ScalarValue::Int(Some(1)),
            field_type: FieldTypeTp::LongLong.into(),
        };
        let branch = |target, end| RpnExpressionNode::JumpIfFalse {
            target,
            end,
            field_type: FieldTypeTp::LongLong.into(),
        };
        let jump = |target| RpnExpressionNode::Jump {
            target,
            field_type: FieldTypeTp::LongLong.into(),
        };
        for nodes in [
            vec![int(), branch(4, 5), int(), jump(6), int()],
            vec![branch(3, 4), int(), jump(4), int()],
            vec![int(), branch(4, 6), int(), jump(6), int(), int()],
            vec![
                int(),
                branch(4, 5),
                int(),
                jump(5),
                RpnExpressionNode::Constant {
                    value: ScalarValue::Bytes(None),
                    field_type: FieldTypeTp::VarString.into(),
                },
            ],
        ] {
            assert!(RpnExpression::finish(nodes, &[], FieldTypeTp::LongLong.into()).is_err());
        }
        let repeat = |end| RpnExpressionNode::Repeat {
            end,
            supported_type: true,
            field_type: FieldTypeTp::LongLong.into(),
        };
        let end = || RpnExpressionNode::RepeatEnd {
            field_type: FieldTypeTp::LongLong.into(),
        };
        for nodes in [
            vec![int(), repeat(99), int(), end()],
            vec![repeat(3), int(), end()],
            vec![int(), repeat(3), end()],
            vec![int(), end()],
        ] {
            assert!(RpnExpression::finish(nodes, &[], FieldTypeTp::LongLong.into()).is_err());
        }
        let bytes = || RpnExpressionNode::Constant {
            value: ScalarValue::Bytes(None),
            field_type: FieldTypeTp::VarString.into(),
        };
        for (target, count) in [(99, 1), (1, 1), (3, 0), (3, 2)] {
            assert!(RpnExpression::finish(
                vec![
                    bytes(),
                    RpnExpressionNode::RegexpArgument {
                        target,
                        count,
                        field_type: FieldTypeTp::LongLong.into()
                    },
                    int()
                ],
                &[],
                FieldTypeTp::LongLong.into()
            )
            .is_err());
        }
        // Native Decimal rejects malformed literal digits during construction in
        // debug builds; release builds still reject them at program admission.
        let invalid = std::panic::catch_unwind(|| tidb_datatype::Decimal::from_literal("x"));
        assert_eq!(invalid.is_err(), cfg!(debug_assertions));
        if let Ok(value) = invalid {
            assert!(RpnExpression::finish(
                vec![RpnExpressionNode::ExpressionConstant {
                    value: ExpressionScalar::Decimal(Some(value.into())),
                    field_type: FieldTypeTp::NewDecimal.into()
                }],
                &[],
                FieldTypeTp::NewDecimal.into()
            )
            .is_err());
        }
        let mut program = RpnExpression::finish(
            vec![int(), branch(4, 5), int(), jump(5), int()],
            &[],
            FieldTypeTp::LongLong.into(),
        )
        .unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| program.push(int()))).is_err()
        );
    }

    #[test]
    fn shared_kernel_rejects_unimplemented_result_shapes() {
        use tidb_datatype::FieldTypeCode;
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        for (code, value, branch_code) in [
            (
                FieldTypeCode::Float,
                Datum::Real(1.0000000000000002),
                FieldTypeCode::Double,
            ),
            (FieldTypeCode::Bit, Datum::Int(3), FieldTypeCode::LongLong),
        ] {
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                FieldType::new(code),
                vec![
                    Expression::Constant(Constant::new(
                        Datum::Int(1),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                    Expression::Constant(Constant::new(value.clone(), FieldType::new(branch_code))),
                    Expression::Constant(Constant::new(value, FieldType::new(branch_code))),
                ],
            ));
            let expected = expression
                .eval(&crate::context::NoColumns, input.get_row(0))
                .unwrap();
            if code == FieldTypeCode::Float {
                assert!(matches!(expected, Datum::Float32(v) if v == 1.0));
            } else {
                assert!(matches!(expected, Datum::Bit(_)));
            }
            assert!(
                SharedExpression::compile(&expression).is_err(),
                "unsupported {code:?} must not silently return a different Datum"
            );
        }
    }

    #[test]
    fn shared_kernel_typed_null_and_program_exit() {
        use tidb_datatype::FieldTypeCode;
        use tidb_query_datatype::FieldTypeAccessor;
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        for code in [FieldTypeCode::Double, FieldTypeCode::NewDecimal] {
            let tp = FieldType::new(code);
            for condition in [0, 1] {
                let value = if code == FieldTypeCode::Double {
                    Datum::Real(1.5)
                } else {
                    Datum::Decimal(tidb_datatype::Decimal::from_int(7))
                };
                let expression = Expression::ScalarFunction(ScalarFunction::new(
                    CiString::new("if"),
                    tp.clone(),
                    vec![
                        Expression::Constant(Constant::new(
                            Datum::Int(condition),
                            FieldType::new(FieldTypeCode::LongLong),
                        )),
                        Expression::Constant(Constant::new(value, tp.clone())),
                        Expression::Constant(Constant::new(Datum::Null, tp.clone())),
                    ],
                ));
                let program = SharedExpression::compile(&expression).unwrap();
                let expected_type = if code == FieldTypeCode::Double {
                    FieldTypeTp::Double
                } else {
                    FieldTypeTp::NewDecimal
                };
                assert_eq!(
                    program
                        .program
                        .ret_field_type(&program.schema)
                        .as_accessor()
                        .tp(),
                    expected_type
                );
                assert!(!program.program.is_last_constant());
                let expected = expression
                    .eval(&crate::context::NoColumns, input.get_row(0))
                    .unwrap();
                assert_eq!(
                    program
                        .evaluate(&input, &[0], &mut EvalContext::default())
                        .unwrap(),
                    vec![expected]
                );
            }
        }
    }

    #[test]
    fn shared_kernel_reads_only_demanded_columns_and_preserves_integer_selection() {
        use tidb_datatype::FieldTypeCode;
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut missing = Column::new(1, integer.clone());
        missing.index = 7;
        let mut input = Chunk::new(&[], 3, 3);
        input.set_num_virtual_rows(3);
        for condition in [0, 1] {
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                integer.clone(),
                vec![
                    Expression::Constant(Constant::new(Datum::Int(condition), integer.clone())),
                    Expression::Column(missing.clone()),
                    Expression::Constant(Constant::new(Datum::Int(9), integer.clone())),
                ],
            ));
            let program = SharedExpression::compile(&expression).unwrap();
            let actual = program.evaluate(&input, &[2, 0, 2], &mut EvalContext::default());
            if condition == 0 {
                assert_eq!(actual.unwrap(), vec![Datum::Int(9); 3]);
                assert_eq!(
                    expression
                        .eval(&crate::context::NoColumns, input.get_row(0))
                        .unwrap(),
                    Datum::Int(9)
                );
            } else {
                assert!(actual.is_err());
            }
        }
        let mut column = Column::new(1, integer.clone());
        column.index = 0;
        let program = SharedExpression::compile(&Expression::Column(column)).unwrap();
        let mut input = Chunk::new(&[integer], 8, 8);
        for row in 0..8 {
            if row == 2 {
                input.append_null(0);
            } else {
                input.append_int64(0, row - 10);
            }
        }
        assert_eq!(
            program
                .evaluate(&input, &[7, 2, 7], &mut EvalContext::default())
                .unwrap(),
            vec![Datum::Int(-3), Datum::Null, Datum::Int(-3)]
        );
    }

    #[test]
    fn shared_kernel_if_true_false_null_matches_original() {
        use tidb_datatype::FieldTypeCode;
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut input = Chunk::new(&[], 1, 1);
        input.set_num_virtual_rows(1);
        for condition in [Datum::Int(0), Datum::Int(1), Datum::Null] {
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                integer.clone(),
                vec![
                    Expression::Constant(Constant::new(condition, integer.clone())),
                    Expression::Constant(Constant::new(Datum::Int(3), integer.clone())),
                    Expression::Constant(Constant::new(Datum::Int(7), integer.clone())),
                ],
            ));
            let expected = expression
                .eval(&crate::context::NoColumns, input.get_row(0))
                .unwrap();
            let program = SharedExpression::compile(&expression).unwrap();
            assert_eq!(
                program
                    .evaluate(&input, &[0], &mut EvalContext::default())
                    .unwrap(),
                vec![expected]
            );
        }
    }

    #[test]
    fn shared_kernel_constant_full_real_and_decimal_domain() {
        use tidb_datatype::{Decimal, FieldTypeCode};
        let mut input = Chunk::new(&[], 8, 8);
        input.set_num_virtual_rows(8);
        let mut ctx = EvalContext::default();
        for bits in [
            0x7ff8000000000001,
            0xfff8000000000042,
            (-0.0f64).to_bits(),
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
        ] {
            let expression = Expression::Constant(Constant::new(
                Datum::Real(f64::from_bits(bits)),
                FieldType::new(FieldTypeCode::Double),
            ));
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("if"),
                FieldType::new(FieldTypeCode::Double),
                vec![
                    Expression::Constant(Constant::new(
                        Datum::Int(1),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                    expression,
                    Expression::Constant(Constant::new(
                        Datum::Real(0.0),
                        FieldType::new(FieldTypeCode::Double),
                    )),
                ],
            ));
            let program = SharedExpression::compile(&expression).unwrap();
            for value in program.evaluate(&input, &[7, 2, 7], &mut ctx).unwrap() {
                let Datum::Real(value) = value else {
                    panic!("real must not become NULL")
                };
                assert_eq!(value.to_bits(), bits);
            }
        }
        let wide =
            Decimal::from_literal(&format!("1{}", "0".repeat(81))).with_declared_shape(82, 0);
        let hidden = Decimal::from_int(8)
            .true_div(&Decimal::from_int(7), 7)
            .unwrap();
        for value in [wide, hidden] {
            let expression = Expression::Constant(Constant::new(
                Datum::Decimal(value.clone()),
                FieldType::new(FieldTypeCode::NewDecimal),
            ));
            let program = SharedExpression::compile(&expression).unwrap();
            let result = program.evaluate(&input, &[7], &mut ctx).unwrap();
            let Datum::Decimal(got) = &result[0] else {
                panic!("decimal must not become NULL")
            };
            assert_eq!(got.coefficient_digits(), value.coefficient_digits());
            assert_eq!(got.scale(), value.scale());
            assert_eq!(got.storage_scale(), value.storage_scale());
            assert_eq!(got.declared_shape(), value.declared_shape());
        }
    }

    #[test]
    fn shared_kernel_exact_decimal_parts_preserve_hidden_precision_and_reject_bad_parts() {
        use tidb_datatype::{Decimal, MyDecimal};
        use tidb_query_datatype::codec::mysql::Decimal as SdkDecimal;
        let value = Decimal::from_int(8)
            .true_div(&Decimal::from_int(7), 7)
            .unwrap();
        assert_eq!(value.to_string(), "1.1428571");
        assert_eq!(value.coefficient_digits(), "1142857142");
        assert_eq!(value.storage_scale(), 9);
        let parts = value.to_my_decimal().unwrap().to_parts();
        let sdk = SdkDecimal::from_parts(parts).unwrap();
        let restored = Decimal::from_my_decimal(&MyDecimal::from_parts(sdk.to_parts()).unwrap());
        assert_eq!(restored.to_string(), "1.1428571");
        assert_eq!(restored.storage_scale(), 9);
        assert_eq!(restored.coefficient_digits(), "1142857142");
        let large = Decimal::from_literal(&format!("{}.5", "9".repeat(40)));
        let parts = large.to_my_decimal().unwrap().to_parts();
        let sdk = SdkDecimal::from_parts(parts).unwrap();
        let restored = Decimal::from_my_decimal(&MyDecimal::from_parts(sdk.to_parts()).unwrap());
        assert_eq!(restored.to_string(), large.to_string());
        for invalid in [
            (82, 0, 0, false, [0; 9]),
            (0, 1, 1, false, [1, 0, 0, 0, 0, 0, 0, 0, 0]),
            (1, 0, 0, false, [10, 0, 0, 0, 0, 0, 0, 0, 0]),
            (1, 0, 82, false, [0; 9]),
            (1, 0, 0, false, [1_000_000_000; 9]),
        ] {
            assert!(SdkDecimal::from_parts(invalid).is_err());
            assert!(MyDecimal::from_parts(invalid).is_err());
        }
    }

    #[test]
    fn shared_kernel_native_and_pb_md5_selection_null_rebind() {
        let mut c = Column::default();
        c.index = 0;
        c.ret_type = Some(native_type());
        let expression = md5(Expression::Column(c));
        let native = SharedExpression::compile(&expression).unwrap();
        let mut input = Chunk::new(&[native_type()], 8, 8);
        for i in 0..8 {
            if i == 2 {
                input.append_null(0);
            } else {
                input.append_bytes(0, b"abc");
            }
        }
        let mut ctx = EvalContext::default();
        let got = native.evaluate(&input, &[7, 2, 7], &mut ctx).unwrap();
        assert_eq!(
            got,
            vec![
                Datum::new_string(b"900150983cd24fb0d6963f7d28e17f72".to_vec()),
                Datum::Null,
                Datum::new_string(b"900150983cd24fb0d6963f7d28e17f72".to_vec())
            ]
        );
        for (i, row) in [7, 2, 7].iter().enumerate() {
            let original = expression
                .eval(&crate::context::NoColumns, input.physical_row(*row))
                .unwrap();
            assert_eq!(original.is_null(), got[i].is_null());
            if !original.is_null() {
                assert_eq!(original.sql_string().unwrap(), got[i].sql_string().unwrap());
            }
        }
        assert!(native.evaluate(&input, &[], &mut ctx).unwrap().is_empty());
        assert!(native.evaluate(&input, &[8], &mut ctx).is_err());
        let mut second = Chunk::new(&[native_type()], 1, 1);
        second.append_bytes(0, b"");
        assert_eq!(
            native.evaluate(&second, &[0], &mut ctx).unwrap(),
            vec![Datum::new_string(
                b"d41d8cd98f00b204e9800998ecf8427e".to_vec()
            )]
        );

        let mut child = tipb::Expr::default();
        child.set_tp(tipb::ExprType::Bytes);
        child.set_val(b"abc".to_vec());
        child.set_field_type(FieldTypeTp::VarString.into());
        let mut pb = tipb::Expr::default();
        pb.set_tp(tipb::ExprType::ScalarFunc);
        pb.set_sig(tipb::ScalarFuncSig::Md5);
        pb.set_field_type(FieldTypeTp::VarString.into());
        pb.mut_children().push(child);
        let program =
            tidb_query_expr::RpnExpressionBuilder::build_from_expr_tree(pb, &mut ctx, 0).unwrap();
        let empty = LazyBatchColumnVec::empty();
        let result = program
            .eval_decoded(&mut ctx, &[], &empty, &[0], 1)
            .unwrap();
        assert_eq!(native_output(result, 1).unwrap()[0], got[0]);
        let literal = SharedExpression::compile(&md5(Expression::Constant(Constant::new(
            Datum::Bytes(b"abc".to_vec()),
            native_type(),
        ))))
        .unwrap();
        assert_eq!(literal.evaluate(&input, &[7], &mut ctx).unwrap()[0], got[0]);
    }
    #[test]
    fn shared_kernel_direct_output_failure_and_alias_contract() {
        use tidb_datatype::{Decimal, FieldTypeCode};
        let real = FieldType::new(FieldTypeCode::Double);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal);
        let rand = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("rand"),
            real.clone(),
            vec![],
        ));
        let overflow = numeric_binary(
            "mul",
            &decimal,
            Datum::Decimal(Decimal::from_literal(&"9".repeat(81))),
            Datum::Decimal(Decimal::from_int(100)),
        );
        let projection = SharedProjection::compile(&[rand.clone(), overflow]).unwrap();
        let mut input = Chunk::new(&[], 2, 2);
        input.set_num_virtual_rows(2);
        let mut output = Chunk::new(&[real.clone(), decimal.clone()], 2, 2);
        output.append_datum(0, &Datum::Real(99.0));
        output.append_datum(1, &Datum::Decimal(Decimal::from_int(88)));
        let session = Session::default();
        let error = projection
            .start_execution(&session)
            .evaluate_into_chunk(&input, &[0, 1], &mut EvalContext::default(), &mut output)
            .unwrap_err();
        assert_eq!(error.code, Some(1690));
        assert!(session.draws.get() > 0);
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output.physical_row(0).get_datum(0, &real),
            Datum::Real(99.0)
        );
        assert_eq!(
            output.physical_row(0).get_datum(1, &decimal),
            Datum::Decimal(Decimal::from_int(88).with_declared_shape(-1, 0))
        );

        let projection = SharedProjection::compile(&[rand.clone(), rand]).unwrap();
        let mut input = Chunk::new(&[real.clone(), real.clone()], 1, 1);
        input.append_datum(0, &Datum::Real(5.0));
        input.append_datum(1, &Datum::Real(6.0));
        let mut output = Chunk::new(&[real.clone(), real.clone()], 1, 1);
        output.make_ref_to(0, &mut input, 0).unwrap();
        let session = Session::default();
        assert!(projection
            .start_execution(&session)
            .evaluate_into_chunk(&input, &[0], &mut EvalContext::default(), &mut output)
            .is_err());
        assert_eq!(session.draws.get(), 0);
        assert_eq!(input.column(0).get_float64(0), 5.0);
        let mut output = Chunk::new(&[real.clone(), real.clone()], 1, 1);
        output.make_ref(0, 1);
        assert!(projection
            .start_execution(&session)
            .evaluate_into_chunk(&input, &[0], &mut EvalContext::default(), &mut output)
            .is_err());
        assert_eq!(session.draws.get(), 0);

        let mut wrong = Chunk::new(&[decimal.clone(), decimal.clone()], 1, 1);
        assert!(projection
            .start_execution(&session)
            .evaluate_into_chunk(&input, &[0], &mut EvalContext::default(), &mut wrong)
            .is_err());
        assert_eq!(session.draws.get(), 0);

        for off in [false, true] {
            let original = Session {
                vectorized_off: off,
                ..Session::default()
            };
            let direct = Session {
                vectorized_off: off,
                ..Session::default()
            };
            let expected = projection
                .start_execution(&original)
                .evaluate(&input, &[0, 0], &mut EvalContext::default())
                .unwrap();
            let mut output = Chunk::new(&[real.clone(), real.clone()], 2, 2);
            projection
                .start_execution(&direct)
                .evaluate_into_chunk(&input, &[0, 0], &mut EvalContext::default(), &mut output)
                .unwrap();
            for col in 0..2 {
                for row in 0..2 {
                    assert_eq!(
                        output.physical_row(row).get_datum(col, &real),
                        expected[col][row]
                    );
                }
            }
            assert_eq!(original.draws.get(), direct.draws.get());
        }
    }

    #[test]
    fn shared_kernel_direct_wide_output_keeps_lossy_contract() {
        use tidb_datatype::{Decimal, FieldTypeCode};
        let ty = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(65)
            .with_decimal(-1);
        let negative_zero = Decimal::from_literal(&format!("-{}", "9".repeat(81)))
            .mul_mysql(&Decimal::from_int(100))
            .0;
        for value in [
            Decimal::from_literal(&"9".repeat(85)),
            negative_zero,
            Decimal::from_literal("0.00000000000000000000000000000000000000001"),
            Decimal::from_literal("-170141183460469231731687303715884105728"),
        ] {
            let projection =
                SharedProjection::compile(&[numeric_literal(Datum::Decimal(value), &ty)]).unwrap();
            let mut input = Chunk::new(&[], 1, 1);
            input.set_num_virtual_rows(1);
            let mut output = Chunk::new(std::slice::from_ref(&ty), 1, 1);
            let owned = projection
                .start_execution(&crate::NoColumns)
                .evaluate(&input, &[0], &mut EvalContext::default())
                .unwrap();
            let mut expected = Chunk::new(std::slice::from_ref(&ty), 1, 1);
            expected.append_datum(0, &owned[0][0]);
            expected.set_num_virtual_rows(1);
            projection
                .start_execution(&crate::NoColumns)
                .evaluate_into_chunk(&input, &[0], &mut EvalContext::default(), &mut output)
                .unwrap();
            assert_eq!(output, expected);
        }
    }
}
