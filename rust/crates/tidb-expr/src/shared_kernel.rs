// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Opt-in native adapter for the original TiKV RPN evaluator.
//!
//! shortcut: this checkpoint admits MD5, byte columns, numeric literals and
//! integer-condition numeric IF. Extend demand and typed kernel admission before
//! enabling a default path; byte literal identity is not yet an output contract.

use tidb_chunk::chunk::Chunk;
use tidb_datatype::Datum;
use tidb_query_datatype::{
    codec::data_type::{ChunkRef, ScalarValue, VectorValue},
    expr::EvalContext,
    EvalType, FieldTypeAccessor, FieldTypeFlag, FieldTypeTp,
};
use tidb_query_expr::{
    ExpressionDecimal, ExpressionScalar, ExpressionVector, InputColumns, InputResult,
    InvocationMode, RpnExpression, RpnExpressionNode, RpnStackNode, RpnStackNodeVectorValue,
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
        use tidb_query_expr::evaluation_error::ErrorInner;
        match *error.0 {
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
    columns: Vec<(usize, EvalType)>,
    schema: Vec<tipb::FieldType>,
}

impl SharedExpression {
    /// Compile the admitted native expression subset without evaluating it.
    pub fn compile(expression: &Expression) -> Result<Self, String> {
        if matches!(expression, Expression::Constant(c) if matches!(c.literal_value(), Some(Datum::Bytes(_) | Datum::String(_))))
            || matches!(expression, Expression::ScalarFunction(f) if f.func_name.lowercase() == "if" && f.ret_type.as_ref().is_some_and(|t| t.eval_type() == tidb_datatype::EvalType::String))
        {
            return Err(
                "byte literal identity is outside this checkpoint's output admission".into(),
            );
        }
        enum Visit<'a> {
            Enter(&'a Expression),
            Md5,
            IntegerLeft(&'a crate::scalar_function::ScalarFunction),
            IntegerRight(&'a crate::scalar_function::ScalarFunction, usize),
            ParseJson,
            JsonLength,
            RegexpLike(usize),
            IfCondition(&'a crate::scalar_function::ScalarFunction, usize),
            IfThen(&'a crate::scalar_function::ScalarFunction, usize, usize),
            IfElse(usize, usize, usize),
        }
        let mut work = vec![Visit::Enter(expression)];
        let mut nodes = Vec::new();
        let mut columns = Vec::new();
        let bytes_type = || tipb::FieldType::from(FieldTypeTp::VarString);
        while let Some(visit) = work.pop() {
            match visit {
                Visit::Enter(Expression::Constant(c)) => {
                    let datum = c
                        .literal_value()
                        .ok_or("shared kernel requires a literal constant")?;
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
                            ExpressionScalar::Decimal(Some(ExpressionDecimal {
                                negative: v.is_negative(),
                                coefficient: v.coefficient_digits().as_bytes().to_vec(),
                                storage_scale: v.storage_scale(),
                                result_scale: v.scale(),
                                declared_shape: v.declared_shape(),
                            })),
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
                        Datum::Bytes(v) => ScalarValue::Bytes(Some(v.clone())),
                        Datum::String(v) => ScalarValue::Bytes(Some(v.bytes().to_vec())),
                        _ => {
                            return Err("shared kernel does not yet admit this literal type".into())
                        }
                    };
                    let field_type = if matches!(value, ScalarValue::Int(_)) {
                        FieldTypeTp::LongLong.into()
                    } else {
                        bytes_type()
                    };
                    nodes.push(RpnExpressionNode::Constant { value, field_type });
                }
                Visit::Enter(Expression::Column(c)) => {
                    let index = usize::try_from(c.index).map_err(|_| "negative column index")?;
                    let tp = c.ret_type.as_ref().ok_or("column requires a field type")?;
                    let admitted =
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
                    let eval_type = match tp.code() {
                        tidb_datatype::FieldTypeCode::VarString
                            if tp.collation_name() == admitted.collation_name() =>
                        {
                            EvalType::Bytes
                        }
                        tidb_datatype::FieldTypeCode::LongLong if !tp.is_unsigned() => {
                            EvalType::Int
                        }
                        _ => return Err("column type is outside shared input admission".into()),
                    };
                    if tp.is_array() {
                        return Err("array columns are not yet admitted".into());
                    }
                    let offset = match columns.iter().position(|(i, _)| *i == index) {
                        Some(i) => {
                            if columns[i].1 != eval_type {
                                return Err("conflicting column types".into());
                            }
                            i
                        }
                        None => {
                            columns.push((index, eval_type));
                            columns.len() - 1
                        }
                    };
                    nodes.push(RpnExpressionNode::ColumnRef { offset });
                }
                Visit::Enter(Expression::ScalarFunction(f))
                    if matches!(f.func_name.lowercase(), "plus" | "mul") =>
                {
                    let admitted = |t: &tidb_datatype::FieldType| {
                        t.code() == tidb_datatype::FieldTypeCode::LongLong
                            && !t.is_unsigned()
                            && !t.is_array()
                    };
                    if f.args.len() != 2
                        || f.ret_type.as_ref().is_none_or(|t| !admitted(t))
                        || f.args
                            .iter()
                            .any(|a| a.static_type().is_none_or(|t| !admitted(t)))
                    {
                        return Err(
                            "arithmetic currently admits signed integer plus/mul only".into()
                        );
                    }
                    work.push(Visit::IntegerLeft(f));
                    work.push(Visit::Enter(&f.args[0]));
                }
                Visit::IntegerLeft(f) => {
                    let at = nodes.len();
                    nodes.push(RpnExpressionNode::NullGuard {
                        target: 0,
                        field_type: FieldTypeTp::LongLong.into(),
                    });
                    work.push(Visit::IntegerRight(f, at));
                    work.push(Visit::Enter(&f.args[1]));
                }
                Visit::IntegerRight(f, guard) => {
                    use tidb_query_expr::impl_arithmetic::{
                        arithmetic_fn_meta, IntIntMultiply, IntIntPlus,
                    };
                    nodes.push(RpnExpressionNode::FnCall {
                        func_meta: if f.func_name.lowercase() == "plus" {
                            arithmetic_fn_meta::<IntIntPlus>()
                        } else {
                            arithmetic_fn_meta::<IntIntMultiply>()
                        },
                        args_len: 2,
                        field_type: FieldTypeTp::LongLong.into(),
                        metadata: Box::new(()),
                    });
                    let end = nodes.len();
                    let RpnExpressionNode::NullGuard { target, .. } = &mut nodes[guard] else {
                        unreachable!()
                    };
                    *target = end;
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
                    if !matches!(
                        declared.code(),
                        tidb_datatype::FieldTypeCode::LongLong
                            | tidb_datatype::FieldTypeCode::Double
                            | tidb_datatype::FieldTypeCode::NewDecimal
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
                        arg.static_type()
                            .is_none_or(|t| t.eval_type() != result_type)
                    }) {
                        return Err(
                            "IF currently requires branches explicitly cast to its result type"
                                .into(),
                        );
                    }
                    let region = nodes.len();
                    nodes.push(RpnExpressionNode::OrderedRows {
                        end: 0,
                        field_type: control_type(f)?,
                    });
                    work.push(Visit::IfCondition(f, region));
                    work.push(Visit::Enter(&f.args[0]));
                }
                Visit::IfCondition(f, region) => {
                    let at = nodes.len();
                    nodes.push(RpnExpressionNode::JumpIfFalse {
                        target: 0,
                        end: 0,
                        field_type: control_type(f)?,
                    });
                    work.push(Visit::IfThen(f, at, region));
                    work.push(Visit::Enter(&f.args[1]));
                }
                Visit::IfThen(f, branch, region) => {
                    let at = nodes.len();
                    nodes.push(RpnExpressionNode::Jump {
                        target: 0,
                        field_type: control_type(f)?,
                    });
                    let RpnExpressionNode::JumpIfFalse { target, .. } = &mut nodes[branch] else {
                        unreachable!()
                    };
                    *target = at + 1;
                    work.push(Visit::IfElse(at, branch, region));
                    work.push(Visit::Enter(&f.args[2]));
                }
                Visit::IfElse(jump, branch, region) => {
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
                    let RpnExpressionNode::OrderedRows { end: stop, .. } = &mut nodes[region]
                    else {
                        unreachable!()
                    };
                    *stop = end;
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
                        work.push(Visit::RegexpLike(f.args.len()));
                    }
                    work.extend(f.args.iter().rev().map(Visit::Enter));
                }
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
                    work.push(Visit::Md5);
                    work.push(Visit::Enter(&f.args[0]));
                }
                Visit::Enter(_) => {
                    return Err("expression is outside shared kernel admission".into())
                }
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
                Visit::RegexpLike(args_len) => nodes.push(RpnExpressionNode::FnCall {
                    func_meta: tidb_query_expr::impl_regexp::regexp_like_fn_meta::<
                        tidb_query_datatype::codec::collation::collator::CollatorUtf8Mb4Bin,
                    >(),
                    args_len,
                    field_type: FieldTypeTp::LongLong.into(),
                    metadata: Box::new(None::<regex::Regex>),
                }),
                Visit::Md5 => nodes.push(RpnExpressionNode::FnCall {
                    func_meta: tidb_query_expr::impl_encryption::md5_fn_meta(),
                    args_len: 1,
                    field_type: bytes_type(),
                    metadata: Box::new(()),
                }),
            }
        }
        let schema: Vec<tipb::FieldType> = columns
            .iter()
            .map(|(_, tp)| match tp {
                EvalType::Int => FieldTypeTp::LongLong.into(),
                _ => bytes_type(),
            })
            .collect();
        let temporary = RpnExpression::from(nodes);
        let exit = match expression {
            Expression::ScalarFunction(f) if f.func_name.lowercase() == "if" => control_type(f)?,
            _ => temporary.ret_field_type(&schema).clone(),
        };
        let program = RpnExpression::finish(temporary.into_inner(), &schema, exit)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            program,
            columns,
            schema,
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
            columns: &self.columns,
        };
        let result = self
            .program
            .eval_input_mode(ctx, &self.schema, &columns, rows, rows.len(), mode)
            .map_err(SharedEvaluationError::from)?;
        native_output(result, rows.len()).map_err(SharedEvaluationError::from)
    }
}

struct NativeInput<'a> {
    chunk: &'a Chunk,
    columns: &'a [(usize, EvalType)],
}

impl InputColumns for NativeInput<'_> {
    fn read<'a>(
        &'a self,
        schema: &'a [tipb::FieldType],
        offset: usize,
        rows: &'a [usize],
        _ctx: &mut EvalContext,
    ) -> InputResult<'a> {
        use tidb_query_datatype::codec::Error;
        let &(index, eval_type) = self
            .columns
            .get(offset)
            .ok_or(Error::ColumnOffset(offset))?;
        if index >= self.chunk.num_cols() {
            return Err(Error::ColumnOffset(index).into());
        }
        if rows.iter().any(|row| *row >= self.chunk.physical_rows()) {
            return Err(Error::InvalidDataType("selected row is outside input".into()).into());
        }
        let column = self.chunk.column(index);
        let mut values = VectorValue::with_capacity(rows.len(), eval_type);
        for row in rows {
            match eval_type {
                EvalType::Int => {
                    values.push_int((!column.is_null(*row)).then(|| column.get_int64(*row)))
                }
                EvalType::Bytes => values
                    .push_bytes((!column.is_null(*row)).then(|| column.get_bytes(*row).to_vec())),
                _ => unreachable!("compiler checks input types"),
            }
        }
        Ok(RpnStackNode::Vector {
            value: RpnStackNodeVectorValue::Generated {
                physical_value: values,
            },
            field_type: &schema[offset],
        })
    }
}

fn control_type(f: &crate::scalar_function::ScalarFunction) -> Result<tipb::FieldType, String> {
    use tidb_datatype::EvalType as NativeType;
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
    let convert = |v: Option<&[u8]>| v.map_or(Datum::Null, |v| Datum::new_string(v.to_vec()));
    match result {
        RpnStackNode::ExpressionVector { value, .. } => match value {
            ExpressionVector::Real(values) => Ok(values
                .into_iter()
                .map(|v| v.map_or(Datum::Null, Datum::Real))
                .collect()),
            ExpressionVector::Decimal(values) => values
                .into_iter()
                .map(|v| match v {
                    None => Ok(Datum::Null),
                    Some(v) => tidb_datatype::Decimal::from_expression_parts(
                        v.negative,
                        v.coefficient,
                        v.storage_scale,
                        v.result_scale,
                        v.declared_shape,
                    )
                    .map(Datum::Decimal)
                    .map_err(str::to_owned),
                })
                .collect(),
        },
        RpnStackNode::ExpressionScalar { value, .. } => {
            let datum = match value.as_ref() {
                ExpressionScalar::Real(v) => v.map_or(Datum::Null, Datum::Real),
                ExpressionScalar::Decimal(None) => Datum::Null,
                ExpressionScalar::Decimal(Some(v)) => Datum::Decimal(
                    tidb_datatype::Decimal::from_expression_parts(
                        v.negative,
                        v.coefficient.clone(),
                        v.storage_scale,
                        v.result_scale,
                        v.declared_shape,
                    )
                    .map_err(str::to_owned)?,
                ),
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
            message.contains("regex") || message.contains("json"),
            "unexpected error: {actual}"
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
        assert!(SharedExpression::compile(&outer)
            .unwrap()
            .evaluate(&input, &[0], &mut EvalContext::default())
            .is_err());
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
}
