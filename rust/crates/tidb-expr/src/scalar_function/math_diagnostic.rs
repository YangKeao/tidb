// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Diagnostic-only source facts. These instructions render text only after an
//! actual math overflow; they cannot evaluate an expression or read a column.
use super::*;

#[derive(Debug)]
pub(crate) enum MathDiagnosticPart {
    Literal(Datum),
    Column(Box<str>),
    Function(Box<str>, usize),
    Binary(NativeArithmeticOperator),
}
#[derive(Debug)]
pub(crate) struct MathDiagnostic(Box<[MathDiagnosticPart]>);
impl MathDiagnostic {
    pub(crate) fn capture(function: &ScalarFunction, parts_hint: usize) -> Option<Self> {
        fn function_parts(
            f: &ScalarFunction,
            parts: &mut Vec<MathDiagnosticPart>,
            root: bool,
        ) -> Option<()> {
            for arg in &f.args {
                expression_parts(arg, parts)?;
            }
            if !root {
                if let Some(op) = binary_op_for_name(f.func_name.lowercase()) {
                    if f.args.len() != 2 {
                        return None;
                    }
                    parts.push(MathDiagnosticPart::Binary(native_arithmetic_operator(op)?));
                    return Some(());
                }
            }
            parts.push(MathDiagnosticPart::Function(
                f.func_name.lowercase().into(),
                f.args.len(),
            ));
            Some(())
        }
        fn expression_parts(expr: &Expression, parts: &mut Vec<MathDiagnosticPart>) -> Option<()> {
            match expr {
                Expression::Constant(c) => {
                    let value = c.literal_value()?;
                    if !matches!(
                        value,
                        Datum::Null
                            | Datum::Int(_)
                            | Datum::UInt(_)
                            | Datum::Real(_)
                            | Datum::Float32(_)
                            | Datum::Decimal(_)
                    ) {
                        return None;
                    }
                    parts.push(MathDiagnosticPart::Literal(value.clone()));
                }
                Expression::Column(c) if !c.orig_name.is_empty() => parts.push(
                    MathDiagnosticPart::Column(c.orig_name.clone().into_boxed_str()),
                ),
                Expression::CorrelatedColumn(c) if !c.column.orig_name.is_empty() => parts.push(
                    MathDiagnosticPart::Column(c.column.orig_name.clone().into_boxed_str()),
                ),
                Expression::ScalarFunction(f) => function_parts(f, parts, false)?,
                _ => return None,
            }
            Some(())
        }
        let mut parts = Vec::with_capacity(parts_hint);
        function_parts(function, &mut parts, true)?;
        assert!(parts.len() <= parts_hint, "admitted diagnostic part count");
        Some(Self(parts.into_boxed_slice()))
    }
    pub(crate) fn render(&self) -> String {
        let mut stack = Vec::<String>::with_capacity(self.0.len());
        for part in &self.0 {
            let text = match part {
                MathDiagnosticPart::Literal(value) => {
                    math_diagnostic_literal(value).expect("captured numeric diagnostic literal")
                }
                MathDiagnosticPart::Column(text) => text.to_string(),
                MathDiagnosticPart::Function(name, arity) => {
                    let begin = stack
                        .len()
                        .checked_sub(*arity)
                        .expect("captured diagnostic arity");

                    let text = native_render_function_expression(name, &stack[begin..]);
                    stack.truncate(begin);
                    text
                }
                MathDiagnosticPart::Binary(op) => {
                    let right = stack.pop().expect("captured diagnostic right");
                    let left = stack.pop().expect("captured diagnostic left");

                    native_render_binary_expression(&left, *op, &right)
                }
            };
            stack.push(text);
        }
        assert_eq!(stack.len(), 1, "captured diagnostic root");
        stack.pop().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn observe(f: impl FnOnce() -> String) -> String {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
            Ok(text) => format!("OK:{text}"),
            Err(p) => format!(
                "PANIC:{:?}",
                p.downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| p.downcast_ref::<&str>().copied())
            ),
        }
    }
    #[test]
    fn diagnostic_only_facts_match_original_renderer_without_evaluating_children() {
        let mut count = 0;
        for name in ["abs", "cot", "exp", "pow", "power"] {
            for value in [
                Datum::Null,
                Datum::Int(i64::MIN),
                Datum::UInt(u64::MAX),
                Datum::Real(f64::NAN),
                Datum::Float32(-0.0),
                Datum::Decimal(tidb_datatype::Decimal::from_literal("1.20")),
                Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
                    false,
                    vec![255],
                    0,
                    0,
                )),
            ] {
                let field = FieldType::new(tidb_datatype::FieldTypeCode::Double);
                let literal =
                    Expression::Constant(crate::constant::Constant::new(value, field.clone()));
                for nested in [false, true] {
                    let argument = if nested {
                        Expression::ScalarFunction(ScalarFunction::new(
                            CiString::new("plus"),
                            field.clone(),
                            vec![literal.clone(), literal.clone()],
                        ))
                    } else {
                        literal.clone()
                    };
                    let f = ScalarFunction::new(CiString::new(name), field.clone(), vec![argument]);
                    let metadata = MathDiagnostic::capture(&f, 8).unwrap();
                    let expected = observe(|| static_math_overflow_expression(&f).unwrap());
                    let actual = observe(|| {
                        tidb_query_expr::local::with_value_kernel_work_limit(65536, || {
                            metadata.render()
                        })
                    });
                    assert_eq!(actual, expected, "{name} nested={nested}");
                    count += 1;
                }
            }
        }
        assert_eq!(count, 70);
    }
}
