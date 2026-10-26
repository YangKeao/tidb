// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Borrowed arithmetic surveys, followed by detached plans after the full gate.
use super::*;
use crate::scalar_function::{
    dynamic_diagnostic::{
        ArgumentDiagnostic, ArgumentRecipe, DiagnosticRecipe, DynamicDiagnostic, LiteralPolicy,
    },
    NativeArithmeticInputMode as Mode,
};
pub(super) enum NumericRecipe<'a> {
    Subject(DiagnosticRecipe<'a>),
    Argument(ArgumentRecipe<'a>),
}
impl<'a> NumericRecipe<'a> {
    fn bindings(&self) -> &[&'a Expression] {
        match self {
            Self::Subject(r) => r.bindings(),
            Self::Argument(r) => r.bindings(),
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::Subject(r) => r.bytes(),
            Self::Argument(r) => r.bytes(),
        }
    }
}
pub(super) struct PendingNumeric<'a> {
    pub ordinal: usize,
    pub extras: Vec<(&'a Expression, NativeView)>,
    pub requests: [[usize; 2]; 2],
    recipes: Vec<NumericRecipe<'a>>,
    alternates: usize,
}
impl PendingNumeric<'_> {
    pub fn freeze(&mut self) -> Result<Box<[NumericPlan]>> {
        let mut plans = Vec::new();
        reserve(&mut plans, self.recipes.len())?;
        let mut base = 2 + self.alternates;
        for recipe in std::mem::take(&mut self.recipes) {
            let bindings = recipe.bindings().len();
            let body = match recipe {
                NumericRecipe::Subject(r) => NumericPlanBody::Subject(r.freeze()?.0),
                NumericRecipe::Argument(r) => NumericPlanBody::Argument(r.freeze()?.0),
            };
            plans.push(NumericPlan {
                base,
                bindings,
                body,
            });
            base = base
                .checked_add(bindings)
                .ok_or_else(|| resource("numeric plan argument extent overflow"))?;
        }
        Ok(plans.into_boxed_slice())
    }
}
pub(super) fn mode_view(
    function: &ScalarFunction,
    index: usize,
    mode: Mode,
    diagnostic: bool,
) -> NativeView {
    let value = match mode {
        Mode::Raw => NativeView::ValueToken,
        Mode::TypedInteger => NativeView::RowTyped(NativeEvalType::Int),
        Mode::Numeric(target) => {
            NativeView::RowTyped(crate::scalar_function::native_numeric_batch_source(
                function.args[index]
                    .static_type()
                    .expect("declared numeric source"),
                crate::scalar_function::dynamic_diagnostic::saved_binary_literal(
                    &function.args[index],
                ),
                target,
            ))
        }
    };
    if diagnostic {
        match value {
            NativeView::RowTyped(target) => NativeView::DiagnosticTyped(target),
            _ => NativeView::DiagnosticValue,
        }
    } else {
        value
    }
}
pub(super) fn survey<'a, N: SourceReference<'a>>(
    function: &'a ScalarFunction,
    view: NativeView,
    ordinal: usize,
    limits: BatchSourceLimits,
    budget: &mut Budget,
) -> Result<PendingNumeric<'a>> {
    let mut extras = Vec::new();
    let mut requests = [[0, 1]; 2];
    let selected = pb_scalar_binary_domain(function, view).map(Mode::Numeric);
    if selected.unwrap_or_else(|| function.native_arithmetic_mode()) == Mode::TypedInteger
        && window_arithmetic_operation(function, view).is_none()
    {
        for index in 0..2 {
            let initial = mode_view(function, index, Mode::TypedInteger, view.diagnostic());
            let general = mode_view(
                function,
                index,
                function.native_arithmetic_generic_mode(),
                view.diagnostic(),
            );
            if initial != general
                && row_typed_context_target(&function.args[index], initial).is_some()
            {
                reserve(&mut extras, 1)?;
                requests[1][index] = 2 + extras.len();
                extras.push((&function.args[index], general));
            }
        }
    }
    let alternates = extras.len();
    let mut recipes = Vec::new();
    reserve(&mut recipes, 6)?;
    for arg in &function.args {
        recipes.push(NumericRecipe::Subject(DynamicDiagnostic::survey_numeric(
            arg,
            true,
            limits.tree.max_nodes,
            limits.max_metadata_bytes,
        )?));
    }
    for float in [false, true] {
        for arg in &function.args {
            recipes.push(NumericRecipe::Argument(ArgumentDiagnostic::survey(
                function,
                arg,
                float,
                limits.tree.max_nodes,
                limits.max_metadata_bytes,
            )?));
        }
    }
    budget.array::<PendingNumeric<'_>>(1)?;
    budget.array::<CompiledNumericParent>(1)?;
    budget.array::<Option<Box<[NumericPlan]>>>(1)?;
    budget.array::<NumericRecipe<'_>>(6)?;
    budget.array::<NumericPlan>(6)?;
    for (index, recipe) in recipes.iter().enumerate() {
        budget.add(recipe.bytes())?;
        let float = index < 2 || index >= 4;
        reserve(&mut extras, recipe.bindings().len())?;
        for &child in recipe.bindings() {
            extras.push((
                child,
                NativeView::DiagnosticConstantRoot(LiteralPolicy::Numeric(float)),
            ));
        }
    }
    budget.array::<(&Expression, NativeView)>(extras.len())?;
    budget.array::<(N, usize, NativeView, bool)>(extras.len())?;
    budget.array::<tipb::FieldType>(extras.len())?;
    budget.array::<usize>(extras.len() + 2)?;
    budget.field(&native_bytes_type(), extras.len())?;
    Ok(PendingNumeric {
        ordinal,
        extras,
        requests,
        recipes,
        alternates,
    })
}
pub(super) fn preparation<'a, N: SourceReference<'a>>(
    node: &CheckedNode<'a, N>,
) -> NativeValuePreparation {
    if row_typed_context_target(node.node, node.view).is_some() {
        NativeValuePreparation::TypedReady
    } else if matches!(node.node.kind(), BorrowedNodeKind::Constant(_)) {
        NativeValuePreparation::Literal
    } else {
        NativeValuePreparation::Computed
    }
}
