// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Runtime diagnostic demand over immutable facts. The compiler alone holds
//! temporary original Constant references; the stored plan retains none.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LiteralPolicy {
    Numeric(bool),
    Math,
}
#[derive(Debug)]
enum Instruction {
    Literal(Datum, LiteralPolicy),
    SavedBinary(tidb_datatype::BinaryLiteral, NativeBinaryLiteralRender),
    Constant(usize, LiteralPolicy),
    Text(Box<str>),
    Column(Box<str>, i64),
    Binary(NativeArithmeticOperator),
    Function(Box<str>, usize),
    CastJson(Option<Box<FieldType>>),
    Stop,
}
#[derive(Debug)]
pub(crate) struct DynamicDiagnostic {
    instructions: Box<[Instruction]>,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DiagnosticStep {
    NeedConstant(usize),
    Done(Option<String>),
}
#[derive(Debug)]
pub(crate) struct DiagnosticState {
    pc: usize,
    waiting: Option<(usize, LiteralPolicy)>,
    stack: Vec<String>,
    done: bool,
}
fn runtime_resource(message: &'static str) -> EvalError {
    EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
        tidb_query_expr::local::LocalError::ResourceLimit(message.into()),
        None,
    ))
}

impl LiteralPolicy {
    pub(crate) fn render(self, value: &Datum) -> Result<Option<String>, EvalError> {
        let supported = match self {
            Self::Math => matches!(
                value,
                Datum::Null
                    | Datum::Int(_)
                    | Datum::UInt(_)
                    | Datum::Real(_)
                    | Datum::Float32(_)
                    | Datum::Decimal(_)
            ),
            Self::Numeric(_) => matches!(
                value,
                Datum::Null
                    | Datum::Int(_)
                    | Datum::UInt(_)
                    | Datum::Real(_)
                    | Datum::Float32(_)
                    | Datum::Decimal(_)
                    | Datum::BinaryLiteral(_)
                    | Datum::String(_)
                    | Datum::Bytes(_)
                    | Datum::Json(_)
            ),
        };
        if !supported {
            return Ok(None);
        }
        // The original formatter remains the owner of literal spelling and
        // malformed-prefix panics. Bound only genuinely demanded formatting.

        let text = match self {
            Self::Numeric(float) => numeric_diagnostic_literal(value, float),
            Self::Math => math_diagnostic_literal(value),
        };

        Ok(text)
    }
}
enum Part<'a> {
    Literal(&'a Datum, LiteralPolicy),
    SavedBinary(&'a tidb_datatype::BinaryLiteral, NativeBinaryLiteralRender),
    Constant(usize, LiteralPolicy),
    Text(&'a str),
    Column(&'a str, i64),
    Binary(NativeArithmeticOperator),
    Function(&'a str, usize),
    CastJson(Option<&'a FieldType>),
    Stop,
}
enum Work<'a> {
    Expression(&'a Expression),
    Emit(Part<'a>),
}
pub(crate) struct DiagnosticRecipe<'a> {
    parts: Vec<Part<'a>>,
    bindings: Vec<&'a Expression>,
    bytes: usize,
    policy: LiteralPolicy,
}
impl<'a> DiagnosticRecipe<'a> {
    pub(crate) fn bindings(&self) -> &[&'a Expression] {
        &self.bindings
    }
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
    pub(crate) fn policy(&self) -> LiteralPolicy {
        self.policy
    }
    pub(crate) fn freeze(self) -> Result<(DynamicDiagnostic, Vec<&'a Expression>), EvalError> {
        let resource = || runtime_resource("diagnostic snapshot allocation failed");
        let mut instructions = Vec::new();
        instructions
            .try_reserve_exact(self.parts.len())
            .map_err(|_| resource())?;
        for part in self.parts {
            instructions.push(match part {
                Part::Literal(value, policy) => Instruction::Literal(value.clone(), policy),
                Part::SavedBinary(value, mode) => Instruction::SavedBinary(value.clone(), mode),
                Part::Constant(index, policy) => Instruction::Constant(index, policy),
                Part::Text(text) => Instruction::Text(text.into()),
                Part::Column(name, id) => Instruction::Column(name.into(), id),
                Part::Binary(op) => Instruction::Binary(op),
                Part::Function(name, arity) => Instruction::Function(name.into(), arity),
                Part::CastJson(field) => Instruction::CastJson(field.map(|field| {
                    Box::new(tidb_datatype::tikv_compat::value::snapshot_field_type(
                        field,
                    ))
                })),
                Part::Stop => Instruction::Stop,
            });
        }
        Ok((
            DynamicDiagnostic {
                instructions: instructions.into_boxed_slice(),
            },
            self.bindings,
        ))
    }
}
impl DynamicDiagnostic {
    pub(crate) fn capture_numeric<'a>(
        expression: &'a Expression,
        float: bool,
        max_nodes: usize,
        max_bytes: usize,
    ) -> Result<(Self, Vec<&'a Expression>), EvalError> {
        Self::survey(
            Some(expression),
            None,
            LiteralPolicy::Numeric(float),
            max_nodes,
            max_bytes,
        )?
        .freeze()
    }
    pub(crate) fn survey_numeric<'a>(
        expression: &'a Expression,
        float: bool,
        max_nodes: usize,
        max_bytes: usize,
    ) -> Result<DiagnosticRecipe<'a>, EvalError> {
        Self::survey(
            Some(expression),
            None,
            LiteralPolicy::Numeric(float),
            max_nodes,
            max_bytes,
        )
    }
    pub(crate) fn capture_math<'a>(
        function: &'a ScalarFunction,
        max_nodes: usize,
        max_bytes: usize,
    ) -> Result<(Self, Vec<&'a Expression>), EvalError> {
        Self::survey(
            None,
            Some(function),
            LiteralPolicy::Math,
            max_nodes,
            max_bytes,
        )?
        .freeze()
    }
    fn survey<'a>(
        expression: Option<&'a Expression>,
        root: Option<&'a ScalarFunction>,
        policy: LiteralPolicy,
        _max_nodes: usize,
        _max_bytes: usize,
    ) -> Result<DiagnosticRecipe<'a>, EvalError> {
        let resource = || runtime_resource("diagnostic metadata extent overflow");
        let mut bytes = 0usize;
        let mut claim = |n: usize| -> Result<(), EvalError> {
            bytes = bytes.checked_add(n).ok_or_else(&resource)?;
            Ok(())
        };
        let mut pending = Vec::new();
        let mut parts = Vec::new();
        let mut bindings = Vec::new();
        let scheduled = root.map_or(1, |f| f.args.len());

        // Bound scaffolding before reserving. No source payload/type/name is
        // cloned or rendered until the complete survey has been accepted.
        claim(
            scheduled
                .checked_add(1)
                .and_then(|n| n.checked_mul(std::mem::size_of::<Work<'_>>()))
                .ok_or_else(&resource)?,
        )?;
        pending
            .try_reserve_exact(scheduled + 1)
            .map_err(|_| resource())?;
        if let Some(root) = root {
            pending.push(Work::Emit(Part::Function(
                root.func_name.lowercase(),
                root.args.len(),
            )));
            pending.extend(root.args.iter().rev().map(Work::Expression));
        } else {
            pending.push(Work::Expression(expression.expect("diagnostic root")));
        }
        while let Some(work) = pending.pop() {
            let mut emit = None;
            match work {
                Work::Emit(part) => emit = Some(part),
                Work::Expression(expression) => match expression {
                    Expression::Constant(c) => {
                        emit = Some(if let Some(value) = c.literal_value() {
                            claim(crate::tikv::identity_actual::copy_extent(value)?)?;
                            Part::Literal(value, policy)
                        } else {
                            claim(std::mem::size_of::<&Expression>())?;
                            let index = bindings.len();
                            bindings.try_reserve_exact(1).map_err(|_| resource())?;
                            bindings.push(expression);
                            Part::Constant(index, policy)
                        });
                    }
                    Expression::Column(c) => {
                        if matches!(policy, LiteralPolicy::Numeric(_)) && c.is_hidden {
                            if let Some(child) = c.virtual_expr.as_deref() {
                                claim(std::mem::size_of::<Work<'_>>())?;
                                pending.try_reserve_exact(1).map_err(|_| resource())?;
                                pending.push(Work::Expression(child));
                                continue;
                            }
                        }
                        emit = Some(match policy {
                            LiteralPolicy::Math if c.orig_name.is_empty() => Part::Stop,
                            LiteralPolicy::Math => Part::Text(&c.orig_name),
                            LiteralPolicy::Numeric(_) => Part::Column(&c.orig_name, c.unique_id),
                        });
                    }
                    Expression::CorrelatedColumn(c) => {
                        emit = Some(match policy {
                            LiteralPolicy::Math if c.column.orig_name.is_empty() => Part::Stop,
                            LiteralPolicy::Math => Part::Text(&c.column.orig_name),
                            LiteralPolicy::Numeric(_) => {
                                Part::Column(&c.column.orig_name, c.column.unique_id)
                            }
                        });
                    }
                    Expression::ScalarFunction(f) => {
                        let mut children: &[Expression] = &[];
                        let instruction = if matches!(policy, LiteralPolicy::Numeric(_))
                            && f.func_name.lowercase() == "cast_json"
                        {
                            if f.args.len() == 1 {
                                children = &f.args;
                                Part::CastJson(f.get_static_type())
                            } else {
                                Part::Stop
                            }
                        } else if let Some(op) = binary_op_for_name(f.func_name.lowercase()) {
                            if let Some(op) =
                                native_arithmetic_operator(op).filter(|_| f.args.len() == 2)
                            {
                                children = &f.args;
                                Part::Binary(op)
                            } else {
                                Part::Stop
                            }
                        } else if matches!(policy, LiteralPolicy::Math) {
                            children = &f.args;
                            Part::Function(f.func_name.lowercase(), f.args.len())
                        } else {
                            Part::Stop
                        };

                        let count = children.len().checked_add(1).ok_or_else(&resource)?;
                        claim(
                            count
                                .checked_mul(std::mem::size_of::<Work<'_>>())
                                .ok_or_else(&resource)?,
                        )?;
                        pending.try_reserve_exact(count).map_err(|_| resource())?;
                        pending.push(Work::Emit(instruction));
                        pending.extend(children.iter().rev().map(Work::Expression));
                    }
                },
            }
            if let Some(part) = emit {
                claim(
                    std::mem::size_of::<Part<'_>>()
                        .checked_add(std::mem::size_of::<Instruction>())
                        .ok_or_else(&resource)?,
                )?;
                match &part {
                    Part::Text(text) | Part::Column(text, _) | Part::Function(text, _) => {
                        claim(text.len())?
                    }
                    Part::CastJson(Some(field)) => claim(
                        field
                            .checked_snapshot_payload_bytes()
                            .ok_or_else(&resource)?,
                    )?,
                    _ => (),
                }
                parts.try_reserve_exact(1).map_err(|_| resource())?;
                parts.push(part);
            }
        }
        drop(claim);
        Ok(DiagnosticRecipe {
            parts,
            bindings,
            bytes,
            policy,
        })
    }
    pub(crate) fn survey_math<'a>(
        function: &'a ScalarFunction,
        max_nodes: usize,
        max_bytes: usize,
    ) -> Result<DiagnosticRecipe<'a>, EvalError> {
        Self::survey(
            None,
            Some(function),
            LiteralPolicy::Math,
            max_nodes,
            max_bytes,
        )
    }
    pub(crate) fn state(&self) -> DiagnosticState {
        DiagnosticState {
            pc: 0,
            waiting: None,
            stack: vec![],
            done: false,
        }
    }
    pub(crate) fn step(&self, state: &mut DiagnosticState) -> Result<DiagnosticStep, EvalError> {
        if state.done {
            return Err(EvalError::Unsupported("diagnostic result already consumed"));
        }
        if let Some((index, _)) = state.waiting {
            return Ok(DiagnosticStep::NeedConstant(index));
        }
        while let Some(instruction) = self.instructions.get(state.pc) {
            state.pc += 1;
            let text = match instruction {
                Instruction::Constant(index, policy) => {
                    state.waiting = Some((*index, *policy));
                    return Ok(DiagnosticStep::NeedConstant(*index));
                }
                Instruction::Stop => return Ok(state.unavailable()),
                Instruction::Literal(value, policy) => policy.render(value)?,
                Instruction::SavedBinary(value, mode) => {
                    let value = value.to_int().value();
                    Some(match mode {
                        NativeBinaryLiteralRender::Signed => (value as i64).to_string(),
                        NativeBinaryLiteralRender::Real => {
                            tidb_datatype::format_float_g_shortest(value as f64)
                        }
                        NativeBinaryLiteralRender::Unsigned => value.to_string(),
                    })
                }
                Instruction::Text(text) => Some(text.to_string()),
                Instruction::Column(name, id) => Some(native_render_column_reference(name, *id)),
                Instruction::Binary(op) => {
                    let right = state.stack.pop().expect("diagnostic right");
                    let left = state.stack.pop().expect("diagnostic left");

                    Some(native_render_binary_expression(&left, *op, &right))
                }
                Instruction::Function(name, arity) => {
                    let begin = state
                        .stack
                        .len()
                        .checked_sub(*arity)
                        .expect("diagnostic function arity");

                    let text = native_render_function_expression(name, &state.stack[begin..]);
                    state.stack.truncate(begin);
                    Some(text)
                }
                Instruction::CastJson(target) => {
                    let child = state.stack.pop().expect("diagnostic cast child");
                    if let Some(target) = target {
                        Some(native_render_cast_expression(
                            &child,
                            &target.source_string(),
                        ))
                    } else {
                        None
                    }
                }
            };
            let Some(text) = text else {
                return Ok(state.unavailable());
            };
            state.push(text)?;
        }
        state.done = true;
        assert_eq!(state.stack.len(), 1, "diagnostic root stack");
        Ok(DiagnosticStep::Done(state.stack.pop()))
    }
    pub(crate) fn resume(
        &self,
        state: &mut DiagnosticState,
        index: usize,
        value: Option<&Datum>,
    ) -> Result<DiagnosticStep, EvalError> {
        let policy = state.accept(index)?;
        let Some(value) = value else {
            return Ok(state.unavailable());
        };
        let Some(text) = policy.render(value)? else {
            return Ok(state.unavailable());
        };
        state.push(text)?;
        self.step(state)
    }
    pub(crate) fn resume_text(
        &self,
        state: &mut DiagnosticState,
        index: usize,
        text: Option<&str>,
    ) -> Result<DiagnosticStep, EvalError> {
        state.accept(index)?;
        let Some(text) = text else {
            return Ok(state.unavailable());
        };

        state.push(text.to_owned())?;
        self.step(state)
    }
}
impl DiagnosticState {
    pub(crate) fn pending_constant(&self) -> Option<usize> {
        self.waiting.map(|(index, _)| index)
    }
    pub(crate) fn retained_bytes(&self) -> Result<usize, EvalError> {
        self.stack.iter().try_fold(
            self.stack
                .capacity()
                .checked_mul(std::mem::size_of::<String>())
                .ok_or_else(|| runtime_resource("diagnostic stack extent overflow"))?,
            |n, text| {
                n.checked_add(text.capacity())
                    .ok_or_else(|| runtime_resource("diagnostic text extent overflow"))
            },
        )
    }

    fn unavailable(&mut self) -> DiagnosticStep {
        self.done = true;
        DiagnosticStep::Done(None)
    }
    fn accept(&mut self, index: usize) -> Result<LiteralPolicy, EvalError> {
        let Some((expected, policy)) = self.waiting else {
            return Err(EvalError::Unsupported("diagnostic has no pending read"));
        };
        if expected != index || self.done {
            return Err(EvalError::Unsupported(
                "diagnostic reply differs from demand",
            ));
        }
        self.waiting = None;
        Ok(policy)
    }
    fn push(&mut self, text: String) -> Result<(), EvalError> {
        self.stack
            .try_reserve_exact(1)
            .map_err(|_| runtime_resource("diagnostic stack allocation failed"))?;
        self.stack.push(text);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dynamic_diagnostic_sparse_decimal_format_is_metered_before_formatter() {
        let value = Datum::Decimal(tidb_datatype::Decimal::from_raw_parts(
            false,
            vec![b'1'],
            1_000_000,
            1_000_000,
        ));
        for policy in [
            LiteralPolicy::Math,
            LiteralPolicy::Numeric(false),
            LiteralPolicy::Numeric(true),
        ] {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tidb_query_expr::local::with_value_kernel_work_limit(65536, || {
                    policy.render(&value)
                })
            }));
            assert!(
                result.is_err(),
                "the malformed native Decimal formatter must unwind"
            );
        }
        assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
    }
}

#[derive(Clone, Copy)]
enum ArgumentSourceType {
    Requested,
    Fixed(EvalType),
}
#[derive(Clone, Copy)]
pub(crate) struct ArgumentSourceFacts {
    field: Option<ArgumentSourceType>,
    binary: bool,
}
pub(crate) fn saved_binary_literal(expression: &Expression) -> bool {
    matches!(expression,Expression::Constant(c) if matches!(c.value,Datum::BinaryLiteral(_)))
}
impl ArgumentSourceFacts {
    pub(crate) fn capture(expression: &Expression) -> Self {
        let binary = saved_binary_literal(expression);
        let field = expression.static_type().map(|f| {
            if binary {
                ArgumentSourceType::Requested
            } else {
                ArgumentSourceType::Fixed(if f.is_hybrid() {
                    EvalType::Int
                } else {
                    f.eval_type()
                })
            }
        });
        Self { field, binary }
    }
    pub(crate) fn binary(self) -> bool {
        self.binary
    }
    pub(crate) fn source(self, target: EvalType) -> Option<EvalType> {
        self.field.map(|source| match source {
            ArgumentSourceType::Requested => target,
            ArgumentSourceType::Fixed(source) => source,
        })
    }
}
/// The diagnostic belongs to the original argument, including construction
/// facts that are independent of the parameter's current value.
pub(crate) struct ArgumentDiagnostic {
    plan: DynamicDiagnostic,
    wrap: Option<(Box<FieldType>, bool)>,
}
pub(crate) struct ArgumentRecipe<'a> {
    plan: DiagnosticRecipe<'a>,
    wrap: Option<(&'a FieldType, bool)>,
    bytes: usize,
}
impl<'a> ArgumentRecipe<'a> {
    pub(crate) fn bindings(&self) -> &[&'a Expression] {
        self.plan.bindings()
    }
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
    pub(crate) fn freeze(self) -> Result<(ArgumentDiagnostic, Vec<&'a Expression>), EvalError> {
        let (plan, bindings) = self.plan.freeze()?;
        let wrap = self.wrap.map(|(field, refine)| {
            (
                Box::new(tidb_datatype::tikv_compat::value::snapshot_field_type(
                    field,
                )),
                refine,
            )
        });
        Ok((ArgumentDiagnostic { plan, wrap }, bindings))
    }
}
impl ArgumentDiagnostic {
    pub(crate) fn survey<'a>(
        function: &ScalarFunction,
        expression: &'a Expression,
        float: bool,
        max_nodes: usize,
        max_bytes: usize,
    ) -> Result<ArgumentRecipe<'a>, EvalError> {
        let policy = LiteralPolicy::Numeric(float);
        let field = expression.static_type();
        let special = if field.is_none() {
            Some((Part::Stop, 0))
        } else if let Expression::Constant(constant) = expression {
            if let Datum::BinaryLiteral(value) = &constant.value {
                Some((
                    Part::SavedBinary(
                        value,
                        native_binary_literal_render(
                            native_numeric_diagnostic_domain(function.numeric_operand_domain()),
                            field.unwrap().is_unsigned(),
                        ),
                    ),
                    value.as_bytes().len(),
                ))
            } else {
                None
            }
        } else {
            None
        };
        let plan = if let Some((part, payload)) = special {
            let bytes = std::mem::size_of::<Part<'_>>()
                .checked_add(std::mem::size_of::<Instruction>())
                .and_then(|n| n.checked_add(std::mem::size_of::<Work<'_>>()))
                .and_then(|n| n.checked_add(payload))
                .ok_or_else(|| {
                    runtime_resource("numeric argument diagnostic metadata extent overflow")
                })?;

            DiagnosticRecipe {
                parts: vec![part],
                bindings: vec![],
                bytes,
                policy,
            }
        } else {
            DynamicDiagnostic::survey(Some(expression), None, policy, max_nodes, max_bytes)?
        };
        let wrap = field
            .filter(|field| {
                native_should_wrap_intdiv_decimal_argument(
                    function.func_name.lowercase() == "intdiv",
                    native_numeric_diagnostic_domain(function.numeric_operand_domain()),
                    field.eval_type() == EvalType::Decimal,
                    matches!(expression,Expression::Constant(c) if c.literal_value().is_some()),
                )
            })
            .map(|field| (field, expression.const_level() == ConstLevel::STRICT));
        let bytes = plan
            .bytes()
            .checked_add(std::mem::size_of::<ArgumentDiagnostic>())
            .and_then(|n| n.checked_add(std::mem::size_of::<ArgumentRecipe<'_>>()))
            .and_then(|n| {
                if let Some((field, _)) = wrap {
                    n.checked_add(std::mem::size_of::<FieldType>())?
                        .checked_add(field.checked_snapshot_payload_bytes()?)
                } else {
                    Some(n)
                }
            })
            .ok_or_else(|| runtime_resource("numeric argument wrapper metadata extent overflow"))?;
        Ok(ArgumentRecipe { plan, wrap, bytes })
    }
    pub(crate) fn state(&self) -> DiagnosticState {
        self.plan.state()
    }
    fn finish_step(
        &self,
        step: DiagnosticStep,
        decimal: Option<&tidb_datatype::Decimal>,
    ) -> Result<DiagnosticStep, EvalError> {
        match (step, &self.wrap) {
            (DiagnosticStep::Done(Some(text)), Some((field, refine))) => Ok(DiagnosticStep::Done(
                Some(native_intdiv_argument_text(text, field, *refine, decimal)),
            )),
            (step, _) => Ok(step),
        }
    }
    pub(crate) fn step(
        &self,
        state: &mut DiagnosticState,
        decimal: Option<&tidb_datatype::Decimal>,
    ) -> Result<DiagnosticStep, EvalError> {
        self.finish_step(self.plan.step(state)?, decimal)
    }
    pub(crate) fn resume(
        &self,
        state: &mut DiagnosticState,
        index: usize,
        value: Option<&Datum>,
        decimal: Option<&tidb_datatype::Decimal>,
    ) -> Result<DiagnosticStep, EvalError> {
        self.finish_step(self.plan.resume(state, index, value)?, decimal)
    }
    pub(crate) fn resume_text(
        &self,
        state: &mut DiagnosticState,
        index: usize,
        text: Option<&str>,
        decimal: Option<&tidb_datatype::Decimal>,
    ) -> Result<DiagnosticStep, EvalError> {
        self.finish_step(self.plan.resume_text(state, index, text)?, decimal)
    }
}

#[cfg(test)]
mod argument_tests {
    use super::*;
    #[test]
    fn numeric_argument_recipe_bounds_saved_payload_and_skips_it_without_a_declaration() {
        let mut constant = crate::constant::Constant::new(
            Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![255; 1_000_000])),
            FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        constant.param_marker = Some(crate::constant::ParamMarker { order: 0 });
        let mut function = ScalarFunction::new(
            CiString::new("plus"),
            FieldType::new(tidb_datatype::FieldTypeCode::Double),
            vec![
                Expression::Constant(constant),
                Expression::Constant(crate::constant::Constant::new(
                    Datum::Int(1),
                    FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                )),
            ],
        );
        let _ = ArgumentDiagnostic::survey(&function, &function.args[0], true, 16, 4096)
            .unwrap()
            .freeze()
            .unwrap();
        let Expression::Constant(c) = &mut function.args[0] else {
            unreachable!()
        };
        c.ret_type = None;
        let recipe =
            ArgumentDiagnostic::survey(&function, &function.args[0], true, 16, 4096).unwrap();
        assert!(recipe.bindings().is_empty());
        let (plan, bindings) = recipe.freeze().unwrap();
        assert!(bindings.is_empty());
        let mut state = plan.state();
        assert_eq!(
            tidb_query_expr::local::with_value_kernel_work_limit(1, || plan.step(&mut state, None))
                .unwrap(),
            DiagnosticStep::Done(None)
        );
        assert!(plan.step(&mut state, None).is_err());
        let facts = ArgumentSourceFacts::capture(&function.args[0]);
        assert!(facts.binary());
        assert_eq!(facts.source(EvalType::Real), None);
        let Expression::Constant(c) = &mut function.args[0] else {
            unreachable!()
        };
        c.value = Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![255, 1]));
        c.ret_type = Some(FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
        let recipe =
            ArgumentDiagnostic::survey(&function, &function.args[0], true, 16, 4096).unwrap();
        let (plan, _) = recipe.freeze().unwrap();
        let mut state = plan.state();
        assert_eq!(
            tidb_query_expr::local::with_value_kernel_work_limit(4096, || plan
                .step(&mut state, None))
            .unwrap(),
            DiagnosticStep::Done(Some("65281".into()))
        );
        assert!(tidb_query_expr::local::value_kernel_work_remaining().is_none());
        let _ = ArgumentDiagnostic::survey(&function, &function.args[0], true, 0, 4096)
            .unwrap()
            .freeze()
            .unwrap();
    }
}
