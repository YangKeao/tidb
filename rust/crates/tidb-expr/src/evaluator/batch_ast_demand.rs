// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Source-specific demand continuations; every child is supplied by HostFrame.
use super::*;
use crate::func::{benchmark_loop_count, ensure_benchmark_eval_type, negate_if};
use crate::tikv::{case_after_truth, case_start, CaseDemand};
use tidb_ast::{BinaryOp, CastStyle};

pub(in crate::evaluator::batch) enum AstProgress {
    Need(usize),
    Prepare { aes: bool },
    Ready(Datum),
    Cipher(crate::tikv::EvaluatedBytesOp, crate::tikv::EvaluatedArgs),
    Regexp(crate::tikv::EvaluatedArgs),
}
enum Stage {
    Start,
    Consumed,
    Eager,
    Select,
    If,
    IfNull,
    Coalesce,
    EagerBetween,
    SequenceSet,
    BetweenLow,
    BetweenHigh,
    PositionLeft,
    PositionRight(Option<String>),
    TrimLeft,
    TrimRight {
        text: Option<Vec<u8>>,
        binary: bool,
    },
    CaseBase,
    CaseCondition(usize),
    In {
        candidate: usize,
        left_count: usize,
        right_count: usize,
        membership: tidb_query_expr::NativeInMembership,
    },
    Length(crate::BuiltStringLength, crate::StringLengthFunction),
    BenchmarkCount {
        count: Option<FieldType>,
        expression: Option<FieldType>,
    },
    BenchmarkLoop(i64),
    Aes(crate::builtin_ext::crypto::AesPreparation),
    RegexpLike,
}
pub(in crate::evaluator::batch) struct AstTask {
    pub(in crate::evaluator::batch) next: Option<usize>,
    stage: Stage,
    raw_string: bool,
    values: Vec<Datum>,
    strings: Vec<String>,
    pub(in crate::evaluator::batch) tokens: Vec<(usize, [u8; native_values::HANDLE_BYTES])>,
}
impl AstTask {
    pub(in crate::evaluator::batch) fn new(raw_string: bool) -> Self {
        Self {
            next: None,
            stage: Stage::Start,
            raw_string,
            values: Vec::new(),
            strings: Vec::new(),
            tokens: Vec::new(),
        }
    }
    pub(in crate::evaluator::batch) fn reserve_reply(&mut self) -> LocalResult<()> {
        self.tokens
            .try_reserve_exact(1)
            .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
        self.values
            .try_reserve_exact(1)
            .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
        self.strings
            .try_reserve_exact(1)
            .map_err(|e| LocalError::ResourceLimit(e.to_string()))?;
        Ok(())
    }
    pub(in crate::evaluator::batch) fn retained_bytes(&self) -> LocalResult<usize> {
        let mut bytes = size_of::<Self>()
            .checked_add(array_bytes::<Datum>(self.values.capacity())?)
            .and_then(|n| n.checked_add(array_bytes::<String>(self.strings.capacity()).ok()?))
            .ok_or_else(|| resource("AST value layout overflow"))?;
        for value in &self.values {
            bytes = bytes
                .checked_add(
                    native_payload_bytes(value)?
                        .checked_mul(2)
                        .ok_or_else(|| resource("AST payload overflow"))?,
                )
                .ok_or_else(|| resource("AST retained extent overflow"))?;
        }
        for text in &self.strings {
            bytes = bytes
                .checked_add(text.capacity())
                .ok_or_else(|| resource("AST string extent overflow"))?;
        }
        bytes = bytes
            .checked_add(array_bytes::<(usize, [u8; native_values::HANDLE_BYTES])>(
                self.tokens.capacity(),
            )?)
            .ok_or_else(|| resource("AST capability extent overflow"))?;
        let extra = match &self.stage {
            Stage::PositionRight(v) => v.as_ref().map_or(0, String::capacity),
            Stage::TrimRight { text, .. } => text.as_ref().map_or(0, Vec::capacity),
            Stage::Aes(aes) => aes
                .owned_payload_bytes()
                .ok_or_else(|| resource("AST AES extent overflow"))?,
            _ => 0,
        };
        bytes
            .checked_add(extra)
            .ok_or_else(|| resource("AST staged extent overflow"))
    }
    fn need(&mut self, index: usize, stage: Stage) -> AstProgress {
        self.next = Some(index);
        self.stage = stage;
        AstProgress::Need(index)
    }
    fn ready(&mut self, value: Datum) -> AstProgress {
        self.next = None;
        AstProgress::Ready(value)
    }
    fn case_next(
        &mut self,
        root: &Expr,
        demand: CaseDemand,
    ) -> std::result::Result<AstProgress, EvalError> {
        let Expr::Case {
            value,
            when_clauses,
            ..
        } = root
        else {
            unreachable!()
        };
        let offset = usize::from(value.is_some());
        Ok(match demand {
            CaseDemand::Condition(i) => self.need(offset + 2 * i, Stage::CaseCondition(i)),
            CaseDemand::Value(i) => self.need(
                if i == when_clauses.len() {
                    offset + 2 * i
                } else {
                    offset + 2 * i + 1
                },
                Stage::Select,
            ),
            CaseDemand::Exhausted => self.ready(crate::tikv::exhausted_case_value()?),
        })
    }
    /// The caller moves this state out of its authenticated task BEFORE callbacks.
    /// An error or unwind consumes the phase; it cannot replay a prior side effect.
    pub(in crate::evaluator::batch) fn advance(
        &mut self,
        root: &Expr,
        arity: usize,
        incoming: Option<Datum>,
        ctx: &dyn Columns,
    ) -> std::result::Result<AstProgress, EvalError> {
        let stage = std::mem::replace(&mut self.stage, Stage::Consumed);
        let index = self.next.take();
        if matches!(stage, Stage::Start) {
            return self.start(root, arity, ctx);
        }
        let value = incoming.expect("authenticated demanded AST child");
        let index = index.expect("live AST phase");
        match stage {
            Stage::Eager => {
                self.values.push(value);
                if index + 1 < arity {
                    Ok(self.need(index + 1, Stage::Eager))
                } else {
                    let value = value::finish(
                        root,
                        std::mem::take(&mut self.values),
                        ctx,
                        self.raw_string,
                    )?;
                    Ok(self.ready(value))
                }
            }
            Stage::Select => Ok(self.ready(crate::tikv::selected_control_value(value)?)),
            Stage::If => {
                let branch = tidb_query_expr::native_if_choose_branch(crate::truthy_of(&value)?);
                Ok(self.need(
                    if branch == tidb_query_expr::NativeIfBranch::Then {
                        1
                    } else {
                        2
                    },
                    Stage::Select,
                ))
            }
            Stage::IfNull => match tidb_query_expr::native_if_null_choose_first(
                (!value.is_null()).then_some(value),
            ) {
                tidb_query_expr::NativeIfNullChoice::Done(value) => {
                    Ok(self.ready(crate::tikv::selected_control_value(value)?))
                }
                tidb_query_expr::NativeIfNullChoice::NeedSecond => Ok(self.need(1, Stage::Select)),
            },
            Stage::Coalesce => {
                if !value.is_null() {
                    // This is the existing copy/exhaustion boundary, with one
                    // already selected actual value and no expression callback.
                    let result = crate::tikv::eval_coalesce_in(ctx, 1, |_, _| Ok(&value))?;
                    Ok(self.ready(result))
                } else if index + 1 < arity {
                    Ok(self.need(index + 1, Stage::Coalesce))
                } else {
                    Ok(self.ready(crate::tikv::eval_coalesce_in::<Datum>(
                        ctx,
                        0,
                        |_, _| unreachable!(),
                    )?))
                }
            }
            Stage::EagerBetween => {
                self.values.push(value);
                Ok(self.need(1, Stage::BetweenLow))
            }
            Stage::SequenceSet => {
                let Expr::Func { args, .. } = root else {
                    unreachable!()
                };
                let Expr::Column(path) = &args[0] else {
                    unreachable!()
                };
                let value = match value {
                    Datum::Null => Datum::Null,
                    Datum::Int(v) => ctx.sequence_setval(path, v)?,
                    Datum::UInt(v) => ctx.sequence_setval(path, v as i64)?,
                    _ => return Err(EvalError::Unsupported("SETVAL value")),
                };
                Ok(self.ready(value))
            }
            Stage::BetweenLow => {
                let ge =
                    crate::ops::eval_binary_in(BinaryOp::Ge, self.values[0].clone(), value, ctx)?;
                self.values.push(ge);
                Ok(self.need(2, Stage::BetweenHigh))
            }
            Stage::BetweenHigh => {
                let ge = self.values.pop().expect("original lower comparison");
                let v = self.values.pop().expect("original base");
                let le = crate::ops::eval_binary_in(BinaryOp::Le, v, value, ctx)?;
                let Expr::Between { not, .. } = root else {
                    unreachable!()
                };
                Ok(self.ready(negate_if(crate::logic_and(ge, le, ctx)?, *not, ctx)?))
            }
            Stage::PositionLeft => {
                Ok(self.need(1, Stage::PositionRight(crate::coerce_str(&value)?)))
            }
            Stage::PositionRight(needle) => Ok(self.ready(crate::string_fn::position_in(
                needle,
                crate::coerce_str(&value)?,
                ctx,
            )?)),
            Stage::TrimLeft => {
                let binary = matches!(value, Datum::Bytes(_));
                let text = crate::coerce_str_bytes(&value)?;
                let Expr::Trim {
                    remstr, direction, ..
                } = root
                else {
                    unreachable!()
                };
                if remstr.is_some() {
                    Ok(self.need(1, Stage::TrimRight { text, binary }))
                } else {
                    Ok(self.ready(crate::string_fn::trim_value_in(
                        text,
                        Some(b" ".to_vec()),
                        direction.unwrap_or(tidb_ast::TrimDirection::Both),
                        binary,
                        ctx,
                    )?))
                }
            }
            Stage::TrimRight { text, binary } => {
                let Expr::Trim { direction, .. } = root else {
                    unreachable!()
                };
                Ok(self.ready(crate::string_fn::trim_value_in(
                    text,
                    crate::coerce_str_bytes(&value)?,
                    direction.unwrap_or(tidb_ast::TrimDirection::Both),
                    binary,
                    ctx,
                )?))
            }
            Stage::CaseBase => {
                self.values.push(value);
                let Expr::Case {
                    when_clauses,
                    else_clause,
                    ..
                } = root
                else {
                    unreachable!()
                };
                self.case_next(root, case_start(when_clauses.len(), else_clause.is_some()))
            }
            Stage::CaseCondition(pair) => {
                let Expr::Case {
                    value: base,
                    when_clauses,
                    else_clause,
                } = root
                else {
                    unreachable!()
                };
                let truth = if base.is_some() {
                    let equal = crate::ops::eval_binary_in(
                        BinaryOp::Eq,
                        self.values[0].clone(),
                        value,
                        ctx,
                    )?;
                    match equal {
                        Datum::Null => None,
                        _ => Some(equal == Datum::Int(1)),
                    }
                } else {
                    crate::truthy_of(&value)?
                };
                self.case_next(
                    root,
                    case_after_truth(when_clauses.len(), else_clause.is_some(), pair, truth),
                )
            }
            Stage::Length(built, function) => {
                let value = match function {
                    crate::StringLengthFunction::Length => {
                        crate::convert_charset::to_binary_by_collation_in(&value, ctx)?
                    }
                    crate::StringLengthFunction::CharLength => value,
                };
                Ok(self.ready(built.eval_in(&value, ctx)?))
            }
            Stage::BenchmarkCount { count, expression } => {
                let Some(count) = benchmark_loop_count(value, count.as_ref(), ctx)? else {
                    return Ok(self.ready(Datum::Null));
                };
                if count < 0 {
                    return Ok(self.ready(Datum::Null));
                }
                ensure_benchmark_eval_type(expression.as_ref())?;
                if count == 0 {
                    Ok(self.ready(Datum::Int(0)))
                } else {
                    Ok(self.need(1, Stage::BenchmarkLoop(count)))
                }
            }
            Stage::BenchmarkLoop(remaining) => {
                if remaining == 1 {
                    Ok(self.ready(Datum::Int(0)))
                } else {
                    Ok(self.need(1, Stage::BenchmarkLoop(remaining - 1)))
                }
            }
            Stage::Aes(mut aes) => match aes.accept(&value, ctx)? {
                Some((op, args)) => Ok(AstProgress::Cipher(op, args)),
                None => Ok(self.need(aes.next_argument(), Stage::Aes(aes))),
            },
            Stage::RegexpLike => {
                let value = crate::cast::cast_arg_as_string(&value, None, ctx)?;
                if value.is_null() {
                    return Ok(AstProgress::Regexp(
                        crate::tikv::EvaluatedArgs::NullWitness(None),
                    ));
                }
                self.strings.push(
                    value.sql_string().map_err(|_| {
                        EvalError::Unsupported("invalid UTF-8 REGEXP_LIKE argument")
                    })?,
                );
                if index + 1 < arity {
                    return Ok(self.need(index + 1, Stage::RegexpLike));
                }
                let mut strings = std::mem::take(&mut self.strings).into_iter();
                let text = strings.next().expect("actual regexp text");
                let pattern = strings.next().expect("actual regexp pattern");
                let match_type = crate::regexp::regexp_match_type_with_collation(
                    &strings.next().unwrap_or_default(),
                    crate::ops::DERIVATION_FREE_COLLATION,
                );
                Ok(AstProgress::Regexp(
                    crate::tikv::EvaluatedArgs::RegexpLike {
                        invocation: tidb_query_expr::NativeRegexpInvocation::new(
                            &Default::default(),
                            &Default::default(),
                            0,
                            false,
                            false,
                        ),
                        text: text.into_bytes(),
                        pattern: pattern.into_bytes(),
                        match_type: match_type.into_bytes(),
                    },
                ))
            }
            Stage::In {
                candidate,
                left_count,
                right_count,
                membership,
            } => {
                self.values.push(value);
                self.advance_in(
                    root,
                    index + 1,
                    candidate,
                    left_count,
                    right_count,
                    membership,
                    ctx,
                )
            }
            Stage::Start | Stage::Consumed => unreachable!("consumed AST phase"),
        }
    }
    pub(in crate::evaluator::batch) fn prepare(
        &mut self,
        root: &Expr,
        aes: bool,
        ctx: &dyn Columns,
    ) -> std::result::Result<AstProgress, EvalError> {
        if aes {
            let Expr::Func { name, args, .. } = root else {
                unreachable!()
            };
            let preparation = crate::builtin_ext::crypto::AesPreparation::new(
                if name.eq_ignore_ascii_case("aes_encrypt") {
                    "aes_encrypt"
                } else {
                    "aes_decrypt"
                },
                args.len(),
                ctx,
            );
            preparation.validate_arity()?;
            Ok(self.need(0, Stage::Aes(preparation)))
        } else {
            Ok(self.need(0, Stage::RegexpLike))
        }
    }
    fn start(
        &mut self,
        root: &Expr,
        arity: usize,
        ctx: &dyn Columns,
    ) -> std::result::Result<AstProgress, EvalError> {
        match root {
            Expr::Cast(cast)
                if matches!(
                    cast.style,
                    CastStyle::DateLiteral | CastStyle::TimeLiteral | CastStyle::TimestampLiteral
                ) =>
            {
                return Err(EvalError::Unsupported("date/time/timestamp literal"))
            }
            Expr::Cast(cast) if cast.array => {
                return Err(EvalError::Unsupported("ARRAY cast type"))
            }
            Expr::Between { .. } => return Ok(self.need(0, Stage::EagerBetween)),
            Expr::Position { .. } => return Ok(self.need(0, Stage::PositionLeft)),
            Expr::Trim { .. } => return Ok(self.need(0, Stage::TrimLeft)),
            Expr::Case {
                value,
                when_clauses,
                else_clause,
            } => {
                if value.is_some() {
                    return Ok(self.need(0, Stage::CaseBase));
                }
                return self.case_next(root, case_start(when_clauses.len(), else_clause.is_some()));
            }
            Expr::In { expr, .. } => {
                let left_count = match &**expr {
                    Expr::Row(v) => v.len(),
                    _ => 1,
                };
                return self.advance_in(root, 0, 0, left_count, 0, Default::default(), ctx);
            }
            Expr::Func { name, args, .. } => {
                let name = name.to_ascii_uppercase();
                let arity_count = if matches!(
                    name.as_str(),
                    "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE"
                ) && matches!(args.as_slice(), [_, Expr::Interval { .. }])
                {
                    3
                } else {
                    args.len()
                };
                crate::builtin_registry::verify_args_by_count(&name, arity_count)?;
                match name.as_str() {
                    "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE" => {
                        if !matches!(args.as_slice(), [_, Expr::Interval { .. }]) {
                            return Err(EvalError::Unsupported("DATE_ADD/DATE_SUB arguments"));
                        }
                    }
                    "NEXTVAL" | "LASTVAL" | "SETVAL" => {
                        let Some(Expr::Column(path)) = args.first() else {
                            return Err(EvalError::Unsupported("sequence function argument"));
                        };
                        match (name.as_str(), args.len()) {
                            ("NEXTVAL", 1) => return Ok(self.ready(ctx.sequence_nextval(path)?)),
                            ("LASTVAL", 1) => return Ok(self.ready(ctx.sequence_lastval(path)?)),
                            ("SETVAL", 2) => return Ok(self.need(0, Stage::SequenceSet)),
                            _ => return Err(EvalError::Unsupported("sequence function arguments")),
                        }
                    }
                    "LENGTH" | "OCTET_LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH"
                        if args.len() == 1 =>
                    {
                        let function = if matches!(name.as_str(), "LENGTH" | "OCTET_LENGTH") {
                            crate::StringLengthFunction::Length
                        } else {
                            crate::StringLengthFunction::CharLength
                        };
                        let built = crate::BuildContext::default()
                            .build_string_length_for_expr(function, &args[0])?;
                        return Ok(self.need(0, Stage::Length(built, function)));
                    }
                    "AES_ENCRYPT" | "AES_DECRYPT" => {
                        return Ok(AstProgress::Prepare { aes: true });
                    }
                    "IF" => {
                        if args.len() != 3 {
                            return Err(EvalError::Unsupported("bad IF arguments"));
                        }
                        return Ok(self.need(0, Stage::If));
                    }
                    "IFNULL" => {
                        if args.len() != 2 {
                            return Err(EvalError::Unsupported("bad IFNULL arguments"));
                        }
                        return Ok(self.need(0, Stage::IfNull));
                    }
                    "COALESCE" => {
                        if arity == 0 {
                            return Ok(self.ready(crate::tikv::eval_coalesce_in::<Datum>(
                                ctx,
                                0,
                                |_, _| unreachable!(),
                            )?));
                        }
                        return Ok(self.need(0, Stage::Coalesce));
                    }
                    "BENCHMARK" => {
                        if args.len() != 2 {
                            return Err(EvalError::Unsupported("BENCHMARK arguments"));
                        }
                        let field = |expr| {
                            check_build_metadata(expr)?;
                            match crate::rewriter::rewrite_expr(expr) {
                                Ok(expr) => Ok(expr.static_type().cloned()),
                                Err(
                                    EvalError::UnknownColumn(_)
                                    | EvalError::UnknownColumnInClause(..),
                                ) => Ok(None),
                                Err(error) => Err(error),
                            }
                        };
                        let count = field(&args[0])?;
                        let expression = field(&args[1])?;
                        return Ok(self.need(0, Stage::BenchmarkCount { count, expression }));
                    }
                    "CHARSET" | "COLLATION" | "COERCIBILITY" => {
                        if args.len() != 1 {
                            return Err(EvalError::Unsupported(
                                "CHARSET/COLLATION/COERCIBILITY arguments",
                            ));
                        }
                        check_build_metadata(&args[0])?;
                        let arg = crate::rewriter::rewrite_expr(&args[0])?;
                        return Ok(self.ready(
                            crate::collation_derive::info_metadata_value(
                                &name.to_ascii_lowercase(),
                                &arg,
                            )
                            .ok_or(EvalError::Unsupported("information metadata function"))?,
                        ));
                    }
                    "REGEXP_LIKE" if matches!(args.len(), 2 | 3) => {
                        return Ok(AstProgress::Prepare { aes: false })
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        if arity == 0 {
            let value = value::finish(root, Vec::new(), ctx, self.raw_string)?;
            Ok(self.ready(value))
        } else {
            Ok(self.need(0, Stage::Eager))
        }
    }
    fn advance_in(
        &mut self,
        root: &Expr,
        index: usize,
        mut candidate: usize,
        left_count: usize,
        mut right_count: usize,
        mut membership: tidb_query_expr::NativeInMembership,
        ctx: &dyn Columns,
    ) -> std::result::Result<AstProgress, EvalError> {
        let Expr::In { expr, list, not } = root else {
            unreachable!()
        };
        if index < left_count {
            return Ok(self.need(
                index,
                Stage::In {
                    candidate,
                    left_count,
                    right_count,
                    membership,
                },
            ));
        }
        let row = matches!(&**expr, Expr::Row(_));
        let next_index = index;
        loop {
            if right_count != 0 && self.values.len() == left_count + right_count {
                let compared = if row {
                    tidb_query_expr::native_row_equality(
                        &self.values[..left_count],
                        &self.values[left_count..],
                        |l, r| {
                            crate::ops::eval_comparison_values_in(
                                BinaryOp::Eq,
                                l.clone(),
                                r.clone(),
                                crate::ops::DERIVATION_FREE_COLLATION,
                                crate::ops::Operands::LITERALS,
                                ctx,
                            )
                            .map(|v| crate::tikv::in_eq_observation(&v))
                        },
                    )
                    .map_err(crate::tikv::in_row_control_error)?
                } else {
                    let v = crate::ops::eval_binary_in(
                        BinaryOp::Eq,
                        self.values[0].clone(),
                        self.values[1].clone(),
                        ctx,
                    )?;
                    membership.observe(crate::tikv::in_eq_observation(&v));
                    self.values.truncate(left_count);
                    candidate += 1;
                    right_count = 0;
                    if candidate == list.len() {
                        return Ok(self.ready(negate_if(
                            crate::tikv::in_control_datum(membership.finish()),
                            *not,
                            ctx,
                        )?));
                    }
                    continue;
                };
                membership.observe(match compared {
                    tidb_query_expr::NativeInControlResult::Null => {
                        tidb_query_expr::NativeInControlValue::Null
                    }
                    tidb_query_expr::NativeInControlResult::Bool(b) => {
                        tidb_query_expr::NativeInControlValue::Int(i64::from(b))
                    }
                });
                self.values.truncate(left_count);
                candidate += 1;
                right_count = 0;
            }
            if candidate == list.len() {
                return Ok(self.ready(negate_if(
                    crate::tikv::in_control_datum(membership.finish()),
                    *not,
                    ctx,
                )?));
            }
            if right_count == 0 {
                right_count = if row {
                    match &list[candidate] {
                        Expr::Row(v) => v.len(),
                        _ => {
                            return Err(crate::tikv::in_row_control_error(
                                tidb_query_expr::NativeInRowError::<EvalError>::ItemMismatch,
                            ))
                        }
                    }
                } else {
                    1
                };
                if right_count == 0 {
                    let compared = tidb_query_expr::native_row_equality::<Datum, EvalError>(
                        &self.values[..left_count],
                        &[],
                        |_, _| unreachable!(),
                    )
                    .map_err(crate::tikv::in_row_control_error)?;
                    membership.observe(match compared {
                        tidb_query_expr::NativeInControlResult::Null => {
                            tidb_query_expr::NativeInControlValue::Null
                        }
                        tidb_query_expr::NativeInControlResult::Bool(b) => {
                            tidb_query_expr::NativeInControlValue::Int(i64::from(b))
                        }
                    });
                    candidate += 1;
                    continue;
                }
            }
            return Ok(self.need(
                next_index,
                Stage::In {
                    candidate,
                    left_count,
                    right_count,
                    membership,
                },
            ));
        }
    }
}

// Bound original metadata-builder copies against the current invocation's
// remaining allowance, after retained runtime inputs have been charged. This
// pure preflight performs neither SQL name lookup nor value evaluation.
fn check_build_metadata(root: &Expr) -> std::result::Result<(), EvalError> {
    let maximum = tidb_query_expr::local::value_kernel_work_remaining()
        .unwrap_or_else(|| ExecutionLimits::default().max_retained_bytes);
    super::metadata::check(
        root,
        BatchSourceLimits {
            tree: CompileLimits::default(),
            max_metadata_bytes: maximum,
        },
    )
    .map_err(|error| {
        EvalError::ExpressionRuntimeFailure(crate::ExpressionRuntimeFailure::from_local_eval(
            error, None,
        ))
    })
}
