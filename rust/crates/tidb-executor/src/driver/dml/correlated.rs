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

use super::*;
use std::sync::Arc;
use tidb_expr::evaluator::{into_eval_error, EvaluatorProgram, EvaluatorSuite};

/// One row expression after Go's expression rewriter has inserted the Apply
/// operators required by correlated subqueries.
pub(super) struct DmlExpression {
    program: Arc<EvaluatorProgram>,
    field_types: Vec<FieldType>,
    applies: Vec<(CorrelatedSubquery, FromScope)>,
}

impl DmlExpression {
    pub(super) fn build(
        expr: &tidb_ast::Expr,
        scope: FromScope,
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
    ) -> Result<Self, DriverError> {
        Self::build_with_prepared_defaults(expr, scope, catalog, current_db, ctx, &[])
    }

    pub(super) fn build_with_prepared_defaults(
        expr: &tidb_ast::Expr,
        mut scope: FromScope,
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
        defaults: &[super::defaults::PreparedNamedDefault],
    ) -> Result<Self, DriverError> {
        let mut rewritten = fold_subqueries(expr, &scope, catalog, current_db, ctx)?;
        let mut applies = Vec::new();
        while expr_has_subquery(&rewritten) {
            let index = scope.width();
            let mut found = None;
            rewritten = extract_correlated_subquery(
                &rewritten, &scope, catalog, current_db, index, &mut found, ctx,
            )?;
            let Some(correlated) = found else {
                break;
            };
            let value_type = if matches!(correlated.kind, SubqueryKind::Scalar) {
                subquery_result_type(&correlated, &scope, catalog, current_db, ctx)
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
            } else {
                FieldType::new(FieldTypeCode::LongLong)
            };
            applies.push((correlated, scope.clone()));
            scope.tables.push(FromTable {
                name: String::new(),
                database: None,
                columns: vec![(format!("__apply_{index}"), value_type)],
                offset: index,
            });
        }
        let expression =
            rewrite_with_prepared_defaults(&rewritten, &ScopeResolver { scope: &scope }, defaults)?;
        let field_types = scope
            .column_list()
            .into_iter()
            .map(|(_, field_type)| field_type)
            .collect();
        Ok(Self {
            program: Arc::new(EvaluatorProgram::new(vec![expression], true)),
            field_types,
            applies,
        })
    }

    pub(super) fn eval(
        &self,
        row: &[Datum],
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
    ) -> Result<Datum, DriverError> {
        let base_width = self.field_types.len() - self.applies.len();
        let mut values = row.iter().take(base_width).cloned().collect::<Vec<_>>();
        for (correlated, scope) in &self.applies {
            values.push(run_correlated_subquery(
                correlated, &values, scope, catalog, current_db, ctx,
            )?);
        }
        let mut chunk = row_chunk(&values, &self.field_types)?;
        if self.field_types.is_empty() {
            chunk.set_num_virtual_rows(1);
        }
        eval_program(&self.program, ctx, chunk.get_row(0))
    }
}

pub(super) enum UpdateExpression {
    Scalar(Arc<EvaluatorProgram>),
    Physical(Arc<EvaluatorProgram>),
    Applied(DmlExpression),
}

impl UpdateExpression {
    pub(super) fn scalar(expression: Expression) -> Self {
        Self::Scalar(Arc::new(EvaluatorProgram::new(vec![expression], true)))
    }

    pub(super) fn applied(expression: DmlExpression) -> Self {
        Self::Applied(expression)
    }

    pub(super) fn physical(expression: Expression) -> Self {
        Self::Physical(Arc::new(EvaluatorProgram::new(vec![expression], true)))
    }

    pub(super) fn eval(
        &self,
        row: &[Datum],
        scalar_row: tidb_chunk::row::Row<'_>,
        physical_row: Option<tidb_chunk::row::Row<'_>>,
        catalog: &Catalog,
        current_db: &str,
        ctx: &crate::StmtContext,
    ) -> Result<Datum, DriverError> {
        match self {
            Self::Scalar(program) => eval_program(program, ctx, scalar_row),
            Self::Physical(program) => eval_program(
                program,
                ctx,
                physical_row.ok_or_else(|| {
                    DriverError::unsupported(
                        "a planned UPDATE expression has no physical input row",
                    )
                })?,
            ),
            Self::Applied(expression) => expression.eval(row, catalog, current_db, ctx),
        }
    }
}

/// Retain only compilation metadata. Each execution borrows the current row;
/// assignment casts must receive scalar Datum kinds rather than typed carriers.
fn eval_program(
    program: &Arc<EvaluatorProgram>,
    ctx: &crate::StmtContext,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Datum, DriverError> {
    let virtual_input = row
        .chunk()
        .is_none_or(|input| row.len() == 0 && input.physical_rows() == 0)
        .then(|| {
            let mut input = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 1);
            input.set_num_virtual_rows(1);
            input
        });
    let input = virtual_input
        .as_ref()
        .or(row.chunk())
        .expect("DML expression input");
    let physical = if virtual_input.is_some() {
        0
    } else {
        row.idx()
    };
    EvaluatorSuite::from_program(Arc::clone(program))
        .eval_selected_for_cast(ctx, input, &[physical])
        .map_err(into_eval_error)
        .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?
        .pop()
        .ok_or_else(|| DriverError::Exec(ExecError::internal("DML expression returned no value")))
}

#[cfg(test)]
mod engine_tests {
    use super::*;
    use tidb_expr::{column::Column, constant::Constant, scalar_function::ScalarFunction};

    fn wide() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }
    fn plus() -> Expression {
        let mut column = Column::new(1, wide());
        column.index = 0;
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            wide(),
            vec![
                Expression::Column(column),
                Expression::Constant(Constant::new(Datum::Int(1), wide())),
            ],
        ))
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn update_uses_live_scalar_or_physical_row_without_reapplying_selection() {
        let catalog = Catalog::default();
        for engine in [false, true] {
            let ctx = crate::StmtContext::for_query().with_tikv_expression(engine);
            let scalar = UpdateExpression::scalar(plus());
            let physical = UpdateExpression::physical(plus());
            let mut input = tidb_chunk::chunk::Chunk::new_with_capacity(&[wide()], 2);
            input.append_datum(0, &Datum::Int(1));
            input.append_datum(0, &Datum::Int(3));
            input.set_sel(Some(vec![1, 0]));
            let mut row = tidb_chunk::mutrow::MutRow::from_datums(&[Datum::Int(9)]);
            for value in [9, 19] {
                row.set_datum(0, &Datum::Int(value));
                for (expr, expected) in [(&scalar, value + 1), (&physical, 4)] {
                    assert_eq!(
                        expr.eval(
                            &[Datum::Int(999)],
                            row.to_row(),
                            Some(input.get_row(0)),
                            &catalog,
                            "test",
                            &ctx
                        )
                        .unwrap(),
                        Datum::Int(expected)
                    );
                }
            }
            assert!(physical
                .eval(&[], row.to_row(), None, &catalog, "test", &ctx)
                .is_err());
            assert_eq!(ctx.tikv_expression_rows(), if engine { 4 } else { 0 });
            for expression in [&scalar, &physical] {
                let (UpdateExpression::Scalar(program) | UpdateExpression::Physical(program)) =
                    expression
                else {
                    unreachable!()
                };
                assert_eq!(program.tikv_compilations(), u64::from(engine));
            }
        }
    }

    #[test]
    fn assignment_scalar_kinds_and_error_recovery() {
        let catalog = Catalog::default();
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[Datum::Int(9)]);
        let bad = tidb_chunk::mutrow::MutRow::from_datums(&[Datum::Int(i64::MAX)]);
        let empty = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        for _engine in [false, true] {
            let ctx = crate::StmtContext::for_query();
            #[cfg(feature = "tikv-expr")]
            let ctx = ctx.with_tikv_expression(_engine);
            let literal = Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![16]));
            let expression = Expression::Constant(Constant::new(
                literal.clone(),
                FieldType::new(FieldTypeCode::VarString),
            ));
            let applied = DmlExpression {
                program: Arc::new(EvaluatorProgram::new(vec![expression.clone()], true)),
                field_types: vec![wide()],
                applies: vec![],
            };
            for expression in [
                UpdateExpression::scalar(expression.clone()),
                UpdateExpression::physical(expression),
                UpdateExpression::applied(applied),
            ] {
                assert_eq!(
                    expression
                        .eval(
                            &[Datum::Int(9)],
                            empty.to_row(),
                            Some(tidb_chunk::row::Row::empty()),
                            &catalog,
                            "test",
                            &ctx
                        )
                        .unwrap(),
                    literal
                );
            }
            let expression = UpdateExpression::scalar(plus());
            assert!(expression
                .eval(&[], bad.to_row(), None, &catalog, "test", &ctx)
                .is_err());
            assert_eq!(
                expression
                    .eval(&[], row.to_row(), None, &catalog, "test", &ctx)
                    .unwrap(),
                Datum::Int(10)
            );
            #[cfg(feature = "tikv-expr")]
            {
                let UpdateExpression::Scalar(program) = expression else {
                    unreachable!()
                };
                assert_eq!(program.tikv_compilations(), u64::from(_engine));
                assert_eq!(ctx.tikv_expression_rows(), u64::from(_engine));
            }
        }
    }

    #[cfg(feature = "tikv-expr")]
    #[test]
    fn applied_constant_has_one_virtual_row_and_evaluates_on_each_call() {
        let catalog = Catalog::default();
        for engine in [false, true] {
            let ctx = crate::StmtContext::for_query().with_tikv_expression(engine);
            let expression = DmlExpression::build(
                &tidb_ast::Expr::Int("7".to_owned()),
                FromScope::for_statement(&ctx),
                &catalog,
                "test",
                &ctx,
            )
            .unwrap();
            for _ in 0..2 {
                assert_eq!(
                    expression.eval(&[], &catalog, "test", &ctx).unwrap(),
                    Datum::Int(7)
                );
            }
            assert_eq!(ctx.tikv_expression_rows(), if engine { 2 } else { 0 });
            assert_eq!(expression.program.tikv_compilations(), u64::from(engine));
        }
    }
}

pub(super) fn dml_table_scope(
    table_ref: &tidb_ast::TableRef,
    database: &str,
    name: &str,
    columns: Vec<(String, FieldType)>,
    ctx: &crate::StmtContext,
) -> FromScope {
    let mut scope = single_table_scope(
        table_ref.alias.as_deref().unwrap_or(name),
        table_ref.alias.is_none().then(|| database.to_owned()),
        columns,
    );
    let statement = FromScope::for_statement(ctx);
    scope.constant_context = statement.constant_context;
    scope.zone = statement.zone;
    scope.like_default_escape = statement.like_default_escape;
    scope.no_unsigned_subtraction = statement.no_unsigned_subtraction;
    scope.div_precision_increment = statement.div_precision_increment;
    scope
}
