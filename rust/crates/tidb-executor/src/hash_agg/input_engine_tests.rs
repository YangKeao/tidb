// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use tidb_ast::CiString;
use tidb_expr::{column::Column, constant::Constant, scalar_function::ScalarFunction};

fn long() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}
fn integer(value: i64) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(value), long()))
}
fn column(index: i64) -> Expression {
    let mut col = Column::new(index + 1, long());
    col.index = index;
    Expression::Column(col)
}
fn plus(arg: Expression) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("plus"),
        long(),
        vec![arg, integer(1)],
    ))
}
fn chunk() -> Chunk {
    let mut input = Chunk::new_with_capacity(&[long(), long()], 3);
    for row in [
        [Datum::Null, Datum::Int(i64::MAX)],
        [Datum::Int(1), Datum::Int(9)],
        [Datum::Int(1), Datum::Int(i64::MAX)],
    ] {
        for (index, datum) in row.iter().enumerate() {
            input.append_datum(index, datum);
        }
    }
    input.set_sel(Some(vec![2, 0, 1]));
    input
}

#[test]
fn multi_argument_null_demand_and_group_cache_are_preserved() {
    for kind in [
        AggKind::Count,
        AggKind::ApproxCountDistinct,
        AggKind::GroupConcat {
            separator: ",".into(),
        },
    ] {
        let mut results = Vec::new();
        for engine in [false, true] {
            let ctx = crate::StmtContext::for_query().with_tikv_expression(engine);
            let mut func = AggFunc::new(kind.clone(), Some(column(0)));
            func.extra_args.push(plus(column(1)));
            let mode = AggInputMode::new(&func);
            let input = chunk();
            let bound = mode.bind(&input);
            let mut state = AggState::new(&func);
            bound
                .update(&func, &ctx, &mut state, input.physical_row(0))
                .unwrap();
            assert_eq!(ctx.tikv_expression_rows(), if engine { 1 } else { 0 });
            assert_eq!(mode.programs.extra[0].tikv_compilations(), 0);
            bound
                .update(&func, &ctx, &mut state, input.physical_row(1))
                .unwrap();
            results.push(state.partial.finish(&func.order_by, 4).unwrap());
            let cloned = mode.clone();
            let mut other_group = AggState::new(&func);
            cloned
                .bind(&input)
                .update(&func, &ctx, &mut other_group, input.physical_row(1))
                .unwrap();
            assert_eq!(ctx.tikv_expression_rows(), if engine { 5 } else { 0 });
            for program in cloned.programs.arguments() {
                assert_eq!(program.tikv_compilations(), if engine { 1 } else { 0 });
            }
            assert!(bound
                .update(&func, &ctx, &mut state, input.physical_row(2))
                .is_err());
            assert_eq!(ctx.tikv_expression_rows(), if engine { 6 } else { 0 });
        }
        assert_eq!(results[0], results[1]);
    }
}

#[test]
fn extras_before_primary_and_sort_after_null_keep_native_order() {
    for engine in [false, true] {
        for kind in [
            AggKind::Avg,
            AggKind::JsonObjectAgg {
                value_type: long(),
                key_is_binary: false,
            },
        ] {
            let ctx = crate::StmtContext::for_query().with_tikv_expression(engine);
            let mut func = AggFunc::new(kind, Some(plus(integer(i64::MAX))));
            func.extra_args.push(column(0));
            let mode = AggInputMode::new(&func);
            let input = chunk();
            assert!(mode
                .bind(&input)
                .update(
                    &func,
                    &ctx,
                    &mut AggState::new(&func),
                    input.physical_row(1)
                )
                .is_err());
            assert_eq!(
                ctx.tikv_expression_rows(),
                if engine { 1 } else { 0 },
                "extra must execute before failing primary"
            );
        }
        // Pin the current native ordering, not a new claim of Go equivalence:
        // GROUP_CONCAT's sort keys are demanded even after a NULL argument.
        let ctx = crate::StmtContext::for_query().with_tikv_expression(engine);
        let mut func = AggFunc::new(
            AggKind::GroupConcat {
                separator: ",".into(),
            },
            Some(column(0)),
        );
        func.order_by.push((plus(integer(i64::MAX)), false));
        let mode = AggInputMode::new(&func);
        let input = chunk();
        assert!(mode
            .bind(&input)
            .update(
                &func,
                &ctx,
                &mut AggState::new(&func),
                input.physical_row(0)
            )
            .is_err());
        assert_eq!(ctx.tikv_expression_rows(), if engine { 1 } else { 0 });
    }
}

#[test]
fn first_row_skips_later_overflow_and_shares_argument_program() {
    for engine in [false, true] {
        let ctx = crate::StmtContext::for_query().with_tikv_expression(engine);
        let func = AggFunc::new(AggKind::FirstRow, Some(plus(column(1))));
        let mode = AggInputMode::new(&func);
        let input = chunk();
        let mut state = AggState::new(&func);
        mode.bind(&input)
            .update(&func, &ctx, &mut state, input.physical_row(1))
            .unwrap();
        mode.bind(&input)
            .update(&func, &ctx, &mut state, input.physical_row(2))
            .unwrap();
        assert_eq!(state.partial.finish(&[], 4).unwrap(), Datum::Int(10));
        let cloned = mode.clone();
        cloned
            .bind(&input)
            .update(
                &func,
                &ctx,
                &mut AggState::new(&func),
                input.physical_row(1),
            )
            .unwrap();
        assert_eq!(ctx.tikv_expression_rows(), if engine { 2 } else { 0 });
        assert_eq!(
            cloned.programs.arg.as_ref().unwrap().tikv_compilations(),
            if engine { 1 } else { 0 }
        );
    }
}
