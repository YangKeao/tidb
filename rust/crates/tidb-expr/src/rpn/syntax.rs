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
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;

use tidb_ast::Expr;
use tidb_chunk::row::Row;
use tidb_datatype::{DateModes, Datum, FieldType, SessionTimeZone};

use super::{Columns, EvalError, PreparedExpression};
use crate::rewriter::{rewrite_expr_resolved, ColumnResolver};

struct SyntaxResolver<'a> {
    context: &'a dyn Columns,
    columns: RefCell<Vec<Vec<String>>>,
    statement_seed: bool,
}

impl ColumnResolver for SyntaxResolver<'_> {
    fn rand_call_occurrence(&self, syntax: &Expr) -> Option<usize> {
        self.statement_seed
            .then_some(syntax as *const Expr as usize)
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        let field = self.context.column_type(path)?;
        let mut columns = self.columns.borrow_mut();
        let index = columns
            .iter()
            .position(|existing| existing == path)
            .unwrap_or_else(|| {
                columns.push(path.to_vec());
                columns.len() - 1
            });
        Some((index, field, i64::try_from(index).ok()?))
    }

    fn time_zone(&self) -> SessionTimeZone {
        self.context.time_zone()
    }

    fn date_modes(&self) -> DateModes {
        self.context.date_modes()
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        self.context.connection_charset_info()
    }

    fn like_default_escape(&self) -> u8 {
        self.context.like_default_escape()
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.context.no_unsigned_subtraction()
    }

    fn div_precision_increment(&self) -> u32 {
        self.context.div_precision_increment()
    }

    fn current_database(&self) -> Option<String> {
        self.context.current_database()
    }
}

/// A syntax expression bound to declared host columns for one execution.
pub struct SyntaxExecution {
    prepared: PreparedExpression,
    binary_literal: bool,
}

impl SyntaxExecution {
    /// Bind names and prepare the same typed program used by physical rows.
    /// The supplied context's evaluation settings apply for the execution.
    pub fn new(syntax: &Expr, context: &dyn Columns) -> Result<Option<Self>, EvalError> {
        Self::prepare(syntax, context, false)
    }

    fn prepare(
        syntax: &Expr,
        context: &dyn Columns,
        statement_seed: bool,
    ) -> Result<Option<Self>, EvalError> {
        let resolver = SyntaxResolver {
            context,
            columns: RefCell::new(Vec::new()),
            statement_seed,
        };
        let expression = match rewrite_expr_resolved(syntax, &resolver) {
            Ok(expression) => expression,
            Err(_) if !statement_seed => return Ok(None),
            Err(error) => return Err(error),
        };
        let paths = resolver.columns.into_inner();
        let Some(mut prepared) = PreparedExpression::prepare_with_source(
            &expression,
            context,
            paths.len(),
            statement_seed,
            false,
        )?
        else {
            return Ok(None);
        };
        prepared.column_paths = paths;
        Ok(Some(Self {
            prepared,
            binary_literal: expression.binary_literal_source().is_some(),
        }))
    }

    /// Evaluate the current named row in this execution.
    pub fn evaluate(&mut self, context: &dyn Columns) -> Result<Datum, EvalError> {
        let value = self.prepared.evaluate_row(context, Row::empty())?;
        Ok(match value {
            Datum::Bytes(bytes) if self.binary_literal => {
                Datum::new_binary_literal(tidb_datatype::BinaryLiteral::from(bytes))
            }
            value => value,
        })
    }
}

pub(crate) fn try_evaluate_ast(
    syntax: &Expr,
    context: &dyn Columns,
) -> Result<Option<Datum>, EvalError> {
    let Some(mut execution) = SyntaxExecution::prepare(syntax, context, true)? else {
        return Ok(None);
    };
    execution.evaluate(context).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_shot_syntax_reuses_statement_rand_occurrences() {
        use std::collections::HashMap;
        struct Context(RefCell<HashMap<usize, crate::MysqlRng>>);
        impl Columns for Context {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn rand_program_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
                Some(
                    self.0
                        .borrow_mut()
                        .entry(key)
                        .or_insert_with(|| crate::MysqlRng::new_with_seed(seed))
                        .gen(),
                )
            }
        }
        let context = Context(RefCell::new(HashMap::new()));
        with_expression("SELECT IF(RAND(7) > 0.5, RAND(7), 0)", |syntax| {
            let expected = crate::MysqlRng::new_with_seed(7);
            let first = expected.gen();
            assert!(first > 0.5);
            assert_eq!(crate::eval_in(syntax, &context), Ok(Datum::Real(first)));
            assert!(expected.gen() < 0.5);
            assert_eq!(crate::eval_in(syntax, &context), Ok(Datum::Real(0.0)));
            assert_eq!(context.0.borrow().len(), 2);
            let mut execution = SyntaxExecution::new(syntax, &context).unwrap().unwrap();
            assert_eq!(execution.evaluate(&context), Ok(Datum::Real(first)));
        });
    }

    struct NamedRow(RefCell<Vec<String>>);

    impl Columns for NamedRow {
        fn column_type(&self, _: &[String]) -> Option<FieldType> {
            Some(FieldType::new(tidb_datatype::FieldTypeCode::LongLong))
        }

        fn get(&self, path: &[String]) -> Option<Datum> {
            let name = path.last()?;
            self.0.borrow_mut().push(name.clone());
            match name.as_str() {
                "enabled" => Some(Datum::Int(0)),
                "value" => Some(Datum::Int(40)),
                _ => None,
            }
        }
    }

    fn with_expression(sql: &str, f: impl FnOnce(&Expr)) {
        let statement = tidb_parser::parse(sql).unwrap();
        let tidb_ast::Stmt::Query(query) = statement else {
            panic!("expected SELECT");
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            panic!("expected SELECT");
        };
        let [tidb_ast::SelectField::Expr { expr, .. }] = select.fields.fields() else {
            panic!("expected one expression");
        };
        f(expr);
    }

    #[test]
    fn aes_uses_the_same_program_and_does_not_read_dead_arguments() {
        use crate::BlockEncryptionMode::*;

        struct AesRow {
            mode: crate::BlockEncryptionMode,
            reads: RefCell<Vec<String>>,
        }
        impl Columns for AesRow {
            fn column_type(&self, _: &[String]) -> Option<FieldType> {
                Some(FieldType::new(tidb_datatype::FieldTypeCode::VarString))
            }

            fn get(&self, path: &[String]) -> Option<Datum> {
                let name = path.last()?;
                self.reads.borrow_mut().push(name.clone());
                match name.as_str() {
                    "plaintext" => Some(Datum::new_string("pingcap")),
                    "key_bytes" => Some(Datum::Null),
                    "null_input" => Some(Datum::Null),
                    _ => panic!("dead argument was read: {name}"),
                }
            }

            fn block_encryption_mode(&self) -> crate::BlockEncryptionMode {
                self.mode
            }
        }

        for (mode, sql, expected) in [
            (
                Aes128Cbc,
                "SELECT HEX(AES_ENCRYPT(null_input, missing_key, missing_iv))",
                &["null_input"][..],
            ),
            (
                Aes128Cbc,
                "SELECT HEX(AES_ENCRYPT(plaintext, key_bytes, missing_iv))",
                &["plaintext", "key_bytes"][..],
            ),
            (
                Aes128Ecb,
                "SELECT HEX(AES_ENCRYPT(plaintext, key_bytes, missing_iv))",
                &["plaintext", "key_bytes"][..],
            ),
        ] {
            let context = AesRow {
                mode,
                reads: RefCell::new(Vec::new()),
            };
            with_expression(sql, |expression| {
                let mut execution = SyntaxExecution::new(expression, &context)
                    .unwrap()
                    .expect("AES compiles inside HEX");
                assert_eq!(execution.evaluate(&context).unwrap(), Datum::Null);
            });
            assert_eq!(*context.reads.borrow(), expected);
        }
    }

    #[test]
    fn nullif_reuses_the_first_occurrences_seed_stream_only_on_its_value_branch() {
        with_expression("SELECT NULLIF(RAND(7), -1)", |expression| {
            let mut execution = SyntaxExecution::new(expression, &crate::NoColumns)
                .unwrap()
                .expect("NULLIF lowers into shared IF and equality");
            // Go evaluates RAND(7) once for equality and advances the same
            // stream again when the first argument is returned.
            assert_eq!(
                execution.evaluate(&crate::NoColumns).unwrap(),
                Datum::Real(0.37600881361962185)
            );
            let mut expected = crate::MysqlRng::new_with_seed(7);
            for _ in 0..3 {
                expected.gen();
            }
            assert_eq!(
                execution.evaluate(&crate::NoColumns).unwrap(),
                Datum::Real(expected.gen())
            );
        });
        with_expression("SELECT NULLIF(RAND(7), RAND(7))", |expression| {
            let mut execution = SyntaxExecution::new(expression, &crate::NoColumns)
                .unwrap()
                .expect("NULLIF lowers into shared IF and equality");
            for _ in 0..2 {
                assert_eq!(execution.evaluate(&crate::NoColumns).unwrap(), Datum::Null);
            }
        });
    }

    #[test]
    fn native_value_functions_compose_with_the_sdk_registry() {
        for (sql, expected) in [
            ("SELECT LENGTH(SM3('abc'))", Datum::Int(64)),
            ("SELECT VEC_DIMS(VEC_FROM_TEXT('[1,2]'))", Datum::Int(2)),
            ("SELECT LENGTH(TRANSLATE('abc','b',''))", Datum::Int(2)),
            ("SELECT LENGTH(WEIGHT_STRING('abc'))", Datum::Int(3)),
            (
                "SELECT LENGTH(WEIGHT_STRING(123 AS BINARY(5)))",
                Datum::Int(5),
            ),
            ("SELECT WEIGHT_STRING(RAND())", Datum::Null),
        ] {
            with_expression(sql, |expression| {
                assert_eq!(
                    try_evaluate_ast(expression, &crate::NoColumns).unwrap(),
                    Some(expected),
                    "{sql}"
                );
            });
        }
    }

    #[test]
    fn benchmark_occurrences_use_the_same_demand_loop_as_conditional_values() {
        for (count, expected, expected_reads) in [
            (3, Datum::Int(0), 3),
            (0, Datum::Int(0), 0),
            (-1, Datum::Null, 0),
        ] {
            let context = NamedRow(RefCell::new(Vec::new()));
            with_expression(
                &format!("SELECT BENCHMARK({count}, value + 1)"),
                |expression| {
                    assert_eq!(
                        try_evaluate_ast(expression, &context).unwrap(),
                        Some(expected)
                    );
                },
            );
            assert_eq!(context.0.into_inner(), vec!["value"; expected_reads]);
        }
    }

    #[test]
    fn typed_variadic_arguments_use_the_standard_casts() {
        for (sql, expected) in [
            ("select NULL REGEXP '7'", Datum::Null),
            ("select 7 REGEXP '7'", Datum::Int(1)),
        ] {
            with_expression(sql, |expr| {
                assert_eq!(
                    try_evaluate_ast(expr, &crate::NoColumns).unwrap(),
                    Some(expected)
                );
            });
        }
    }

    #[test]
    fn generated_text_and_date_signatures_compose_in_the_same_program() {
        for (sql, expected) in [
            ("select char_length('猫b')", Datum::Int(2)),
            ("select char_length(x'e78cab62')", Datum::Int(4)),
            (
                "select insert('甲乙丙', 2, 1, '丁')",
                Datum::new_string("甲丁丙".to_owned()),
            ),
            (
                "select char_length(date_add('2024-02-28', interval 1 day))",
                Datum::Int(10),
            ),
        ] {
            with_expression(sql, |expr| {
                assert_eq!(
                    try_evaluate_ast(expr, &crate::NoColumns).unwrap(),
                    Some(expected),
                    "{sql}",
                );
            });
        }
    }

    #[test]
    fn shared_json_calls_distinguish_documents_from_values() {
        for (sql, expected) in [
            ("select json_set(null, '$', 'x')", "NULL"),
            (
                "select cast(json_array_append('[1]', '$', cast('[2,3]' as json)) as char)",
                "STR:[1, [2, 3]]",
            ),
            (
                "select cast(json_array('not json', '[1]', true) as char)",
                r#"STR:["not json", "[1]", true]"#,
            ),
            (
                "select cast(json_object('raw','[1]') as char)",
                r#"STR:{"raw": "[1]"}"#,
            ),
            (
                "select cast(json_set('{}','$.x','[1]') as char)",
                r#"STR:{"x": "[1]"}"#,
            ),
        ] {
            with_expression(sql, |expr| {
                let value = try_evaluate_ast(expr, &crate::NoColumns)
                    .unwrap()
                    .expect(sql);
                assert_eq!(value.label(), expected, "{sql}");
            });
        }
    }

    #[test]
    fn string_selectors_read_only_the_selected_columns() {
        struct TextRow(RefCell<Vec<String>>);

        impl Columns for TextRow {
            fn column_type(&self, _: &[String]) -> Option<FieldType> {
                Some(FieldType::new(tidb_datatype::FieldTypeCode::VarString))
            }

            fn get(&self, path: &[String]) -> Option<Datum> {
                let name = path.last()?;
                self.0.borrow_mut().push(name.clone());
                match name.as_str() {
                    "good" => Some(Datum::new_string("ok".to_owned())),
                    "empty" => Some(Datum::new_string(String::new())),
                    "bad" => panic!("an unselected argument was read"),
                    _ => None,
                }
            }
        }

        for (sql, answer, reads) in [
            ("select elt(2, bad, good)", "STR:ok", vec!["good"]),
            ("select elt(0, bad)", "NULL", vec![]),
            (
                "select make_set(6, bad, empty, good)",
                "STR:,ok",
                vec!["empty", "good"],
            ),
            ("select make_set(null, bad)", "NULL", vec![]),
        ] {
            with_expression(sql, |expr| {
                let context = TextRow(RefCell::new(Vec::new()));
                let value = try_evaluate_ast(expr, &context).unwrap().expect(sql);
                assert_eq!(value.label(), answer, "{sql}");
                assert_eq!(*context.0.borrow(), reads, "{sql}");
            });
        }
    }

    #[test]
    fn syntax_names_feed_the_same_typed_program_on_demand() {
        with_expression("select if(enabled, missing, value + 2)", |expr| {
            let context = NamedRow(RefCell::new(Vec::new()));
            assert_eq!(
                try_evaluate_ast(expr, &context).unwrap(),
                Some(Datum::Int(42))
            );
            assert_eq!(*context.0.borrow(), ["enabled", "value"]);
        });
    }
}
