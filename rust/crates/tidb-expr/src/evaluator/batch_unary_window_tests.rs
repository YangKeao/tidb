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
use crate::{column::Column, constant::Constant};
use tidb_ast::CiString;
use tidb_datatype::{Decimal, FieldTypeCode, FieldTypeFlags, SessionTimeZone};
use tidb_query_expr::local::HostSignature;
#[derive(Default)]
struct Trace {
    events: RefCell<Vec<String>>,
}
impl Columns for Trace {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("unary window cannot evaluate an expression")
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        SessionTimeZone::utc()
    }
    fn div_precision_increment(&self) -> u32 {
        self.events.borrow_mut().push("precision".into());
        4
    }
    fn no_unsigned_subtraction(&self) -> bool {
        self.events.borrow_mut().push("unsigned-mode".into());
        false
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        crate::ErrorLevel::Warn
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
    fn handle_truncate(&self, message: &str) -> std::result::Result<(), EvalError> {
        self.events.borrow_mut().push(format!("truncate:{message}"));
        Ok(())
    }
}
struct Inputs<'a> {
    hosts: NativeBatchHosts<'a>,
    store: &'a RefCell<NativeValueStore>,
    schema: Vec<tipb::FieldType>,
    values: &'a [Datum],
    literal: bool,
    reads: Vec<InputRow>,
    fail: Option<usize>,
}
impl LocalRuntimeServices for Inputs<'_> {
    fn binding_schema(&self) -> &[tipb::FieldType] {
        &self.schema
    }
    fn host_services(&mut self) -> Option<&mut dyn LocalHostServices> {
        Some(&mut self.hosts)
    }
    fn read_input(
        &mut self,
        _: &mut EvalContext,
        slot: usize,
        row: InputRow,
        _: &tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        assert_eq!(slot, 0);
        self.reads.push(row);
        if self.fail == Some(row.occurrence) {
            return Err(binding("unary input sentinel"));
        }
        let value = &self.values[if self.literal { 0 } else { row.input_row }];
        let token = self.store.borrow_mut().insert_clone(
            value,
            NativeValueRole::CharInput,
            NativeValueOrigin { node: 1, row },
        )?;
        Ok(VectorValue::from_scalar(
            &ScalarValue::Bytes(Some(token.to_vec())),
            1,
        ))
    }
}
fn run_case(
    operation: SqlBatchHostOperation,
    source: FieldType,
    target: FieldType,
    values: Vec<Datum>,
    literal: bool,
    selection: &[usize],
    fail: Option<usize>,
) {
    let argument = if literal {
        Expression::Constant(Constant::new(values[0].clone(), source.clone()))
    } else {
        let mut column = Column::new(1, source.clone());
        column.index = 0;
        column.orig_name = "t.x".into();
        Expression::Column(column)
    };
    let name = match operation {
        SqlBatchHostOperation::AbsIntWindowNative => "abs",
        SqlBatchHostOperation::CastDecimalWindowNative => "cast_decimal",
        _ => "cast_json",
    };
    let function = ScalarFunction::new(CiString::new(name), target.clone(), vec![argument]);
    let metadata = [NativeHostMetadata::UnaryWindow {
        protobuf_owner: false,
        operation,
        result_type: target.clone(),
        source_type: source.clone(),
        source: if literal {
            WindowArithmeticSource::Literal
        } else {
            WindowArithmeticSource::Column
        },
        argument_node: 1,
        output_node: 0,
        output_role: NativeValueRole::Output,
        overflow_expression: if name == "abs" {
            crate::scalar_function::static_math_overflow_expression(&function)
                .map(String::into_boxed_str)
        } else {
            None
        },
    }];
    let carrier: tipb::FieldType = tidb_query_datatype::FieldTypeTp::VarString.into();
    let catalog = HostCatalog::for_sql_batch(vec![(
        operation,
        HostSignature {
            arg_types: vec![carrier.clone()].into_boxed_slice(),
            return_type: carrier.clone(),
        },
    )])
    .unwrap();
    let root = LocalExpr::HostCall {
        slot: catalog.slot(0).unwrap(),
        args: vec![LocalExpr::InputSlot {
            slot: 0,
            field_type: carrier.clone(),
        }]
        .into_boxed_slice(),
        return_type: carrier.clone(),
    };
    let schema = vec![carrier];
    let facts = BatchFacts::sql_native_batch_with_hosts(
        &root,
        &schema,
        vec![OrdinaryCallSite::sql_native_batch(
            0,
            OrdinarySourceId::new(801, 0),
        )],
        CompileLimits::default(),
        &catalog,
    )
    .unwrap();
    let mut program =
        compile_batch(&root, &schema, LocalCompileContext::default(), &facts).unwrap();
    assert!(program.requires_whole_selection());
    let mut input = if literal {
        Chunk::new_with_capacity(&[], values.len())
    } else {
        Chunk::new_with_capacity(std::slice::from_ref(&source), values.len())
    };
    if literal {
        input.set_num_virtual_rows(values.len());
    } else {
        for value in &values {
            input.append_datum(0, value);
        }
    }
    input.set_sel(Some(selection.to_vec()));
    // Model the genuine raw column import, rather than supplying a different
    // Datum kind from the one its declared native storage returns.
    let raw = if literal {
        values.clone()
    } else {
        (0..values.len())
            .map(|row| input.physical_row(row).get_datum(0, &source))
            .collect()
    };
    let store = RefCell::new(NativeValueStore::new().unwrap());
    store.borrow_mut().begin_window().unwrap();
    let context = Trace::default();
    let mut service = Inputs {
        hosts: NativeBatchHosts::new(catalog, &metadata, &context).with_native_values(Some(&store)),
        store: &store,
        schema,
        values: &raw,
        literal,
        reads: Vec::new(),
        fail,
    };
    let result = program.eval_with_bindings(
        ExecutionLimits::default(),
        &mut EvalContext::default(),
        values.len(),
        selection,
        &mut service,
    );
    let native_error = service.hosts.take_native_error();
    let expected_reads: Vec<_> = selection
        .iter()
        .enumerate()
        .map(|(occurrence, &input_row)| InputRow {
            occurrence,
            input_row,
        })
        .take(fail.map_or(selection.len(), |index| index + 1))
        .collect();
    assert_eq!(service.reads, expected_reads);
    if fail.is_some() {
        assert!(
            matches!(result,Err(LocalError::BindingContract(ref reason)) if reason=="unary input sentinel")
        );
        assert!(native_error.is_none());
    } else {
        let reference = Trace::default();
        let expected = crate::scalar_function::try_eval_batch(
            &Expression::ScalarFunction(function),
            &reference,
            &input,
        );
        match (result, native_error, expected) {
            (Ok(actual), None, Ok(Some(expected))) => {
                assert_eq!(actual.len(), expected.len());
                for (occurrence, expected) in expected.iter().enumerate() {
                    let ScalarValueRef::Bytes(Some(token)) = actual.get_scalar_ref(occurrence)
                    else {
                        panic!("native output handle")
                    };
                    let actual = store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin {
                                node: 0,
                                row: InputRow {
                                    occurrence,
                                    input_row: selection[occurrence],
                                },
                            },
                        )
                        .unwrap()
                        .clone();
                    assert_eq!(actual, *expected, "{name} row{occurrence}");
                }
            }
            (Err(LocalError::HostContract(_)), Some(actual), Err(expected)) => {
                assert_eq!(actual, expected, "{name}")
            }
            (actual, error, expected) => panic!(
                "{name} mismatched native disposition: {actual:?} / {error:?} / {expected:?}"
            ),
        }
        assert_eq!(
            context.events.take(),
            reference.events.take(),
            "{name} literal={literal}"
        );
    }
    assert!(service.hosts.slots.iter().all(|slot| slot.task.is_none()));
}
fn int() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0)
}
#[test]
fn unary_windows_match_native_abs_decimal_json_values_warnings_and_source_phases() {
    let integer = int();
    let string = FieldType::new(FieldTypeCode::Varchar).with_flen(128);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(4);
    let json = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
    let selections = vec![
        vec![],
        vec![0],
        vec![2, 0, 2],
        (0..2050).map(|index| [2, 0, 2][index % 3]).collect(),
    ];
    let time =
        tidb_datatype::parse_datetime("2026-10-18 12:34:56.123456", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
    let cases = vec![
        (
            SqlBatchHostOperation::AbsIntWindowNative,
            integer.clone(),
            integer.clone(),
            vec![Datum::Int(-7), Datum::Null, Datum::Int(2)],
            false,
        ),
        (
            SqlBatchHostOperation::AbsIntWindowNative,
            integer.clone(),
            integer.clone(),
            vec![Datum::Int(i64::MIN), Datum::Int(0), Datum::Null],
            false,
        ),
        (
            SqlBatchHostOperation::AbsIntWindowNative,
            integer.clone(),
            integer.clone(),
            vec![Datum::UInt(u64::MAX); 3],
            true,
        ),
        (
            SqlBatchHostOperation::CastDecimalWindowNative,
            integer.clone(),
            decimal.clone(),
            vec![Datum::Int(-7), Datum::Null, Datum::Int(i64::MAX)],
            false,
        ),
        (
            SqlBatchHostOperation::CastDecimalWindowNative,
            integer.clone().with_unsigned(true),
            decimal.clone(),
            vec![Datum::UInt(u64::MAX), Datum::Null, Datum::UInt(1)],
            false,
        ),
        (
            SqlBatchHostOperation::CastDecimalWindowNative,
            integer.clone(),
            decimal.clone(),
            vec![Datum::UInt(u64::MAX); 3],
            true,
        ),
        (
            SqlBatchHostOperation::CastDecimalWindowNative,
            integer.clone(),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(3)
                .with_decimal(1),
            vec![Datum::Int(1000), Datum::Null, Datum::Int(-1000)],
            false,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            integer.clone(),
            json.clone(),
            vec![Datum::Int(-1), Datum::Null, Datum::Int(3)],
            false,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            integer.clone().with_unsigned(true),
            json.clone(),
            vec![Datum::UInt(u64::MAX), Datum::Null, Datum::UInt(1)],
            false,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            string.clone(),
            json.clone(),
            vec![
                Datum::new_string("{\"a\":[1,null]}"),
                Datum::Null,
                Datum::new_string("bad JSON"),
            ],
            false,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            string.clone(),
            FieldType::new(FieldTypeCode::Json),
            vec![
                Datum::new_string("bad JSON"),
                Datum::Null,
                Datum::new_string("ok"),
            ],
            false,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            decimal.clone(),
            json.clone(),
            vec![
                Datum::Decimal(Decimal::from_literal("1.2300")),
                Datum::Null,
                Datum::Decimal(Decimal::from_int(-2)),
            ],
            false,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            FieldType::new(FieldTypeCode::Double),
            json.clone(),
            vec![Datum::Real(1.5), Datum::Null, Datum::Real(f64::NAN)],
            false,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            FieldType::new(FieldTypeCode::Datetime).with_decimal(6),
            json.clone(),
            vec![Datum::Time(time), Datum::Null, Datum::Time(time)],
            false,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            FieldType::new(FieldTypeCode::Enum),
            FieldType::new(FieldTypeCode::Json),
            vec![
                Datum::Enum(
                    tidb_datatype::MysqlEnum::new("b", 2),
                    tidb_datatype::Collation::Utf8Mb4Bin
                );
                3
            ],
            true,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            FieldType::new(FieldTypeCode::Set),
            FieldType::new(FieldTypeCode::Json),
            vec![
                Datum::Set(
                    tidb_datatype::MysqlSet::new("a,b", 3),
                    tidb_datatype::Collation::Utf8Mb4Bin
                );
                3
            ],
            true,
        ),
        (
            SqlBatchHostOperation::CastJsonWindowNative,
            string.clone(),
            json,
            vec![Datum::new_string("[1,true]"); 3],
            true,
        ),
    ];
    for (operation, source, target, values, literal) in cases {
        for selection in &selections {
            run_case(
                operation,
                source.clone(),
                target.clone(),
                values.clone(),
                literal,
                selection,
                None,
            );
        }
    }
}
#[test]
fn unary_windows_preserve_late_input_failure_before_early_abs_overflow() {
    let selection: Vec<_> = (0..2050).collect();
    for operation in [
        SqlBatchHostOperation::AbsIntWindowNative,
        SqlBatchHostOperation::CastDecimalWindowNative,
        SqlBatchHostOperation::CastJsonWindowNative,
    ] {
        let target = match operation {
            SqlBatchHostOperation::AbsIntWindowNative => int(),
            SqlBatchHostOperation::CastDecimalWindowNative => {
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(65)
                    .with_decimal(4)
            }
            _ => FieldType::new(FieldTypeCode::Json),
        };
        let mut values = vec![Datum::Int(1); 2050];
        values[0] = Datum::Int(i64::MIN);
        run_case(
            operation,
            int(),
            target,
            values,
            false,
            &selection,
            Some(2049),
        );
    }
}

#[test]
fn unary_json_admission_and_wide_decimal_work_refuse_before_context_or_formatting() {
    for unsupported in [false, true] {
        let source = FieldType::new(if unsupported {
            FieldTypeCode::VectorFloat32
        } else {
            FieldTypeCode::NewDecimal
        });
        let target = FieldType::new(FieldTypeCode::Json).with_flags(FieldTypeFlags::PARSE_TO_JSON);
        let operation = SqlBatchHostOperation::CastJsonWindowNative;
        let metadata = [NativeHostMetadata::UnaryWindow {
            protobuf_owner: false,
            operation,
            result_type: target,
            source_type: source.clone(),
            source: WindowArithmeticSource::Literal,
            argument_node: 1,
            output_node: 0,
            output_role: NativeValueRole::Output,
            overflow_expression: None,
        }];
        let carrier: tipb::FieldType = tidb_query_datatype::FieldTypeTp::VarString.into();
        let schema = vec![carrier.clone()];
        let catalog = HostCatalog::for_sql_batch(vec![(
            operation,
            HostSignature {
                arg_types: vec![carrier.clone()].into_boxed_slice(),
                return_type: carrier.clone(),
            },
        )])
        .unwrap();
        let root = LocalExpr::HostCall {
            slot: catalog.slot(0).unwrap(),
            args: vec![LocalExpr::InputSlot {
                slot: 0,
                field_type: carrier.clone(),
            }]
            .into_boxed_slice(),
            return_type: carrier,
        };
        let facts = BatchFacts::sql_native_batch_with_hosts(
            &root,
            &schema,
            vec![OrdinaryCallSite::sql_native_batch(
                0,
                OrdinarySourceId::new(802, 0),
            )],
            CompileLimits::default(),
            &catalog,
        )
        .unwrap();
        let mut program =
            compile_batch(&root, &schema, LocalCompileContext::default(), &facts).unwrap();
        let context = Trace::default();
        let store = RefCell::new(NativeValueStore::new().unwrap());
        store.borrow_mut().begin_window().unwrap();
        let values = vec![if unsupported {
            Datum::Null
        } else {
            Datum::Decimal(Decimal::from_literal("1.25"))
        }];
        let mut service = Inputs {
            hosts: NativeBatchHosts::new(catalog, &metadata, &context)
                .with_native_values(Some(&store)),
            store: &store,
            schema,
            values: &values,
            literal: true,
            reads: Vec::new(),
            fail: None,
        };
        let result = program.eval_with_bindings(
            ExecutionLimits::default(),
            &mut EvalContext::default(),
            1,
            &[0],
            &mut service,
        );
        if unsupported {
            assert!(matches!(result, Err(LocalError::HostContract(_))));
            assert_eq!(
                service.hosts.take_native_error().unwrap(),
                crate::builtin_ext::json::validate_json_cast_source(Some(&source)).unwrap_err()
            );
            assert!(service.reads.is_empty());
        } else {
            let output = result.unwrap();
            let ScalarValueRef::Bytes(Some(token)) = output.get_scalar_ref(0) else {
                panic!("native JSON handle")
            };
            let actual = store
                .borrow()
                .get(
                    token,
                    NativeValueRole::Output,
                    NativeValueOrigin {
                        node: 0,
                        row: InputRow {
                            occurrence: 0,
                            input_row: 0,
                        },
                    },
                )
                .unwrap()
                .clone();
            assert_eq!(actual.sql_string().unwrap(), "1.25");
            assert_eq!(
                service.reads,
                vec![InputRow {
                    occurrence: 0,
                    input_row: 0
                }]
            );
            assert!(service.hosts.take_native_error().is_none());
        }
        assert_eq!(
            *context.events.borrow(),
            if unsupported { vec![] } else { vec!["zone"] }
        );
        assert!(service.hosts.slots.iter().all(|slot| slot.task.is_none()));
    }
}
