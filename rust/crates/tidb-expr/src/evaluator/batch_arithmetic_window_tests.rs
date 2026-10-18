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
use std::cell::RefCell;
use tidb_ast::CiString;
use tidb_datatype::{Decimal, FieldTypeCode, SessionTimeZone};
use tidb_query_expr::local::HostSignature;

#[derive(Default)]
struct Trace {
    events: RefCell<Vec<String>>,
    no_unsigned: bool,
}
impl Columns for Trace {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("window provider cannot evaluate an expression")
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        crate::NoColumns.time_zone()
    }
    fn div_precision_increment(&self) -> u32 {
        self.events.borrow_mut().push("precision".into());
        4
    }
    fn no_unsigned_subtraction(&self) -> bool {
        self.events.borrow_mut().push("unsigned-mode".into());
        self.no_unsigned
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
    values: &'a [Vec<Datum>; 2],
    reads: Vec<(usize, InputRow)>,
    fail: Option<(usize, usize)>,
}
impl LocalRuntimeServices for Inputs<'_> {
    fn binding_schema(&self) -> &[tipb::FieldType] {
        &self.schema
    }
    fn read_input(
        &mut self,
        _: &mut EvalContext,
        slot: usize,
        row: InputRow,
        _: &tipb::FieldType,
    ) -> LocalResult<VectorValue> {
        self.reads.push((slot, row));
        if self.fail == Some((slot, row.occurrence)) {
            return Err(binding("window input sentinel"));
        }
        let token = self.store.borrow_mut().insert_clone(
            &self.values[slot][row.input_row],
            NativeValueRole::CharInput,
            NativeValueOrigin {
                node: slot + 1,
                row,
            },
        )?;
        Ok(VectorValue::from_scalar(
            &ScalarValue::Bytes(Some(token)),
            1,
        ))
    }
    fn host_services(&mut self) -> Option<&mut dyn LocalHostServices> {
        Some(&mut self.hosts)
    }
}
fn execute_case(
    name: &str,
    fields: [FieldType; 2],
    result: FieldType,
    values: [Vec<Datum>; 2],
    literal: [bool; 2],
    selection: &[usize],
    no_unsigned: bool,
    fail: Option<(usize, usize)>,
) {
    let args = [0, 1].map(|index| {
        if literal[index] {
            Expression::Constant(Constant::new(
                values[index][0].clone(),
                fields[index].clone(),
            ))
        } else {
            let mut column = Column::new(index as i64 + 1, fields[index].clone());
            column.index = index as i64;
            column.orig_name = format!("t.x{index}");
            Expression::Column(column)
        }
    });
    let function = ScalarFunction::new(CiString::new(name), result.clone(), args.to_vec());
    let op = crate::scalar_function::binary_op_for_name(name).unwrap();
    let crate::scalar_function::NativeArithmeticInputMode::Numeric(domain) =
        function.native_arithmetic_generic_mode()
    else {
        panic!("real batch source required")
    };
    let integer = result.eval_type() == NativeEvalType::Int
        && domain == NativeEvalType::Int
        && [0, 1].into_iter().all(|index| {
            crate::scalar_function::native_numeric_batch_source(&fields[index], false, domain)
                == NativeEvalType::Int
        });
    let operation = match name {
        "plus" => SqlBatchHostOperation::AddWindowNative,
        "minus" => SqlBatchHostOperation::SubtractWindowNative,
        "mul" => SqlBatchHostOperation::MultiplyWindowNative,
        "mod" => SqlBatchHostOperation::ModuloWindowNative,
        "div" => SqlBatchHostOperation::DivideWindowNative,
        "intdiv" => SqlBatchHostOperation::IntegerDivideWindowNative,
        _ => panic!("unknown arithmetic protocol fixture"),
    };
    let metadata = vec![NativeHostMetadata::Arithmetic {
        batch: Some(WindowArithmeticMetadata {
            domain,
            integer,
            output_integer: result.eval_type() == NativeEvalType::Int,
            sources: literal.map(|literal| {
                if literal {
                    WindowArithmeticSource::Literal
                } else {
                    WindowArithmeticSource::Column
                }
            }),
        }),
        operation,
        name: name.into(),
        result_type: result.clone(),
        argument_nodes: [1, 2],
        source_types: fields.clone(),
        literal,
        binary_literal: [false; 2],
        mode: function.native_arithmetic_mode(),
        generic_mode: function.native_arithmetic_generic_mode(),
        subjects: args.each_ref().map(|arg| {
            crate::scalar_function::static_numeric_expression_text(arg, true)
                .map(String::into_boxed_str)
        }),
        subject_format_failures: [None; 2],
        overflow_format_failure: None,
        overflow_expressions: [false, true].map(|float| {
            crate::scalar_function::static_arithmetic_overflow_expression(&function, op, float)
                .map(String::into_boxed_str)
        }),
        intdiv: (name == "intdiv").then(|| {
            Box::new(crate::scalar_function::NativeIntDivDiagnostics::new(
                &function,
                args.each_ref().map(|arg| {
                    crate::scalar_function::static_numeric_argument_text(&function, arg, false)
                        .map(String::into_boxed_str)
                }),
                [None; 2],
            ))
        }),
        output_node: 0,
        output_role: NativeValueRole::Output,
    }];
    let carrier: tipb::FieldType = tidb_query_datatype::FieldTypeTp::VarString.into();
    let catalog = HostCatalog::for_sql_batch(vec![(
        operation,
        HostSignature {
            arg_types: vec![carrier.clone(); 2].into_boxed_slice(),
            return_type: carrier.clone(),
        },
    )])
    .unwrap();
    let root = LocalExpr::HostCall {
        slot: catalog.slot(0).unwrap(),
        args: [0, 1]
            .map(|slot| LocalExpr::InputSlot {
                slot,
                field_type: carrier.clone(),
            })
            .into(),
        return_type: carrier.clone(),
    };
    let schema = vec![carrier; 2];
    let facts = BatchFacts::sql_native_batch_with_hosts(
        &root,
        &schema,
        vec![OrdinaryCallSite::sql_native_batch(
            0,
            OrdinarySourceId::new(99, 0),
        )],
        CompileLimits::default(),
        &catalog,
    )
    .unwrap();
    let mut program =
        compile_batch(&root, &schema, LocalCompileContext::default(), &facts).unwrap();
    let native = Trace {
        no_unsigned,
        ..Trace::default()
    };
    let store = RefCell::new(NativeValueStore::new(8 * 1024 * 1024).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let mut input = Inputs {
        hosts: NativeBatchHosts::new(catalog, &metadata, &native, 8 * 1024 * 1024)
            .with_native_values(Some(&store)),
        store: &store,
        schema,
        values: &values,
        reads: Vec::new(),
        fail,
    };
    let actual = program.eval_with_bindings(
        ExecutionLimits::default(),
        &mut EvalContext::default(),
        values[0].len(),
        selection,
        &mut input,
    );
    if fail.is_some() {
        assert!(matches!(actual, Err(LocalError::BindingContract(_))));
        assert!(input.hosts.take_native_error().is_none());
    } else {
        let old = Trace {
            no_unsigned,
            ..Trace::default()
        };
        let mut chunk = Chunk::new_with_capacity(&fields, values[0].len());
        for row in 0..values[0].len() {
            for slot in 0..2 {
                // A literal argument does not read its physical input column.
                // Do not invent a chunk encoding of its possibly wider value.
                chunk.append_datum(
                    slot,
                    if literal[slot] {
                        &Datum::Null
                    } else {
                        &values[slot][row]
                    },
                );
            }
        }
        chunk.set_sel(Some(selection.to_vec()));
        let expected = crate::scalar_function::try_eval_batch(
            &Expression::ScalarFunction(function),
            &old,
            &chunk,
        );
        match (actual, expected) {
            (Ok(actual), Ok(Some(expected))) => {
                assert_eq!(actual.len(), expected.len());
                for (position, expected) in expected.iter().enumerate() {
                    let ScalarValueRef::Bytes(Some(token)) = actual.get_scalar_ref(position) else {
                        panic!("native output handle")
                    };
                    let value = store
                        .borrow()
                        .get(
                            token,
                            NativeValueRole::Output,
                            NativeValueOrigin {
                                node: 0,
                                row: InputRow {
                                    occurrence: position,
                                    input_row: selection[position],
                                },
                            },
                        )
                        .unwrap()
                        .clone();
                    assert_eq!(&value, expected, "{name} row{position}");
                }
            }
            (Err(LocalError::HostContract(message)), Err(expected)) => {
                // The existing SQL-host seam retains the actual native error
                // separately from its fixed driver sentinel.
                assert_eq!(message, "native SQL context operation failed");
                assert_eq!(input.hosts.take_native_error(), Some(expected));
            }
            (actual, expected) => panic!("window/native mismatch: {actual:?}, {expected:?}"),
        }
        assert_eq!(
            native.events.take(),
            old.events.take(),
            "{name} diagnostics/context"
        );
    }
    assert!(input.hosts.slots.iter().all(|slot| slot.task.is_none()));
    let mut expected_reads = Vec::new();
    'outer: for slot in 0..2 {
        for (occurrence, &input_row) in selection.iter().enumerate() {
            expected_reads.push((
                slot,
                InputRow {
                    occurrence,
                    input_row,
                },
            ));
            if fail == Some((slot, occurrence)) {
                break 'outer;
            }
        }
    }
    assert_eq!(input.reads, expected_reads);
    if !selection.is_empty() {
        input.fail = None;
        input.reads.clear();
        native.events.borrow_mut().clear();
        input.hosts.max_retained_bytes = 0;
        let limited = program.eval_with_bindings(
            ExecutionLimits::default(),
            &mut EvalContext::default(),
            values[0].len(),
            selection,
            &mut input,
        );
        assert!(matches!(limited, Err(LocalError::ResourceLimit(_))));
        assert!(input.reads.is_empty());
        assert!(input.hosts.slots.iter().all(|slot| slot.task.is_none()));
        store.borrow_mut().begin_window().unwrap();
        // Admit task/map and the source vector, then refuse the conversion
        // allowance before its first context read. The whole left input
        // phase has completed; no right input or stale success is returned.
        input.hosts.max_retained_bytes = input.hosts.retained_bytes().unwrap()
            + selection.len() * std::mem::size_of::<usize>()
            + selection.len() * std::mem::size_of::<Datum>()
            + 100;
        let limited = program.eval_with_bindings(
            ExecutionLimits::default(),
            &mut EvalContext::default(),
            values[0].len(),
            selection,
            &mut input,
        );
        assert!(matches!(limited, Err(LocalError::ResourceLimit(_))));
        assert_eq!(
            input.reads,
            selection
                .iter()
                .enumerate()
                .map(|(occurrence, &input_row)| (
                    0,
                    InputRow {
                        occurrence,
                        input_row
                    }
                ))
                .collect::<Vec<_>>()
        );
        assert!(native.events.borrow().is_empty());
        assert!(input.hosts.take_native_error().is_none());
        assert!(input.hosts.slots.iter().all(|slot| slot.task.is_none()));
    }
}
#[test]
fn native_window_arithmetic_matches_actual_bulk_values_context_and_argument_phases() {
    let integer = FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0);
    let real = FieldType::new(FieldTypeCode::Double)
        .with_flen(-1)
        .with_decimal(-1);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(40)
        .with_decimal(4);
    let string = FieldType::new(FieldTypeCode::VarString)
        .with_flen(-1)
        .with_decimal(-1);
    for name in ["plus", "minus", "mul"] {
        for selection in [vec![], vec![0, 1, 2], vec![2, 0, 2]] {
            for no_unsigned in [false, true] {
                execute_case(
                    name,
                    [integer.clone(), integer.clone()],
                    integer.clone(),
                    [
                        vec![Datum::Null, Datum::Int(-7), Datum::Int(11)],
                        vec![Datum::Int(3), Datum::Int(2), Datum::Int(0)],
                    ],
                    [false; 2],
                    &selection,
                    no_unsigned,
                    None,
                );
                execute_case(
                    name,
                    [real.clone(), real.clone()],
                    real.clone(),
                    [
                        vec![Datum::Null, Datum::Real(-1.25), Datum::Real(7.5)],
                        vec![Datum::Real(3.0), Datum::Real(0.5), Datum::Real(0.0)],
                    ],
                    [false; 2],
                    &selection,
                    no_unsigned,
                    None,
                );
                execute_case(
                    name,
                    [string.clone(), decimal.clone()],
                    decimal.clone(),
                    [
                        vec![
                            Datum::new_string("12.5"),
                            Datum::Null,
                            Datum::new_string("3x"),
                        ],
                        vec![
                            Datum::Decimal(Decimal::from_int(3)),
                            Datum::Decimal(Decimal::from_int(2)),
                            Datum::Decimal(Decimal::from_int(0)),
                        ],
                    ],
                    [false; 2],
                    &selection,
                    no_unsigned,
                    None,
                );
                execute_case(
                    name,
                    [integer.clone(), integer.clone()],
                    integer.clone(),
                    [vec![Datum::Int(3); 3], vec![Datum::Int(2); 3]],
                    [true; 2],
                    &selection,
                    no_unsigned,
                    None,
                );
            }
        }
    }
    for name in ["plus", "minus", "mul"] {
        for left_unsigned in [false, true] {
            for right_unsigned in [false, true] {
                for no_unsigned in [false, true] {
                    execute_case(
                        name,
                        [
                            integer.clone().with_unsigned(left_unsigned),
                            integer.clone().with_unsigned(right_unsigned),
                        ],
                        integer
                            .clone()
                            .with_unsigned(left_unsigned || right_unsigned),
                        [
                            vec![Datum::UInt(u64::MAX), Datum::Int(0), Datum::Int(i64::MAX)],
                            vec![Datum::Int(1), Datum::UInt(u64::MAX), Datum::Int(2)],
                        ],
                        [false; 2],
                        &[2, 0, 2],
                        no_unsigned,
                        None,
                    );
                }
            }
        }
    }
    execute_case(
        "plus",
        [real.clone(), real.clone()],
        real,
        [
            vec![Datum::Real(f64::MAX); 3],
            vec![Datum::Real(f64::MAX); 3],
        ],
        [false; 2],
        &[2, 0, 2],
        false,
        None,
    );
    // Right input failure must win over the left lane's eventual overflow.
    execute_case(
        "plus",
        [integer.clone(), integer.clone()],
        integer,
        [vec![Datum::Int(i64::MAX); 3], vec![Datum::Int(1); 3]],
        [false; 2],
        &[2, 0, 2],
        false,
        Some((1, 1)),
    );
}

#[test]
fn native_window_arithmetic_keeps_whole_2050_occurrence_phases_and_results() {
    let integer = FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0);
    let real = FieldType::new(FieldTypeCode::Double)
        .with_flen(-1)
        .with_decimal(-1);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(40)
        .with_decimal(4);
    let string = FieldType::new(FieldTypeCode::VarString)
        .with_flen(-1)
        .with_decimal(-1);
    let selection: Vec<_> = (0..2050).map(|position| [2, 0, 2][position % 3]).collect();
    execute_case(
        "plus",
        [integer.clone(), integer.clone()],
        integer.clone(),
        [
            vec![Datum::Null, Datum::Int(-7), Datum::Int(11)],
            vec![Datum::Int(3), Datum::Int(2), Datum::Int(0)],
        ],
        [false; 2],
        &selection,
        false,
        None,
    );
    execute_case(
        "minus",
        [real.clone(), real.clone()],
        real,
        [
            vec![Datum::Null, Datum::Real(-1.25), Datum::Real(7.5)],
            vec![Datum::Real(3.0), Datum::Real(0.5), Datum::Real(0.0)],
        ],
        [false; 2],
        &selection,
        false,
        None,
    );
    execute_case(
        "mul",
        [string, decimal.clone()],
        decimal,
        [
            vec![
                Datum::new_string("12.5"),
                Datum::Null,
                Datum::new_string("3x"),
            ],
            vec![
                Datum::Decimal(Decimal::from_int(3)),
                Datum::Decimal(Decimal::from_int(2)),
                Datum::Decimal(Decimal::from_int(0)),
            ],
        ],
        [false; 2],
        &selection,
        false,
        None,
    );
    execute_case(
        "plus",
        [integer.clone(), integer.clone()],
        integer,
        [vec![Datum::Int(i64::MAX); 3], vec![Datum::Int(1); 3]],
        [false; 2],
        &selection,
        false,
        Some((1, 0)),
    );
}

#[test]
fn native_window_quotients_keep_values_diagnostics_full_sources_and_terminal_resource_failures() {
    let integer = FieldType::new(FieldTypeCode::LongLong)
        .with_flen(20)
        .with_decimal(0);
    let real = FieldType::new(FieldTypeCode::Double);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal)
        .with_flen(65)
        .with_decimal(4);
    let string = FieldType::new(FieldTypeCode::VarString);
    for name in ["mod", "div", "intdiv"] {
        for (fields, values) in [
            (
                [integer.clone(), integer.clone()],
                [
                    vec![Datum::Null, Datum::Int(i64::MIN), Datum::Int(7)],
                    vec![Datum::Int(3), Datum::Int(-1), Datum::Int(0)],
                ],
            ),
            (
                [integer.clone().with_unsigned(true), integer.clone()],
                [
                    vec![Datum::UInt(u64::MAX), Datum::UInt(1), Datum::Null],
                    vec![Datum::Int(1), Datum::Int(-2), Datum::Int(0)],
                ],
            ),
            (
                [real.clone(), real.clone()],
                [
                    vec![Datum::Null, Datum::Real(-1.5), Datum::Real(7.0)],
                    vec![Datum::Real(2.0), Datum::Real(0.0), Datum::Real(-0.0)],
                ],
            ),
            (
                [decimal.clone(), decimal.clone()],
                [
                    vec![
                        Datum::Decimal(Decimal::from_literal("1.0000")),
                        Datum::Null,
                        Datum::Decimal(Decimal::from_literal("9223372036854775808")),
                    ],
                    vec![
                        Datum::Decimal(Decimal::from_literal("3.0000")),
                        Datum::Decimal(Decimal::from_int(0)),
                        Datum::Decimal(Decimal::from_int(1)),
                    ],
                ],
            ),
            (
                [string.clone(), decimal.clone()],
                [
                    vec![
                        Datum::new_string("12.5"),
                        Datum::Null,
                        Datum::new_string("3tail"),
                    ],
                    vec![
                        Datum::Decimal(Decimal::from_int(3)),
                        Datum::Decimal(Decimal::from_int(2)),
                        Datum::Decimal(Decimal::from_int(0)),
                    ],
                ],
            ),
        ] {
            let args = [0, 1]
                .map(|index| Expression::Column(Column::new(index as i64, fields[index].clone())));
            let result =
                crate::builtin_arithmetic::infer_arithmetic_type(name, &args[0], &args[1]).unwrap();
            for selection in [vec![], vec![0], vec![2, 0, 2], vec![1, 0]] {
                execute_case(
                    name,
                    fields.clone(),
                    result.clone(),
                    values.clone(),
                    [false; 2],
                    &selection,
                    false,
                    None,
                );
                if !selection.is_empty() {
                    execute_case(
                        name,
                        fields.clone(),
                        result.clone(),
                        values.clone(),
                        [false; 2],
                        &selection,
                        false,
                        Some((1, 0)),
                    );
                }
            }
        }
    }
}
