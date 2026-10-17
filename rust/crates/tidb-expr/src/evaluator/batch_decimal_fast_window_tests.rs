use super::*;
use tidb_query_expr::local::HostSignature;

struct NoContextCalls;
impl Columns for NoContextCalls {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no expression callback")
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        panic!("fast phase cannot read time zone")
    }
    fn div_precision_increment(&self) -> u32 {
        panic!("fast phase cannot read precision")
    }
    fn no_unsigned_subtraction(&self) -> bool {
        panic!("fast phase cannot read unsigned mode")
    }
    fn enable_vectorized_expression(&self) -> bool {
        panic!("fast phase precedes vector flag")
    }
    fn append_warning(&self, _: u16, _: &str) {
        panic!("fast phase cannot warn")
    }
    fn handle_truncate(&self, _: &str) -> std::result::Result<(), EvalError> {
        panic!("fast phase cannot truncate")
    }
}
struct Inputs<'a> {
    hosts: NativeBatchHosts<'a>,
    store: &'a RefCell<NativeValueStore>,
    schema: Vec<tipb::FieldType>,
    values: &'a [Vec<Outcome>; 2],
    reads: Vec<(usize, InputRow)>,
    fail: Option<(usize, usize)>,
    bad_role: Option<(usize, usize)>,
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
            return Err(binding("fast decimal input sentinel"));
        }
        let origin = NativeValueOrigin {
            node: slot + 1,
            row,
        };
        let token = if self.bad_role == Some((slot, row.occurrence)) {
            self.store
                .borrow_mut()
                .insert_clone(&Datum::Null, NativeValueRole::Output, origin)?
        } else {
            self.store
                .borrow_mut()
                .insert_decimal_fast(self.values[slot][row.input_row].clone(), origin)?
        };
        Ok(VectorValue::from_scalar(
            &ScalarValue::Bytes(Some(token)),
            1,
        ))
    }
    fn host_services(&mut self) -> Option<&mut dyn LocalHostServices> {
        Some(&mut self.hosts)
    }
}
fn run_case(
    operation: SqlBatchHostOperation,
    values: [Vec<Outcome>; 2],
    selection: &[usize],
    fail: Option<(usize, usize)>,
) {
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
            OrdinarySourceId::new(111, 0),
        )],
        CompileLimits::default(),
        &catalog,
    )
    .unwrap();
    let mut program =
        compile_batch(&root, &schema, LocalCompileContext::default(), &facts).unwrap();
    assert!(program.requires_whole_selection());
    let metadata = [NativeHostMetadata::DecimalFast {
        operation,
        argument_nodes: [1, 2],
        output_node: 0,
    }];
    let store = RefCell::new(NativeValueStore::new(64 << 20).unwrap());
    store.borrow_mut().begin_window().unwrap();
    let context = NoContextCalls;
    let mut input = Inputs {
        hosts: NativeBatchHosts::new(catalog, &metadata, &context, 64 << 20)
            .with_native_values(Some(&store)),
        store: &store,
        schema,
        values: &values,
        reads: Vec::new(),
        fail,
        bad_role: None,
    };
    let result = program.eval_with_bindings(
        ExecutionLimits::default(),
        &mut EvalContext::default(),
        values[0].len(),
        selection,
        &mut input,
    );
    let mut expected_reads = Vec::new();
    let mut unsupported = false;
    let mut failed = false;
    'phase: for slot in 0..2 {
        for (occurrence, &input_row) in selection.iter().enumerate() {
            expected_reads.push((
                slot,
                InputRow {
                    occurrence,
                    input_row,
                },
            ));
            if fail == Some((slot, occurrence)) {
                failed = true;
                break 'phase;
            }
        }
        if selection
            .iter()
            .any(|&row| matches!(values[slot][row], Outcome::Unsupported))
        {
            unsupported = true;
            break;
        }
    }
    assert_eq!(input.reads, expected_reads);
    if failed {
        assert!(
            matches!(result, Err(LocalError::BindingContract(ref reason)) if reason.to_string()=="fast decimal input sentinel")
        );
    } else {
        let VectorValue::Bytes(output) = result.unwrap() else {
            panic!("fast capability vector required")
        };
        let mut expected = Vec::new();
        if !unsupported {
            let old_op = match operation {
                SqlBatchHostOperation::AddWindowNative => {
                    crate::tikv::BinaryArithmeticOperation::Add
                }
                SqlBatchHostOperation::SubtractWindowNative => {
                    crate::tikv::BinaryArithmeticOperation::Subtract
                }
                _ => crate::tikv::BinaryArithmeticOperation::Multiply,
            };
            for &row in selection {
                let (Outcome::Value(left), Outcome::Value(right)) =
                    (&values[0][row], &values[1][row])
                else {
                    unreachable!()
                };
                // Reference retains its old ready-arguments entry; the new
                // provider above calls only the shared plain coefficient kernel.
                let value = crate::tikv::eval_arithmetic_decimal_fast_in(
                    old_op,
                    *left,
                    *right,
                    &crate::NoColumns,
                )
                .unwrap();
                unsupported |= matches!(value, Outcome::Unsupported);
                expected.push(value);
                if unsupported {
                    break;
                }
            }
        }
        for (occurrence, &input_row) in selection.iter().enumerate() {
            let expected = if unsupported {
                &Outcome::Unsupported
            } else {
                &expected[occurrence]
            };
            let token = output.get(occurrence).unwrap();
            let origin = NativeValueOrigin {
                node: 0,
                row: InputRow {
                    occurrence,
                    input_row,
                },
            };
            assert_eq!(
                store.borrow().decimal_fast(token, origin).unwrap(),
                expected
            );
            assert!(store
                .borrow()
                .get(token, NativeValueRole::Output, origin)
                .is_err());
        }
    }
    assert!(input.hosts.slots.iter().all(|slot| slot.task.is_none()));
    assert!(input.hosts.take_native_error().is_none());
    // Terminal provider rejection cannot enter another phase or leak a task.
    if !selection.is_empty() {
        input.reads.clear();
        input.fail = None;
        input.hosts.max_retained_bytes = 0;
        store.borrow_mut().begin_window().unwrap();
        let result = program.eval_with_bindings(
            ExecutionLimits::default(),
            &mut EvalContext::default(),
            values[0].len(),
            selection,
            &mut input,
        );
        assert!(matches!(result, Err(LocalError::ResourceLimit(_))));
        assert!(input.reads.is_empty());
        assert!(input.hosts.slots.iter().all(|slot| slot.task.is_none()));
        input.reads.clear();
        store.borrow_mut().begin_window().unwrap();
        input.hosts.max_retained_bytes = input.hosts.retained_bytes().unwrap()
            + selection.len() * size_of::<usize>()
            + selection.len() * size_of::<Option<FastValue>>()
            - 1;
        let result = program.eval_with_bindings(
            ExecutionLimits::default(),
            &mut EvalContext::default(),
            values[0].len(),
            selection,
            &mut input,
        );
        assert!(matches!(result, Err(LocalError::ResourceLimit(_))));
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
        assert!(input.hosts.slots.iter().all(|slot| slot.task.is_none()));
        input.reads.clear();
        input.hosts.max_retained_bytes = 64 << 20;
        input.bad_role = Some((0, selection.len() - 1));
        store.borrow_mut().begin_window().unwrap();
        let result = program.eval_with_bindings(
            ExecutionLimits::default(),
            &mut EvalContext::default(),
            values[0].len(),
            selection,
            &mut input,
        );
        assert!(
            matches!(result, Err(LocalError::BindingContract(_))),
            "a late invalid purpose must not be hidden by an earlier Unsupported outcome"
        );
        assert!(input.hosts.slots.iter().all(|slot| slot.task.is_none()));
    }
}
fn fast(coefficient: i128, storage_scale: u32, scale: u32) -> Outcome {
    Outcome::Value(Some(FastValue {
        coefficient,
        storage_scale,
        scale,
    }))
}
#[test]
fn decimal_fast_window_matches_native_coefficients_nulls_and_domain_refusals() {
    let cases = [
        [fast(123, 2, 1), fast(20, 2, 1)],
        [fast(-5, 1, 1), fast(7, 0, 0)],
        [fast(i128::MIN, 0, 0), fast(i128::MIN, 0, 0)],
        [fast(i128::MAX, 0, 0), fast(1, 0, 0)],
        [fast(i128::MAX, 0, 0), fast(2, 0, 0)],
        [fast(1, 30, 30), fast(1, 1, 1)],
        [Outcome::Value(None), fast(i128::MIN, 0, 0)],
        [fast(i128::MAX, 0, 0), Outcome::Value(None)],
        [Outcome::Value(None), Outcome::Value(None)],
    ];
    for operation in [
        SqlBatchHostOperation::AddWindowNative,
        SqlBatchHostOperation::SubtractWindowNative,
        SqlBatchHostOperation::MultiplyWindowNative,
    ] {
        for case in &cases {
            let values = [vec![case[0].clone(); 3], vec![case[1].clone(); 3]];
            for selection in [&[][..], &[0][..], &[2, 0, 2][..]] {
                run_case(operation, values.clone(), selection, None);
            }
        }
    }
}
#[test]
fn decimal_fast_window_preserves_whole_operand_refusal_and_terminal_failure_order() {
    let selection: Vec<_> = (0..2050).collect();
    for operation in [
        SqlBatchHostOperation::AddWindowNative,
        SqlBatchHostOperation::SubtractWindowNative,
        SqlBatchHostOperation::MultiplyWindowNative,
    ] {
        let base = [vec![fast(1, 0, 0); 2050], vec![fast(2, 0, 0); 2050]];
        run_case(operation, base.clone(), &selection, None);
        let mut left = base.clone();
        left[0][2049] = Outcome::Unsupported;
        run_case(operation, left, &selection, Some((1, 0)));
        let mut early = base.clone();
        early[0][0] = Outcome::Unsupported;
        run_case(operation, early, &selection, None);
        let mut right = base.clone();
        right[1][2049] = Outcome::Unsupported;
        run_case(operation, right, &selection, None);
        let mut null = base.clone();
        null[0][0] = Outcome::Value(None);
        run_case(operation, null, &selection, Some((1, 0)));
        run_case(operation, base, &selection, Some((0, 2049)));
    }
}
