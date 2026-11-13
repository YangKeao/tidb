use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tidb_datatype::Datum;
use tidb_executor::StmtContext;
use tidb_expr::{Columns, MysqlRng};

#[test]
fn shared_execution_borrows_real_statement_effects() {
    use tidb_ast::CiString;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{Decimal, FieldType, FieldTypeCode};
    use tidb_expr::{
        constant::{Constant, ParamMarker},
        expression::Expression,
        scalar_function::ScalarFunction,
        shared_kernel::SharedProjection,
    };
    let integer = FieldType::new(FieldTypeCode::LongLong);
    let decimal = FieldType::new(FieldTypeCode::NewDecimal);
    let real = FieldType::new(FieldTypeCode::Double);
    let text = FieldType::new(FieldTypeCode::VarString);
    let literal = |value, ty: &FieldType| Expression::Constant(Constant::new(value, ty.clone()));
    let call = |name, ty: &FieldType, args| {
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ty.clone(), args))
    };
    let mut parameter = Constant::new(Datum::Null, decimal.clone());
    parameter.param_marker = Some(ParamMarker { order: 0 });
    let projection = SharedProjection::compile(&[
        call("rand", &real, vec![literal(Datum::Int(7), &integer)]),
        call("rand", &real, vec![]),
        call(
            "mul",
            &decimal,
            vec![
                Expression::Constant(parameter),
                literal(Datum::Decimal(Decimal::from_int(1)), &decimal),
            ],
        ),
        call(
            "setvar",
            &text,
            vec![
                literal(Datum::new_string(b"state".to_vec()), &text),
                literal(Datum::new_string(b"stored".to_vec()), &text),
            ],
        ),
    ])
    .unwrap();
    let vars = Arc::new(Mutex::new(HashMap::new()));
    let rng = Arc::new(MysqlRng::new_with_seed(1));
    let context = StmtContext::for_query()
        .with_rand_session(Arc::clone(&rng))
        .with_user_vars(Arc::clone(&vars))
        .with_prepared_params(Arc::from([Datum::Int(42)]));
    let mut execution = projection.start_execution(&context);
    let seeded = MysqlRng::new_with_seed(7);
    let unseeded = MysqlRng::new_with_seed(1);
    for _ in 0..2 {
        let mut input = Chunk::new_empty(&[]);
        input.set_num_virtual_rows(1);
        let result = execution
            .evaluate(&input, &[0], &mut Default::default())
            .unwrap();
        assert_eq!(
            result,
            vec![
                vec![Datum::Real(seeded.gen())],
                vec![Datum::Real(unseeded.gen())],
                vec![Datum::Decimal(Decimal::from_int(42))],
                vec![Datum::new_string(b"stored".to_vec())]
            ]
        );
        assert_eq!(
            vars.lock().unwrap().get("state"),
            Some(&Datum::new_string(b"stored".to_vec()))
        );
    }
    let mut input = Chunk::new_empty(&[]);
    input.set_num_virtual_rows(1);
    let result = projection
        .start_execution(&context)
        .evaluate(&input, &[0], &mut Default::default())
        .unwrap();
    assert_eq!(
        result[0],
        vec![Datum::Real(MysqlRng::new_with_seed(7).gen())]
    );
    assert_eq!(result[1], vec![Datum::Real(unseeded.gen())]);
    assert_eq!(context.param_value(0).unwrap(), Datum::Int(42));
    assert_eq!(rng.gen(), unseeded.gen());
}
