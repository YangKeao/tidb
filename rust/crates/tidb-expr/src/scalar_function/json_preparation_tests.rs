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

#[test]
fn json_source_preparation_preserves_actual_metadata_and_row_batch_admission() {
    use super::{cast_json_argument_value, numeric_batch_candidate, ScalarFunction};
    use crate::builtin_ext::json::{
        cast_json_prepared, json_cast_source_supported, validate_json_cast_source,
    };
    use crate::column::Column;
    use crate::context::Columns;
    use crate::expression::Expression;
    use crate::{Datum, EvalError, JsonError};
    use tidb_ast::CiString;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{
        Collation, EvalType, FieldType, FieldTypeCode, FieldTypeFlags, MysqlEnum, MysqlSet,
    };

    fn binary(result: Result<Datum, EvalError>, tag: u8, bytes: &[u8]) {
        let Datum::Json(value) = result.unwrap() else {
            panic!("expected a binary JSON result");
        };
        assert_eq!(value.type_code(), tag);
        assert_eq!(value.value(), bytes);
    }
    fn function(
        source: Option<&FieldType>,
        target: Option<&FieldType>,
        index: i64,
    ) -> ScalarFunction {
        let mut column = Column::new(1, FieldType::new(FieldTypeCode::VarString));
        column.ret_type = source.cloned();
        column.index = index;
        let mut function = ScalarFunction::new(
            CiString::new("cast_json"),
            FieldType::new(FieldTypeCode::Json),
            vec![Expression::Column(column)],
        );
        function.ret_type = target.cloned();
        function
    }
    let text = FieldType::new(FieldTypeCode::VarString);
    let value_target = FieldType::new(FieldTypeCode::Json);
    let mut document_target = value_target.clone();
    document_target.add_flags(FieldTypeFlags::PARSE_TO_JSON);
    let vector = FieldType::new(FieldTypeCode::VectorFloat32);
    let unknown_vector =
        FieldType::new(FieldTypeCode::Unknown(225)).with_collation(Collation::DEFAULT);
    assert!(!json_cast_source_supported(None));
    assert!(matches!(
        validate_json_cast_source(None),
        Err(EvalError::Unsupported("a JSON cast with no source type"))
    ));
    assert!(matches!(
        cast_json_prepared(&Datum::Null, None, None),
        Err(EvalError::Unsupported("a JSON cast with no source type"))
    ));
    assert!(!json_cast_source_supported(Some(&vector)));
    assert!(
        matches!(validate_json_cast_source(Some(&vector)), Err(EvalError::Vector(message)) if message == "cannot cast from vector to json")
    );
    assert!(json_cast_source_supported(Some(&unknown_vector)));
    validate_json_cast_source(Some(&unknown_vector)).unwrap();
    assert_eq!(
        cast_json_prepared(&Datum::Null, Some(&unknown_vector), None).unwrap(),
        Datum::Null
    );
    binary(
        cast_json_prepared(
            &Datum::new_string("1"),
            Some(&unknown_vector),
            Some(&document_target),
        ),
        0x09,
        &[1, 0, 0, 0, 0, 0, 0, 0],
    );

    // These are direct prepared-value DTO cases, not claims that a typed
    // producer returns a particular Datum variant for every source field.
    let mut unsigned = FieldType::new(FieldTypeCode::LongLong);
    unsigned.add_flags(FieldTypeFlags::UNSIGNED);
    let year = FieldType::new(FieldTypeCode::Year);
    for source in [&unsigned, &year] {
        binary(
            cast_json_prepared(&Datum::Int(-1), Some(source), None),
            0x0a,
            &[255; 8],
        );
        binary(
            cast_json_argument_value(&function(Some(source), None, 0), Datum::Int(-1)),
            0x0a,
            &[255; 8],
        );
    }
    let array_year = year.clone().with_array(true);
    binary(
        cast_json_prepared(&Datum::Int(-1), Some(&array_year), None),
        0x09,
        &[255; 8],
    );
    let mut boolean_unsigned = unsigned.clone();
    boolean_unsigned.add_flags(FieldTypeFlags::IS_BOOLEAN);
    for value in [Datum::Int(-1), Datum::UInt(u64::MAX)] {
        binary(
            cast_json_prepared(&value, Some(&boolean_unsigned), None),
            0x04,
            &[1],
        );
    }
    binary(
        cast_json_prepared(
            &Datum::Int(0),
            Some(&boolean_unsigned),
            Some(&document_target),
        ),
        0x04,
        &[2],
    );
    for target in [None, Some(&value_target)] {
        binary(
            cast_json_prepared(&Datum::new_string("1"), Some(&text), target),
            0x0c,
            &[1, 49],
        );
        binary(
            cast_json_argument_value(&function(Some(&text), target, 0), Datum::new_string("1")),
            0x0c,
            &[1, 49],
        );
    }
    binary(
        cast_json_prepared(&Datum::new_string("1"), Some(&text), Some(&document_target)),
        0x09,
        &[1, 0, 0, 0, 0, 0, 0, 0],
    );
    assert!(matches!(
        cast_json_prepared(
            &Datum::new_string("bad"),
            Some(&text),
            Some(&document_target)
        ),
        Err(EvalError::Json(JsonError::InvalidText))
    ));
    assert!(matches!(
        cast_json_argument_value(&function(None, Some(&document_target), 0), Datum::Null),
        Err(EvalError::Unsupported("a JSON cast with no source type"))
    ));

    for (value, code) in [
        (
            Datum::Enum(MysqlEnum::new("1", 7), Collation::DEFAULT),
            FieldTypeCode::Enum,
        ),
        (
            Datum::Set(MysqlSet::new("1", 1), Collation::DEFAULT),
            FieldTypeCode::Set,
        ),
    ] {
        let source = FieldType::new(code).with_collation(Collation::DEFAULT);
        let source_eval: EvalType = source.eval_type();
        assert_eq!(source_eval, EvalType::String);
        binary(
            cast_json_prepared(&value, Some(&source), Some(&document_target)),
            0x09,
            &[1, 0, 0, 0, 0, 0, 0, 0],
        );
        let mut integer_source = source.clone();
        integer_source.add_flags(FieldTypeFlags::ENUM_SET_AS_INT);
        assert_eq!(integer_source.eval_type(), EvalType::Int);
        // With ETInt the original hybrid is not turned into Bytes: its
        // ordinary scalar conversion is a JSON string, not parsed text.
        binary(
            cast_json_prepared(&value, Some(&integer_source), Some(&document_target)),
            0x0c,
            &[1, 49],
        );
        let array_source = source.with_array(true);
        assert_eq!(array_source.eval_type(), EvalType::Json);
        binary(
            cast_json_prepared(&value, Some(&array_source), Some(&document_target)),
            0x0c,
            &[1, 49],
        );
        let mut unknown =
            FieldType::new(FieldTypeCode::Unknown(247)).with_collation(Collation::DEFAULT);
        unknown.add_flags(FieldTypeFlags::ENUM_SET_AS_INT);
        assert_eq!(unknown.eval_type(), EvalType::String);
        binary(
            cast_json_prepared(&value, Some(&unknown), Some(&document_target)),
            0x09,
            &[1, 0, 0, 0, 0, 0, 0, 0],
        );
    }

    struct Session;
    impl Columns for Session {
        fn get(&self, name: &[String]) -> Option<Datum> {
            panic!("row/column test must not escape through AST lookup: {name:?}")
        }
    }
    let owner = crate::AsciiPoolOwner::new(
        crate::AsciiPoolPolicy::checked(4, 4, 16 << 20, 1 << 20, 2 << 20, 64, 8, 1 << 16).unwrap(),
    )
    .unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&Session, |ctx| {
        let mut vectors = Chunk::new_with_capacity(std::slice::from_ref(&vector), 1);
        vectors.append_null(0);
        let row = vectors.get_row(0);
        let vector_cast = function(Some(&vector), Some(&document_target), 0);
        let Expression::Column(column) = &vector_cast.args[0] else { unreachable!(); };
        assert_eq!(column.eval(row).unwrap(), Datum::Null);
        assert!(matches!(vector_cast.eval(ctx, row), Err(EvalError::Vector(message)) if message == "cannot cast from vector to json"));
        // A real Column getter's bounds error establishes pre-child priority;
        // no invented Datum kind or callback stands in for that getter.
        let invalid_column_cast = function(Some(&vector), Some(&document_target), 99);
        let Expression::Column(column) = &invalid_column_cast.args[0] else { unreachable!(); };
        assert!(matches!(column.eval(row), Err(EvalError::Unsupported("column index is outside the input row"))));
        assert!(matches!(invalid_column_cast.eval(ctx, row), Err(EvalError::Vector(message)) if message == "cannot cast from vector to json"));
        let missing = function(None, Some(&document_target), 0);
        assert!(matches!(missing.eval(ctx, row), Err(EvalError::Unsupported("a JSON cast with no source type"))));
        for function in [vector_cast, invalid_column_cast, missing] {
            let expression = Expression::ScalarFunction(function);
            assert!(numeric_batch_candidate(&expression, ctx).unwrap().is_none());
        }

        let mut strings = Chunk::new_with_capacity(std::slice::from_ref(&text), 3);
        strings.append_bytes(0, b"1");
        strings.append_null(0);
        strings.append_bytes(0, b"2");
        for (target, tag, first, last) in [
            (&document_target, 0x09, vec![1, 0, 0, 0, 0, 0, 0, 0], vec![2, 0, 0, 0, 0, 0, 0, 0]),
            (&value_target, 0x0c, vec![1, 49], vec![1, 50]),
        ] {
            let cast = function(Some(&text), Some(target), 0);
            binary(cast.eval(ctx, strings.get_row(0)), tag, &first);
            assert_eq!(cast.eval(ctx, strings.get_row(1)).unwrap(), Datum::Null);
            binary(cast.eval(ctx, strings.get_row(2)), tag, &last);
            let expression = Expression::ScalarFunction(cast);
            let candidate = numeric_batch_candidate(&expression, ctx).unwrap().expect("real typed string-column JSON candidate");
            assert_eq!(candidate.target(), EvalType::Json);
            let mut results = candidate.eval_native(ctx, &strings).unwrap().into_iter();
            binary(Ok(results.next().unwrap()), tag, &first);
            assert_eq!(results.next().unwrap(), Datum::Null);
            binary(Ok(results.next().unwrap()), tag, &last);
            assert!(results.next().is_none());
        }
    });
    drop(scope);
    execution.close();
    // Batch evidence is candidate + native evaluation, not a projection-suite
    // route seal, PB selection proof, or a pool/resource accounting claim.
}
