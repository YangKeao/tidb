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
fn argument_integer_preserves_json_effect_order_identity_source_flags_and_worker_refusal() {
    use crate::{
        Columns, Datum, EvalError, ExpressionAdapterFailureClass, ReadyValuePoolOwner,
        ReadyValuePoolPolicy,
    };
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{
        BinaryJSON, Collation, FieldType, FieldTypeCode, FieldTypeFlags, MysqlEnum, MysqlSet,
        SessionTimeZone, VectorFloat32,
    };

    #[derive(Default)]
    struct Session {
        events: RefCell<Vec<String>>,
        fail_truncate: Cell<bool>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("argument value was already evaluated");
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push("zone".to_owned());
            SessionTimeZone::Named(chrono_tz::America::Los_Angeles)
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.events.borrow_mut().push(format!("truncate:{message}"));
            if self.fail_truncate.get() {
                Err(EvalError::Unsupported("argument integer truncate veto"))
            } else {
                Ok(())
            }
        }
        fn append_warning(&self, code: u16, _: &str) {
            self.events.borrow_mut().push(format!("append:{code}"));
        }
    }
    let ctx = Session::default();
    let unsigned = FieldType::new(FieldTypeCode::LongLong).with_flags(FieldTypeFlags::UNSIGNED);
    let signed = FieldType::new(FieldTypeCode::LongLong);
    for value in [Datum::Int(-1), Datum::UInt(u64::MAX), Datum::Null] {
        for source in [None, Some(&unsigned), Some(&signed)] {
            assert_eq!(
                crate::cast::cast_arg_as_int(&value, source, &ctx),
                Ok(value.clone())
            );
            assert!(ctx.events.borrow().is_empty());
        }
    }
    for (document, expected, warning) in [
        ("3", 3, None),
        ("-2", -2, None),
        ("18446744073709551615", -1, None),
        ("{}", 0, Some("Truncated incorrect INTEGER value: '{}'")),
        ("true", 0, Some("Truncated incorrect INTEGER value: 'true'")),
        (
            "\"12x\"",
            0,
            Some("Truncated incorrect INTEGER value: '\"12x\"'"),
        ),
    ] {
        let value = Datum::Json(BinaryJSON::parse(document).unwrap());
        assert_eq!(
            crate::cast::cast_arg_as_int(&value, Some(&unsigned), &ctx),
            Ok(Datum::Int(expected))
        );
        let expected: Vec<_> = warning
            .into_iter()
            .map(|text| format!("truncate:{text}"))
            .collect();
        assert_eq!(ctx.events.take(), expected);
    }
    ctx.fail_truncate.set(true);
    assert_eq!(
        crate::cast::cast_arg_as_int(
            &Datum::Json(BinaryJSON::parse("{}").unwrap()),
            Some(&unsigned),
            &ctx
        ),
        Err(EvalError::Unsupported("argument integer truncate veto")),
    );
    assert_eq!(
        ctx.events.take(),
        ["truncate:Truncated incorrect INTEGER value: '{}'".to_owned()]
    );
    assert_eq!(
        crate::cast::cast_arg_as_int(&Datum::new_string("1x"), None, &ctx),
        Err(EvalError::Unsupported("argument integer truncate veto")),
    );
    assert_eq!(
        ctx.events.take(),
        ["truncate:Truncated incorrect INTEGER value: '1x'".to_owned()]
    );
    ctx.fail_truncate.set(false);
    assert_eq!(
        crate::cast::cast_arg_as_int(&Datum::new_string("-2"), Some(&unsigned), &ctx),
        Ok(Datum::UInt(u64::MAX - 1))
    );
    assert_eq!(
        ctx.events.take(),
        ["append:8031".to_owned(), "zone".to_owned()]
    );
    assert_eq!(
        crate::cast::cast_arg_as_int(&Datum::new_string("-2"), None, &ctx),
        Ok(Datum::Int(-2))
    );
    assert_eq!(ctx.events.take(), ["zone".to_owned()]);
    for value in [
        Datum::Enum(MysqlEnum::new("not-numeric", 17), Collation::DEFAULT),
        Datum::Set(MysqlSet::new("not-numeric", 17), Collation::DEFAULT),
    ] {
        assert_eq!(
            crate::cast::cast_arg_as_int(&value, None, &ctx),
            Ok(Datum::Int(17))
        );
        assert_eq!(ctx.events.take(), ["zone".to_owned()]);
        assert_eq!(
            crate::cast::cast_arg_as_int(&value, Some(&unsigned), &ctx),
            Ok(Datum::UInt(17))
        );
        assert!(ctx.events.take().is_empty());
    }
    for (value, message) in [
        (Datum::MinNotNull, "range sentinel cast operand"),
        (Datum::MaxValue, "range sentinel cast operand"),
        (
            Datum::VectorFloat32(VectorFloat32::default()),
            "a vector can only be cast to string or vector",
        ),
    ] {
        assert_eq!(
            crate::cast::cast_arg_as_int(&value, Some(&unsigned), &ctx),
            Err(EvalError::Unsupported(message))
        );
        assert!(ctx.events.take().is_empty());
    }
    for slots in [0, 1] {
        let owner = ReadyValuePoolOwner::new(
            ReadyValuePoolPolicy::checked(
                slots,
                slots,
                16 << 20,
                1 << 20,
                2 << 20,
                64,
                16,
                1 << 16,
            )
            .unwrap(),
        )
        .unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        scope.with_columns(&ctx, |bound| {
            let result = crate::cast::cast_arg_as_int(&Datum::Real(2.25), Some(&unsigned), bound);
            if slots == 0 {
                assert!(
                    matches!(result, Err(EvalError::ExpressionAdapterFailure(error))
                    if error.class() == ExpressionAdapterFailureClass::PoolResource)
                );
            } else {
                assert_eq!(result, Ok(Datum::UInt(2)));
            }
            assert!(ctx.events.take().is_empty());
            assert_eq!(
                crate::cast::cast_arg_as_int(&Datum::UInt(7), None, bound),
                Ok(Datum::UInt(7))
            );
            assert!(ctx.events.take().is_empty());
        });
        drop(scope);
        execution.close();
    }
}
