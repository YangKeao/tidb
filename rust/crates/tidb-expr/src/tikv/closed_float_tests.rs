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
fn closed_float_bridges_keep_display_value_profiles_actual_sources_and_veto_order() {
    use super::{eval_cast_double_in, eval_cast_float_in, eval_cast_float_value};
    use crate::{Columns, Datum, EvalError};
    use std::cell::{Cell, RefCell};
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, GoString, MySqlDuration, MysqlEnum,
        SessionTimeZone, Time, TimeType,
    };

    #[derive(Default)]
    struct Session {
        messages: RefCell<Vec<String>>,
        veto: Cell<bool>,
    }
    impl Columns for Session {
        fn get(&self, _: &[String]) -> Option<Datum> {
            panic!("operand is already evaluated");
        }
        fn time_zone(&self) -> SessionTimeZone {
            panic!("float conversion must not request a zone");
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.messages.borrow_mut().push(message.to_owned());
            if self.veto.get() {
                Err(EvalError::Unsupported("closed float truncate veto"))
            } else {
                Ok(())
            }
        }
    }
    let ctx = Session::default();
    let json = Datum::Json(BinaryJSON::parse("\"2.5\"").unwrap());
    assert_eq!(eval_cast_double_in(&ctx, &json), Ok(0.0));
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect FLOAT value: '\"2.5\"'".to_owned()]
    );
    assert_eq!(eval_cast_float_value(&json), 2.5);
    assert!(ctx.messages.borrow().is_empty());
    for value in [
        Datum::new_bytes([b'1', b'2', 0xff]),
        Datum::new_string(vec![b'1', b'2', 0xff]),
    ] {
        assert_eq!(eval_cast_double_in(&ctx, &value), Ok(12.0));
        assert_eq!(
            ctx.messages.take(),
            ["Truncated incorrect DOUBLE value: '12�'".to_owned()]
        );
        assert_eq!(eval_cast_float_value(&value), 0.0);
    }
    for (value, expected) in [
        (
            Datum::Enum(
                MysqlEnum::new(GoString::from_bytes([0xff]), 17),
                Collation::DEFAULT,
            ),
            17.0,
        ),
        (Datum::Bit(BinaryLiteral::from(vec![1; 9])), u64::MAX as f64),
        (
            Datum::Time(Time::from_raw_parts(
                CoreTime::from_date(2024, 1, 2, 0, 0, 0, 0),
                TimeType::Date,
                0,
            )),
            20240102.0,
        ),
        (
            Datum::Duration(MySqlDuration::new(1, 2, 3, 0, 0).unwrap()),
            10203.0,
        ),
        (Datum::Raw(b"42".to_vec()), 0.0),
    ] {
        assert_eq!(eval_cast_double_in(&ctx, &value), Ok(expected));
        assert_eq!(eval_cast_float_value(&value), expected);
        assert!(ctx.messages.borrow().is_empty());
    }
    for value in [Datum::Real(-0.0), Datum::Float32(-0.0)] {
        assert_eq!(
            eval_cast_double_in(&ctx, &value).unwrap().to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(
            eval_cast_float_in(&ctx, &value).unwrap().to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(
            eval_cast_float_value(&value).to_bits(),
            (-0.0_f64).to_bits()
        );
    }
    for value in [Datum::Real(f64::NAN), Datum::Float32(f64::NAN)] {
        assert!(eval_cast_double_in(&ctx, &value).unwrap().is_nan());
        assert!(eval_cast_float_in(&ctx, &value).unwrap().is_nan());
        assert!(eval_cast_float_value(&value).is_nan());
    }
    assert!(ctx.messages.borrow().is_empty());
    ctx.veto.set(true);
    assert_eq!(
        eval_cast_float_in(&ctx, &Datum::new_string("1e999")),
        Err(EvalError::Unsupported("closed float truncate veto"))
    );
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect DOUBLE value: '1e999'".to_owned()]
    );
    ctx.veto.set(false);
    assert!(matches!(
        eval_cast_float_in(&ctx, &Datum::new_string("1e999")),
        Err(EvalError::ConstantFloatCastOverflow { .. })
    ));
    assert_eq!(
        ctx.messages.take(),
        ["Truncated incorrect DOUBLE value: '1e999'".to_owned()]
    );
}
