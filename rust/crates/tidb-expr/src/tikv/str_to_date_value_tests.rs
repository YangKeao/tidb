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

//! Original date parser values, warnings, metadata identity and mode effects.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::cell::{Cell, RefCell};
use tidb_datatype::{DateModes, FieldTypeCode, SessionTimeZone};
struct Context {
    cache: Option<ReadyValueCache>,
    reject: bool,
    bits: usize,
    events: RefCell<Vec<String>>,
    calls: Cell<usize>,
}
impl Context {
    fn new(cache: bool, bits: usize) -> Self {
        Self {
            cache: cache.then(ReadyValueCache::new),
            reject: false,
            bits,
            events: RefCell::new(vec![]),
            calls: Cell::new(0),
        }
    }
    fn reset(&self) {
        self.events.borrow_mut().clear();
        self.calls.set(0);
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("date parser must not invent sources")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject,
            "date parser attempted retired Ready worker lookup"
        );
        self.cache.as_ref()
    }
    fn context_id(&self) -> u64 {
        8201
    }
    fn date_modes(&self) -> DateModes {
        let n = self.calls.get();
        self.calls.set(n + 1);
        self.events
            .borrow_mut()
            .push(format!("modes:{n}:{}", self.bits));
        DateModes {
            no_zero_date: self.bits & 1 != 0,
            no_zero_in_date: self.bits & 2 != 0,
            allow_invalid_dates: self.bits & 4 != 0,
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        SessionTimeZone::utc()
    }
}
fn inputs() -> Vec<Datum> {
    let mut values = crate::ops::arithmetic::tests::fixtures();
    values.extend(
        [
            "",
            "2026-10-18",
            "2020-02-29",
            "2021-02-29",
            "0000-00-00",
            "01",
            "2019",
            "366",
            "999",
            "24:00:00",
            "23:59:59",
            "12:34:56.1234567",
            "12:01:02 AM",
            "12:01:02 PM",
            "2026-10-18 12:34:56.123456",
            "20261018",
            "1st October 2026",
            "October 18 2026",
            "Oct 18 2026",
            "one一😂",
            " \t2026-10-18 trailing",
            "2026/10/18",
            "2019-13-01",
            "2019-00-01",
            "2019-01-00",
            "69",
            "70",
            "x=2026,p18",
            "9999-12-31",
            "0001-01-01",
        ]
        .map(Datum::new_string),
    );
    values
}
fn formats() -> Vec<Datum> {
    let mut values = crate::ops::arithmetic::tests::fixtures();
    values.extend(
        [
            "",
            "%Y-%m-%d",
            "%Y%m%d",
            "%Y-%m-%d %H:%i:%s.%f",
            "%Y-%m-%d %T",
            "%H:%i:%s",
            "%T",
            "%r",
            "%h:%i:%s %p",
            "%H:%i:%s %p",
            "%H:%i:%s.%f",
            "%f",
            "%Y",
            "%y",
            "%m",
            "%d",
            "%j",
            "%m,%Y",
            "%d,%m",
            "%d,%Y",
            "%M %d %Y",
            "%b %d %Y",
            "%D %M %Y",
            "%%",
            "%.",
            "%@%Y%.%#",
            "%W %Y-%m-%d",
            "%Y/%m/%d",
            "%q",
            " %Y-%m-%d ",
            "literal",
        ]
        .map(Datum::new_string),
    );
    values
}
fn targets() -> [Option<FieldTypeCode>; 9] {
    [
        None,
        Some(FieldTypeCode::Datetime),
        Some(FieldTypeCode::Date),
        Some(FieldTypeCode::Duration),
        Some(FieldTypeCode::Timestamp),
        Some(FieldTypeCode::VarString),
        Some(FieldTypeCode::Unknown(12)),
        Some(FieldTypeCode::Unknown(7)),
        Some(FieldTypeCode::Unknown(255)),
    ]
}
fn observation(result: &Result<Datum, EvalError>, ctx: &Context) -> String {
    let identity = result
        .as_ref()
        .ok()
        .map(|v| identity_value::encode(v).expect("actual date identity"));
    let cause = match result {
        Err(EvalError::ExpressionRuntimeFailure(e)) => {
            Some((e.local_error().to_string(), e.phase()))
        }
        _ => None,
    };
    format!(
        "{result:?}|{identity:?}|{cause:?}|{:?}\n",
        ctx.events.borrow()
    )
}
#[test]
fn original_str_to_date_values_warning_modes_and_target_identity_witness() {
    let inputs = inputs();
    let formats = formats();
    let mut hash = 0xcbf29ce484222325;
    let mut count = 0;
    for profile in 0..3 {
        for bits in 0..8 {
            let mut ctx = Context::new(profile != 0, bits);
            for target in targets() {
                for input in &inputs {
                    for format in &formats {
                        if profile == 2 {
                            ctx = Context::new(true, bits)
                        }
                        ctx.reset();
                        let result =
                            eval_str_to_date_in(&ctx, &[input.clone(), format.clone()], target);
                        let independent_ctx = Context::new(false, bits);
                        let independent = super::str_to_date::str_to_date_bounded(
                            &independent_ctx,
                            &[input.clone(), format.clone()],
                            target,
                            64 << 20,
                        );
                        assert_eq!(observation(&result,&ctx),observation(&independent,&independent_ctx),"OLD date program vs actual SDK state: input={input:?} format={format:?} target={target:?}");
                        for byte in observation(&result, &ctx).bytes() {
                            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                        }
                        count += 1;
                    }
                }
            }
            for target in targets() {
                for args in [
                    vec![],
                    vec![Datum::Null],
                    vec![Datum::Null, Datum::Null, Datum::Null],
                ] {
                    ctx.reset();
                    let result = eval_str_to_date_in(&ctx, &args, target);
                    let independent_ctx = Context::new(false, bits);
                    let independent = super::str_to_date::str_to_date_bounded(
                        &independent_ctx,
                        &args,
                        target,
                        64 << 20,
                    );
                    assert_eq!(
                        observation(&result, &ctx),
                        observation(&independent, &independent_ctx)
                    );

                    for byte in observation(&result, &ctx).bytes() {
                        hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
                    }
                    count += 1;
                }
            }
        }
    }
    eprintln!("OLD STR_TO_DATE count={count} hash={hash:016x}");
    assert_eq!(count, 844344);
    assert_eq!(hash, 0xd7dd9f9ee5b55431);
}
#[test]
fn str_to_date_actual_value_does_not_lookup_retired_worker() {
    let mut ctx = Context::new(true, 0);
    ctx.reject = true;
    assert_eq!(
        eval_str_to_date_in(
            &ctx,
            &[
                Datum::new_string("2020-02-29"),
                Datum::new_string("%Y-%m-%d")
            ],
            None
        ),
        Ok(Datum::new_string("2020-02-29"))
    );
}
