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
fn frozen_partial_calendar_evidence() -> (
    Vec<String>,
    Vec<usize>,
    Vec<Option<String>>,
    serde_json::Value,
) {
    use std::io::Read;
    fn fingerprint(data: &[u8]) -> u64 {
        data.iter().fold(0xcbf29ce484222325, |h, b| {
            (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
        })
    }
    let original = include_bytes!("testdata/str_to_date_original_n7b.json.gz");
    let oracle = include_bytes!("testdata/str_to_date_go_partial_calendar_n7d.json");
    assert_eq!(fingerprint(original), 0x11c5df8ddf89f73);
    assert_eq!(fingerprint(oracle), 0x86209b6ea68469e9);
    let mut decoded = String::new();
    flate2::read::GzDecoder::new(original.as_slice())
        .read_to_string(&mut decoded)
        .unwrap();
    let original: serde_json::Value = serde_json::from_str(&decoded).unwrap();
    assert_eq!(original["historical_hash"], "d7dd9f9ee5b55431");
    assert_eq!(original["count"], 844344);
    let dictionary = original["dictionary"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect();
    let indices: Vec<_> = original["indices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap() as usize)
        .collect();
    assert_eq!(indices.len(), 281448);
    let go: serde_json::Value = serde_json::from_slice(oracle).unwrap();
    let calendars = original["calendars"].as_array().unwrap();
    assert_eq!(calendars.len(), 3906);
    let mut accepted = vec![None; 3906];
    for answer in go["calendars"].as_array().unwrap() {
        let pair = answer["pair"].as_u64().unwrap() as usize;
        assert_eq!(answer["fields"], calendars[pair]["fields"]);
        assert_eq!(answer["flags"], calendars[pair]["flags"]);
        assert!(accepted[pair].is_none());
        accepted[pair] = Some(answer["text"].as_str().unwrap().to_owned());
    }
    for (pair, calendar) in calendars.iter().enumerate() {
        let expected = !calendar.is_null()
            && calendar["fields"][1] == 0
            && calendar["fields"][2].as_i64().unwrap() <= 31;
        assert_eq!(
            accepted[pair].is_some(),
            expected,
            "complete original zero-month calendar domain"
        );
    }
    assert_eq!(accepted.iter().flatten().count(), 168);
    (dictionary, indices, accepted, go)
}
#[test]
fn original_str_to_date_recorded_history_and_explicit_go_partial_calendar_deltas() {
    let (dictionary, indices, accepted, _) = frozen_partial_calendar_evidence();
    let inputs = inputs();
    let formats = formats();
    assert_eq!((inputs.len(), formats.len()), (62, 63));
    let mut historical_hash = 0xcbf29ce484222325u64;
    let mut count = 0;
    let mut deltas = 0;
    for profile in 0..3 {
        for bits in 0..8 {
            let mut ctx = Context::new(profile != 0, bits);
            for target in targets() {
                for (i, input) in inputs.iter().enumerate() {
                    for (j, format) in formats.iter().enumerate() {
                        if profile == 2 {
                            ctx = Context::new(true, bits)
                        }
                        ctx.reset();
                        let result =
                            eval_str_to_date_in(&ctx, &[input.clone(), format.clone()], target);
                        let actual = observation(&result, &ctx);
                        let original = &dictionary[indices[count % indices.len()]];
                        if bits & 1 == 0 && accepted[i * formats.len() + j].is_some() {
                            let expected = Ok(Datum::new_string(
                                accepted[i * formats.len() + j].as_ref().unwrap().as_str(),
                            ));
                            let expected_ctx = Context::new(false, bits);
                            expected_ctx.date_modes();
                            assert_eq!(actual,observation(&expected,&expected_ctx),"Go-validated partial-calendar delta input={input:?} format={format:?} target={target:?} bits={bits}");
                            assert_ne!(&actual, original);
                            assert!(original.starts_with("Ok(Null)|"));
                            deltas += 1;
                        } else {
                            assert_eq!(&actual,original,"UNCHANGED original record input={input:?} format={format:?} target={target:?} bits={bits}");
                        }
                        // HISTORICAL replay: hash captured OLD observations, never pretend
                        // the intentionally corrected producer still generates this digest.
                        for byte in original.bytes() {
                            historical_hash =
                                (historical_hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
                        }
                        count += 1;
                    }
                }
            }
            for target in targets() {
                for args in [vec![], vec![Datum::Null], vec![Datum::Null; 3]] {
                    ctx.reset();
                    let result = eval_str_to_date_in(&ctx, &args, target);
                    let original = &dictionary[indices[count % indices.len()]];
                    assert_eq!(&observation(&result, &ctx), original);
                    for byte in original.bytes() {
                        historical_hash =
                            (historical_hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
                    }
                    count += 1;
                }
            }
        }
    }
    assert_eq!(count, 844344);
    assert_eq!(deltas, 18144);
    assert_eq!(historical_hash, 0xd7dd9f9ee5b55431);
    eprintln!("Original844344 d7dd9f9ee5b55431 replayed;826200 unchanged observations and18144 explicit Go-calendar deltas verified");
}
#[test]
fn str_to_date_original_go_parser_partial_and_full_zero_fixtures() {
    let (_, _, _, go) = frozen_partial_calendar_evidence();
    for case in go["parser"].as_array().unwrap().iter().take(10) {
        assert_eq!(case["success"], true);
        let ctx = Context::new(false, 2); // NO_ZERO_IN_DATE remains on.
        let result = eval_str_to_date_in(
            &ctx,
            &[
                Datum::new_string(case["input"].as_str().unwrap()),
                Datum::new_string(case["format"].as_str().unwrap()),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            result,
            Datum::new_string(case["text"].as_str().unwrap()),
            "original Go parser fixture {case}"
        );
        assert_eq!(*ctx.events.borrow(), vec!["modes:0:2"]);
    }
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
