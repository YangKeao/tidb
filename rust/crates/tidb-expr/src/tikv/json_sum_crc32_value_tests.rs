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

//! Original JSON checksum actual values, first errors and cache observations.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::cell::RefCell;

struct Context {
    cache: Option<ReadyValueCache>,
    reject: bool,
    events: RefCell<Vec<String>>,
}
impl Context {
    fn new(cache: bool) -> Self {
        Self {
            cache: cache.then(ReadyValueCache::new),
            reject: false,
            events: RefCell::new(vec![]),
        }
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("JSON checksum must not invent sources")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject,
            "JSON checksum attempted retired Ready worker lookup"
        );
        self.cache.as_ref()
    }
    fn context_id(&self) -> u64 {
        8197
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.events.borrow_mut().push("zone".into());
        tidb_datatype::SessionTimeZone::utc()
    }
    fn connection_charset_info(&self) -> (&str, &str) {
        self.events.borrow_mut().push("connection".into());
        ("utf8mb4", "utf8mb4_bin")
    }
}
fn values() -> Vec<Datum> {
    let atoms = [
        r#""a""#,
        r#""一😂\u0000""#,
        "-1",
        "1.25",
        "null",
        "true",
        "[]",
        "{}",
    ];
    let mut docs = vec!["[]".to_owned()];
    for length in 1..=3 {
        for mut word in 0..8_usize.pow(length) {
            let mut members = Vec::new();
            for _ in 0..length {
                members.push(atoms[word % 8]);
                word /= 8;
            }
            docs.push(format!("[{}]", members.join(",")));
        }
    }
    docs.extend(
        [
            "null",
            "1",
            "1.0",
            "-0.0",
            "true",
            "{}",
            r#""text""#,
            "[1.0,-0.0,1e6,0.00001]",
            "[18446744073709551615,-9223372036854775808]",
            "[1e308,1e-308]",
        ]
        .map(str::to_owned),
    );
    let mut values = crate::ops::arithmetic::tests::fixtures();
    for document in docs {
        values.push(Datum::new_string(document.as_str()));
        values.push(Datum::Bytes(document.as_bytes().to_vec()));
        values.push(Datum::Json(
            tidb_datatype::BinaryJSON::parse(&document).unwrap(),
        ));
    }
    for invalid in [b"[".as_slice(), b"", b"[1,]", &[255]] {
        values.push(Datum::Bytes(invalid.to_vec()));
    }
    values
}
fn observation(value: &Result<Datum, EvalError>, ctx: &Context) -> String {
    let identity = value
        .as_ref()
        .ok()
        .map(|value| identity_value::encode(value).expect("actual result identity"));
    let cause = match value {
        Err(EvalError::ExpressionRuntimeFailure(error)) => {
            Some((error.local_error().to_string(), error.phase()))
        }
        _ => None,
    };
    format!(
        "{value:?}|{identity:?}|{cause:?}|{:?}\n",
        ctx.events.borrow()
    )
}
fn observe(hash: &mut u64, value: &Result<Datum, EvalError>, ctx: &Context) {
    for byte in observation(value, ctx).bytes() {
        *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
}

#[test]
fn original_json_sum_crc32_values_errors_and_cache_witness() {
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for profile in 0..3 {
        let mut ctx = Context::new(profile != 0);
        for _ in 0..2 {
            for value in values() {
                if profile == 2 {
                    ctx = Context::new(true);
                }
                ctx.events.borrow_mut().clear();
                let actual = eval_json_sum_crc32_in(&ctx, &value);
                let independent = super::json_sum_crc32::json_sum_crc32_bounded(&value, 64 << 20);
                assert_eq!(
                    observation(&actual, &ctx),
                    observation(&independent, &Context::new(false)),
                    "OLD worker vs actual SDK JSON checksum: {value:?}"
                );

                observe(&mut hash, &actual, &ctx);
                cases += 1;
            }
        }
    }
    eprintln!("OLD JSON_SUM_CRC32 cases={cases} hash={hash:016x}");
    assert_eq!(cases, 10926);
    assert_eq!(hash, 0xe762b2ff1936744f);
}
#[test]
fn json_sum_crc32_actual_value_does_not_lookup_retired_worker() {
    let mut ctx = Context::new(true);
    ctx.reject = true;
    assert_eq!(
        eval_json_sum_crc32_in(&ctx, &Datum::new_string("[1,2,3]")),
        Ok(Datum::Int(4505025631))
    );
}
