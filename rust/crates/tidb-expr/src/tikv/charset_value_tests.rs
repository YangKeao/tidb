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

//! Original charset reader, exact/effective metadata and result observations.
use super::*;
use crate::{Columns, Datum, EvalError};
use std::cell::RefCell;
use tidb_datatype::{Collation, FieldType, FieldTypeCode};
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
        panic!("charset must not invent sources")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject,
            "charset attempted retired Ready worker lookup"
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
fn names() -> [&'static str; 14] {
    [
        "",
        "binary",
        "utf8",
        "utf8mb4",
        "ascii",
        "latin1",
        "gbk",
        "gb18030",
        "utf8mb3",
        "GBK",
        "UTF8",
        "Binary",
        "unknown",
        "utf8mb4\0",
    ]
}
fn values() -> Vec<Datum> {
    let mut values = crate::ops::arithmetic::tests::fixtures();
    for bytes in [
        b"".as_slice(),
        b"A",
        "一".as_bytes(),
        "😂".as_bytes(),
        &[255],
        &[128],
        &[0xe4, 0xb8],
        &[0xc2],
        &[0xc2, 0xa9],
        &[0xd2, 0xbb],
        &[0x81, 0x30, 0x81, 0x30],
        &[0x94, 0x39, 0xfc, 0x36],
        &[b'A', 255, b'B'],
        &[0],
        &[0x81],
        &[0xed, 0xa0, 0x80],
    ] {
        values.push(Datum::Bytes(bytes.to_vec()));
    }
    values.push(Datum::new_string("一列"));
    values.push(Datum::new_collation_string("😂", Collation::GbkBin));
    assert_eq!(values.len(), 50);
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
fn independent(
    original: &Result<Datum, EvalError>,
    ctx: &Context,
    run: impl FnOnce() -> Result<Datum, EvalError>,
) {
    let actual = run();
    let next = Context::new(false);
    assert_eq!(
        observation(original, ctx),
        observation(&actual, &next),
        "independent OLD charset worker vs actual borrowed byte kernel"
    );
}
fn isolated(name: &str) -> bool {
    if std::env::var("TIDB_N6Z_CHARSET_WITNESS").as_deref() == Ok(name) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("TIDB_N6Z_CHARSET_WITNESS", name)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated charset witness: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    true
}
#[test]
fn original_charset_binary_reader_null_bytes_and_exact_name_witness() {
    if isolated("tikv::charset_value_tests::original_charset_binary_reader_null_bytes_and_exact_name_witness") {return;}
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for profile in 0..3 {
        let mut ctx = Context::new(profile != 0);
        for charset in names() {
            for value in values() {
                for encode in [true, false] {
                    ctx.events.borrow_mut().clear();
                    let actual = if encode {
                        eval_to_binary_in(&ctx, &value, charset)
                    } else {
                        eval_from_binary_in(&ctx, &value, charset)
                    };
                    independent(&actual, &ctx, || {
                        if encode {
                            super::convert_charset::to_binary_bounded(&value, charset, 64 << 20)
                        } else {
                            super::convert_charset::from_binary_bounded(&value, charset, 64 << 20)
                        }
                    });
                    observe(&mut hash, &actual, &ctx);
                    cases += 1;
                    if profile == 2 {
                        ctx = Context::new(true);
                    }
                }
            }
        }
    }
    eprintln!("OLD charset binary cases={cases} hash={hash:016x}");
    assert_eq!(cases, 4200);
    assert_eq!(hash, 0xd7142611e2de991d);
}
#[test]
fn original_charset_convert_using_exact_effective_target_and_reader_witness() {
    if isolated("tikv::charset_value_tests::original_charset_convert_using_exact_effective_target_and_reader_witness") {return;}
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for profile in 0..3 {
        let mut ctx = Context::new(profile != 0);
        for collation in [Collation::Binary, Collation::Utf8Mb4Bin, Collation::GbkBin] {
            for exact in names() {
                let field = FieldType::new(FieldTypeCode::VarString)
                    .with_collation(collation)
                    .with_charset_name(exact);
                for target in names() {
                    for value in values() {
                        ctx.events.borrow_mut().clear();
                        let actual = eval_convert_using_in(&ctx, &value, &field, target);
                        independent(&actual, &ctx, || {
                            super::convert_charset::convert_using_bounded(
                                &value,
                                &field,
                                target,
                                64 << 20,
                            )
                        });
                        observe(&mut hash, &actual, &ctx);
                        cases += 1;
                        if profile == 2 {
                            ctx = Context::new(true);
                        }
                    }
                }
            }
        }
    }
    eprintln!("OLD charset convert cases={cases} hash={hash:016x}");
    assert_eq!(cases, 88200);
    assert_eq!(hash, 0xe3a22bd8fcc8c70f);
}
#[test]
fn original_charset_live_gb_default_and_actual_source_null_witness() {
    const CHILD: &str = "TIDB_N6Z_CHARSET_MODE_CHILD";
    const NAME:&str="tikv::charset_value_tests::original_charset_live_gb_default_and_actual_source_null_witness";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", NAME, "--nocapture"])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated old charset defaults: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        return;
    }
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    for mode in [false, true] {
        tidb_datatype::set_new_collation_enabled(mode);
        for cache in [false, true] {
            let ctx = Context::new(cache);
            for source in ["binary", "gbk", "gb18030", "UNKNOWN"] {
                let field = FieldType::new(FieldTypeCode::VarString).with_charset_name(source);
                for target in ["gbk", "gb18030"] {
                    for value in values() {
                        ctx.events.borrow_mut().clear();
                        let actual = eval_convert_using_in(&ctx, &value, &field, target);
                        independent(&actual, &ctx, || {
                            super::convert_charset::convert_using_bounded(
                                &value,
                                &field,
                                target,
                                64 << 20,
                            )
                        });
                        observe(&mut hash, &actual, &ctx);
                        cases += 1;
                    }
                }
            }
            ctx.events.borrow_mut().clear();
            let actual = eval_charset_null_in(&ctx);
            independent(&actual, &ctx, super::convert_charset::source_null);
            observe(&mut hash, &actual, &ctx);
            cases += 1;
        }
    }
    eprintln!("OLD charset defaults/null cases={cases} hash={hash:016x}");
    assert_eq!(cases, 1604);
    assert_eq!(hash, 0x7d8948d00184e03d);
}
#[test]
fn charset_binary_must_not_lookup_retired_ready_workers() {
    let mut ctx = Context::new(true);
    ctx.reject = true;
    assert_eq!(
        eval_to_binary_in(&ctx, &Datum::new_string("一"), "gbk"),
        Ok(Datum::Bytes(vec![0xd2, 0xbb]))
    );
}
#[test]
fn charset_convert_must_not_lookup_retired_ready_workers() {
    let mut ctx = Context::new(true);
    ctx.reject = true;
    assert!(eval_convert_using_in(
        &ctx,
        &Datum::new_string("😂"),
        &FieldType::new(FieldTypeCode::VarString).with_charset_name("utf8mb4"),
        "gbk"
    )
    .is_ok());
}
#[test]
fn charset_actual_null_must_not_lookup_retired_ready_workers() {
    let mut ctx = Context::new(true);
    ctx.reject = true;
    assert_eq!(eval_charset_null_in(&ctx), Ok(Datum::Null));
}
