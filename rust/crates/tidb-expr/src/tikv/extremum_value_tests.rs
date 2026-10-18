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

//! Independent observations of original GREATEST/LEAST argument programs.
use super::*;
use crate::builtin_ext::{GlCmpStringMode, GlSignature};
use crate::{Columns, Datum, EvalError};
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use tidb_datatype::{Collation, DateModes, EvalType, SessionTimeZone};
struct Context {
    cache: Option<ReadyValueCache>,
    reject_cache: bool,
    level: crate::ErrorLevel,
    events: RefCell<Vec<String>>,
    warnings: RefCell<Vec<(u16, String)>>,
    modes: Cell<usize>,
    zone: Cell<usize>,
}
impl Context {
    fn new(cache: bool, level: crate::ErrorLevel) -> Self {
        Self {
            cache: cache.then(ReadyValueCache::new),
            reject_cache: false,
            level,
            events: RefCell::new(vec![]),
            warnings: RefCell::new(vec![]),
            modes: Cell::new(0),
            zone: Cell::new(0),
        }
    }
    fn reset(&self) {
        self.events.borrow_mut().clear();
        self.warnings.borrow_mut().clear();
        self.modes.set(0);
        self.zone.set(0);
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("extremum must not invent source lookups")
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        assert!(
            !self.reject_cache,
            "extremum attempted retired Ready worker lookup"
        );
        self.cache.as_ref()
    }
    fn context_id(&self) -> u64 {
        8187
    }
    fn truncate_level(&self) -> crate::ErrorLevel {
        self.events
            .borrow_mut()
            .push(format!("truncate:{:?}", self.level));
        self.level
    }
    fn date_modes(&self) -> DateModes {
        let n = self.modes.get();
        self.modes.set(n + 1);
        self.events.borrow_mut().push(format!("modes:{n}"));
        DateModes {
            no_zero_date: n % 2 == 0,
            no_zero_in_date: n % 2 != 0,
            allow_invalid_dates: n % 2 != 0,
        }
    }
    fn time_zone(&self) -> SessionTimeZone {
        let n = self.zone.get();
        self.zone.set(n + 1);
        self.events.borrow_mut().push(format!("zone:{n}"));
        if n % 2 == 0 {
            SessionTimeZone::utc()
        } else {
            SessionTimeZone::Named(chrono_tz::America::Los_Angeles)
        }
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.events.borrow_mut().push("now".into());
        Some((1792308000, 123456000, 0))
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.warnings.borrow_mut().push((code, message.to_owned()));
    }
}
fn signatures() -> Vec<Option<GlSignature>> {
    let mut signatures = vec![None];
    for arg_type in EvalType::ALL {
        signatures.push(Some(GlSignature {
            arg_type,
            cmp_string_mode: GlCmpStringMode::Directly,
            ret_date: arg_type == EvalType::Datetime,
        }));
    }
    for cmp_string_mode in [GlCmpStringMode::AsDate, GlCmpStringMode::AsDatetime] {
        signatures.push(Some(GlSignature {
            arg_type: EvalType::String,
            cmp_string_mode,
            ret_date: false,
        }));
    }
    signatures
}
fn isolated(name: &str) -> bool {
    if std::env::var("TIDB_N6Y_EXTREMUM_WITNESS").as_deref() == Ok(name) {
        // Only this isolated test process owns the hook. Raw metadata can
        // reach original invariant panics; record them rather than deleting
        // those private accepted-input observations or changing SQL errors.
        std::panic::set_hook(Box::new(|_| {}));
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("TIDB_N6Y_EXTREMUM_WITNESS", name)
        .output()
        .unwrap();
    eprint!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "isolated original extremum witness"
    );
    true
}
fn observation(value: &std::thread::Result<Result<Datum, EvalError>>, ctx: &Context) -> String {
    let observation = match value {
        Ok(value) => {
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
            format!("{value:?}|{identity:?}|{cause:?}")
        }
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .expect("original string panic");
            format!("PANIC:{message}")
        }
    };
    format!(
        "{observation}|{:?}|{:?}\n",
        ctx.events.borrow(),
        ctx.warnings.borrow()
    )
}
fn observe(hash: &mut u64, value: &std::thread::Result<Result<Datum, EvalError>>, ctx: &Context) {
    for byte in observation(value, ctx).bytes() {
        *hash = (*hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
}

fn independent(
    original: &std::thread::Result<Result<Datum, EvalError>>,
    ctx: &Context,
    input: &[Datum],
    want: Ordering,
    signature: Option<GlSignature>,
    decimals: &[i64],
    constant: bool,
    collation: Collation,
) {
    let mut next = Context::new(false, ctx.level);
    next.reject_cache = true;
    let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::extremum::eval_extremum_values_bounded(
            &next,
            input,
            want,
            signature,
            decimals,
            constant,
            collation,
            64 << 20,
        )
    }));
    let old = observation(original, ctx);
    let new = observation(&actual, &next);
    if old != new {
        eprintln!("EXTREMUM independent mismatch input={input:?} want={want:?} signature={signature:?} decimals={decimals:?} constant={constant}\nOLD {old}NEW {new}");
    }
    assert_eq!(
        old, new,
        "independent original argument programs vs genuine SDK values"
    );
}
#[test]
fn original_extremum_values_rebuild_poisoned_authority_after_panic_witness() {
    if isolated("tikv::extremum_value_tests::original_extremum_values_rebuild_poisoned_authority_after_panic_witness") {return;}
    let values = crate::ops::arithmetic::tests::fixtures();
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    let mut panics = 0;
    for (cache, level) in [
        (false, crate::ErrorLevel::Warn),
        (true, crate::ErrorLevel::Error),
        (true, crate::ErrorLevel::Ignore),
    ] {
        let mut ctx = Context::new(cache, level);
        for signature in signatures() {
            for want in [Ordering::Less, Ordering::Equal, Ordering::Greater] {
                for left in &values {
                    for right in &values {
                        for input in [
                            vec![left.clone(), right.clone()],
                            vec![left.clone(), right.clone(), Datum::Null],
                            vec![left.clone(), left.clone(), right.clone()],
                        ] {
                            ctx.reset();
                            let value =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    eval_extremum_in(
                                        &ctx,
                                        &input,
                                        want,
                                        signature,
                                        &[0, 3, 7],
                                        cases % 2 == 0,
                                        Collation::Utf8Mb4GeneralCi,
                                    )
                                }));

                            independent(
                                &value,
                                &ctx,
                                &input,
                                want,
                                signature,
                                &[0, 3, 7],
                                cases % 2 == 0,
                                Collation::Utf8Mb4GeneralCi,
                            );
                            panics += usize::from(value.is_err());
                            observe(&mut hash, &value, &ctx);
                            cases += 1;
                            // A caught private invariant panic poisons the OLD mutex.
                            // Keep that historical cohort separately; fresh authority
                            // is needed to observe the next actual value computation.
                            if value.is_err() {
                                ctx = Context::new(cache, level);
                            }
                        }
                    }
                }
            }
        }
    }
    eprintln!("OLD extremum fresh-authority values cases={cases} panics={panics} hash={hash:016x}");
    assert_eq!(cases, 331776);
    // Filled only from original active argument-program observations.
    assert_eq!(hash, 0x645bc94e1a54782d); // Distinct OLD fresh-authority cohort; historical poisoned digest is preserved separately.
}
#[test]
fn original_extremum_arity_singleton_collation_and_temporal_text_witness() {
    if isolated("tikv::extremum_value_tests::original_extremum_arity_singleton_collation_and_temporal_text_witness") {return;}
    let values = crate::ops::arithmetic::tests::fixtures();
    let mut layouts = vec![vec![]];
    layouts.extend(values.into_iter().map(|value| vec![value]));
    for (a, b) in [
        ("a ", "A"),
        ("99-1-1", "2020-01-01"),
        ("2020-02-31", "zzz"),
        ("2020-01-01 01:02:03.123456", "0000-00-00"),
        ("2020-01-01T01:02:03Z", "invalid"),
    ] {
        layouts.push(vec![
            Datum::new_string(a),
            Datum::new_string(b),
            Datum::new_string(a),
        ]);
    }
    layouts.push(vec![Datum::Bytes(vec![255]), Datum::new_string("z")]);
    let mut hash = 0xcbf29ce484222325;
    let mut cases = 0;
    let mut panics = 0;
    for collation in [
        Collation::Binary,
        Collation::Utf8Mb4Bin,
        Collation::Utf8Mb4GeneralCi,
        Collation::GbkBin,
    ] {
        for signature in signatures() {
            for want in [Ordering::Less, Ordering::Equal, Ordering::Greater] {
                let ctx = Context::new(true, crate::ErrorLevel::Warn);
                for input in &layouts {
                    for decimals in [&[][..], &[-1][..], &[0, 3, 7][..]] {
                        for constant in [false, true] {
                            ctx.reset();
                            let value =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    eval_extremum_in(
                                        &ctx, input, want, signature, decimals, constant, collation,
                                    )
                                }));

                            independent(
                                &value, &ctx, input, want, signature, decimals, constant, collation,
                            );
                            panics += usize::from(value.is_err());
                            observe(&mut hash, &value, &ctx);
                            cases += 1;
                        }
                    }
                }
            }
        }
    }
    eprintln!("OLD extremum boundary cases={cases} panics={panics} hash={hash:016x}");
    assert_eq!(cases, 33696);
    assert_eq!(hash, 0x5a25c52fc09e4be5);
}
#[test]
fn extremum_values_must_not_lookup_retired_ready_workers() {
    let mut ctx = Context::new(true, crate::ErrorLevel::Warn);
    ctx.reject_cache = true;
    assert_eq!(
        eval_extremum_in(
            &ctx,
            &[Datum::Int(2), Datum::Int(1)],
            Ordering::Less,
            None,
            &[],
            false,
            Collation::Binary
        ),
        Ok(Datum::Int(1))
    );
}
