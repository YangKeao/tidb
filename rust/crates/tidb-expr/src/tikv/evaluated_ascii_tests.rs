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

//! Actual C4 workers through explicit capabilities and the ASCII value dispatcher.
//! These tests do not establish all business-wrapper/entrypoint propagation or
//! statement lifetimes. They do not measure allocator requests/factory peaks or
//! prove every caller's integrated return coercion.

use super::*;
use crate::constant::Constant;
use crate::context::{BlockEncryptionMode, ErrorLevel, EvalError};
use crate::Columns;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tidb_datatype::{
    ConversionFlags, CoreTime, DateModes, Datum, FieldType, FieldTypeCode, SessionTimeZone, Time,
    TimeType,
};
use tidb_query_expr::local::LocalError;

// Keep the old ASCII-only assertions thin: they still invoke the actual shared
// worker; this projection accepts no Bytes result and supplies no replacement.
trait AsciiComputedValue {
    fn value(&self) -> Option<i64>;
}

impl AsciiComputedValue for ComputedValue {
    fn value(&self) -> Option<i64> {
        match self {
            ComputedValue::Int(value) => value.value(),
            ComputedValue::Bytes(_)
            | ComputedValue::NativeVector(_)
            | ComputedValue::Ieee754Bits(_)
            | ComputedValue::Decimal(_)
            | ComputedValue::DecimalFast(_)
            | ComputedValue::DecimalDivision(_)
            | ComputedValue::Int128(_)
            | ComputedValue::Uncompress(_)
            | ComputedValue::JsonReport(_) => {
                panic!("ASCII assertion received a non-Int result")
            }
        }
    }
}

// Test-thread-local timing seam only: no swappable worker/backend, no global
// callback, and no cfg(test) field in the production PoolCore payload. The
// callback is removed and the RefCell borrow released before it can block.
std::thread_local! {
    static AFTER_EPOCH_READ_HOOK: RefCell<Option<Box<dyn FnOnce()>>> =
        const { RefCell::new(None) };
    static EVAL_ONE_OBSERVATION: RefCell<Option<EvalOneObservation>> =
        const { RefCell::new(None) };
}

#[derive(Debug, Clone, Copy)]
struct EvalOneObservation {
    // Counts adapter entry into eval_args, NOT official fn_ptr invocations.
    facade_entries: usize,
    // Actual C4 getter values, only when the real eval_one call is reached.
    // None after a refused/disposed call is not a fabricated zero counter.
    before_kernel_invocations: Option<u64>,
    after_kernel_invocations: Option<u64>,
}

fn arm_eval_one_observation() {
    EVAL_ONE_OBSERVATION.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "only one observation may be armed on this thread"
        );
        *slot = Some(EvalOneObservation {
            facade_entries: 0,
            before_kernel_invocations: None,
            after_kernel_invocations: None,
        });
    });
}

fn take_eval_one_observation() -> EvalOneObservation {
    EVAL_ONE_OBSERVATION.with(|slot| slot.borrow_mut().take().expect("observation was armed"))
}

pub(super) fn before_eval_one_for_test(actual_kernel_invocations: u64) {
    EVAL_ONE_OBSERVATION.with(|slot| {
        if let Some(observation) = slot.borrow_mut().as_mut() {
            observation.facade_entries = observation
                .facade_entries
                .checked_add(1)
                .expect("test facade-entry counter overflow");
            observation
                .before_kernel_invocations
                .get_or_insert(actual_kernel_invocations);
        }
    });
}

pub(super) fn after_eval_one_for_test(actual_kernel_invocations: u64) {
    EVAL_ONE_OBSERVATION.with(|slot| {
        if let Some(observation) = slot.borrow_mut().as_mut() {
            observation.after_kernel_invocations = Some(actual_kernel_invocations);
        }
    });
}

fn set_after_epoch_read_hook(hook: impl FnOnce() + 'static) {
    AFTER_EPOCH_READ_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "epoch-read hook is one-shot per test thread"
        );
        *slot = Some(Box::new(hook));
    });
}

pub(super) fn after_epoch_read_for_test() {
    let hook = AFTER_EPOCH_READ_HOOK.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

// Deliberately explicit TEST policies, not proposed product defaults. The
// creation reservation is a ledger allowance, NOT a measured factory peak.
const TEST_WORKER_CAP: usize = 1 << 20;
const TEST_CREATION_RESERVATION: usize = 2 << 20;
const TEST_POOL_BYTES: usize = 16 << 20;
const TEST_CALL_BYTES: usize = 1 << 16;

fn test_policy(max_workers: usize, max_creating: usize) -> AsciiPoolPolicy {
    AsciiPoolPolicy::checked(
        max_workers,
        max_creating,
        TEST_POOL_BYTES,
        TEST_WORKER_CAP,
        TEST_CREATION_RESERVATION,
        64,
        8,
        TEST_CALL_BYTES,
    )
    .unwrap()
}

const COLUMNS_FORWARDED_METHODS: [&str; 63] = [
    "get",
    "context_id",
    "use_plan_cache",
    "skip_plan_cache_for_comparison",
    "enable_vectorized_expression",
    "param_value",
    "current_insert_value",
    "get_param_value",
    "bounded_staleness_safe_time",
    "connection_charset_info",
    "no_unsigned_subtraction",
    "now",
    "cast_time_to_year_through_concat",
    "sysdate_is_now",
    "current_database",
    "current_user",
    "login_user",
    "current_role",
    "current_resource_group",
    "connection_id",
    "tidb_decode_key",
    "acquire_advisory_lock",
    "advisory_lock_owner",
    "release_advisory_lock",
    "release_all_advisory_locks",
    "found_rows",
    "current_tso",
    "ddl_owner_info",
    "sysvar",
    "tidb_info",
    "block_encryption_mode",
    "division_by_zero_level",
    "truncate_level",
    "type_flags",
    "strict_sql_mode",
    "handle_truncate",
    "handle_group_concat_cut",
    "handle_sleep_incorrect_argument",
    "sleep_for",
    "append_warning",
    "append_note",
    "warning_count",
    "truncate_warnings",
    "take_warnings_since",
    "max_allowed_packet",
    "handle_allowed_packet_overflowed",
    "date_modes",
    "handle_division_by_zero",
    "get_uservar",
    "set_uservar",
    "row_count",
    "last_insert_id",
    "set_last_insert_id",
    "time_zone",
    "like_default_escape",
    "default_week_format",
    "windowing_use_high_precision",
    "div_precision_increment",
    "rand_next",
    "rand_seeded_next",
    "sequence_nextval",
    "sequence_lastval",
    "sequence_setval",
];

// Intentional overrides, NOT two more forwarded native methods.
const COLUMNS_CAPABILITY_METHODS: [&str; 2] =
    ["evaluated_ascii_scope", "evaluated_ascii_execution"];

// A deliberately small source drift check, not a Rust parser. These specific
// source blocks have unindented closing braces and one method per declaration.
// A shape change fails loudly instead of silently excluding the new methods.
fn declared_methods<'a>(source: &'a str, marker: &str) -> Vec<&'a str> {
    let header = source
        .lines()
        .find(|line| {
            let line = line.trim_start();
            (line.starts_with("impl ")
                || line.starts_with("impl<")
                || line.starts_with("pub trait "))
                && line.contains(marker)
        })
        .expect("source inventory declaration exists, not merely its marker string");
    let start = source.find(header).expect("declaration belongs to source");
    let (_, body) = source[start..].split_once('{').expect("source block opens");
    let (body, _) = body.split_once("\n}").expect("source block closes");
    body.lines()
        .filter_map(|line| line.trim_start().strip_prefix("fn "))
        .map(|declaration| {
            declaration
                .split(['(', '<'])
                .next()
                .expect("method has a name")
                .trim()
        })
        .collect()
}

#[test]
fn columns_method_sets_have_no_unreviewed_forwarding_drift() {
    let forwarded = BTreeSet::from(COLUMNS_FORWARDED_METHODS);
    let capabilities = BTreeSet::from(COLUMNS_CAPABILITY_METHODS);
    assert_eq!(forwarded.len(), 63, "inventory must not hide duplicates");
    assert_eq!(capabilities.len(), 2);
    assert!(forwarded.is_disjoint(&capabilities));
    let expected: BTreeSet<_> = forwarded.union(&capabilities).copied().collect();
    assert_eq!(
        expected.len(),
        65,
        "63 forwarded plus 2 special capabilities"
    );
    for (source, marker) in [
        (include_str!("../context.rs"), "pub trait Columns"),
        (
            include_str!("evaluated_ascii.rs"),
            "Columns for ScopedAsciiColumns",
        ),
    ] {
        let methods = declared_methods(source, marker);
        assert_eq!(methods.len(), 65, "method declaration count: {marker}");
        assert_eq!(
            methods.into_iter().collect::<BTreeSet<_>>(),
            expected,
            "review forwarding and behavioral coverage when Columns changes: {marker}"
        );
    }
    // The ordinary-method sentinel deliberately inherits default-None
    // capabilities. Separate runtime tests below observe both overrides.
    let sentinel = declared_methods(
        include_str!("evaluated_ascii_tests.rs"),
        "impl Columns for ForwardingSentinel<'_>",
    );
    assert_eq!(sentinel.len(), 63);
    assert_eq!(sentinel.into_iter().collect::<BTreeSet<_>>(), forwarded);
    // These are the three concrete sessionless overrides at the frozen source
    // checkpoint, NOT three additional Columns trait methods.
    assert_eq!(
        declared_methods(include_str!("../context.rs"), "impl Columns for NoColumns"),
        ["get"]
    );
    assert_eq!(
        declared_methods(
            include_str!("../context.rs"),
            "impl Columns for ZonedNoColumns"
        ),
        ["get", "time_zone"]
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeCall {
    method: &'static str,
    arguments: String,
}

#[derive(Debug, Clone, PartialEq)]
struct NativeEffects {
    warnings: Vec<(u16, String)>,
    notes: Vec<(u16, String)>,
    uservars: BTreeMap<String, Datum>,
    locks: BTreeMap<String, usize>,
    skips: Vec<(usize, String)>,
    rng: u64,
    seeded: BTreeMap<(usize, i64), u64>,
    sequence: i64,
    last_insert_id: u64,
}

// A native Columns sentinel, never a substitute worker or backend. Every
// method overrides the method itself, especially defaults whose implementation
// would otherwise call another policy method and mask an omitted delegation.
struct ForwardingSentinel<'a> {
    calls: RefCell<Vec<NativeCall>>,
    effects: RefCell<NativeEffects>,
    fail: bool,
    probe: Option<&'a dyn Fn()>,
}

impl<'a> ForwardingSentinel<'a> {
    fn new(fail: bool, probe: Option<&'a dyn Fn()>) -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            effects: RefCell::new(NativeEffects {
                warnings: vec![(42000, "preexisting warning".into())],
                notes: Vec::new(),
                uservars: BTreeMap::from([("existing".into(), Datum::Int(-601))]),
                locks: BTreeMap::new(),
                skips: Vec::new(),
                rng: 23,
                seeded: BTreeMap::new(),
                sequence: 7001,
                last_insert_id: 9001,
            }),
            fail,
            probe,
        }
    }

    fn record(&self, method: &'static str, arguments: String) {
        if let Some(probe) = self.probe {
            probe();
        }
        self.calls
            .borrow_mut()
            .push(NativeCall { method, arguments });
    }

    fn answer<T>(&self, method: &'static str, value: T) -> Result<T, EvalError> {
        if self.fail {
            Err(EvalError::UnknownColumn(format!(
                "sentinel original error: {method}"
            )))
        } else {
            Ok(value)
        }
    }
}

impl Columns for ForwardingSentinel<'_> {
    fn get(&self, path: &[String]) -> Option<Datum> {
        self.record("get", format!("{:p}:{path:?}", path.as_ptr()));
        Some(Datum::Bytes(vec![0xff, 0, 17]))
    }

    fn context_id(&self) -> u64 {
        self.record("context_id", String::new());
        1701
    }

    fn use_plan_cache(&self) -> bool {
        self.record("use_plan_cache", String::new());
        true
    }

    fn skip_plan_cache_for_comparison(&self, constant: &Constant, target: &str) {
        self.record(
            "skip_plan_cache_for_comparison",
            format!(
                "{constant:p}:{:?}:{:p}:{target:?}",
                constant.value,
                target.as_ptr()
            ),
        );
        self.effects
            .borrow_mut()
            .skips
            .push((constant as *const Constant as usize, target.into()));
    }

    fn enable_vectorized_expression(&self) -> bool {
        self.record("enable_vectorized_expression", String::new());
        false
    }

    fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
        self.record("param_value", format!("{order}"));
        self.answer("param_value", Datum::UInt(order as u64 + 2000))
    }

    fn current_insert_value(&self, offset: usize) -> Result<Option<Datum>, EvalError> {
        self.record("current_insert_value", format!("{offset}"));
        self.answer(
            "current_insert_value",
            Some(Datum::Int(offset as i64 + 3000)),
        )
    }

    fn get_param_value(&self, idx: usize) -> Result<Datum, EvalError> {
        self.record("get_param_value", format!("{idx}"));
        self.answer("get_param_value", Datum::Int(idx as i64 + 4000))
    }

    fn bounded_staleness_safe_time(&self) -> Option<Time> {
        self.record("bounded_staleness_safe_time", String::new());
        Some(
            Time::new(
                CoreTime::from_date(2024, 3, 15, 17, 18, 19, 123456),
                TimeType::DateTime,
                6,
            )
            .unwrap(),
        )
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        self.record("connection_charset_info", String::new());
        ("gb18030", "gb18030_chinese_ci")
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.record("no_unsigned_subtraction", String::new());
        true
    }

    fn now(&self) -> Option<(i64, u32, i32)> {
        self.record("now", String::new());
        Some((1_713_333_333, 987_654_321, -7 * 3600))
    }

    fn cast_time_to_year_through_concat(&self) -> bool {
        self.record("cast_time_to_year_through_concat", String::new());
        true
    }

    fn sysdate_is_now(&self) -> bool {
        self.record("sysdate_is_now", String::new());
        true
    }

    fn current_database(&self) -> Option<String> {
        self.record("current_database", String::new());
        Some("sentinel_db".into())
    }

    fn current_user(&self) -> Option<String> {
        self.record("current_user", String::new());
        Some("grant@sentinel".into())
    }

    fn login_user(&self) -> Option<String> {
        self.record("login_user", String::new());
        Some("login@sentinel".into())
    }

    fn current_role(&self) -> Option<String> {
        self.record("current_role", String::new());
        Some("`sentinel_role`@`host`".into())
    }

    fn current_resource_group(&self) -> Option<String> {
        self.record("current_resource_group", String::new());
        Some("sentinel_group".into())
    }

    fn connection_id(&self) -> Option<u64> {
        self.record("connection_id", String::new());
        Some(8123)
    }

    fn tidb_decode_key(&self, input: &[u8]) -> Vec<u8> {
        self.record("tidb_decode_key", format!("{:p}:{input:?}", input.as_ptr()));
        let mut decoded = vec![0xfe, 0];
        decoded.extend_from_slice(input);
        decoded
    }

    fn acquire_advisory_lock(&self, name: &str, timeout: Duration) -> Result<bool, EvalError> {
        self.record(
            "acquire_advisory_lock",
            format!("{:p}:{name:?}:{timeout:?}", name.as_ptr()),
        );
        self.answer("acquire_advisory_lock", ())?;
        *self
            .effects
            .borrow_mut()
            .locks
            .entry(name.into())
            .or_default() += 1;
        Ok(true)
    }

    fn advisory_lock_owner(&self, name: &str) -> Result<Option<u64>, EvalError> {
        self.record(
            "advisory_lock_owner",
            format!("{:p}:{name:?}", name.as_ptr()),
        );
        self.answer("advisory_lock_owner", Some(8123))
    }

    fn release_advisory_lock(&self, name: &str) -> Result<bool, EvalError> {
        self.record(
            "release_advisory_lock",
            format!("{:p}:{name:?}", name.as_ptr()),
        );
        self.answer("release_advisory_lock", ())?;
        let mut state = self.effects.borrow_mut();
        if let Some(references) = state.locks.get_mut(name) {
            *references -= 1;
            if *references == 0 {
                state.locks.remove(name);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn release_all_advisory_locks(&self) -> Result<usize, EvalError> {
        self.record("release_all_advisory_locks", String::new());
        self.answer("release_all_advisory_locks", ())?;
        let mut state = self.effects.borrow_mut();
        let count = state.locks.values().sum();
        state.locks.clear();
        Ok(count)
    }

    fn found_rows(&self) -> Option<u64> {
        self.record("found_rows", String::new());
        Some(888)
    }

    fn current_tso(&self) -> i64 {
        self.record("current_tso", String::new());
        9123456
    }

    fn ddl_owner_info(&self) -> Result<bool, EvalError> {
        self.record("ddl_owner_info", String::new());
        self.answer("ddl_owner_info", true)
    }

    fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        self.record("sysvar", format!("{scope:?}:{:p}:{name:?}", name.as_ptr()));
        Some(Datum::Bytes(
            format!("sentinel:{scope:?}:{name}").into_bytes(),
        ))
    }

    fn tidb_info(&self) -> String {
        self.record("tidb_info", String::new());
        "sentinel process identity, not the default printer".into()
    }

    fn block_encryption_mode(&self) -> BlockEncryptionMode {
        self.record("block_encryption_mode", String::new());
        BlockEncryptionMode::Aes256Cfb
    }

    fn division_by_zero_level(&self) -> ErrorLevel {
        self.record("division_by_zero_level", String::new());
        ErrorLevel::Error
    }

    fn truncate_level(&self) -> ErrorLevel {
        self.record("truncate_level", String::new());
        ErrorLevel::Ignore
    }

    fn type_flags(&self) -> ConversionFlags {
        self.record("type_flags", String::new());
        // Intentionally disagrees with truncate_level/date_modes. Delegating
        // their defaults instead of THIS override must be observable.
        tidb_datatype::DEFAULT_STATEMENT_FLAGS
            .with_ignore_truncate_err(false)
            .with_truncate_as_warning(false)
            .with_ignore_zero_in_date_err(false)
            .with_ignore_invalid_date_err(false)
    }

    fn strict_sql_mode(&self) -> bool {
        self.record("strict_sql_mode", String::new());
        false
    }

    fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
        self.record(
            "handle_truncate",
            format!("{:p}:{message:?}", message.as_ptr()),
        );
        self.effects
            .borrow_mut()
            .warnings
            .push((42101, format!("truncate:{message}")));
        self.answer("handle_truncate", ())
    }

    fn handle_group_concat_cut(&self, message: &str) -> Result<(), EvalError> {
        self.record(
            "handle_group_concat_cut",
            format!("{:p}:{message:?}", message.as_ptr()),
        );
        self.effects
            .borrow_mut()
            .warnings
            .push((42102, format!("group:{message}")));
        self.answer("handle_group_concat_cut", ())
    }

    fn handle_sleep_incorrect_argument(&self) -> Result<(), EvalError> {
        self.record("handle_sleep_incorrect_argument", String::new());
        self.answer("handle_sleep_incorrect_argument", ())
    }

    fn sleep_for(&self, duration: Duration) -> bool {
        self.record("sleep_for", format!("{duration:?}"));
        // No sleeping: return the nondefault killed/interrupted result.
        true
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.record(
            "append_warning",
            format!("{code}:{:p}:{message:?}", message.as_ptr()),
        );
        self.effects
            .borrow_mut()
            .warnings
            .push((code, message.into()));
    }

    fn append_note(&self, code: u16, message: &str) {
        self.record(
            "append_note",
            format!("{code}:{:p}:{message:?}", message.as_ptr()),
        );
        self.effects.borrow_mut().notes.push((code, message.into()));
    }

    fn warning_count(&self) -> usize {
        self.record("warning_count", String::new());
        self.effects.borrow().warnings.len()
    }

    fn truncate_warnings(&self, bookmark: usize) {
        self.record("truncate_warnings", format!("{bookmark}"));
        self.effects.borrow_mut().warnings.truncate(bookmark);
    }

    fn take_warnings_since(&self, bookmark: usize) -> Vec<(u16, String)> {
        self.record("take_warnings_since", format!("{bookmark}"));
        let mut state = self.effects.borrow_mut();
        let at = bookmark.min(state.warnings.len());
        state.warnings.split_off(at)
    }

    fn max_allowed_packet(&self) -> u64 {
        self.record("max_allowed_packet", String::new());
        713
    }

    fn handle_allowed_packet_overflowed(&self, expr_name: &str) -> Result<(), EvalError> {
        self.record(
            "handle_allowed_packet_overflowed",
            format!("{:p}:{expr_name:?}", expr_name.as_ptr()),
        );
        self.effects
            .borrow_mut()
            .warnings
            .push((42103, format!("packet:{expr_name}")));
        self.answer("handle_allowed_packet_overflowed", ())
    }

    fn date_modes(&self) -> DateModes {
        self.record("date_modes", String::new());
        DateModes {
            no_zero_date: false,
            no_zero_in_date: false,
            allow_invalid_dates: true,
        }
    }

    fn handle_division_by_zero(&self) -> Result<(), EvalError> {
        self.record("handle_division_by_zero", String::new());
        self.effects
            .borrow_mut()
            .warnings
            .push((42104, "sentinel division".into()));
        self.answer("handle_division_by_zero", ())
    }

    fn get_uservar(&self, name: &str) -> Option<Datum> {
        self.record("get_uservar", format!("{:p}:{name:?}", name.as_ptr()));
        self.effects.borrow().uservars.get(name).cloned()
    }

    fn set_uservar(&self, name: &str, value: Datum) {
        self.record(
            "set_uservar",
            format!("{:p}:{name:?}:{value:?}", name.as_ptr()),
        );
        self.effects
            .borrow_mut()
            .uservars
            .insert(name.into(), value);
    }

    fn row_count(&self) -> Option<i64> {
        self.record("row_count", String::new());
        Some(-43)
    }

    fn last_insert_id(&self) -> Option<u64> {
        self.record("last_insert_id", String::new());
        Some(self.effects.borrow().last_insert_id)
    }

    fn set_last_insert_id(&self, value: u64) {
        self.record("set_last_insert_id", format!("{value}"));
        self.effects.borrow_mut().last_insert_id = value;
    }

    fn time_zone(&self) -> SessionTimeZone {
        self.record("time_zone", String::new());
        SessionTimeZone::Fixed {
            name: "sentinel UTC-07".into(),
            offset_secs: -7 * 3600,
        }
    }

    fn like_default_escape(&self) -> u8 {
        self.record("like_default_escape", String::new());
        b'!'
    }

    fn default_week_format(&self) -> i64 {
        self.record("default_week_format", String::new());
        5
    }

    fn windowing_use_high_precision(&self) -> bool {
        self.record("windowing_use_high_precision", String::new());
        false
    }

    fn div_precision_increment(&self) -> u32 {
        self.record("div_precision_increment", String::new());
        13
    }

    fn rand_next(&self) -> Option<f64> {
        self.record("rand_next", String::new());
        let mut state = self.effects.borrow_mut();
        state.rng += 1;
        Some(state.rng as f64 / 1024.0)
    }

    fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
        self.record("rand_seeded_next", format!("{key}:{seed}"));
        let mut state = self.effects.borrow_mut();
        let next = state.seeded.entry((key, seed)).or_insert(40);
        *next += 1;
        Some(*next as f64 / 1024.0)
    }

    fn sequence_nextval(&self, path: &[String]) -> Result<Datum, EvalError> {
        self.record("sequence_nextval", format!("{:p}:{path:?}", path.as_ptr()));
        self.answer("sequence_nextval", ())?;
        let mut state = self.effects.borrow_mut();
        state.sequence += 1;
        Ok(Datum::Int(state.sequence))
    }

    fn sequence_lastval(&self, path: &[String]) -> Result<Datum, EvalError> {
        self.record("sequence_lastval", format!("{:p}:{path:?}", path.as_ptr()));
        self.answer(
            "sequence_lastval",
            Datum::Int(self.effects.borrow().sequence),
        )
    }

    fn sequence_setval(&self, path: &[String], value: i64) -> Result<Datum, EvalError> {
        self.record(
            "sequence_setval",
            format!("{:p}:{path:?}:{value}", path.as_ptr()),
        );
        self.answer("sequence_setval", ())?;
        self.effects.borrow_mut().sequence = value;
        Ok(Datum::Int(value))
    }
}

struct NativeInputs {
    path: Vec<String>,
    constant: Constant,
    target: String,
    name: String,
    message: String,
    key: Vec<u8>,
}

impl NativeInputs {
    fn new() -> Self {
        Self {
            path: vec!["Db.Mixed".into(), "table".into(), "Column".into()],
            constant: Constant::new(Datum::UInt(205), FieldType::new(FieldTypeCode::LongLong)),
            target: "decimal(17,3)".into(),
            name: "MixedCase\0native".into(),
            message: "native sentinel message \0 raw boundary".into(),
            key: vec![0, 0xff, 7, 0xfe],
        }
    }
}

// Debug rendering is used only to put heterogeneous native RETURN values in
// one comparison transcript, never to classify/map an engine error. The
// sentinel's invocation transcript separately checks argument identity/order,
// and NativeEffects compares all mutations, including mutations before Err.
fn exercise_columns(columns: &dyn Columns, input: &NativeInputs) -> Vec<(&'static str, String)> {
    let mut returned = Vec::new();
    macro_rules! observe {
        ($method:ident($($argument:expr),* $(,)?)) => {
            returned.push((stringify!($method), format!("{:?}", columns.$method($($argument),*))));
        };
    }
    observe!(get(&input.path));
    observe!(context_id());
    observe!(use_plan_cache());
    observe!(skip_plan_cache_for_comparison(
        &input.constant,
        &input.target
    ));
    observe!(enable_vectorized_expression());
    observe!(param_value(19));
    observe!(param_value(2));
    observe!(current_insert_value(11));
    observe!(get_param_value(7));
    observe!(bounded_staleness_safe_time());
    observe!(connection_charset_info());
    observe!(no_unsigned_subtraction());
    observe!(now());
    observe!(cast_time_to_year_through_concat());
    observe!(sysdate_is_now());
    observe!(current_database());
    observe!(current_user());
    observe!(login_user());
    observe!(current_role());
    observe!(current_resource_group());
    observe!(connection_id());
    observe!(tidb_decode_key(&input.key));
    observe!(acquire_advisory_lock(
        &input.name,
        Duration::from_nanos(123456789)
    ));
    observe!(acquire_advisory_lock(&input.name, Duration::ZERO));
    observe!(advisory_lock_owner(&input.name));
    observe!(release_advisory_lock(&input.name));
    observe!(release_all_advisory_locks());
    observe!(release_advisory_lock(&input.name));
    observe!(found_rows());
    observe!(current_tso());
    observe!(ddl_owner_info());
    observe!(sysvar(None, &input.name));
    observe!(sysvar(Some(tidb_ast::SysVarScope::Global), &input.name));
    observe!(tidb_info());
    observe!(block_encryption_mode());
    observe!(division_by_zero_level());
    observe!(truncate_level());
    observe!(type_flags());
    observe!(strict_sql_mode());
    observe!(handle_truncate(&input.message));
    observe!(handle_group_concat_cut(&input.message));
    observe!(handle_sleep_incorrect_argument());
    // Never request real waiting, even if a broken forwarder used the default.
    observe!(sleep_for(Duration::ZERO));
    observe!(warning_count());
    observe!(append_warning(43001, &input.message));
    observe!(append_note(43002, &input.message));
    observe!(warning_count());
    observe!(truncate_warnings(2));
    observe!(warning_count());
    observe!(append_warning(43003, &input.target));
    observe!(take_warnings_since(1));
    observe!(warning_count());
    observe!(take_warnings_since(99));
    observe!(max_allowed_packet());
    observe!(handle_allowed_packet_overflowed(&input.name));
    observe!(date_modes());
    observe!(handle_division_by_zero());
    observe!(get_uservar("existing"));
    observe!(get_uservar(&input.name));
    observe!(set_uservar(&input.name, Datum::Bytes(vec![0, 0xfe, 31])));
    observe!(get_uservar(&input.name));
    observe!(row_count());
    observe!(last_insert_id());
    observe!(set_last_insert_id(0xfedc_ba98_7654_3210));
    observe!(last_insert_id());
    observe!(time_zone());
    observe!(like_default_escape());
    observe!(default_week_format());
    observe!(windowing_use_high_precision());
    observe!(div_precision_increment());
    observe!(rand_next());
    observe!(rand_next());
    observe!(rand_seeded_next(37, -981));
    observe!(rand_seeded_next(37, -981));
    observe!(rand_seeded_next(38, -981));
    observe!(sequence_nextval(&input.path));
    observe!(sequence_lastval(&input.path));
    observe!(sequence_setval(&input.path, -810));
    observe!(sequence_nextval(&input.path));
    observe!(sequence_lastval(&input.path));
    assert_eq!(
        returned
            .iter()
            .map(|(method, _)| *method)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(COLUMNS_FORWARDED_METHODS),
        "each inventoried method needs a behavioral observation"
    );
    returned
}

#[test]
fn ordinary_columns_forward_all_63_methods_through_nested_wrappers() {
    let input = NativeInputs::new();
    let owner = AsciiPoolOwner::new(test_policy(2, 2)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let outer = execution.scope();
    let inner = execution.scope();
    for fail in [false, true] {
        let bare = ForwardingSentinel::new(fail, None);
        let expected = exercise_columns(&bare, &input);
        assert_eq!(
            bare.calls
                .borrow()
                .iter()
                .map(|call| call.method)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(COLUMNS_FORWARDED_METHODS),
            "the native sentinel itself must override every policy default"
        );
        for nested in [false, true] {
            let wrapped = ForwardingSentinel::new(fail, None);
            let actual = outer.with_columns(&wrapped, |first| {
                if nested {
                    inner.with_columns(first, |second| {
                        outer.with_columns(second, |third| exercise_columns(third, &input))
                    })
                } else {
                    exercise_columns(first, &input)
                }
            });
            assert_eq!(
                actual, expected,
                "returned values/errors, fail={fail}, nested={nested}"
            );
            assert_eq!(
                *wrapped.calls.borrow(),
                *bare.calls.borrow(),
                "argument identity and native effect order, fail={fail}, nested={nested}"
            );
            assert_eq!(
                *wrapped.effects.borrow(),
                *bare.effects.borrow(),
                "native mutations, fail={fail}, nested={nested}"
            );
        }
    }
    assert!(outer.lease.borrow().is_none());
    assert!(inner.lease.borrow().is_none());
    assert!(!outer.busy.get());
    assert!(!inner.busy.get());
}

// The pointer is an identity witness for the unique Box body, not an allocation
// size measurement. Storage below is the actual C4 nonmutating observation.
fn scope_worker_observation(scope: &AsciiScope) -> (usize, u64, usize, usize, usize) {
    let parked = scope.lease.borrow();
    let worker = parked.as_ref().unwrap().worker.as_ref().unwrap();
    let storage = worker.retained_storage().unwrap();
    assert!(worker.is_healthy());
    (
        worker.as_ref() as *const _ as usize,
        worker.kernel_invocations(),
        storage.inline_bytes(),
        storage.owned_heap_bytes(),
        storage.total_bytes(),
    )
}

// Borrowing, non-Send/non-Sync native context: no 'static requirement and no
// substitute backend. Its advertised execution intentionally may disagree
// with its active scope; the scoped wrapper must resolve that disagreement.
struct AdvertisedAsciiColumns<'a> {
    scope: Option<&'a crate::AsciiScope>,
    execution: &'a crate::AsciiExecution,
}

impl Columns for AdvertisedAsciiColumns<'_> {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn evaluated_ascii_scope(&self) -> Option<&crate::AsciiScope> {
        self.scope
    }

    fn evaluated_ascii_execution(&self) -> Option<&crate::AsciiExecution> {
        Some(self.execution)
    }
}

// The implicit Sized bound is intentional: this is the existing C: Columns
// calling shape, not a new production dispatcher or dyn-only replacement.
fn value_through_sized_columns<C: Columns>(columns: &C, value: &Datum) -> Result<Datum, EvalError> {
    columns
        .evaluated_ascii_scope()
        .expect("this operation explicitly bound its scope")
        .evaluate_value(value)
}

fn dispatch_bytes_family(
    operation: EvaluatedBytesOp,
    value: &Datum,
    columns: &dyn Columns,
) -> Result<Datum, EvalError> {
    let name = match operation {
        EvaluatedBytesOp::Ascii => "ASCII",
        EvaluatedBytesOp::Length => {
            return crate::BuildContext::default()
                .build_string_length(
                    crate::StringLengthFunction::Length,
                    FieldType::new(FieldTypeCode::VarString),
                )
                .eval_in(value, columns);
        }
        EvaluatedBytesOp::BitLength => "BIT_LENGTH",
        EvaluatedBytesOp::LTrim => "LTRIM",
        EvaluatedBytesOp::RTrim => "RTRIM",
        EvaluatedBytesOp::UnHex => "UNHEX",
        EvaluatedBytesOp::Crc32 => "CRC32",
        EvaluatedBytesOp::Reverse | EvaluatedBytesOp::ReverseUtf8 => "REVERSE",
        EvaluatedBytesOp::Quote => "QUOTE",
        EvaluatedBytesOp::HexInt | EvaluatedBytesOp::HexStr => "HEX",
        EvaluatedBytesOp::OctInt | EvaluatedBytesOp::OctStringNative => "OCT",
        EvaluatedBytesOp::Bin => "BIN",
        EvaluatedBytesOp::BitCount => "BIT_COUNT",
        EvaluatedBytesOp::Md5 => "MD5",
        EvaluatedBytesOp::Sha1 => "SHA1",
        EvaluatedBytesOp::InetAton => "INET_ATON",
        EvaluatedBytesOp::InetNtoa => "INET_NTOA",
        EvaluatedBytesOp::Inet6Aton => "INET6_ATON",
        EvaluatedBytesOp::Inet6Ntoa => "INET6_NTOA",
        EvaluatedBytesOp::AsinRaw => "ASIN",
        EvaluatedBytesOp::AcosRaw => "ACOS",
        EvaluatedBytesOp::SqrtRaw => "SQRT",
        EvaluatedBytesOp::SignRaw => "SIGN",
        EvaluatedBytesOp::RadiansRaw => "RADIANS",
        EvaluatedBytesOp::DegreesRaw => "DEGREES",
        EvaluatedBytesOp::PiRaw => panic!("PI requires its original empty argument tuple"),
        EvaluatedBytesOp::IsIpv4Nullable => "IS_IPV4",
        EvaluatedBytesOp::IsIpv6Nullable => "IS_IPV6",
        EvaluatedBytesOp::IsIpv4CompatNullable => "IS_IPV4_COMPAT",
        EvaluatedBytesOp::IsIpv4MappedNullable => "IS_IPV4_MAPPED",
        EvaluatedBytesOp::SpaceNative => "SPACE",
        EvaluatedBytesOp::ToBase64Native => "TO_BASE64",
        EvaluatedBytesOp::FromBase64ValueNative => "FROM_BASE64",
        EvaluatedBytesOp::FromBase64Native => {
            return crate::func::eval_func_values_in(
                "FROM_BASE64",
                std::slice::from_ref(value),
                columns,
            )
            .unwrap();
        }
        EvaluatedBytesOp::RepeatNative => panic!("REPEAT needs its original argument pair"),
        EvaluatedBytesOp::Lower | EvaluatedBytesOp::LowerUtf8Ready => "LOWER",
        EvaluatedBytesOp::Upper | EvaluatedBytesOp::UpperUtf8Ready => "UPPER",
        EvaluatedBytesOp::OrdNative => "ORD",
        EvaluatedBytesOp::UncompressedLengthNative => "UNCOMPRESSED_LENGTH",
        EvaluatedBytesOp::LnNative => "LN",
        EvaluatedBytesOp::Log2Native => "LOG2",
        EvaluatedBytesOp::LogNative | EvaluatedBytesOp::PowNative => {
            panic!("binary real operations need their original pair")
        }
        EvaluatedBytesOp::Insert | EvaluatedBytesOp::InsertUtf8Native => {
            panic!("INSERT requires all four original operands")
        }
        EvaluatedBytesOp::ConcatNative
        | EvaluatedBytesOp::ConcatWsNative
        | EvaluatedBytesOp::EltNative => {
            panic!("variadic string operations need their original argument list")
        }
        EvaluatedBytesOp::JsonValidTextNative
        | EvaluatedBytesOp::JsonValidBinaryNative
        | EvaluatedBytesOp::JsonValidOtherNative
        | EvaluatedBytesOp::JsonTypeTextNative
        | EvaluatedBytesOp::JsonTypeBinaryNative
        | EvaluatedBytesOp::JsonDepthNative => {
            panic!("JSON introspection needs its original source domains")
        }
        EvaluatedBytesOp::JsonStorageFreeNative
        | EvaluatedBytesOp::JsonStorageSizeNative
        | EvaluatedBytesOp::JsonQuoteNative => {
            panic!("JSON storage and quoting need their original source domains")
        }
        EvaluatedBytesOp::YearCoreNative
        | EvaluatedBytesOp::MonthCoreNative
        | EvaluatedBytesOp::DayOfMonthCoreNative
        | EvaluatedBytesOp::QuarterCoreNative => {
            panic!("calendar fields need their original time cores")
        }
        EvaluatedBytesOp::HourTextNative
        | EvaluatedBytesOp::MinuteTextNative
        | EvaluatedBytesOp::SecondTextNative
        | EvaluatedBytesOp::HourNanosNative
        | EvaluatedBytesOp::MinuteNanosNative
        | EvaluatedBytesOp::SecondNanosNative => {
            panic!("HMS calls need their original text or signed nanoseconds")
        }
        EvaluatedBytesOp::MonthNameTextNative | EvaluatedBytesOp::TimeToSecTextNative => {
            panic!("month names and seconds need their original text")
        }
        EvaluatedBytesOp::PeriodAddNative
        | EvaluatedBytesOp::PeriodDiffNative
        | EvaluatedBytesOp::GetFormatNative
        | EvaluatedBytesOp::GetFormatNullNative => {
            panic!("period and format calls need their original arguments and NULL demand")
        }
        EvaluatedBytesOp::DayOfWeekTextNative
        | EvaluatedBytesOp::WeekdayTextNative
        | EvaluatedBytesOp::DayOfYearTextNative
        | EvaluatedBytesOp::DayNameTextNative => {
            panic!("weekday fields need their original date text")
        }
        EvaluatedBytesOp::DateDiffTextNative
        | EvaluatedBytesOp::DateDiffNullNative
        | EvaluatedBytesOp::DateDiffCoreNative
        | EvaluatedBytesOp::ToDaysTextNative
        | EvaluatedBytesOp::ToSecondsTextNative
        | EvaluatedBytesOp::TsoLogicalNative => {
            panic!("date differences, day counts and TSO need their original arguments")
        }
        EvaluatedBytesOp::WeekDateTextNative
        | EvaluatedBytesOp::WeekTextNative
        | EvaluatedBytesOp::YearWeekTextNative
        | EvaluatedBytesOp::WeekOfYearTextNative
        | EvaluatedBytesOp::WeekNullNative
        | EvaluatedBytesOp::WeekCoreNative
        | EvaluatedBytesOp::PasswordNative
        | EvaluatedBytesOp::Sm3Native => {
            panic!("week and auth calls need their original argument domains")
        }
        EvaluatedBytesOp::MakeDateNative
        | EvaluatedBytesOp::FromDaysNative
        | EvaluatedBytesOp::MakeTimePartsNative
        | EvaluatedBytesOp::SecToTimeNative => {
            panic!("temporal construction needs its original numeric and precision domains")
        }
        EvaluatedBytesOp::DateFormatTextNative
        | EvaluatedBytesOp::DateFormatCoreNative
        | EvaluatedBytesOp::DateFormatNullNative
        | EvaluatedBytesOp::DateFormatMissingNative
        | EvaluatedBytesOp::DurationTextProbeNative
        | EvaluatedBytesOp::TimeFormatTextNative
        | EvaluatedBytesOp::LastDayTextNative => {
            panic!("temporal formatting needs its original text, core and demand domains")
        }
        EvaluatedBytesOp::AddIntSsNative
        | EvaluatedBytesOp::AddIntSuNative
        | EvaluatedBytesOp::AddIntUsNative
        | EvaluatedBytesOp::AddIntUuNative
        | EvaluatedBytesOp::SubIntSsNative
        | EvaluatedBytesOp::SubIntSuNative
        | EvaluatedBytesOp::SubIntUsNative
        | EvaluatedBytesOp::SubIntUuNative
        | EvaluatedBytesOp::SubIntSuForcedNative
        | EvaluatedBytesOp::SubIntUsForcedNative
        | EvaluatedBytesOp::SubIntUuForcedNative
        | EvaluatedBytesOp::MulIntSignedNative
        | EvaluatedBytesOp::MulIntUnsignedNative
        | EvaluatedBytesOp::AddRealNative
        | EvaluatedBytesOp::SubRealNative
        | EvaluatedBytesOp::MulRealNative
        | EvaluatedBytesOp::AddDecimalNative
        | EvaluatedBytesOp::SubDecimalNative
        | EvaluatedBytesOp::MulDecimalNative
        | EvaluatedBytesOp::AddVectorNative
        | EvaluatedBytesOp::SubVectorNative
        | EvaluatedBytesOp::MulVectorNative
        | EvaluatedBytesOp::BinaryArithmeticNullNative
        | EvaluatedBytesOp::AddInt128SignedLegacy
        | EvaluatedBytesOp::AddInt128UnsignedLegacy
        | EvaluatedBytesOp::AddInt128RejectLeftLegacy
        | EvaluatedBytesOp::AddInt128RejectRightLegacy
        | EvaluatedBytesOp::SubInt128SignedLegacy
        | EvaluatedBytesOp::SubInt128UnsignedLegacy
        | EvaluatedBytesOp::SubInt128RejectLeftLegacy
        | EvaluatedBytesOp::SubInt128RejectRightLegacy
        | EvaluatedBytesOp::MulInt128SignedLegacy
        | EvaluatedBytesOp::MulInt128UnsignedLegacy
        | EvaluatedBytesOp::AddRealLegacy
        | EvaluatedBytesOp::SubRealLegacy
        | EvaluatedBytesOp::MulRealLegacy
        | EvaluatedBytesOp::AddDecimalLegacy
        | EvaluatedBytesOp::SubDecimalLegacy
        | EvaluatedBytesOp::MulDecimalLegacy
        | EvaluatedBytesOp::BinaryArithmeticMissingLegacy
        | EvaluatedBytesOp::AddDecimalFastNative
        | EvaluatedBytesOp::SubDecimalFastNative
        | EvaluatedBytesOp::MulDecimalFastNative
        | EvaluatedBytesOp::ModIntSsNative
        | EvaluatedBytesOp::ModIntSuNative
        | EvaluatedBytesOp::ModIntUsNative
        | EvaluatedBytesOp::ModIntUuNative
        | EvaluatedBytesOp::ModInt128Legacy
        | EvaluatedBytesOp::ModRealNative
        | EvaluatedBytesOp::ModRealLegacy
        | EvaluatedBytesOp::ModDecimalNative
        | EvaluatedBytesOp::DivRealNative
        | EvaluatedBytesOp::DivRealLegacy
        | EvaluatedBytesOp::DivDecimalNative
        | EvaluatedBytesOp::DivDecimalLegacy => {
            panic!("binary arithmetic needs its actual pair and signature profile")
        }
        EvaluatedBytesOp::UnaryPlusIntNative
        | EvaluatedBytesOp::UnaryPlusBitsNative
        | EvaluatedBytesOp::UnaryPlusDecimalNative
        | EvaluatedBytesOp::UnaryPlusBytesNative
        | EvaluatedBytesOp::UnaryMinusIntNative
        | EvaluatedBytesOp::UnaryMinusUIntNative
        | EvaluatedBytesOp::UnaryMinusIntConstantNative
        | EvaluatedBytesOp::UnaryMinusUIntConstantNative
        | EvaluatedBytesOp::UnaryMinusBitsNative
        | EvaluatedBytesOp::UnaryMinusDecimalNative
        | EvaluatedBytesOp::UnaryNullNative => {
            panic!("unary signs need their actual value kind and operand descriptor")
        }
        EvaluatedBytesOp::LikeNative
        | EvaluatedBytesOp::IlikeNative
        | EvaluatedBytesOp::LikeLegacyNative
        | EvaluatedBytesOp::LikeNullIntNative
        | EvaluatedBytesOp::LikeMissingLegacyNative => {
            panic!("LIKE calls need their actual arguments, demand and cache handles")
        }
        EvaluatedBytesOp::RegexpLikeNative
        | EvaluatedBytesOp::RegexpSubstrNative
        | EvaluatedBytesOp::RegexpInstrNative
        | EvaluatedBytesOp::RegexpReplaceNative
        | EvaluatedBytesOp::RegexpLikeLegacyCiNative
        | EvaluatedBytesOp::RegexpLikeLegacyBinNative
        | EvaluatedBytesOp::RegexpNullIntNative
        | EvaluatedBytesOp::RegexpNullBytesNative
        | EvaluatedBytesOp::RegexpMissingLegacyNative => {
            panic!("regexp calls need their actual arguments, demand and cache handles")
        }
        EvaluatedBytesOp::CompareIntSsNative(_)
        | EvaluatedBytesOp::CompareIntSuNative(_)
        | EvaluatedBytesOp::CompareIntUsNative(_)
        | EvaluatedBytesOp::CompareIntUuNative(_)
        | EvaluatedBytesOp::CompareInt128Legacy(_)
        | EvaluatedBytesOp::CompareRealNative(_)
        | EvaluatedBytesOp::CompareRealLegacy(_)
        | EvaluatedBytesOp::CompareDecimalNative(_)
        | EvaluatedBytesOp::CompareBytesNative(_)
        | EvaluatedBytesOp::CompareVectorNative(_)
        | EvaluatedBytesOp::CompareTimeCoreNative(_)
        | EvaluatedBytesOp::CompareDurationNative(_)
        | EvaluatedBytesOp::CompareJsonNative(_)
        | EvaluatedBytesOp::CompareNullNative
        | EvaluatedBytesOp::CompareMissingLegacy => {
            panic!("comparison needs its actual domain pair, finite predicate and NULL demand")
        }
        EvaluatedBytesOp::VecAsTextNative => "VEC_AS_TEXT",
        EvaluatedBytesOp::VecDimsNative => "VEC_DIMS",
        EvaluatedBytesOp::VecFromTextNative => "VEC_FROM_TEXT",
        EvaluatedBytesOp::VecL2NormNative => "VEC_L2_NORM",
        EvaluatedBytesOp::VecL1DistanceNative
        | EvaluatedBytesOp::VecL2DistanceNative
        | EvaluatedBytesOp::VecNegativeInnerProductNative
        | EvaluatedBytesOp::VecCosineDistanceNative
        | EvaluatedBytesOp::VecRealNullNative => {
            panic!("vector distances need their original pair and NULL demand")
        }
        EvaluatedBytesOp::SqlEncodeNative
        | EvaluatedBytesOp::SqlDecodeNative
        | EvaluatedBytesOp::SqlCryptNullNative => {
            panic!("SQL crypt needs its original data/password coercion demand")
        }
        EvaluatedBytesOp::AesEncrypt128EcbNative
        | EvaluatedBytesOp::AesEncrypt192EcbNative
        | EvaluatedBytesOp::AesEncrypt256EcbNative
        | EvaluatedBytesOp::AesEncrypt128CbcNative
        | EvaluatedBytesOp::AesEncrypt192CbcNative
        | EvaluatedBytesOp::AesEncrypt256CbcNative
        | EvaluatedBytesOp::AesEncrypt128OfbNative
        | EvaluatedBytesOp::AesEncrypt192OfbNative
        | EvaluatedBytesOp::AesEncrypt256OfbNative
        | EvaluatedBytesOp::AesEncrypt128CfbNative
        | EvaluatedBytesOp::AesEncrypt192CfbNative
        | EvaluatedBytesOp::AesEncrypt256CfbNative
        | EvaluatedBytesOp::AesDecrypt128EcbNative
        | EvaluatedBytesOp::AesDecrypt192EcbNative
        | EvaluatedBytesOp::AesDecrypt256EcbNative
        | EvaluatedBytesOp::AesDecrypt128CbcNative
        | EvaluatedBytesOp::AesDecrypt192CbcNative
        | EvaluatedBytesOp::AesDecrypt256CbcNative
        | EvaluatedBytesOp::AesDecrypt128OfbNative
        | EvaluatedBytesOp::AesDecrypt192OfbNative
        | EvaluatedBytesOp::AesDecrypt256OfbNative
        | EvaluatedBytesOp::AesDecrypt128CfbNative
        | EvaluatedBytesOp::AesDecrypt192CfbNative
        | EvaluatedBytesOp::AesDecrypt256CfbNative
        | EvaluatedBytesOp::AesNullNative => {
            panic!("AES needs its original data/key/IV and genuine NULL demand")
        }
        EvaluatedBytesOp::GroupingBitAndNative
        | EvaluatedBytesOp::GroupingNumericCmpNative
        | EvaluatedBytesOp::GroupingNumericSetNative
        | EvaluatedBytesOp::GroupingNullNative => {
            panic!("GROUPING needs its actual grouping id, mark sets and NULL demand")
        }
        EvaluatedBytesOp::JsonContainsSerdeNative
        | EvaluatedBytesOp::JsonContainsPathSerdeNative
        | EvaluatedBytesOp::JsonOverlapsSerdeNative
        | EvaluatedBytesOp::JsonMemberOfSerdeNative
        | EvaluatedBytesOp::JsonLengthSerdeNative
        | EvaluatedBytesOp::JsonLengthPathSerdeNative
        | EvaluatedBytesOp::JsonPathExistsSerdeNative
        | EvaluatedBytesOp::JsonMemberOfBinaryLegacy
        | EvaluatedBytesOp::JsonPredicateNullNative
        | EvaluatedBytesOp::JsonPredicateMissingLegacy => {
            panic!("JSON predicates need actual documents, paths and presence")
        }
        EvaluatedBytesOp::JsonArraySerdeNative
        | EvaluatedBytesOp::JsonObjectSerdeNative
        | EvaluatedBytesOp::JsonKeysSerdeNative
        | EvaluatedBytesOp::JsonKeysPathSerdeNative
        | EvaluatedBytesOp::JsonPrettySerdeNative
        | EvaluatedBytesOp::JsonOutputNullNative => {
            panic!("JSON outputs need their actual argument list, document/path or NULL witness")
        }
        EvaluatedBytesOp::JsonSearchSerdeNative => {
            panic!("JSON_SEARCH needs the actual document, parsed paths and matching inputs")
        }
        EvaluatedBytesOp::JsonExtractSerdeNative
        | EvaluatedBytesOp::JsonInsertSerdeNative
        | EvaluatedBytesOp::JsonSetSerdeNative
        | EvaluatedBytesOp::JsonReplaceSerdeNative
        | EvaluatedBytesOp::JsonRemoveSerdeNative
        | EvaluatedBytesOp::JsonArrayAppendSerdeNative
        | EvaluatedBytesOp::JsonArrayInsertSerdeNative => {
            panic!("JSON path operations need actual parsed paths and ordered values")
        }
        EvaluatedBytesOp::JsonReplaceRawLegacy
        | EvaluatedBytesOp::JsonArrayAppendRawLegacy
        | EvaluatedBytesOp::JsonArrayAppendEmptyLegacy
        | EvaluatedBytesOp::JsonValueAbsentLegacy => {
            panic!("legacy JSON outputs need original raw values and observed presence")
        }
        EvaluatedBytesOp::JsonUnquoteTextNative | EvaluatedBytesOp::JsonUnquoteBinaryNative => {
            panic!("JSON_UNQUOTE needs its actual text or binary document domain")
        }
        EvaluatedBytesOp::UtcDateNative
        | EvaluatedBytesOp::UtcTimestampNative
        | EvaluatedBytesOp::CurrentTimeWithoutFspNative
        | EvaluatedBytesOp::CurrentTimeWithFspNative
        | EvaluatedBytesOp::UtcTimeWithoutFspNative
        | EvaluatedBytesOp::UtcTimeWithFspNative
        | EvaluatedBytesOp::UtcTimeNullNative => {
            panic!("clock functions need actual UTC clock fields, precision and NULL demand")
        }
        EvaluatedBytesOp::JsonMergeSerdeNative
        | EvaluatedBytesOp::JsonMergePatchSerdeNative
        | EvaluatedBytesOp::JsonMergePatchRawLegacy => {
            panic!("JSON merge needs its actual ordered values and presence frame")
        }
        EvaluatedBytesOp::NowNative
        | EvaluatedBytesOp::CurrentDateNative
        | EvaluatedBytesOp::SysdateNative => {
            panic!("local clock functions need the original clock tuple and precision")
        }
        EvaluatedBytesOp::DateCoreNative | EvaluatedBytesOp::DateCorePredicateLegacy => {
            panic!("DATE needs its original temporal core and mode or nullable predicate role")
        }
        EvaluatedBytesOp::WeightStringNative
        | EvaluatedBytesOp::WeightStringCharNative
        | EvaluatedBytesOp::WeightStringBinaryNative
        | EvaluatedBytesOp::WeightStringNumericNative
        | EvaluatedBytesOp::FormatLocaleNative => {
            panic!("weight and locale formatting need their original operands and metadata")
        }
        EvaluatedBytesOp::AnyValueNative | EvaluatedBytesOp::NameConstNative => {
            panic!("identity needs the selected actual Datum payload and metadata")
        }
        EvaluatedBytesOp::TidbParseTsoNative | EvaluatedBytesOp::TimeDiffTextNative => {
            panic!("TSO and TIMEDIFF need their actual operands and conditional demand")
        }
        EvaluatedBytesOp::TimeNative
        | EvaluatedBytesOp::MicrosecondNative
        | EvaluatedBytesOp::MicrosecondLegacy => {
            panic!("TIME and MICROSECOND need their actual text or nullable nanoseconds")
        }
        EvaluatedBytesOp::AddTimeNative
        | EvaluatedBytesOp::SubTimeNative
        | EvaluatedBytesOp::TimeAddRightDatetimeNative => {
            panic!("ADDTIME and SUBTIME need actual coerced operands and type metadata")
        }
        EvaluatedBytesOp::TimestampAddNative | EvaluatedBytesOp::TimestampAddPrefixNullNative => {
            panic!("TIMESTAMPADD needs actual operands or its genuine NULL prefix")
        }
        EvaluatedBytesOp::DateLiteralNative | EvaluatedBytesOp::TimestampLiteralNative => {
            panic!("temporal literals need their actual text, modes and owned session timezone")
        }
        EvaluatedBytesOp::Timestamp1Native
        | EvaluatedBytesOp::Timestamp2BaseNative
        | EvaluatedBytesOp::Timestamp2AddNative
        | EvaluatedBytesOp::TimestampNullNative => {
            panic!("TIMESTAMP needs its actual parse text, computed base, or evaluated NULL")
        }
        EvaluatedBytesOp::UnixTimestampNowNative
        | EvaluatedBytesOp::UnixTimestampNullNative
        | EvaluatedBytesOp::UnixTimestampParseNative
        | EvaluatedBytesOp::UnixTimestampValueNative
        | EvaluatedBytesOp::UnixTimestampIntLegacy
        | EvaluatedBytesOp::UnixTimestampDecLegacy => {
            panic!("UNIX_TIMESTAMP needs its actual clock, NULL, text or typed time and zone")
        }
        EvaluatedBytesOp::FromUnixTimeNumericNative
        | EvaluatedBytesOp::FromUnixTimeTextNative
        | EvaluatedBytesOp::FromUnixTimeLocalNative
        | EvaluatedBytesOp::FromUnixTimeLegacy
        | EvaluatedBytesOp::FromUnixTimeNullNative => {
            panic!("FROM_UNIXTIME needs its actual numeric, text, epoch or nullable decimal input")
        }
        EvaluatedBytesOp::IfNullHeadNative | EvaluatedBytesOp::IfNullFinishNative => {
            panic!("IFNULL needs its actual first value or original head report and demanded second value")
        }
        EvaluatedBytesOp::IfHeadNative | EvaluatedBytesOp::IfFinishNative => {
            panic!(
                "IF needs its actual nullable condition or original head report and chosen value"
            )
        }
        EvaluatedBytesOp::ConvertTzNative => {
            panic!("CONVERT_TZ needs its three actual nullable coerced strings")
        }
        EvaluatedBytesOp::IntDivDecimalSignedNative
        | EvaluatedBytesOp::IntDivDecimalUnsignedNative
        | EvaluatedBytesOp::IntDivDecimalLegacy
        | EvaluatedBytesOp::IntDivIntSsNative
        | EvaluatedBytesOp::IntDivIntUsNative
        | EvaluatedBytesOp::IntDivIntSuNative
        | EvaluatedBytesOp::IntDivIntUuNative
        | EvaluatedBytesOp::IntDivInt128Legacy => {
            panic!("integer DIV needs both original operands and its signedness policy")
        }
        EvaluatedBytesOp::TidbShardNative => "TIDB_SHARD",
        EvaluatedBytesOp::VitessHashNative => "VITESS_HASH",
        EvaluatedBytesOp::FormatBytesNative => "FORMAT_BYTES",
        EvaluatedBytesOp::FormatNanoTimeNative => "FORMAT_NANO_TIME",
        EvaluatedBytesOp::IsUuidNative => "IS_UUID",
        EvaluatedBytesOp::UuidVersionNative => "UUID_VERSION",
        EvaluatedBytesOp::UuidTimestampNative => "UUID_TIMESTAMP",
        EvaluatedBytesOp::UuidToBinParseNative
        | EvaluatedBytesOp::UuidToBinSwapNative
        | EvaluatedBytesOp::BinToUuidNative
        | EvaluatedBytesOp::TranslateUtf8Native
        | EvaluatedBytesOp::TranslateBinaryNative
        | EvaluatedBytesOp::TranslateNullNative => {
            panic!("UUID conversion and TRANSLATE need their original arguments and demand")
        }
        EvaluatedBytesOp::CompressGoNative | EvaluatedBytesOp::UncompressNative => {
            panic!("compression calls need their original nullable bytes")
        }
        EvaluatedBytesOp::CharNative
        | EvaluatedBytesOp::ConvNative
        | EvaluatedBytesOp::ConvBinaryLiteralNative
        | EvaluatedBytesOp::ConvLegacy => {
            panic!("CHAR and CONV need their original operands and domains")
        }
        EvaluatedBytesOp::SinGoNative
        | EvaluatedBytesOp::CosGoNative
        | EvaluatedBytesOp::TanGoNative
        | EvaluatedBytesOp::CotGoNative
        | EvaluatedBytesOp::AtanGoNative
        | EvaluatedBytesOp::Atan2GoNative
        | EvaluatedBytesOp::SinLibmLegacy
        | EvaluatedBytesOp::CosLibmLegacy
        | EvaluatedBytesOp::CotLibmLegacy
        | EvaluatedBytesOp::AtanLibmLegacy
        | EvaluatedBytesOp::Atan2LibmLegacy => {
            panic!("trigonometric calls need their original numeric operands")
        }
        EvaluatedBytesOp::ExpGoNative | EvaluatedBytesOp::Log10GoNative => {
            panic!("EXP and LOG10 need their original numeric operands")
        }
        EvaluatedBytesOp::AbsIntNative
        | EvaluatedBytesOp::AbsUIntNative
        | EvaluatedBytesOp::AbsRealNative
        | EvaluatedBytesOp::AbsDecimalNative
        | EvaluatedBytesOp::CeilIntNative
        | EvaluatedBytesOp::FloorIntNative
        | EvaluatedBytesOp::CeilRealNative
        | EvaluatedBytesOp::FloorRealNative
        | EvaluatedBytesOp::CeilDecimalNative
        | EvaluatedBytesOp::FloorDecimalNative
        | EvaluatedBytesOp::RoundIntNative
        | EvaluatedBytesOp::RoundIntWithScaleNative
        | EvaluatedBytesOp::RoundRealNative
        | EvaluatedBytesOp::RoundDecimalNative
        | EvaluatedBytesOp::TruncateIntNative
        | EvaluatedBytesOp::TruncateUIntNative
        | EvaluatedBytesOp::TruncateIntUnsignedScaleNative
        | EvaluatedBytesOp::TruncateRealNative
        | EvaluatedBytesOp::TruncateDecimalNative
        | EvaluatedBytesOp::RoundInt128Legacy
        | EvaluatedBytesOp::RoundRealLegacy
        | EvaluatedBytesOp::RoundDecimalLegacy
        | EvaluatedBytesOp::MathNullWitnessNative => {
            panic!("typed math needs its exact operand domain and NULL demand")
        }
        EvaluatedBytesOp::FieldBytesNative
        | EvaluatedBytesOp::FieldIntNative
        | EvaluatedBytesOp::FieldRealNative
        | EvaluatedBytesOp::MakeSetNative
        | EvaluatedBytesOp::ExportSetNative => {
            panic!("FIELD and set operations need their original operand domains")
        }
        EvaluatedBytesOp::StrcmpNative
        | EvaluatedBytesOp::Locate2Native
        | EvaluatedBytesOp::Locate3Native
        | EvaluatedBytesOp::Locate3BytesExtNative
        | EvaluatedBytesOp::Locate3Utf8ExtNative
        | EvaluatedBytesOp::FindInSetNative
        | EvaluatedBytesOp::FindInSetPreparedNative => {
            panic!("collation search needs its complete operands and policy")
        }
        EvaluatedBytesOp::Substring2BytesNative
        | EvaluatedBytesOp::Substring2Utf8Native
        | EvaluatedBytesOp::Substring3BytesNative
        | EvaluatedBytesOp::Substring3Utf8Native
        | EvaluatedBytesOp::Substring2BytesLegacy
        | EvaluatedBytesOp::Substring2Utf8Legacy
        | EvaluatedBytesOp::Substring3BytesLegacy
        | EvaluatedBytesOp::Substring3Utf8Legacy => {
            panic!("SUBSTRING needs its original argument demand and arity")
        }
        EvaluatedBytesOp::Sha2Native => panic!("SHA2 needs its original argument pair"),
        EvaluatedBytesOp::LowerAsciiNative | EvaluatedBytesOp::UpperAsciiNative => {
            panic!("legacy ASCII case requires its raw legacy entry")
        }
        EvaluatedBytesOp::TrimBothNative
        | EvaluatedBytesOp::TrimLeadingNative
        | EvaluatedBytesOp::TrimTrailingNative
        | EvaluatedBytesOp::SubstringIndexSignedNative
        | EvaluatedBytesOp::SubstringIndexUnsignedNative
        | EvaluatedBytesOp::LpadBytesNative
        | EvaluatedBytesOp::RpadBytesNative
        | EvaluatedBytesOp::LpadUtf8Native
        | EvaluatedBytesOp::RpadUtf8Native => {
            panic!("trim and pad families require their complete operands")
        }
        EvaluatedBytesOp::IsNull => "ISNULL",
        EvaluatedBytesOp::IsTrue => "ISTRUE",
        EvaluatedBytesOp::IsFalse => "ISFALSE",
        EvaluatedBytesOp::IsTrueWithNull => "ISTRUE_WITH_NULL",
        EvaluatedBytesOp::UnaryNot => {
            return crate::apply_unary(tidb_ast::UnaryOp::Not, value.clone(), columns);
        }
        EvaluatedBytesOp::IsNotNull
        | EvaluatedBytesOp::IsNotTrue
        | EvaluatedBytesOp::IsNotFalse => {
            panic!("composed predicates need their original expression entry")
        }
        EvaluatedBytesOp::BitNeg => {
            return crate::apply_unary(tidb_ast::UnaryOp::BitNeg, value.clone(), columns);
        }
        EvaluatedBytesOp::Left
        | EvaluatedBytesOp::LeftUtf8
        | EvaluatedBytesOp::Right
        | EvaluatedBytesOp::RightUtf8
        | EvaluatedBytesOp::Replace
        | EvaluatedBytesOp::BitAnd
        | EvaluatedBytesOp::BitOr
        | EvaluatedBytesOp::BitXor
        | EvaluatedBytesOp::LeftShift
        | EvaluatedBytesOp::RightShift
        | EvaluatedBytesOp::LogicalAnd
        | EvaluatedBytesOp::LogicalOr
        | EvaluatedBytesOp::LogicalXor => {
            panic!("multi-argument families need their original argument tuple")
        }
        EvaluatedBytesOp::CharLength | EvaluatedBytesOp::CharLengthUtf8 => {
            let collation = if operation == EvaluatedBytesOp::CharLength {
                tidb_datatype::Collation::Binary
            } else {
                tidb_datatype::Collation::DEFAULT
            };
            return crate::BuildContext::default()
                .build_string_length(
                    crate::StringLengthFunction::CharLength,
                    FieldType::new(FieldTypeCode::VarString).with_collation(collation),
                )
                .eval_in(value, columns);
        }
    };
    crate::func::eval_func_values(name, std::slice::from_ref(value), columns)
        .expect("closed family has its existing frontend entry")
}

fn date_diff_days_native(
    name: &str,
    values: &[Datum],
    columns: &dyn Columns,
) -> Result<Datum, EvalError> {
    if name == "DATEDIFF" {
        // Exercise the calendar body's own guard, not func's separate len==2
        // admission gate (whose wrong-arity result is not replaced here).
        crate::time_fn::calendar::date_diff_in(values, columns)
    } else {
        crate::time_fn::dispatch(name, values, columns).unwrap()
    }
}

fn date_diff_days_function(
    name: &str,
    values: Vec<Datum>,
    pb: bool,
    unreadable_tail: bool,
) -> crate::scalar_function::ScalarFunction {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    let result_type = FieldType::new(FieldTypeCode::LongLong);
    let mut args = values
        .into_iter()
        .map(|value| {
            let source_type = if matches!(&value, Datum::Int(_)) {
                FieldTypeCode::LongLong
            } else {
                FieldTypeCode::VarString
            };
            Expression::Constant(Constant::new(value, FieldType::new(source_type)))
        })
        .collect::<Vec<_>>();
    if unreadable_tail {
        args.push(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("__undemanded_date_diff_suffix__"),
            result_type.clone(),
            Vec::new(),
        )));
    }
    if pb {
        assert_eq!(
            name, "DATEDIFF",
            "only the existing PB DateDiff signature is admitted"
        );
        ScalarFunction::from_pb(
            PbBuiltin::new(tidb_proto::tipb::ScalarFuncSig::DateDiff).unwrap(),
            result_type,
            args,
        )
    } else {
        ScalarFunction::new(tidb_ast::CiString::new(name), result_type, args)
    }
}

struct WeekAuthMode {
    mode: Cell<i64>,
    reads: RefCell<Vec<bool>>,
}

impl Columns for WeekAuthMode {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn default_week_format(&self) -> i64 {
        let before_probe = EVAL_ONE_OBSERVATION.with(|slot| {
            slot.borrow().as_ref().is_some_and(|value| {
                value.facade_entries == 0
                    && value.before_kernel_invocations.is_none()
                    && value.after_kernel_invocations.is_none()
            })
        });
        self.reads.borrow_mut().push(before_probe);
        self.mode.get()
    }
}

fn assert_week_auth_two_calls(observation: EvalOneObservation) {
    assert_eq!(observation.facade_entries, 2);
    // The observer retains FIRST-before and LAST-after across different workers.
    // These are actual getters, not a same-worker delta or cumulative counter.
    assert!(observation.before_kernel_invocations.is_some());
    assert!(observation
        .after_kernel_invocations
        .is_some_and(|value| value > 0));
}

fn week_auth_pb(
    values: Vec<Datum>,
    unreadable_tail: bool,
) -> crate::scalar_function::ScalarFunction {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    let field = FieldType::new(FieldTypeCode::LongLong);
    let mut args = values
        .into_iter()
        .map(|value| {
            let code = if matches!(&value, Datum::Int(_)) {
                FieldTypeCode::LongLong
            } else {
                FieldTypeCode::VarString
            };
            Expression::Constant(Constant::new(value, FieldType::new(code)))
        })
        .collect::<Vec<_>>();
    if unreadable_tail {
        args.push(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("__undemanded_week_suffix__"),
            field.clone(),
            Vec::new(),
        )));
    }
    ScalarFunction::from_pb(
        PbBuiltin::new(tidb_proto::tipb::ScalarFuncSig::WeekWithoutMode).unwrap(),
        field,
        args,
    )
}

#[derive(Default)]
struct ConstructTimeWarnings(RefCell<Vec<(u16, String, bool)>>);

impl Columns for ConstructTimeWarnings {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn append_warning(&self, code: u16, message: &str) {
        let before_admission = EVAL_ONE_OBSERVATION.with(|slot| {
            slot.borrow().as_ref().is_some_and(|value| {
                value.facade_entries == 0
                    && value.before_kernel_invocations.is_none()
                    && value.after_kernel_invocations.is_none()
            })
        });
        self.0
            .borrow_mut()
            .push((code, message.to_owned(), before_admission));
    }
}

fn construct_time_function(
    name: &str,
    values: Vec<Datum>,
    result_code: FieldTypeCode,
) -> crate::scalar_function::ScalarFunction {
    let args = values
        .into_iter()
        .map(|value| {
            crate::expression::Expression::Constant(Constant::new(
                value,
                FieldType::new(FieldTypeCode::LongLong),
            ))
        })
        .collect();
    crate::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new(name),
        FieldType::new(result_code).with_decimal(0),
        args,
    )
}

fn format_time_pb(
    values: Vec<Datum>,
    unreadable_tail: bool,
) -> crate::scalar_function::ScalarFunction {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    let field = FieldType::new(FieldTypeCode::VarString);
    let mut args = values
        .into_iter()
        .map(|value| Expression::Constant(Constant::new(value, field.clone())))
        .collect::<Vec<_>>();
    if unreadable_tail {
        args.push(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("__undemanded_date_format_suffix__"),
            field.clone(),
            Vec::new(),
        )));
    }
    ScalarFunction::from_pb(
        PbBuiltin::new(tidb_proto::tipb::ScalarFuncSig::DateFormatSig).unwrap(),
        field,
        args,
    )
}

#[test]
fn binary_arithmetic_dispatch_distinguishes_fast_outcomes_and_infra() {
    let left = NativeDecimalFastValue {
        coefficient: 12,
        storage_scale: 1,
        scale: 1,
    };
    let right = NativeDecimalFastValue {
        coefficient: 3,
        ..left
    };
    let minimum = NativeDecimalFastValue {
        coefficient: i128::MIN,
        storage_scale: 0,
        scale: 0,
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // SUB must first negate its rhs: MIN-MIN is Unsupported, not a computed
        // zero or SQL NULL. These are the old checked-coefficient policy cases.
        for (operation, left, right, expected) in [
            (
                BinaryArithmeticOperation::Add,
                Some(left),
                Some(right),
                NativeDecimalFastOutcome::Value(Some(NativeDecimalFastValue {
                    coefficient: 15,
                    storage_scale: 1,
                    scale: 1,
                })),
            ),
            (
                BinaryArithmeticOperation::Subtract,
                Some(minimum),
                Some(minimum),
                NativeDecimalFastOutcome::Unsupported,
            ),
            (
                BinaryArithmeticOperation::Multiply,
                Some(left),
                Some(right),
                NativeDecimalFastOutcome::Value(Some(NativeDecimalFastValue {
                    coefficient: 36,
                    storage_scale: 2,
                    scale: 2,
                })),
            ),
            (
                BinaryArithmeticOperation::Add,
                None,
                Some(right),
                NativeDecimalFastOutcome::Value(None),
            ),
        ] {
            arm_eval_one_observation();
            let result = eval_arithmetic_decimal_fast_in(operation, left, right, columns);
            assert_wide_math_c4(take_eval_one_observation());
            assert_eq!(result, Ok(expected));
        }
    });
    let current = scope_worker_observation(&scope);
    assert_eq!(current.2 + current.3, current.4);
    assert!(current.4 <= TEST_WORKER_CAP);
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (left, right) in [(Some(left), Some(right)), (None, None)] {
            arm_eval_one_observation();
            let result = eval_arithmetic_decimal_fast_in(BinaryArithmeticOperation::Add, left, right, columns);
            let observation = take_eval_one_observation();
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        // A representation error also remains typed Err, never Unsupported.
        // This small invalid shape exercises the real bridge, not allocation/OOM.
        arm_eval_one_observation();
        let result = eval_arithmetic_decimal_fast_in(BinaryArithmeticOperation::Add,
            Some(NativeDecimalFastValue { coefficient: 1, storage_scale: 0, scale: 1 }), None, columns);
        assert!(matches!(result, Err(EvalError::ExpressionRuntimeFailure(_))));
        assert_eq!(take_eval_one_observation().facade_entries, 0);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn binary_arithmetic_dispatch_keeps_profiles_and_legacy_presence() {
    use tidb_ast::BinaryOp;
    use tidb_datatype::Decimal;

    struct Mode {
        forced: Cell<bool>,
        reads: Cell<usize>,
    }
    impl Columns for Mode {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn no_unsigned_subtraction(&self) -> bool {
            self.reads.set(self.reads.get() + 1);
            self.forced.get()
        }
    }
    let mode = Mode {
        forced: Cell::new(false),
        reads: Cell::new(0),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&mode, |columns| {
        for forced in [false, true] {
            mode.forced.set(forced);
            let reads = mode.reads.get();
            let (result, observation) = observe_wide_math(|| {
                crate::ops::eval_binary_in(BinaryOp::Minus, Datum::UInt(2), Datum::UInt(3), columns)
            });
            assert_wide_math_c4(observation);
            assert_eq!(mode.reads.get(), reads + 1);
            assert_eq!(result, if forced { Ok(Datum::Int(-1)) } else { Err(EvalError::IntOverflow) });
            let operation = if forced { EvaluatedBytesOp::SubIntUuForcedNative } else { EvaluatedBytesOp::SubIntUuNative };
            assert_eq!(scope.lease.borrow().as_ref().unwrap().worker.as_ref().unwrap().operation(), operation);
        }
        let reads = mode.reads.get();
        let (result, observation) = observe_wide_math(|| {
            crate::ops::eval_binary_in(BinaryOp::Plus, Datum::UInt(2), Datum::UInt(3), columns)
        });
        assert_eq!(result, Ok(Datum::UInt(5)));
        assert_wide_math_c4(observation);
        assert_eq!(mode.reads.get(), reads);
        let (result, observation) = observe_wide_math(|| {
            crate::ops::eval_binary_in(BinaryOp::Mul, Datum::Real(f64::MAX), Datum::Real(2.0), columns)
        });
        assert_eq!(result, Err(EvalError::FloatOverflow));
        assert_wide_math_c4(observation);

        // Legacy range checks constrain the result, not the input's i128 bits.
        // Explicit reject-left/right signatures still have their original errors.
        for (profile, left, right, expected) in [
            (LegacyIntegerArithmetic::AddSigned, -1, 2, Ok(Some(1))),
            (LegacyIntegerArithmetic::AddUnsigned, 1_i128 << 64, -1, Ok(Some(i128::from(u64::MAX)))),
            (LegacyIntegerArithmetic::AddRejectLeft, -1, 2, Err(EvalError::DataOutOfRange { value: "BIGINT UNSIGNED", expression: "ADD".to_owned() })),
            (LegacyIntegerArithmetic::AddRejectRight, 2, -1, Err(EvalError::DataOutOfRange { value: "BIGINT UNSIGNED", expression: "ADD".to_owned() })),
            (LegacyIntegerArithmetic::SubUnsigned, 0, 1, Err(EvalError::DataOutOfRange { value: "BIGINT UNSIGNED", expression: "SUBTRACT".to_owned() })),
            (LegacyIntegerArithmetic::MulSigned, i128::from(i64::MAX), 2, Err(EvalError::DataOutOfRange { value: "BIGINT", expression: "MULTIPLY".to_owned() })),
        ] {
            arm_eval_one_observation();
            let result = eval_legacy_integer_arithmetic_in(profile, LegacyBinaryArgs::Values(left, right), columns);
            assert_wide_math_c4(take_eval_one_observation());
            assert_eq!(result, expected);
        }
        arm_eval_one_observation();
        let result = eval_legacy_real_arithmetic_in(BinaryArithmeticOperation::Multiply, LegacyBinaryArgs::Values(f64::MAX, 2.0), columns);
        assert_eq!(result, Ok(Some(f64::INFINITY)));
        assert_wide_math_c4(take_eval_one_observation());
        arm_eval_one_observation();
        let result = eval_legacy_decimal_arithmetic_in(BinaryArithmeticOperation::Add,
            LegacyBinaryArgs::Values(Decimal::from_literal("0.10"), Decimal::from_literal("0.20")), columns);
        assert_eq!(result.unwrap().unwrap().to_string(), "0.30");
        assert_wide_math_c4(take_eval_one_observation());

        for missing in [false, true] {
            let operation = if missing { EvaluatedBytesOp::BinaryArithmeticMissingLegacy } else { EvaluatedBytesOp::BinaryArithmeticNullNative };
            arm_eval_one_observation();
            assert_eq!(eval_legacy_integer_arithmetic_in(LegacyIntegerArithmetic::AddSigned,
                if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns), Ok(None));
            assert_wide_math_c4(take_eval_one_observation());
            assert_eq!(scope.lease.borrow().as_ref().unwrap().worker.as_ref().unwrap().operation(), operation);
            arm_eval_one_observation();
            assert_eq!(eval_legacy_real_arithmetic_in(BinaryArithmeticOperation::Add,
                if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns), Ok(None));
            assert_wide_math_c4(take_eval_one_observation());
            arm_eval_one_observation();
            assert_eq!(eval_legacy_decimal_arithmetic_in(BinaryArithmeticOperation::Add,
                if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns), Ok(None));
            assert_wide_math_c4(take_eval_one_observation());
        }
        arm_eval_one_observation();
        let result = eval_legacy_integer_arithmetic_in(LegacyIntegerArithmetic::AddSigned, LegacyBinaryArgs::NullWitness(Some(0)), columns);
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert_eq!(take_eval_one_observation().facade_entries, 0);
        assert_eq!(mode.reads.get(), reads);
    });
    assert!(!scope.busy.get());
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn json_merge_sdk_preserves_nullable_documents_and_raw_codec_results() {
    use serde_json::json;
    use tidb_datatype::BinaryJSON;
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (values, expected) in [
            (Vec::new(), "[]"),
            (
                vec![json!({"a":1}), json!({"a":2}), json!([3]), json!({"b":4})],
                "[{\"a\":[1,2]},3,{\"b\":4}]",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    JsonMergeSerdeNative,
                    columns,
                    || super::super::prepare_json_array_args(&values),
                    EvaluatedBytesResult::into_json_datum,
                )
            });
            assert_eq!(
                result,
                Ok(Datum::Json(BinaryJSON::parse(expected).unwrap()))
            );
            assert_wide_math_c4(observation);
        }
        for (values, expected) in [
            (vec![Some(json!({"a":1})), None, Some(json!({"b":2}))], None),
            (
                vec![None, Some(json!(null)), Some(json!({"b":2}))],
                Some("{\"b\":2}"),
            ),
            (
                vec![Some(json!({"a":1})), Some(json!({"a":null,"b":2}))],
                Some("{\"b\":2}"),
            ),
            (vec![Some(json!(null))], Some("null")),
            (vec![None], None),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    JsonMergePatchSerdeNative,
                    columns,
                    || super::super::prepare_json_merge_patch_args(&values),
                    EvaluatedBytesResult::into_json_datum,
                )
            });
            assert_eq!(
                result,
                Ok(expected.map_or(Datum::Null, |text| Datum::Json(
                    BinaryJSON::parse(text).unwrap()
                )))
            );
            assert_wide_math_c4(observation);
        }
        for (values, expected) in [
            (Vec::new(), None),
            (
                vec![
                    BinaryJSON::parse("{\"a\":1}").unwrap(),
                    BinaryJSON::parse("{\"a\":null,\"b\":2}").unwrap(),
                ],
                Some(BinaryJSON::parse("{\"b\":2}").unwrap()),
            ),
            (vec![BinaryJSON::from_encoded_parts(0xff, vec![7])], None),
        ] {
            let (result, observation) =
                observe_wide_math(|| crate::eval_legacy_json_merge_patch_in(&values, columns));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        let wide = BinaryJSON::from_typed_value(&tidb_datatype::BinaryJSONValue::Uint64(u64::MAX))
            .unwrap();
        let (result, observation) = observe_wide_math(|| {
            crate::eval_legacy_json_merge_patch_in(std::slice::from_ref(&wide), columns)
        });
        let actual = result.unwrap().unwrap();
        assert_eq!(
            (actual.type_code(), actual.value()),
            (wide.type_code(), wide.value())
        );
        assert_wide_math_c4(observation);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn json_merge_sdk_rejects_bad_frames_and_preserves_empty_patch_panic() {
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let mut bad_presence = 1_u64.to_le_bytes().to_vec();
        bad_presence.push(2);
        let mut empty_raw_value = 1_u64.to_le_bytes().to_vec();
        empty_raw_value.extend_from_slice(&0_u64.to_le_bytes());
        for (operation, args) in [
            (JsonMergeSerdeNative, EvaluatedArgs::Bytes(Some(Vec::new()))),
            (
                JsonMergePatchSerdeNative,
                EvaluatedArgs::Bytes(Some(bad_presence)),
            ),
            (
                JsonMergePatchRawLegacy,
                EvaluatedArgs::Bytes(Some(empty_raw_value)),
            ),
            (JsonMergePatchSerdeNative, EvaluatedArgs::NullWitness(None)),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(args),
                    EvaluatedBytesResult::into_bytes,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [JsonMergeSerdeNative, JsonMergePatchSerdeNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || if operation == JsonMergeSerdeNative {
                    super::super::prepare_json_array_args(&[])
                } else {
                    super::super::prepare_json_merge_patch_args(&[])
                },
                EvaluatedBytesResult::into_bytes,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let (result, observation) = observe_wide_math(|| crate::eval_legacy_json_merge_patch_in(&[], columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    // The old empty native PATCH indexes its empty list. Keep that real panic,
    // caught only outside the driver, rather than inventing a SQL NULL result.
    arm_eval_one_observation();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        scope.with_columns(&crate::NoColumns, |columns| {
            evaluate_args_in(
                JsonMergePatchSerdeNative,
                columns,
                || super::super::prepare_json_merge_patch_args(&[]),
                EvaluatedBytesResult::into_bytes,
            )
        })
    }));
    let observation = take_eval_one_observation();
    assert!(panic.is_err());
    assert_eq!(observation.facade_entries, 1);
    assert_eq!(observation.before_kernel_invocations, Some(0));
    assert_eq!(observation.after_kernel_invocations, None);
    assert!(scope.poisoned.get());
    assert!(scope.lease.borrow().is_none());
    let disposed = owner.snapshot().unwrap();
    assert_eq!((disposed.live, disposed.idle, disposed.retired), (0, 0, 1));
    assert_eq!(disposed.reserved_bytes, disposed.base_bytes);
    assert!(matches!(scope.evaluate_value(&Datum::Null),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopePoisoned));
    assert_eq!(owner.snapshot().unwrap(), disposed);
    drop(scope);
    execution.close();
}

#[test]
fn scoped_prepared_gateway_keeps_authority_through_nested_pack_and_cleanup() {
    struct Authority<'a> {
        scope: Option<&'a AsciiScope>,
        execution: &'a AsciiExecution,
        scope_reads: Cell<usize>,
        execution_reads: Cell<usize>,
    }
    impl Columns for Authority<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            Some(Datum::Int(77))
        }
        fn evaluated_ascii_scope(&self) -> Option<&AsciiScope> {
            let reads = self.scope_reads.replace(self.scope_reads.get() + 1);
            assert_eq!(
                reads, 0,
                "the original scope authority must not be rediscovered"
            );
            self.scope
        }
        fn evaluated_ascii_execution(&self) -> Option<&AsciiExecution> {
            let reads = self.execution_reads.replace(self.execution_reads.get() + 1);
            assert_eq!(
                reads, 0,
                "the original execution authority must not be rediscovered"
            );
            Some(self.execution)
        }
    }
    // Each route gets the same one-slot, actual computed-bytes pipeline.
    // Actions cover success, a nested frontend Err, callback unwind and an
    // epoch closed by the callback before its second-stage admission.
    for route in 0..3 {
        for action in 0..4 {
            let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
            let execution = owner.begin_execution().unwrap();
            let scope = execution.scope();
            let decoy_owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
            let decoy_execution = decoy_owner.begin_execution().unwrap();
            let authority = Authority {
                scope: (route == 1).then_some(&scope),
                execution: if route == 1 {
                    &decoy_execution
                } else {
                    &execution
                },
                scope_reads: Cell::new(0),
                execution_reads: Cell::new(0),
            };
            let columns: &dyn Columns = if route == 0 {
                &crate::NoColumns
            } else {
                &authority
            };
            let seen_execution = RefCell::new(None::<AsciiExecution>);
            let prepares = Cell::new(0);
            let packs = Cell::new(0);
            let frontend = EvalError::Unsupported("nested frontend sentinel");
            arm_eval_one_observation();
            let caught = catch_unwind(AssertUnwindSafe(|| {
                evaluate_prepared_args_scoped_in(
                    columns,
                    || {
                        prepares.set(prepares.get() + 1);
                        Ok((
                            EvaluatedBytesOp::Reverse,
                            EvaluatedArgs::Bytes(Some(b"abc".to_vec())),
                        ))
                    },
                    |computed, bound| {
                        packs.set(packs.get() + 1);
                        let head = computed.into_bytes()?;
                        assert_eq!(head.as_deref(), Some(b"cba".as_slice()));
                        let selected = bound.evaluated_ascii_scope().unwrap();
                        let selected_execution = bound.evaluated_ascii_execution().unwrap();
                        assert!(Arc::ptr_eq(
                            &selected.execution.core,
                            &selected_execution.core
                        ));
                        assert_eq!(selected.execution.epoch, selected_execution.epoch);
                        if route != 0 {
                            assert!(Arc::ptr_eq(&selected_execution.core, &execution.core));
                            assert_eq!(selected_execution.epoch, execution.epoch);
                        }
                        if route == 1 {
                            assert!(std::ptr::eq(selected, &scope));
                        }
                        assert_eq!(
                            bound.get(&[]),
                            if route == 0 {
                                None
                            } else {
                                Some(Datum::Int(77))
                            }
                        );
                        assert!(!selected.busy.get());
                        assert!(selected.lease.try_borrow_mut().is_ok());
                        *seen_execution.borrow_mut() = Some(selected_execution.clone());
                        match action {
                            0 => {
                                let first = scope_worker_observation(selected);
                                let result = evaluate_prepared_args_scoped_in(
                                    bound,
                                    || Ok((EvaluatedBytesOp::Reverse, EvaluatedArgs::Bytes(head))),
                                    |computed, nested| {
                                        assert!(std::ptr::eq(
                                            nested.evaluated_ascii_scope().unwrap(),
                                            selected
                                        ));
                                        let nested_execution =
                                            nested.evaluated_ascii_execution().unwrap();
                                        assert!(Arc::ptr_eq(
                                            &nested_execution.core,
                                            &selected_execution.core
                                        ));
                                        assert_eq!(
                                            nested_execution.epoch,
                                            selected_execution.epoch
                                        );
                                        computed.into_bytes()
                                    },
                                )?;
                                let second = scope_worker_observation(selected);
                                assert_eq!(first.0, second.0);
                                assert_eq!(second.1, first.1 + 1);
                                Ok(result)
                            }
                            1 => evaluate_prepared_args_scoped_in(
                                bound,
                                || Err(frontend.clone()),
                                |_, _| -> Result<Option<Vec<u8>>, EvalError> {
                                    panic!("failed preparation must not pack")
                                },
                            ),
                            2 => panic!("native callback unwind after the first parked result"),
                            3 => {
                                selected_execution.close();
                                evaluate_prepared_args_scoped_in(
                                    bound,
                                    || Ok((EvaluatedBytesOp::Reverse, EvaluatedArgs::Bytes(head))),
                                    |_, _| -> Result<Option<Vec<u8>>, EvalError> {
                                        panic!("closed second stage must not pack")
                                    },
                                )
                            }
                            _ => unreachable!(),
                        }
                    },
                )
            }));
            let observation = take_eval_one_observation();
            assert_eq!((prepares.get(), packs.get()), (1, 1));
            assert_eq!(
                (authority.scope_reads.get(), authority.execution_reads.get()),
                match route {
                    0 => (0, 0),
                    1 => (1, 0),
                    2 => (1, 1),
                    _ => unreachable!(),
                }
            );
            assert_eq!(observation.facade_entries, if action == 0 { 2 } else { 1 });
            assert_eq!(observation.before_kernel_invocations, Some(0));
            assert_eq!(
                observation.after_kernel_invocations,
                Some(if action == 0 { 2 } else { 1 })
            );
            match action {
                0 => assert_eq!(caught.unwrap(), Ok(Some(b"abc".to_vec()))),
                1 => assert_eq!(caught.unwrap(), Err(frontend)),
                2 => assert!(caught.is_err()),
                3 => assert!(
                    matches!(caught.unwrap(), Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == crate::ExpressionAdapterFailureClass::PoolClosed)
                ),
                _ => unreachable!(),
            }
            let selected_execution = seen_execution.into_inner().unwrap();
            let selected_owner = AsciiPoolOwner {
                core: Arc::clone(&selected_execution.core),
            };
            assert_eq!(selected_owner.snapshot().unwrap().factory_successes, 1);
            assert_eq!(
                selected_execution
                    .core
                    .check_epoch(selected_execution.epoch)
                    .is_ok(),
                route != 0 && action != 3
            );
            if route == 1 {
                assert_eq!(scope.poisoned.get(), action >= 2);
            }
            assert_eq!(decoy_owner.snapshot().unwrap().factory_attempts, 0);
            if route == 0 {
                assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
            }
            drop(scope);
            selected_execution.close();
            execution.close();
            decoy_execution.close();
            let snapshot = selected_owner.snapshot().unwrap();
            assert_eq!(
                (
                    snapshot.live,
                    snapshot.idle,
                    snapshot.creating,
                    snapshot.retiring,
                    snapshot.uncertain
                ),
                (0, 0, 0, 0, 0)
            );
            assert_eq!(snapshot.retired, 1);
            assert_eq!(snapshot.reserved_bytes, snapshot.base_bytes);
        }
    }
    // Both entry points preserve the original frontend error before any C4
    // work on the genuine no-capability route, without calling the packer.
    for scoped in [false, true] {
        let frontend = EvalError::Unsupported("prepare before one-shot allocation");
        let (result, observation) = observe_wide_math(|| {
            if scoped {
                evaluate_prepared_args_scoped_in(
                    &crate::NoColumns,
                    || Err(frontend.clone()),
                    |_, _| -> Result<Option<Vec<u8>>, EvalError> {
                        panic!("frontend error must not pack")
                    },
                )
            } else {
                evaluate_prepared_args_in(
                    &crate::NoColumns,
                    || Err(frontend.clone()),
                    |_| -> Result<Option<Vec<u8>>, EvalError> {
                        panic!("frontend error must not pack")
                    },
                )
            }
        });
        assert_eq!(result, Err(frontend));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    }
}

#[test]
fn temporal_literal_gateway_keeps_zone_binding_business_reports_and_refusals() {
    use tidb_datatype::{CoreTime, SessionTimeZone, TimeType};
    use tidb_query_expr::{decode_native_temporal_literal_result, NativeTemporalLiteralResult};
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // The same compiled worker must observe each invocation's actual zone,
        // rather than retaining the first zone in a pool key or EvalConfig.
        for (zone, expected) in [
            (SessionTimeZone::utc(), "2011-03-13 02:00:00.000000"),
            (
                SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
                "2011-03-13 03:00:00.000000",
            ),
            (SessionTimeZone::utc(), "2011-03-13 02:00:00.000000"),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    EvaluatedBytesOp::TimestampLiteralNative,
                    columns,
                    || {
                        Ok(EvaluatedArgs::TemporalText {
                            value: b"2011-03-13 01:59:59.9999999".to_vec(),
                            modes: 0,
                            zone,
                        })
                    },
                    EvaluatedBytesResult::into_bytes,
                )
            });
            let bytes = result
                .unwrap()
                .expect("a temporal literal always returns an actual report");
            match decode_native_temporal_literal_result(&bytes).unwrap() {
                NativeTemporalLiteralResult::Value(value) => {
                    assert_eq!(
                        value.kind,
                        tidb_query_datatype::codec::mysql::TimeType::DateTime
                    );
                    assert_eq!(value.fsp, 6);
                    assert_eq!(
                        Time::from_raw_parts(
                            CoreTime::from_raw(value.raw),
                            TimeType::DateTime,
                            value.fsp
                        )
                        .to_string(),
                        expected
                    );
                }
                NativeTemporalLiteralResult::WrongValue { .. } => {
                    panic!("valid timestamp must return the computed Time")
                }
            }
            assert_wide_math_c4(observation);
        }
        assert_eq!(owner.snapshot().unwrap().factory_successes, 1);
        for (operation, code, message) in [
            (
                EvaluatedBytesOp::DateLiteralNative,
                1292,
                "Incorrect date value: 'not-a-literal'",
            ),
            (
                EvaluatedBytesOp::TimestampLiteralNative,
                1525,
                "Incorrect datetime value: 'not-a-literal'",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || {
                        Ok(EvaluatedArgs::TemporalText {
                            value: b"not-a-literal".to_vec(),
                            modes: 0,
                            zone: SessionTimeZone::utc(),
                        })
                    },
                    EvaluatedBytesResult::into_bytes,
                )
            });
            let bytes = result
                .unwrap()
                .expect("business failure is a computed report, not NULL");
            match decode_native_temporal_literal_result(&bytes).unwrap() {
                NativeTemporalLiteralResult::WrongValue {
                    code: actual_code,
                    message: actual_message,
                } => {
                    assert_eq!(actual_code, code);
                    assert_eq!(actual_message, message);
                }
                NativeTemporalLiteralResult::Value(_) => {
                    panic!("invalid literal must preserve its hard error")
                }
            }
            assert_wide_math_c4(observation);
        }
        for operation in [
            EvaluatedBytesOp::DateLiteralNative,
            EvaluatedBytesOp::TimestampLiteralNative,
        ] {
            for (value, modes) in [
                (vec![0xff], 0),
                (b"2020-01-01".to_vec(), 8),
                (b"2020-01-01".to_vec(), -1),
            ] {
                let (result, observation) = observe_wide_math(|| {
                    evaluate_args_in(
                        operation,
                        columns,
                        || {
                            Ok(EvaluatedArgs::TemporalText {
                                value,
                                modes,
                                zone: SessionTimeZone::utc(),
                            })
                        },
                        EvaluatedBytesResult::into_bytes,
                    )
                });
                assert!(matches!(
                    result,
                    Err(EvalError::ExpressionRuntimeFailure(_))
                ));
                assert_eq!(observation.facade_entries, 1);
                assert_eq!(
                    observation.before_kernel_invocations,
                    observation.after_kernel_invocations
                );
            }
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [EvaluatedBytesOp::DateLiteralNative, EvaluatedBytesOp::TimestampLiteralNative] {
            for text in ["2020-01-01", "not-a-literal"] {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(operation, columns,
                    || Ok(EvaluatedArgs::TemporalText {
                        value: text.as_bytes().to_vec(), modes: 0, zone: SessionTimeZone::utc(),
                    }), EvaluatedBytesResult::into_bytes,
                ));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn legacy_microsecond_sdk_preserves_nullable_nanos_fsp_inertness_and_refusals() {
    for max_workers in [1, 0] {
        let owner = AsciiPoolOwner::new(test_policy(max_workers, max_workers)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        scope.with_columns(&crate::NoColumns, |columns| {
            for fsp in [-1, 0, 6, 7, i64::MAX] {
                for (nanos, expected) in [
                    (None, None),
                    (Some(0), Some(0)),
                    (Some(-999), Some(0)),
                    (Some(-1_001), Some(1)),
                    (Some(1_234_567_890), Some(234_567)),
                    (Some(i64::MIN), Some(854_775)),
                    (Some(i64::MAX), Some(854_775)),
                ] {
                    let value = nanos.map(|nanos| tidb_datatype::MySqlDuration::from_raw_parts(nanos, fsp).nanoseconds());
                    let (result, observation) = observe_wide_math(|| eval_legacy_microsecond_in(value, columns));
                    if max_workers == 0 {
                        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                        assert_eq!(observation.facade_entries, 0);
                        assert_eq!(observation.before_kernel_invocations, None);
                        assert_eq!(observation.after_kernel_invocations, None);
                    } else {
                        assert_eq!(result, Ok(expected));
                        assert_wide_math_c4(observation);
                        assert_eq!(scope.lease.borrow().as_ref().unwrap().worker.as_ref().unwrap().operation(), EvaluatedBytesOp::MicrosecondLegacy);
                    }
                }
            }
            if max_workers != 0 {
                // A successful signed result is not an ordinary byte result;
                // refusal after the real worker does not poison or replay it.
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    EvaluatedBytesOp::MicrosecondLegacy, columns,
                    || Ok(EvaluatedArgs::Int(Some(1_001))), EvaluatedBytesResult::into_bytes,
                ));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
                assert_wide_math_c4(observation);
            }
        });
        assert!(!scope.poisoned.get());
        if max_workers == 0 {
            assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
        }
        drop(scope);
        execution.close();
    }
}

#[test]
fn decimal_integer_div_sdk_captures_raw_precision_and_preserves_legacy_zero_order() {
    use tidb_datatype::Decimal;
    struct Precision {
        raw: [u32; 2],
        reads: Cell<usize>,
        zeros: Cell<usize>,
    }
    impl Columns for Precision {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn div_precision_increment(&self) -> u32 {
            let index = self.reads.get();
            self.reads.set(index + 1);
            self.raw[index]
        }
        fn handle_division_by_zero(&self) -> Result<(), EvalError> {
            self.zeros.set(self.zeros.get() + 1);
            Ok(())
        }
    }
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    for (left, right, unsigned, raw, reads, expected) in [
        ("7", "2", false, [0, 99], 1, Datum::Int(3)),
        ("7", "2", false, [31, 4], 2, Datum::Int(3)),
        ("7.0000", "2", false, [4, 99], 1, Datum::Int(3)),
        (
            "18446744073709551615",
            "1",
            true,
            [4, 99],
            1,
            Datum::UInt(u64::MAX),
        ),
        ("-0.9", "1", true, [4, 99], 1, Datum::UInt(0)),
        ("7", "0", false, [u32::MAX, u32::MAX], 0, Datum::Null),
    ] {
        let ctx = Precision {
            raw,
            reads: Cell::new(0),
            zeros: Cell::new(0),
        };
        scope.with_columns(&ctx, |columns| {
            let (result, observation) = observe_wide_math(|| {
                eval_decimal_integer_division_in(
                    &Decimal::from_literal(left),
                    &Decimal::from_literal(right),
                    unsigned,
                    columns,
                )
            });
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        });
        assert_eq!(ctx.reads.get(), reads);
        assert_eq!(ctx.zeros.get(), usize::from(right == "0"));
    }
    let ctx = Precision {
        raw: [0, 0],
        reads: Cell::new(0),
        zeros: Cell::new(0),
    };
    scope.with_columns(&ctx, |columns| {
        for (left, right, expected) in [
            ("-7.9", "2", Some(-3)),
            ("9223372036854775807", "1", Some(i128::from(i64::MAX))),
            ("-9223372036854775808", "1", Some(i128::from(i64::MIN))),
            ("9223372036854775808", "1", None),
        ] {
            let (result, observation) = observe_wide_math(|| {
                eval_legacy_decimal_integer_division_in(
                    LegacyBinaryArgs::Values(
                        Decimal::from_literal(left),
                        Decimal::from_literal(right),
                    ),
                    columns,
                )
            });
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        for coefficient in [Vec::new(), vec![0xff]] {
            for zero in [Vec::new(), b"000".to_vec()] {
                let (result, observation) = observe_wide_math(|| {
                    eval_legacy_decimal_integer_division_in(
                        LegacyBinaryArgs::Values(
                            Decimal::from_raw_parts(false, coefficient.clone(), 0, 0),
                            Decimal::from_raw_parts(false, zero, 0, 0),
                        ),
                        columns,
                    )
                });
                assert_eq!(result, Ok(None));
                assert_wide_math_c4(observation);
                assert_eq!(
                    scope
                        .lease
                        .borrow()
                        .as_ref()
                        .unwrap()
                        .worker
                        .as_ref()
                        .unwrap()
                        .operation(),
                    EvaluatedBytesOp::IntDivDecimalLegacy
                );
            }
        }
        // Nonzero arithmetic still has the documented shared-math input domain;
        // invalid raw storage is an actual failure, never a substituted zero.
        let (result, observation) = observe_wide_math(|| {
            eval_legacy_decimal_integer_division_in(
                LegacyBinaryArgs::Values(
                    Decimal::from_raw_parts(false, Vec::new(), 0, 0),
                    Decimal::from_literal("1"),
                ),
                columns,
            )
        });
        assert!(matches!(
            result,
            Err(EvalError::ExpressionRuntimeFailure(_))
        ));
        assert_wide_math_c4(observation);
        for (args, operation) in [
            (
                LegacyBinaryArgs::Missing,
                EvaluatedBytesOp::BinaryArithmeticMissingLegacy,
            ),
            (
                LegacyBinaryArgs::NullWitness(None),
                EvaluatedBytesOp::BinaryArithmeticNullNative,
            ),
        ] {
            let (result, observation) =
                observe_wide_math(|| eval_legacy_decimal_integer_division_in(args, columns));
            assert_eq!(result, Ok(None));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
    });
    assert_eq!(ctx.reads.get(), 0);
    assert_eq!(ctx.zeros.get(), 0);
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn decimal_integer_div_sdk_refuses_false_presence_budget_and_invalid_reports() {
    use tidb_datatype::Decimal;
    struct Observed {
        reads: Cell<usize>,
        diagnostics: Cell<usize>,
    }
    impl Columns for Observed {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn div_precision_increment(&self) -> u32 {
            self.reads.set(self.reads.get() + 1);
            4
        }
        fn handle_division_by_zero(&self) -> Result<(), EvalError> {
            self.diagnostics.set(self.diagnostics.get() + 1);
            Ok(())
        }
        fn handle_truncate(&self, _: &str) -> Result<(), EvalError> {
            self.diagnostics.set(self.diagnostics.get() + 1);
            Ok(())
        }
    }
    let ctx = Observed {
        reads: Cell::new(0),
        diagnostics: Cell::new(0),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&ctx, |columns| {
        for operation in [EvaluatedBytesOp::IntDivDecimalSignedNative, EvaluatedBytesOp::IntDivDecimalUnsignedNative] {
            let frame = || Some(encode_decimal_intdiv_operand(decimal_intdiv_view(&Decimal::from_literal("1"))).unwrap());
            let (result, observation) = observe_wide_math(|| evaluate_args_in(operation, columns,
                || Ok(EvaluatedArgs::BytesIntIntBytes(frame(), None, None, frame())),
                |computed| finish_decimal_integer_division(computed, false, columns),
            ));
            assert!(matches!(result, Err(EvalError::ExpressionRuntimeFailure(_))));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(observation.before_kernel_invocations, observation.after_kernel_invocations);
        }
        let (result, observation) = observe_wide_math(|| eval_legacy_decimal_integer_division_in(LegacyBinaryArgs::NullWitness(Some(0)), columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert_eq!(observation.facade_entries, 0);
        for computed in [EvaluatedBytesResult::Bytes(None), EvaluatedBytesResult::Bytes(Some(Vec::new())),
            EvaluatedBytesResult::Bytes(Some(vec![0xff])), EvaluatedBytesResult::Int(Datum::Null)] {
            assert!(matches!(finish_decimal_integer_division(computed, false, columns),
                Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        }
    });
    assert_eq!(ctx.reads.get(), 0);
    assert_eq!(ctx.diagnostics.get(), 0);
    drop(scope);
    execution.close();

    for policy in [
        test_policy(0, 0),
        AsciiPoolPolicy::checked(
            1,
            1,
            TEST_POOL_BYTES,
            TEST_WORKER_CAP,
            TEST_CREATION_RESERVATION,
            64,
            8,
            0,
        )
        .unwrap(),
    ] {
        let no_slots = policy.max_workers == 0;
        let owner = AsciiPoolOwner::new(policy).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        scope.with_columns(&ctx, |columns| {
            for unsigned in [false, true] {
                let (result, observation) = observe_wide_math(|| eval_decimal_integer_division_in(
                    &Decimal::from_literal("7"), &Decimal::from_literal("2"), unsigned, columns,
                ));
                if no_slots {
                    assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                    assert_eq!(observation.facade_entries, 0);
                } else {
                    assert!(matches!(result, Err(EvalError::ExpressionRuntimeFailure(_))));
                    assert_eq!(observation.before_kernel_invocations, observation.after_kernel_invocations);
                }
            }
            let (result, observation) = observe_wide_math(|| eval_legacy_decimal_integer_division_in(
                LegacyBinaryArgs::Values(Decimal::from_raw_parts(false, vec![0xff], 0, 0), Decimal::from_literal("0")), columns,
            ));
            if no_slots {
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                assert_eq!(observation.facade_entries, 0);
            } else {
                assert!(matches!(result, Err(EvalError::ExpressionRuntimeFailure(_))));
                assert_eq!(observation.before_kernel_invocations, observation.after_kernel_invocations);
            }
        });
        assert!(!scope.poisoned.get());
        drop(scope);
        execution.close();
    }
    assert_eq!(ctx.reads.get(), 4);
    assert_eq!(ctx.diagnostics.get(), 0);

    // Raw RHS UTF-8 is demanded by the original zero predicate before any
    // precision read, admission or worker call, under the native scope guard.
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    arm_eval_one_observation();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        scope.with_columns(&ctx, |columns| {
            eval_decimal_integer_division_in(
                &Decimal::from_literal("1"),
                &Decimal::from_raw_parts(false, vec![0xff], 0, 0),
                false,
                columns,
            )
        })
    }));
    let observation = take_eval_one_observation();
    assert!(panic.is_err());
    assert_eq!(ctx.reads.get(), 4);
    assert_eq!(observation.facade_entries, 0);
    assert_eq!(observation.before_kernel_invocations, None);
    assert_eq!(observation.after_kernel_invocations, None);
    assert!(scope.poisoned.get());
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn integer_div_sdk_preserves_signed_pair_policy_and_full_legacy_quotients() {
    use EvaluatedBytesOp::*;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, left, right, expected) in [
            (IntDivIntSsNative, -13, 11, Ok(Datum::Int(-1))),
            (IntDivIntSsNative, i64::MIN, -1, Err(EvalError::IntOverflow)),
            (IntDivIntUsNative, -1, 1, Ok(Datum::UInt(u64::MAX))),
            (IntDivIntUsNative, 1, -2, Ok(Datum::UInt(0))),
            (IntDivIntUsNative, 13, -11, Err(EvalError::IntOverflow)),
            (IntDivIntSuNative, -1, 2, Ok(Datum::UInt(0))),
            (IntDivIntSuNative, -13, 11, Err(EvalError::IntOverflow)),
            (IntDivIntUuNative, -1, 1, Ok(Datum::UInt(u64::MAX))),
        ] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns, || Ok(EvaluatedArgs::Int2(Some(left), Some(right))),
                |computed| if operation == IntDivIntSsNative { computed.into_int_datum() } else { computed.into_uint_bits_datum() },
            ));
            assert_eq!(result, expected);
            assert_wide_math_c4(observation);
        }
        for operation in [IntDivIntSsNative, IntDivIntUsNative, IntDivIntSuNative, IntDivIntUuNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns, || Ok(EvaluatedArgs::Int2(Some(i64::MIN), Some(0))),
                |computed| if operation == IntDivIntSsNative { computed.into_int_datum() } else { computed.into_uint_bits_datum() },
            ));
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
            assert_eq!(scope.lease.borrow().as_ref().unwrap().worker.as_ref().unwrap().operation(), operation);
        }
        for (args, expected, operation) in [
            (LegacyBinaryArgs::Values(i128::MAX, 1), Some(i128::MAX), IntDivInt128Legacy),
            (LegacyBinaryArgs::Values(i128::MIN, 1), Some(i128::MIN), IntDivInt128Legacy),
            (LegacyBinaryArgs::Values(-13, 11), Some(-1), IntDivInt128Legacy),
            (LegacyBinaryArgs::Values(i128::MAX, 0), None, IntDivInt128Legacy),
            (LegacyBinaryArgs::Missing, None, BinaryArithmeticMissingLegacy),
            (LegacyBinaryArgs::NullWitness(None), None, BinaryArithmeticNullNative),
        ] {
            let (result, observation) = observe_wide_math(|| eval_legacy_integer_arithmetic_in(LegacyIntegerArithmetic::IntDivide, args, columns));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
            assert_eq!(scope.lease.borrow().as_ref().unwrap().worker.as_ref().unwrap().operation(), operation);
        }
        // The new operation identity cannot silently select old REAL, decimal,
        // or fast-decimal '/' policies through their general arithmetic SDKs.
        assert!(matches!(eval_legacy_real_arithmetic_in(BinaryArithmeticOperation::IntDivide,
            LegacyBinaryArgs::Values(1.0, 1.0), columns),
            Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert!(matches!(eval_legacy_decimal_arithmetic_in(BinaryArithmeticOperation::IntDivide,
            LegacyBinaryArgs::Values(crate::Decimal::from_literal("1"), crate::Decimal::from_literal("1")), columns),
            Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert!(matches!(eval_arithmetic_decimal_fast_in(BinaryArithmeticOperation::IntDivide, None, None, columns),
            Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn integer_div_sdk_keeps_refusals_and_legacy_overflow_panic_retirement() {
    use EvaluatedBytesOp::*;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [IntDivIntSsNative, IntDivIntUsNative, IntDivIntSuNative, IntDivIntUuNative, IntDivInt128Legacy] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || Ok(if operation == IntDivInt128Legacy {
                    EvaluatedArgs::Int1282(None, Some(1))
                } else {
                    EvaluatedArgs::Int2(None, Some(1))
                }),
                |_| -> Result<Datum, EvalError> { panic!("refused operand presence must not reach packing") },
            ));
            assert!(matches!(result, Err(EvalError::ExpressionRuntimeFailure(_))));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(observation.before_kernel_invocations, observation.after_kernel_invocations);
        }
        assert!(matches!(eval_legacy_integer_arithmetic_in(LegacyIntegerArithmetic::IntDivide,
            LegacyBinaryArgs::NullWitness(Some(0)), columns),
            Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [IntDivIntSsNative, IntDivIntUsNative, IntDivIntSuNative, IntDivIntUuNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns, || Ok(EvaluatedArgs::Int2(Some(i64::MIN), Some(-1))),
                EvaluatedBytesResult::into_int_datum,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for args in [LegacyBinaryArgs::Values(i128::MIN, -1), LegacyBinaryArgs::Values(1, 0), LegacyBinaryArgs::Missing, LegacyBinaryArgs::NullWitness(None)] {
            let (result, observation) = observe_wide_math(|| eval_legacy_integer_arithmetic_in(LegacyIntegerArithmetic::IntDivide, args, columns));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    arm_eval_one_observation();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        scope.with_columns(&crate::NoColumns, |columns| {
            eval_legacy_integer_arithmetic_in(
                LegacyIntegerArithmetic::IntDivide,
                LegacyBinaryArgs::Values(i128::MIN, -1),
                columns,
            )
        })
    }));
    let observation = take_eval_one_observation();
    assert!(panic.is_err());
    assert_eq!(observation.facade_entries, 1);
    assert_eq!(observation.before_kernel_invocations, Some(0));
    assert_eq!(observation.after_kernel_invocations, None);
    assert!(scope.poisoned.get());
    assert!(scope.lease.borrow().is_none());
    let disposed = owner.snapshot().unwrap();
    assert_eq!((disposed.live, disposed.idle, disposed.retired), (0, 0, 1));
    assert_eq!(disposed.reserved_bytes, disposed.base_bytes);
    assert!(matches!(scope.evaluate_value(&Datum::Null),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopePoisoned));
    assert_eq!(owner.snapshot().unwrap(), disposed);
    drop(scope);
    execution.close();
}

#[test]
fn tso_timediff_sdk_preserves_actual_offsets_nullable_demand_and_computed_outputs() {
    use tidb_datatype::{CoreTime, Time, TimeType};
    use EvaluatedBytesOp::*;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (tso, offset, expected) in [
            (None, None, None),
            (Some(0), None, None),
            (Some(i64::MIN), None, None),
            (
                Some(1),
                Some(0),
                Some(CoreTime::from_date(1970, 1, 1, 0, 0, 0, 0)),
            ),
            (
                Some(1001_i64 << 18),
                Some(0),
                Some(CoreTime::from_date(1970, 1, 1, 0, 0, 1, 1000)),
            ),
            (
                Some(1_i64 << 18),
                Some(-1),
                Some(CoreTime::from_date(1969, 12, 31, 23, 59, 59, 1000)),
            ),
            (
                Some(1),
                Some(i32::MAX),
                Some(CoreTime::from_date(2038, 1, 19, 3, 14, 7, 0)),
            ),
            (
                Some(1),
                Some(i32::MIN),
                Some(CoreTime::from_date(1901, 12, 13, 20, 45, 52, 0)),
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    TidbParseTsoNative,
                    columns,
                    || Ok(EvaluatedArgs::Int2(tso, offset.map(i64::from))),
                    EvaluatedBytesResult::into_identity_datum,
                )
            });
            let expected = expected.map_or(Datum::Null, |core| {
                Datum::Time(Time::new(core, TimeType::DateTime, 6).unwrap())
            });
            assert_identity_datum_bits(&result.unwrap(), &expected);
            assert_wide_math_c4(observation);
            assert_eq!(owner.snapshot().unwrap().factory_successes, 1);
        }
        for (left, right, expected) in [
            (None, None, None),
            (Some("not-a-time"), None, None),
            (Some("  "), None, None),
            (Some("01:00:00"), None, None),
            (Some("01:00:00"), Some("bad"), None),
            (Some("2000-01-01"), Some("01:00:00"), None),
            (
                Some("10:00:00.100"),
                Some("01:02:03.4"),
                Some("08:57:56.700"),
            ),
            (Some("-10:00:00"), Some("01:00:00"), Some("-11:00:00")),
            (
                Some("00:00:00"),
                Some("00:00:00.000001"),
                Some("-00:00:00.000001"),
            ),
            (
                Some("900:00:00.123"),
                Some("00:00:00"),
                Some("838:59:59.000"),
            ),
            (
                Some("2000-01-01 00:00:00.1"),
                Some("1999-12-31 23:59:59.09"),
                Some("00:00:01.01"),
            ),
            (Some("2001-00-02"), Some("2001-00-01"), Some("24:00:00")),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    TimeDiffTextNative,
                    columns,
                    || {
                        Ok(EvaluatedArgs::Bytes2(
                            left.map(|s| s.as_bytes().to_vec()),
                            right.map(|s| s.as_bytes().to_vec()),
                        ))
                    },
                    |computed| {
                        Ok(computed
                            .into_bytes()?
                            .map_or(Datum::Null, Datum::new_string))
                    },
                )
            });
            assert_eq!(result, Ok(expected.map_or(Datum::Null, Datum::new_string)));
            assert_wide_math_c4(observation);
            assert_eq!(owner.snapshot().unwrap().factory_successes, 2);
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn tso_timediff_sdk_rejects_false_presence_and_preserves_preparation_and_zero_slots() {
    use EvaluatedBytesOp::*;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (TidbParseTsoNative, EvaluatedArgs::Int2(None, Some(0))),
            (TidbParseTsoNative, EvaluatedArgs::Int2(Some(0), Some(0))),
            (TidbParseTsoNative, EvaluatedArgs::Int2(Some(1), None)),
            (
                TidbParseTsoNative,
                EvaluatedArgs::Int2(Some(1), Some(i64::from(i32::MAX) + 1)),
            ),
            (
                TidbParseTsoNative,
                EvaluatedArgs::Int2(Some(1), Some(i64::from(i32::MIN) - 1)),
            ),
            (TidbParseTsoNative, EvaluatedArgs::Bytes(None)),
            (
                TimeDiffTextNative,
                EvaluatedArgs::Bytes2(None, Some(b"01:00:00".to_vec())),
            ),
            (
                TimeDiffTextNative,
                EvaluatedArgs::Bytes2(Some(b"bad".to_vec()), Some(b"01:00:00".to_vec())),
            ),
            (
                TimeDiffTextNative,
                EvaluatedArgs::Bytes2(Some(vec![0xff]), None),
            ),
            (
                TimeDiffTextNative,
                EvaluatedArgs::Bytes2(Some(b"01:00:00".to_vec()), Some(vec![0xff])),
            ),
            (TimeDiffTextNative, EvaluatedArgs::Int2(None, None)),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(args),
                    EvaluatedBytesResult::into_bytes,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [TidbParseTsoNative, TimeDiffTextNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns, || Err(EvalError::Unsupported("original input preparation failed")),
                EvaluatedBytesResult::into_bytes,
            ));
            assert_eq!(result, Err(EvalError::Unsupported("original input preparation failed")));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (operation, args) in [
            (TidbParseTsoNative, EvaluatedArgs::Int2(None, None)),
            (TidbParseTsoNative, EvaluatedArgs::Int2(Some(-1), None)),
            (TidbParseTsoNative, EvaluatedArgs::Int2(Some(1), Some(i64::from(i32::MAX)))),
            (TimeDiffTextNative, EvaluatedArgs::Bytes2(None, None)),
            (TimeDiffTextNative, EvaluatedArgs::Bytes2(Some(b"bad".to_vec()), None)),
            (TimeDiffTextNative, EvaluatedArgs::Bytes2(Some(b"01:00:00".to_vec()), None)),
            (TimeDiffTextNative, EvaluatedArgs::Bytes2(Some(b"01:00:00".to_vec()), Some(b"00:00:00".to_vec()))),
        ] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(operation, columns, || Ok(args), EvaluatedBytesResult::into_bytes));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

fn assert_identity_datum_bits(actual: &Datum, expected: &Datum) {
    use Datum::*;
    assert_eq!(actual.kind(), expected.kind());
    match (actual, expected) {
        (Null, Null) | (MinNotNull, MinNotNull) | (MaxValue, MaxValue) => {}
        (Int(a), Int(b)) => assert_eq!(a, b),
        (UInt(a), UInt(b)) => assert_eq!(a, b),
        (Real(a), Real(b)) | (Float32(a), Float32(b)) => assert_eq!(a.to_bits(), b.to_bits()),
        (Decimal(a), Decimal(b)) => {
            assert_eq!(a.coefficient_bytes(), b.coefficient_bytes());
            assert_eq!(
                (
                    a.is_negative(),
                    a.scale(),
                    a.storage_scale(),
                    a.declared_shape()
                ),
                (
                    b.is_negative(),
                    b.scale(),
                    b.storage_scale(),
                    b.declared_shape()
                )
            );
        }
        (String(a), String(b)) => {
            assert_eq!(a.bytes(), b.bytes());
            assert_eq!(a.collation(), b.collation());
        }
        (Bytes(a), Bytes(b)) | (Raw(a), Raw(b)) => assert_eq!(a, b),
        (BinaryLiteral(a), BinaryLiteral(b)) | (Bit(a), Bit(b)) => {
            assert_eq!(a.as_bytes(), b.as_bytes())
        }
        (Duration(a), Duration(b)) => {
            assert_eq!((a.nanoseconds(), a.fsp()), (b.nanoseconds(), b.fsp()))
        }
        (Enum(a, ac), Enum(b, bc)) => {
            assert_eq!(
                (a.name_bytes(), a.value(), ac),
                (b.name_bytes(), b.value(), bc)
            );
        }
        (Set(a, ac), Set(b, bc)) => {
            assert_eq!(
                (a.name_bytes(), a.value(), ac),
                (b.name_bytes(), b.value(), bc)
            );
        }
        (Time(a), Time(b)) => assert_eq!(
            (a.core_time().raw(), a.kind(), a.fsp()),
            (b.core_time().raw(), b.kind(), b.fsp())
        ),
        (Json(a), Json(b)) => assert_eq!((a.type_code(), a.value()), (b.type_code(), b.value())),
        (VectorFloat32(a), VectorFloat32(b)) => {
            assert_eq!(a.len(), b.len());
            for (left, right) in a.elements().iter().zip(b.elements()) {
                assert_eq!(left.to_bits(), right.to_bits());
            }
        }
        _ => panic!("identity changed the selected Datum kind"),
    }
}

#[test]
fn datum_identity_sdk_returns_all_original_value_bits_through_both_workers() {
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, MySqlDuration, MysqlEnum,
        MysqlSet, StringDatum, Time, TimeType, VectorFloat32,
    };
    let mut vector = VectorFloat32::init(tidb_datatype::MAX_VECTOR_DIMENSION + 1);
    vector.elements_mut()[0] = f32::from_bits(0x7fc0_1234);
    vector.elements_mut()[1] = -0.0;
    vector.elements_mut()[2] = f32::INFINITY;
    let values = vec![
        Datum::Null,
        Datum::MinNotNull,
        Datum::MaxValue,
        Datum::Int(i64::MIN),
        Datum::UInt(u64::MAX),
        Datum::Decimal(
            Decimal::from_raw_parts(true, b"0000123456".to_vec(), 2, 9)
                .with_declared_shape(i64::MIN, i64::MAX),
        ),
        Datum::Real(f64::from_bits(0x7ff8_0000_0000_1234)),
        Datum::Float32(f64::from_bits(0x3ff0_0000_0000_0001)),
        Datum::String(StringDatum::new(
            vec![0xff, 0, b'a'],
            Collation::Utf8Mb4GeneralCi,
        )),
        Datum::Bytes(vec![0, 0xff]),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![0, 0x80, 0xff])),
        Datum::Duration(MySqlDuration::from_raw_parts(i64::MIN, i64::MAX)),
        Datum::Enum(
            MysqlEnum::new(vec![0xff, 0, b'e'], u64::MAX),
            Collation::Utf8Mb4Bin,
        ),
        Datum::Bit(BinaryLiteral::from(vec![0, 0, 0x80])),
        Datum::Set(
            MysqlSet::new(vec![0xfe, b',', 0], u64::MAX),
            Collation::Binary,
        ),
        Datum::Time(Time::from_raw_parts(
            CoreTime::from_raw(u64::MAX),
            TimeType::Timestamp,
            7,
        )),
        Datum::Json(BinaryJSON::from_encoded_parts(0xff, vec![0, 0xfe])),
        Datum::Raw(vec![0xff, 0, 0x80]),
        Datum::VectorFloat32(vector),
        Datum::Real(-0.0),
        Datum::Float32(f64::from_bits(0xfff8_0000_0000_5678)),
        Datum::Decimal(Decimal::from_raw_parts(
            true,
            vec![0xff, 0, b'0'],
            u32::MAX,
            0,
        )),
        Datum::Time(Time::from_raw_parts(
            CoreTime::from_raw(0x0123_4567_89ab_cdef),
            TimeType::Date,
            0,
        )),
    ];
    // Keep the beyond-SQL-dimension vector within a deliberately adequate call
    // budget; this proves representation identity, not a waived resource gate.
    let policy = AsciiPoolPolicy::checked(
        1,
        1,
        TEST_POOL_BYTES,
        TEST_WORKER_CAP,
        TEST_CREATION_RESERVATION,
        64,
        8,
        8 * TEST_CALL_BYTES,
    )
    .unwrap();
    let owner = AsciiPoolOwner::new(policy).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (index, operation) in [
            EvaluatedBytesOp::AnyValueNative,
            EvaluatedBytesOp::NameConstNative,
        ]
        .into_iter()
        .enumerate()
        {
            for value in &values {
                let (result, observation) = observe_wide_math(|| {
                    evaluate_args_in(
                        operation,
                        columns,
                        || super::super::prepare_datum_identity_args(value),
                        EvaluatedBytesResult::into_identity_datum,
                    )
                });
                assert_identity_datum_bits(&result.unwrap(), value);
                assert_wide_math_c4(observation);
                assert_eq!(
                    scope
                        .lease
                        .borrow()
                        .as_ref()
                        .unwrap()
                        .worker
                        .as_ref()
                        .unwrap()
                        .operation(),
                    operation
                );
                assert_eq!(
                    owner.snapshot().unwrap().factory_successes,
                    (index + 1) as u64
                );
            }
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [EvaluatedBytesOp::AnyValueNative, EvaluatedBytesOp::NameConstNative] {
            for value in &values {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    operation, columns, || super::super::prepare_datum_identity_args(value),
                    EvaluatedBytesResult::into_identity_datum,
                ));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn datum_identity_sdk_rejects_malformed_present_frames_without_null_fallbacks() {
    use tidb_query_expr::{encode_native_identity, NativeIdentityRef};
    let mut bad_shape_presence = encode_native_identity(NativeIdentityRef::Decimal {
        negative: false,
        scale: 0,
        storage_scale: 0,
        declared_shape: Some((1, 0)),
        coefficient: b"0",
    })
    .unwrap();
    // Shared codec schema: byte 10 is the shape-presence bit. Clearing it
    // while retaining nonzero shape fields violates canonical physical absence.
    bad_shape_presence[10] = 0;
    let mut bad_sign = encode_native_identity(NativeIdentityRef::Decimal {
        negative: false,
        scale: 0,
        storage_scale: 0,
        declared_shape: None,
        coefficient: b"0",
    })
    .unwrap();
    bad_sign[1] = 2;
    let frames = vec![
        Vec::new(),
        vec![0],
        vec![19],
        vec![255],
        vec![1, 0],
        vec![2, 0],  // sentinels have no payload
        vec![3, 0],  // missing seven actual integer bytes
        vec![8, 16], // invalid collation representation
        vec![18, 0], // partial f32 word, not a NULL vector
        bad_shape_presence,
        bad_sign,
    ];
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [
            EvaluatedBytesOp::AnyValueNative,
            EvaluatedBytesOp::NameConstNative,
        ] {
            for frame in &frames {
                let (result, observation) = observe_wide_math(|| {
                    evaluate_args_in(
                        operation,
                        columns,
                        || Ok(EvaluatedArgs::Bytes(Some(frame.clone()))),
                        EvaluatedBytesResult::into_identity_datum,
                    )
                });
                assert!(matches!(
                    result,
                    Err(EvalError::ExpressionRuntimeFailure(_))
                ));
                assert_eq!(observation.facade_entries, 1);
                assert_eq!(
                    observation.before_kernel_invocations,
                    observation.after_kernel_invocations
                );
            }
            // Real SQL NULL is the worker's physical nullable input and output,
            // and remains distinct from every malformed present frame above.
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || super::super::prepare_datum_identity_args(&Datum::Null),
                    EvaluatedBytesResult::into_identity_datum,
                )
            });
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
    });
    for frame in frames {
        assert!(
            matches!(EvaluatedBytesResult::Bytes(Some(frame)).into_identity_datum(),
            Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
        );
    }
    assert!(
        matches!(EvaluatedBytesResult::Int(Datum::Null).into_identity_datum(),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
    );
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn weight_format_sdk_preserves_original_metadata_and_nullable_locale_outputs() {
    use EvaluatedBytesOp::*;
    let numeric_code = i64::from(tidb_datatype::FieldTypeCode::LongLong.mysql_type());
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (new_mode, expected) in [(true, vec![0, 65]), (false, vec![b'A'])] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    WeightStringNative,
                    columns,
                    || super::super::prepare_weight_string_args(b"A".to_vec(), 7, new_mode),
                    |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::new_bytes)),
                )
            });
            assert_eq!(result, Ok(Datum::new_bytes(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(owner.snapshot().unwrap().factory_successes, 1);
        }
        for (operation, args, expected) in [
            (
                WeightStringCharNative,
                super::super::prepare_weight_padded_args(
                    b"ab".to_vec(),
                    4,
                    Some(u64::MAX),
                    6,
                    Some(true),
                )
                .unwrap(),
                Some(b"ab".to_vec()),
            ),
            (
                WeightStringCharNative,
                super::super::prepare_weight_padded_args(
                    "中文".as_bytes().to_vec(),
                    1,
                    None,
                    0,
                    Some(true),
                )
                .unwrap(),
                Some("中".as_bytes().to_vec()),
            ),
            (
                WeightStringCharNative,
                super::super::prepare_weight_padded_args(
                    b"ab".to_vec(),
                    i64::MIN,
                    None,
                    0,
                    Some(false),
                )
                .unwrap(),
                Some(Vec::new()),
            ),
            // Original stub-collation metadata is retained: BINARY selects its
            // own key policy, and packet overflow never demands any key policy.
            (
                WeightStringBinaryNative,
                super::super::prepare_weight_padded_args(
                    b"ab".to_vec(),
                    4,
                    Some(2),
                    11,
                    Some(true),
                )
                .unwrap(),
                Some(vec![b'a', b'b', 0, 0]),
            ),
            (
                WeightStringBinaryNative,
                super::super::prepare_weight_padded_args(b"ab".to_vec(), 1, None, 11, Some(true))
                    .unwrap(),
                Some(vec![b'a']),
            ),
            (
                WeightStringCharNative,
                super::super::prepare_weight_padded_args(b"ab".to_vec(), 4, Some(1), 11, None)
                    .unwrap(),
                None,
            ),
            (
                WeightStringBinaryNative,
                super::super::prepare_weight_padded_args(b"ab".to_vec(), 4, Some(1), 11, None)
                    .unwrap(),
                None,
            ),
            (
                WeightStringNumericNative,
                EvaluatedArgs::Int(Some(numeric_code)),
                None,
            ),
            (GetFormatNullNative, EvaluatedArgs::Bytes(None), None),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(args),
                    |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::new_bytes)),
                )
            });
            assert_eq!(result, Ok(expected.map_or(Datum::Null, Datum::new_bytes)));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        for (number, locale, precision, expected) in [
            ("1234.5", None, 2, "1,234.50"),
            ("1234.5", Some("de_DE"), 2, "1.234,50"),
            ("1234.5", Some("unknown"), i64::MIN, "1,235"),
            (
                "1",
                Some("en_US"),
                i64::MAX,
                "1.000000000000000000000000000000",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    FormatLocaleNative,
                    columns,
                    || {
                        Ok(EvaluatedArgs::BytesBytesInt(
                            Some(number.as_bytes().to_vec()),
                            locale.map(|value| value.as_bytes().to_vec()),
                            Some(precision),
                        ))
                    },
                    |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::new_bytes)),
                )
            });
            assert_eq!(result, Ok(Datum::new_bytes(expected.as_bytes().to_vec())));
            assert_wide_math_c4(observation);
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn weight_format_sdk_rejects_bad_metadata_and_zero_slot_fallbacks() {
    use EvaluatedBytesOp::*;
    let EvaluatedArgs::Bytes2(_, Some(metadata)) =
        super::super::prepare_weight_padded_args(Vec::new(), i64::MIN, None, 15, None).unwrap()
    else {
        panic!("padded weights require actual byte metadata");
    };
    assert_eq!(metadata.len(), 19);
    assert_eq!(&metadata[..8], &i64::MIN.to_le_bytes());
    assert_eq!(&metadata[8..17], &[0; 9]);
    assert_eq!(&metadata[17..], &[15, 2]);
    for tag in [-1, 16, 256, i64::MAX] {
        assert!(
            matches!(super::super::prepare_weight_string_args(Vec::new(), tag, true),
            Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
        );
        assert!(
            matches!(super::super::prepare_weight_padded_args(Vec::new(), 0, Some(0), tag, Some(true)),
            Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
        );
    }
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (
                WeightStringNative,
                EvaluatedArgs::Bytes2(Some(Vec::new()), Some(vec![0, 2])),
            ),
            (
                WeightStringCharNative,
                super::super::prepare_weight_padded_args(b"ab".to_vec(), 4, None, 0, Some(true))
                    .unwrap(),
            ),
            (
                WeightStringBinaryNative,
                super::super::prepare_weight_padded_args(b"ab".to_vec(), 1, Some(0), 0, Some(true))
                    .unwrap(),
            ),
            (
                WeightStringCharNative,
                super::super::prepare_weight_padded_args(b"ab".to_vec(), 4, Some(1), 0, Some(true))
                    .unwrap(),
            ),
            (
                WeightStringBinaryNative,
                super::super::prepare_weight_padded_args(b"ab".to_vec(), 1, None, 0, None).unwrap(),
            ),
            (WeightStringNumericNative, EvaluatedArgs::Int(None)),
            (
                WeightStringNumericNative,
                EvaluatedArgs::Int(Some(i64::from(
                    tidb_datatype::FieldTypeCode::VarString.mysql_type(),
                ))),
            ),
            (
                FormatLocaleNative,
                EvaluatedArgs::BytesBytesInt(Some(vec![0xff]), None, Some(0)),
            ),
            (
                FormatLocaleNative,
                EvaluatedArgs::BytesBytesInt(Some(vec![b'1']), Some(vec![0xff]), Some(0)),
            ),
            (
                FormatLocaleNative,
                EvaluatedArgs::BytesBytesInt(Some(vec![b'1']), None, None),
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(args),
                    EvaluatedBytesResult::into_bytes,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (WeightStringNative, super::super::prepare_weight_string_args(b"a".to_vec(), 0, true).unwrap()),
            (WeightStringCharNative, super::super::prepare_weight_padded_args(b"a".to_vec(), 2, Some(0), 0, None).unwrap()),
            (WeightStringBinaryNative, super::super::prepare_weight_padded_args(b"a".to_vec(), 2, Some(1), 0, Some(true)).unwrap()),
            (WeightStringNumericNative, EvaluatedArgs::Int(Some(i64::from(tidb_datatype::FieldTypeCode::LongLong.mysql_type())))),
            (FormatLocaleNative, EvaluatedArgs::BytesBytesInt(Some(vec![b'1']), None, Some(0))),
            (GetFormatNullNative, EvaluatedArgs::Bytes(None)),
        ] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(operation, columns, || Ok(args), EvaluatedBytesResult::into_bytes));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn date_core_sdk_preserves_computed_bits_modes_nullable_predicate_and_refusals() {
    use tidb_datatype::{CoreTime, DateModes, TimeType};
    let modes = |no_zero_date, no_zero_in_date, allow_invalid_dates| DateModes {
        no_zero_date,
        no_zero_in_date,
        allow_invalid_dates,
    };
    let high = CoreTime::from_date(9000, 1, 2, 23, 59, 59, 123456).raw() | 15;
    let midnight = CoreTime::from_date(9000, 1, 2, 0, 0, 0, 0).raw();
    assert_ne!(high & (1_u64 << 63), 0);
    let EvaluatedArgs::BytesInt(Some(bytes), Some(flags)) =
        super::super::prepare_date_args(high, modes(true, true, true)).unwrap()
    else {
        panic!("DATE requires its actual core and three mode bits");
    };
    assert_eq!(bytes, high.to_le_bytes().to_vec());
    assert_eq!(flags, 7);
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (core, mode, expected) in [
            (high, modes(true, true, true), Some(midnight)),
            (0, modes(false, false, false), Some(0)),
            (0, modes(true, false, false), None),
            // The original reserved bit makes this nonzero before DATE projection.
            (1, modes(true, false, true), Some(0)),
            (1, modes(false, true, true), None),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    EvaluatedBytesOp::DateCoreNative,
                    columns,
                    || super::super::prepare_date_args(core, mode),
                    EvaluatedBytesResult::into_date_core_datum,
                )
            });
            match (result.unwrap(), expected) {
                (Datum::Time(value), Some(bits)) => {
                    assert_eq!(value.core_time().raw(), bits);
                    assert_eq!(value.kind(), TimeType::Date);
                    assert_eq!(value.fsp(), 0);
                }
                (Datum::Null, None) => {}
                other => panic!("unexpected DATE projection {other:?}"),
            }
            assert_wide_math_c4(observation);
        }
        for (core, expected) in [
            (None, None),
            (Some(0), Some(0)),
            (Some(1), Some(0)),
            (Some(high), Some(1)),
        ] {
            let (result, observation) =
                observe_wide_math(|| crate::eval_legacy_date_in(core, columns));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                EvaluatedBytesOp::DateCorePredicateLegacy
            );
        }
        let (result, observation) = observe_wide_math(|| {
            evaluate_args_in(
                EvaluatedBytesOp::DateDiffNullNative,
                columns,
                || Ok(EvaluatedArgs::NullWitness(None)),
                EvaluatedBytesResult::into_date_core_datum,
            )
        });
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation);
        for args in [
            EvaluatedArgs::BytesInt(Some(vec![0; 7]), Some(0)),
            EvaluatedArgs::BytesInt(Some(vec![0; 8]), Some(8)),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    EvaluatedBytesOp::DateCoreNative,
                    columns,
                    || Ok(args),
                    EvaluatedBytesResult::into_date_core_datum,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
    });
    assert!(
        matches!(EvaluatedBytesResult::Bytes(None).into_date_core_datum(),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
    );
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [EvaluatedBytesOp::DateCoreNative, EvaluatedBytesOp::DateDiffNullNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || if operation == EvaluatedBytesOp::DateCoreNative {
                    super::super::prepare_date_args(high, DateModes::default())
                } else {
                    Ok(EvaluatedArgs::NullWitness(None))
                },
                EvaluatedBytesResult::into_date_core_datum,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
        }
        for core in [None, Some(high)] {
            let (result, observation) = observe_wide_math(|| crate::eval_legacy_date_in(core, columns));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn local_clock_sdk_keeps_now_truncation_sysdate_rounding_and_date_offsets() {
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, clock, fsp, expected) in [
            (
                NowNative,
                (-1, 500_000_000, 1),
                Some(0),
                "1970-01-01 00:00:00",
            ),
            (
                NowNative,
                (86_399, 999_999_500, 0),
                Some(6),
                "1970-01-01 23:59:59.999999",
            ),
            (
                SysdateNative,
                (-1, 500_000_000, 1),
                Some(0),
                "1970-01-01 00:00:01",
            ),
            (
                SysdateNative,
                (86_399, 999_999_500, 0),
                Some(6),
                "1970-01-02 00:00:00.000000",
            ),
            (CurrentDateNative, (0, u32::MAX, -1), None, "1969-12-31"),
            (CurrentDateNative, (86_399, 0, 1), None, "1970-01-02"),
            (
                NowNative,
                (0, 0, 19_800),
                Some(4),
                "1970-01-01 05:30:00.0000",
            ),
            (
                SysdateNative,
                (0, 123_500_000, -3_600),
                Some(3),
                "1969-12-31 23:00:00.124",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || super::super::prepare_clock_args(clock, fsp),
                    |computed| {
                        Ok(computed
                            .into_bytes()?
                            .map_or(Datum::Null, Datum::new_string))
                    },
                )
            });
            assert_eq!(result, Ok(Datum::new_string(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        // The original clock/FSP data may change without changing recipe identity.
        assert_eq!(owner.snapshot().unwrap().factory_successes, 5);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn local_clock_sdk_rejects_wrong_roles_precision_and_zero_slot_fallbacks() {
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (CurrentDateNative, EvaluatedArgs::Bytes(Some(vec![0; 17]))),
            (
                CurrentDateNative,
                EvaluatedArgs::BytesInt(Some(vec![0; 16]), Some(0)),
            ),
            (NowNative, EvaluatedArgs::Bytes(Some(vec![0; 16]))),
            (
                SysdateNative,
                EvaluatedArgs::BytesInt(Some(vec![0; 16]), None),
            ),
            (
                SysdateNative,
                EvaluatedArgs::BytesInt(Some(vec![0; 16]), Some(-1)),
            ),
            (NowNative, EvaluatedArgs::NullWitness(None)),
            (CurrentDateNative, EvaluatedArgs::NoArgs),
            (SysdateNative, EvaluatedArgs::NoArgs),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(args),
                    EvaluatedBytesResult::into_bytes,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
        for operation in [NowNative, SysdateNative] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || super::super::prepare_clock_args((0, 0, 0), Some(7)),
                    EvaluatedBytesResult::into_bytes,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [NowNative, CurrentDateNative, SysdateNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || super::super::prepare_clock_args((-1, 999_999_500, 1), if operation == CurrentDateNative { None } else { Some(6) }),
                EvaluatedBytesResult::into_bytes,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn clock_sdk_preserves_actual_clock_fields_and_native_precision_policies() {
    use EvaluatedBytesOp::*;

    let EvaluatedArgs::Bytes(Some(raw)) =
        super::super::prepare_clock_args((i64::MIN, u32::MAX, i32::MIN), None).unwrap()
    else {
        panic!("unparameterized clock must use one actual byte owner");
    };
    assert_eq!(raw.len(), 16);
    assert_eq!(&raw[..8], &i64::MIN.to_le_bytes());
    assert_eq!(&raw[8..12], &u32::MAX.to_le_bytes());
    assert_eq!(&raw[12..], &i32::MIN.to_le_bytes());

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, clock, fsp, expected) in [
            (UtcDateNative, (-1, 999_999_500, 1), None, "1969-12-31"),
            (
                UtcTimestampNative,
                (-1, 999_999_500, 1),
                Some(6),
                "1970-01-01 00:00:00.000000",
            ),
            (
                CurrentTimeWithoutFspNative,
                (-1, 999_999_500, 1),
                None,
                "00:00:00",
            ),
            (
                CurrentTimeWithFspNative,
                (-1, 999_999_500, 1),
                Some(6),
                "00:00:00.999999",
            ),
            (
                UtcTimeWithoutFspNative,
                (-1, 999_999_500, 1),
                None,
                "23:59:59",
            ),
            (
                UtcTimeWithFspNative,
                (-1, 999_999_500, 1),
                Some(6),
                "23:59:59.999999",
            ),
            (
                UtcTimeWithFspNative,
                (-1, 999_999_500, 1),
                Some(0),
                "00:00:00",
            ),
            // Ignored raw fields are transported, not normalized or rejected.
            (UtcDateNative, (0, u32::MAX, i32::MIN), None, "1970-01-01"),
            (
                UtcTimeWithoutFspNative,
                (0, u32::MAX, i32::MIN),
                None,
                "00:00:00",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || super::super::prepare_clock_args(clock, fsp),
                    |computed| {
                        Ok(computed
                            .into_bytes()?
                            .map_or(Datum::Null, Datum::new_string))
                    },
                )
            });
            assert_eq!(result, Ok(Datum::new_string(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        let (result, observation) = observe_wide_math(|| {
            evaluate_args_in(
                UtcTimeNullNative,
                columns,
                || Ok(EvaluatedArgs::NullWitness(None)),
                |computed| {
                    Ok(computed
                        .into_bytes()?
                        .map_or(Datum::Null, Datum::new_string))
                },
            )
        });
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn clock_sdk_rejects_invalid_precision_roles_and_zero_slot_answers() {
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (UtcDateNative, EvaluatedArgs::Bytes(Some(vec![0; 15]))),
            (UtcTimestampNative, EvaluatedArgs::Bytes(Some(vec![0; 16]))),
            (
                CurrentTimeWithFspNative,
                EvaluatedArgs::BytesInt(Some(vec![0; 16]), Some(-1)),
            ),
            (UtcTimeWithoutFspNative, EvaluatedArgs::Bytes(None)),
            (UtcTimeNullNative, EvaluatedArgs::NoArgs),
            (UtcTimeNullNative, EvaluatedArgs::NullWitness(Some(0))),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(args),
                    EvaluatedBytesResult::into_bytes,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
        for operation in [
            UtcTimestampNative,
            CurrentTimeWithFspNative,
            UtcTimeWithFspNative,
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || super::super::prepare_clock_args((0, 0, 0), Some(7)),
                    EvaluatedBytesResult::into_bytes,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [UtcDateNative, UtcTimestampNative, CurrentTimeWithoutFspNative, CurrentTimeWithFspNative, UtcTimeWithoutFspNative, UtcTimeWithFspNative, UtcTimeNullNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || if operation == UtcTimeNullNative {
                    Ok(EvaluatedArgs::NullWitness(None))
                } else {
                    let fsp = if matches!(operation, UtcTimestampNative | CurrentTimeWithFspNative | UtcTimeWithFspNative) { Some(6) } else { None };
                    super::super::prepare_clock_args((-1, 999_999_500, 1), fsp)
                },
                EvaluatedBytesResult::into_bytes,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn json_unquote_sdk_keeps_text_and_binary_domains_separate() {
    use tidb_datatype::BinaryJSON;
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (input, expected) in [
            ("plain", "plain"),
            ("\"a\\n\"", "a\n"),
            (" \"a\" ", " \"a\" "),
            ("\"incomplete", "\"incomplete"),
            ("", ""),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    JsonUnquoteTextNative,
                    columns,
                    || Ok(EvaluatedArgs::Bytes(Some(input.as_bytes().to_vec()))),
                    |computed| {
                        Ok(computed
                            .into_bytes()?
                            .map_or(Datum::Null, Datum::new_string))
                    },
                )
            });
            assert_eq!(result, Ok(Datum::new_string(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(owner.snapshot().unwrap().factory_successes, 1);
        }
        let quoted_payload = "\"a\\n\"";
        for (document, expected) in [
            (
                BinaryJSON::from_value(&serde_json::Value::String(quoted_payload.to_owned()))
                    .unwrap(),
                quoted_payload,
            ),
            (BinaryJSON::parse("{\"a\":1}").unwrap(), "{\"a\": 1}"),
            // Original raw Display writes nothing for an invalid non-string.
            (BinaryJSON::from_encoded_parts(0xff, Vec::new()), ""),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    JsonUnquoteBinaryNative,
                    columns,
                    || super::super::prepare_json_binary_args(&document),
                    |computed| {
                        Ok(computed
                            .into_bytes()?
                            .map_or(Datum::Null, Datum::new_string))
                    },
                )
            });
            assert_eq!(result, Ok(Datum::new_string(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(owner.snapshot().unwrap().factory_successes, 2);
        }
        let (result, observation) = observe_wide_math(|| {
            evaluate_args_in(
                JsonOutputNullNative,
                columns,
                || Ok(EvaluatedArgs::NullWitness(None)),
                |computed| {
                    Ok(computed
                        .into_bytes()?
                        .map_or(Datum::Null, Datum::new_string))
                },
            )
        });
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation);
        assert_eq!(owner.snapshot().unwrap().factory_successes, 3);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn json_unquote_sdk_preserves_refusals_and_raw_display_panic_lifecycle() {
    use tidb_datatype::BinaryJSON;
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, bytes) in [
            (JsonUnquoteTextNative, vec![0xff]),
            (JsonUnquoteTextNative, b"\"a\" \"b\"".to_vec()),
            (JsonUnquoteBinaryNative, Vec::new()),
            (
                JsonUnquoteBinaryNative,
                vec![tidb_datatype::JSON_TYPE_CODE_STRING, 1, 0xff],
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(EvaluatedArgs::Bytes(Some(bytes))),
                    EvaluatedBytesResult::into_bytes,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let nan = BinaryJSON::from_encoded_parts(
        tidb_datatype::JSON_TYPE_CODE_FLOAT64,
        f64::NAN.to_le_bytes().to_vec(),
    );
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [JsonUnquoteTextNative, JsonUnquoteBinaryNative, JsonOutputNullNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || match operation {
                    JsonUnquoteTextNative => Ok(EvaluatedArgs::Bytes(Some(b"\"ok\"".to_vec()))),
                    JsonUnquoteBinaryNative => super::super::prepare_json_binary_args(&nan),
                    JsonOutputNullNative => Ok(EvaluatedArgs::NullWitness(None)),
                    _ => unreachable!(),
                },
                EvaluatedBytesResult::into_bytes,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    // Catch only OUTSIDE the unchanged driver. The actual raw Display error
    // panics in the kernel, then its normal drop guards poison and retire.
    arm_eval_one_observation();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        scope.with_columns(&crate::NoColumns, |columns| {
            evaluate_args_in(
                JsonUnquoteBinaryNative,
                columns,
                || super::super::prepare_json_binary_args(&nan),
                EvaluatedBytesResult::into_bytes,
            )
        })
    }));
    let observation = take_eval_one_observation();
    assert!(panic.is_err());
    assert_eq!(observation.facade_entries, 1);
    assert_eq!(observation.before_kernel_invocations, Some(0));
    assert_eq!(observation.after_kernel_invocations, None);
    assert!(scope.poisoned.get());
    assert!(scope.lease.borrow().is_none());
    let disposed = owner.snapshot().unwrap();
    assert_eq!((disposed.live, disposed.idle, disposed.retired), (0, 0, 1));
    assert_eq!(disposed.reserved_bytes, disposed.base_bytes);
    assert!(matches!(scope.evaluate_value(&Datum::Null),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopePoisoned));
    assert_eq!(owner.snapshot().unwrap(), disposed);
    drop(scope);
    execution.close();
}

#[test]
fn legacy_json_output_sdk_preserves_raw_identity_steps_and_path_metadata() {
    use tidb_datatype::{parse_json_path_expr, BinaryJSON, JSONPathExpression};

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let document = BinaryJSON::parse("{\"a\":1,\"b\":2}").unwrap();
        let paths = [
            parse_json_path_expr("$.a").unwrap(),
            parse_json_path_expr("$.missing").unwrap(),
        ];
        let values = [
            BinaryJSON::parse("9").unwrap(),
            BinaryJSON::parse("0").unwrap(),
        ];
        let (result, observation) = observe_wide_math(|| {
            crate::eval_legacy_json_replace_in(&document, &paths, &values, columns)
        });
        assert_eq!(
            result,
            Ok(Some(BinaryJSON::parse("{\"a\":9,\"b\":2}").unwrap()))
        );
        assert_wide_math_c4(observation);
        let wide = BinaryJSON::from_typed_value(&tidb_datatype::BinaryJSONValue::Uint64(u64::MAX))
            .unwrap();
        let root = [parse_json_path_expr("$").unwrap()];
        let (result, observation) = observe_wide_math(|| {
            crate::eval_legacy_json_replace_in(
                &document,
                &root,
                std::slice::from_ref(&wide),
                columns,
            )
        });
        let actual = result.unwrap().unwrap();
        assert_eq!(
            (actual.type_code(), actual.value()),
            (wide.type_code(), wide.value())
        );
        assert_wide_math_c4(observation);

        let path = parse_json_path_expr("$.a").unwrap();
        let json_null = BinaryJSON::parse("null").unwrap();
        let document = BinaryJSON::parse("{\"a\":[1]}").unwrap();
        let (result, observation) = observe_wide_math(|| {
            crate::eval_legacy_json_array_append_step_in(
                &document,
                Some((&path, &json_null)),
                columns,
            )
        });
        let appended = result.unwrap().unwrap();
        assert_eq!(appended, BinaryJSON::parse("{\"a\":[1,null]}").unwrap());
        assert_wide_math_c4(observation);
        let three = BinaryJSON::parse("3").unwrap();
        let (result, observation) = observe_wide_math(|| {
            crate::eval_legacy_json_array_append_step_in(&appended, Some((&path, &three)), columns)
        });
        assert_eq!(
            result,
            Ok(Some(BinaryJSON::parse("{\"a\":[1,null,3]}").unwrap()))
        );
        assert_wide_math_c4(observation);

        let document = BinaryJSON::parse("{\"*\":[1]}").unwrap();
        let quoted = parse_json_path_expr("$.\"*\"").unwrap();
        let flagged = JSONPathExpression::default().push_back_key("*");
        assert_eq!(quoted.legs(), flagged.legs());
        assert!(!quoted.could_match_multiple_values());
        assert!(flagged.could_match_multiple_values());
        for (path, expected) in [
            (&quoted, Some(BinaryJSON::parse("{\"*\":[1,3]}").unwrap())),
            (&flagged, None),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::eval_legacy_json_array_append_step_in(
                    &document,
                    Some((path, &three)),
                    columns,
                )
            });
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        let raw = BinaryJSON::from_encoded_parts(0xff, vec![7, 8]);
        let (result, observation) =
            observe_wide_math(|| crate::eval_legacy_json_array_append_step_in(&raw, None, columns));
        let actual = result.unwrap().unwrap();
        assert_eq!(
            (actual.type_code(), actual.value()),
            (raw.type_code(), raw.value())
        );
        assert_wide_math_c4(observation);
        // REPLACE with no pairs still decodes, unlike APPEND's raw identity.
        let (result, observation) =
            observe_wide_math(|| crate::eval_legacy_json_replace_in(&raw, &[], &[], columns));
        assert_eq!(result, Ok(None));
        assert_wide_math_c4(observation);
        let (result, observation) =
            observe_wide_math(|| crate::eval_legacy_json_output_none_in(columns));
        assert_eq!(result, Ok(None));
        assert_wide_math_c4(observation);
        assert_eq!(
            scope
                .lease
                .borrow()
                .as_ref()
                .unwrap()
                .worker
                .as_ref()
                .unwrap()
                .operation(),
            EvaluatedBytesOp::JsonValueAbsentLegacy
        );
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn legacy_json_output_sdk_distinguishes_business_none_packets_and_zero_slots() {
    use tidb_datatype::{parse_json_path_expr, BinaryJSON};
    use EvaluatedBytesOp::*;

    assert!(
        matches!(legacy_json_output_result(EvaluatedBytesResult::Bytes(Some(Vec::new()))),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
    );
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let path = parse_json_path_expr("$.a").unwrap();
        let value = BinaryJSON::parse("2").unwrap();
        let scalar = BinaryJSON::parse("{\"a\":1}").unwrap();
        let (result, observation) = observe_wide_math(|| {
            eval_legacy_json_array_append_step_in(&scalar, Some((&path, &value)), columns)
        });
        assert_eq!(result, Ok(None)); // Legacy APPEND does not wrap a scalar target.
        assert_wide_math_c4(observation);
        let malformed = BinaryJSON::from_encoded_parts(0xff, vec![7]);
        let (result, observation) = observe_wide_math(|| {
            eval_legacy_json_array_append_step_in(&malformed, Some((&path, &value)), columns)
        });
        let actual = result.unwrap().unwrap(); // Extract failure is the old no-op.
        assert_eq!(
            (actual.type_code(), actual.value()),
            (malformed.type_code(), malformed.value())
        );
        assert_wide_math_c4(observation);
        for (operation, args) in [
            (
                JsonReplaceRawLegacy,
                EvaluatedArgs::Bytes3([Some(Vec::new()), Some(Vec::new()), Some(Vec::new())]),
            ),
            (
                JsonArrayAppendRawLegacy,
                EvaluatedArgs::Bytes2(Some(vec![0xff]), Some(Vec::new())),
            ),
            (
                JsonArrayAppendEmptyLegacy,
                EvaluatedArgs::Bytes(Some(Vec::new())),
            ),
            (JsonValueAbsentLegacy, EvaluatedArgs::NullWitness(None)),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(operation, columns, || Ok(args), legacy_json_output_result)
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
        let (result, observation) = observe_wide_math(|| {
            eval_legacy_json_replace_in(&scalar, std::slice::from_ref(&path), &[], columns)
        });
        assert!(matches!(
            result,
            Err(EvalError::ExpressionRuntimeFailure(_))
        ));
        assert_eq!(
            observation.before_kernel_invocations,
            observation.after_kernel_invocations
        );
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let document = BinaryJSON::parse("{\"a\":[1]}").unwrap();
        let path = parse_json_path_expr("$.a").unwrap();
        let value = BinaryJSON::parse("2").unwrap();
        for operation in [JsonReplaceRawLegacy, JsonArrayAppendRawLegacy, JsonArrayAppendEmptyLegacy, JsonValueAbsentLegacy] {
            let (result, observation) = observe_wide_math(|| match operation {
                JsonReplaceRawLegacy => eval_legacy_json_replace_in(&document, std::slice::from_ref(&path), std::slice::from_ref(&value), columns),
                JsonArrayAppendRawLegacy => eval_legacy_json_array_append_step_in(&document, Some((&path, &value)), columns),
                JsonArrayAppendEmptyLegacy => eval_legacy_json_array_append_step_in(&document, None, columns),
                JsonValueAbsentLegacy => eval_legacy_json_output_none_in(columns),
                _ => unreachable!(),
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn json_path_sdk_preserves_selection_and_ordered_mutation_results() {
    use serde_json::json;
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, document, path_texts, values, expected) in [
            (
                JsonExtractSerdeNative,
                json!({"a": 1}),
                vec!["$.a", "$.a"],
                vec![],
                Some("[1, 1]"),
            ),
            (
                JsonExtractSerdeNative,
                json!([1]),
                vec!["$[*]"],
                vec![],
                Some("[1]"),
            ),
            (
                JsonExtractSerdeNative,
                json!({}),
                vec!["$.missing"],
                vec![],
                None,
            ),
            (
                JsonInsertSerdeNative,
                json!({"a": 1}),
                vec!["$.a", "$.b"],
                vec![json!(2), json!(3)],
                Some("{\"a\": 1, \"b\": 3}"),
            ),
            (
                JsonSetSerdeNative,
                json!({}),
                vec!["$.a", "$.a.b"],
                vec![json!({}), json!(1)],
                Some("{\"a\": {\"b\": 1}}"),
            ),
            (
                JsonReplaceSerdeNative,
                json!({"a": 1}),
                vec!["$.a", "$.missing"],
                vec![json!(2), json!(3)],
                Some("{\"a\": 2}"),
            ),
            (
                JsonRemoveSerdeNative,
                json!([0, 1, 2, 3]),
                vec!["$[1]", "$[1]"],
                vec![],
                Some("[0, 3]"),
            ),
            // Actual empty low-level path list, not an invented SQL call arity.
            (
                JsonRemoveSerdeNative,
                json!({"a": 1}),
                vec![],
                vec![],
                Some("{\"a\": 1}"),
            ),
            (
                JsonArrayAppendSerdeNative,
                json!(1),
                vec!["$"],
                vec![json!(2)],
                Some("[1, 2]"),
            ),
            (
                JsonArrayInsertSerdeNative,
                json!([1, 2]),
                vec!["$[last]", "$[last-9]"],
                vec![json!(9), json!(8)],
                Some("[8, 1, 9, 2]"),
            ),
        ] {
            let paths: Vec<_> = path_texts
                .into_iter()
                .map(|path| tidb_query_expr::parse_native_json_path(path).unwrap())
                .collect();
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || {
                        if matches!(operation, JsonExtractSerdeNative | JsonRemoveSerdeNative) {
                            super::super::prepare_json_paths_args(&document, &paths)
                        } else {
                            super::super::prepare_json_path_values_args(&document, &paths, &values)
                        }
                    },
                    EvaluatedBytesResult::into_json_datum,
                )
            });
            let expected = expected.map_or(Datum::Null, |text| {
                Datum::Json(tidb_datatype::BinaryJSON::parse(text).unwrap())
            });
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        let (result, observation) = observe_wide_math(|| {
            evaluate_args_in(
                JsonOutputNullNative,
                columns,
                || Ok(EvaluatedArgs::NullWitness(None)),
                EvaluatedBytesResult::into_json_datum,
            )
        });
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn json_path_sdk_rejects_bad_roles_counts_and_zero_slot_fallbacks() {
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (
                JsonExtractSerdeNative,
                EvaluatedArgs::Bytes2(Some(b"{}".to_vec()), Some(Vec::new())),
            ),
            (JsonRemoveSerdeNative, EvaluatedArgs::NoArgs),
            (JsonSetSerdeNative, EvaluatedArgs::NullWitness(None)),
            (JsonOutputNullNative, EvaluatedArgs::NullWitness(Some(0))),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(args),
                    EvaluatedBytesResult::into_json_datum,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
        for (operation, path_text, values) in [
            (JsonRemoveSerdeNative, "$", Vec::new()),
            (
                JsonArrayInsertSerdeNative,
                "$.a",
                vec![serde_json::json!(1)],
            ),
            (JsonSetSerdeNative, "$.a", Vec::new()),
        ] {
            let paths = [tidb_query_expr::parse_native_json_path(path_text).unwrap()];
            let document = serde_json::json!({});
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || {
                        if operation == JsonRemoveSerdeNative {
                            super::super::prepare_json_paths_args(&document, &paths)
                        } else {
                            super::super::prepare_json_path_values_args(&document, &paths, &values)
                        }
                    },
                    EvaluatedBytesResult::into_json_datum,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [JsonExtractSerdeNative, JsonInsertSerdeNative, JsonSetSerdeNative, JsonReplaceSerdeNative, JsonRemoveSerdeNative, JsonArrayAppendSerdeNative, JsonArrayInsertSerdeNative, JsonOutputNullNative] {
            let document = serde_json::json!([1, 2]);
            let paths = [tidb_query_expr::parse_native_json_path("$[0]").unwrap()];
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || match operation {
                    JsonOutputNullNative => Ok(EvaluatedArgs::NullWitness(None)),
                    JsonExtractSerdeNative | JsonRemoveSerdeNative => super::super::prepare_json_paths_args(&document, &paths),
                    _ => super::super::prepare_json_path_values_args(&document, &paths, &[serde_json::json!(9)]),
                },
                EvaluatedBytesResult::into_json_datum,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn json_output_sdk_owns_arrays_objects_keys_and_native_pretty_text() {
    use serde_json::json;
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let expected_json = |text| Datum::Json(tidb_datatype::BinaryJSON::parse(text).unwrap());
    scope.with_columns(&crate::NoColumns, |columns| {
        for (values, expected) in [
            (Vec::new(), "[]"),
            (
                vec![json!(null), json!(1.0), json!(u64::MAX), json!("1")],
                "[null, 1.0, 18446744073709551615, \"1\"]",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    JsonArraySerdeNative,
                    columns,
                    || super::super::prepare_json_array_args(&values),
                    EvaluatedBytesResult::into_json_datum,
                )
            });
            assert_eq!(result, Ok(expected_json(expected)));
            assert_wide_math_c4(observation);
        }
        for (pairs, expected) in [
            (Vec::new(), "{}"),
            (
                vec![
                    ("b".to_owned(), json!(1)),
                    ("a".to_owned(), json!(2)),
                    ("b".to_owned(), json!(3)),
                ],
                "{\"a\": 2, \"b\": 3}",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    JsonObjectSerdeNative,
                    columns,
                    || super::super::prepare_json_object_args(&pairs),
                    EvaluatedBytesResult::into_json_datum,
                )
            });
            assert_eq!(result, Ok(expected_json(expected)));
            assert_wide_math_c4(observation);
        }
        for (operation, document, path, expected) in [
            (
                JsonKeysSerdeNative,
                json!({"b": 1, "a": 2}),
                None,
                expected_json("[\"a\", \"b\"]"),
            ),
            (
                JsonKeysPathSerdeNative,
                json!({"outer": {"z": 1, "a": 0}}),
                Some("$.outer"),
                expected_json("[\"a\", \"z\"]"),
            ),
            (
                JsonKeysPathSerdeNative,
                json!({"a": 1}),
                Some("$.missing"),
                Datum::Null,
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || super::super::prepare_json_serde_args(&document, None, path),
                    EvaluatedBytesResult::into_json_datum,
                )
            });
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        // Fixed native float-format cutoffs, not serde's generic pretty printer.
        let document: serde_json::Value = serde_json::from_str("[1e-16,1e-15,1e15,1.0]").unwrap();
        let (result, observation) = observe_wide_math(|| {
            evaluate_args_in(
                JsonPrettySerdeNative,
                columns,
                || super::super::prepare_json_serde_args(&document, None, None),
                |computed| {
                    Ok(computed
                        .into_bytes()?
                        .map_or(Datum::Null, Datum::new_string))
                },
            )
        });
        assert_eq!(
            result,
            Ok(Datum::new_string(
                "[\n  1e-16,\n  0.000000000000001,\n  1e15,\n  1.0\n]"
            ))
        );
        assert_wide_math_c4(observation);
        let (result, observation) = observe_wide_math(|| {
            evaluate_args_in(
                JsonOutputNullNative,
                columns,
                || Ok(EvaluatedArgs::NullWitness(None)),
                EvaluatedBytesResult::into_json_datum,
            )
        });
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation);
    });
    // Projection-only edge: a present JSON null must never become SQL NULL.
    assert_eq!(
        EvaluatedBytesResult::Bytes(Some(b"null".to_vec())).into_json_datum(),
        Ok(expected_json("null"))
    );
    assert_eq!(
        EvaluatedBytesResult::Bytes(None).into_json_datum(),
        Ok(Datum::Null)
    );
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn json_output_sdk_keeps_projection_contracts_and_zero_slot_refusals() {
    use EvaluatedBytesOp::*;

    assert!(
        matches!(EvaluatedBytesResult::Bytes(Some(vec![0xff])).into_json_datum(),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
    );
    assert_eq!(
        EvaluatedBytesResult::Bytes(Some(b"{".to_vec())).into_json_datum(),
        Err(EvalError::Json(crate::JsonError::InvalidText))
    );
    assert!(
        matches!(EvaluatedBytesResult::JsonReport(crate::tikv::JsonReportOutcome::Null).into_json_datum(),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
    );

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (JsonArraySerdeNative, EvaluatedArgs::Bytes(Some(Vec::new()))),
            (
                JsonObjectSerdeNative,
                EvaluatedArgs::Bytes(Some(1_u64.to_le_bytes().to_vec())),
            ),
            (JsonArraySerdeNative, EvaluatedArgs::NoArgs),
            (
                JsonKeysPathSerdeNative,
                EvaluatedArgs::Bytes2(Some(b"{}".to_vec()), Some(b"$[*]".to_vec())),
            ),
            (
                JsonPrettySerdeNative,
                EvaluatedArgs::Bytes(Some(b"{".to_vec())),
            ),
            (JsonOutputNullNative, EvaluatedArgs::NullWitness(Some(0))),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(args),
                    EvaluatedBytesResult::into_bytes,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [JsonArraySerdeNative, JsonObjectSerdeNative, JsonKeysSerdeNative, JsonKeysPathSerdeNative, JsonPrettySerdeNative, JsonOutputNullNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || match operation {
                    JsonArraySerdeNative => super::super::prepare_json_array_args(&[]),
                    JsonObjectSerdeNative => super::super::prepare_json_object_args(&[]),
                    JsonKeysSerdeNative | JsonPrettySerdeNative => super::super::prepare_json_serde_args(&serde_json::json!({}), None, None),
                    JsonKeysPathSerdeNative => super::super::prepare_json_serde_args(&serde_json::json!({}), None, Some("$")),
                    JsonOutputNullNative => Ok(EvaluatedArgs::NullWitness(None)),
                    _ => unreachable!(),
                },
                EvaluatedBytesResult::into_bytes,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn json_predicate_sdk_keeps_actual_serde_values_and_legacy_binary_membership() {
    use tidb_datatype::BinaryJSON;
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, first, second, path, expected) in [
            (
                JsonContainsSerdeNative,
                "[1,2]",
                Some("1"),
                None,
                Datum::Int(1),
            ),
            (
                JsonContainsPathSerdeNative,
                "{\"a\":[1,2]}",
                Some("1"),
                Some("$.a"),
                Datum::Int(1),
            ),
            (
                JsonOverlapsSerdeNative,
                "[1,2]",
                Some("[2,3]"),
                None,
                Datum::Int(1),
            ),
            (
                JsonMemberOfSerdeNative,
                "{\"a\":1}",
                Some("[{\"a\":1}]"),
                None,
                Datum::Int(1),
            ),
            (
                JsonLengthSerdeNative,
                "{\"a\":1,\"b\":[2]}",
                None,
                None,
                Datum::Int(2),
            ),
            (
                JsonLengthPathSerdeNative,
                "{\"a\":[1,2,3]}",
                None,
                Some("$.a"),
                Datum::Int(3),
            ),
            (
                JsonPathExistsSerdeNative,
                "{\"a\":null}",
                None,
                Some("$.a"),
                Datum::Int(1),
            ),
            (
                JsonLengthPathSerdeNative,
                "{\"a\":1}",
                None,
                Some("$.missing"),
                Datum::Null,
            ),
            // A JSON null is an actual scalar document, not a SQL NULL witness.
            (JsonLengthSerdeNative, "null", None, None, Datum::Int(1)),
        ] {
            let first: serde_json::Value = serde_json::from_str(first).unwrap();
            let second: Option<serde_json::Value> =
                second.map(|text| serde_json::from_str(text).unwrap());
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || super::super::prepare_json_serde_args(&first, second.as_ref(), path),
                    EvaluatedBytesResult::into_int_datum,
                )
            });
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        for (target, document, expected) in [
            ("1.0", "[1]", 1),
            ("\"1\"", "[1]", 0),
            ("18446744073709551615", "[18446744073709551615]", 1),
            ("null", "[null]", 1),
        ] {
            let (result, observation) = observe_wide_math(|| {
                eval_legacy_json_member_of_in(
                    LegacyBinaryArgs::Values(
                        BinaryJSON::parse(target).unwrap(),
                        BinaryJSON::parse(document).unwrap(),
                    ),
                    columns,
                )
            });
            assert_eq!(result, Ok(Some(expected)));
            assert_wide_math_c4(observation);
        }
        // Scalar raw fallback is intentional; these bytes cannot become serde values.
        let raw = BinaryJSON::from_encoded_parts(0xff, vec![7]);
        let (result, observation) = observe_wide_math(|| {
            eval_legacy_json_member_of_in(LegacyBinaryArgs::Values(raw.clone(), raw), columns)
        });
        assert_eq!(result, Ok(Some(1)));
        assert_wide_math_c4(observation);
        for (args, operation) in [
            (LegacyBinaryArgs::NullWitness(None), JsonPredicateNullNative),
            (LegacyBinaryArgs::Missing, JsonPredicateMissingLegacy),
        ] {
            let (result, observation) =
                observe_wide_math(|| eval_legacy_json_member_of_in(args, columns));
            assert_eq!(result, Ok(None));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn json_predicate_sdk_rejects_invalid_transport_without_sql_or_zero_slot_answers() {
    use tidb_datatype::BinaryJSON;
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (JsonLengthSerdeNative, EvaluatedArgs::Bytes(Some(b"{".to_vec()))),
            (JsonContainsSerdeNative, EvaluatedArgs::Bytes2(None, Some(b"1".to_vec()))),
            (JsonPathExistsSerdeNative, EvaluatedArgs::Bytes2(Some(b"{}".to_vec()), Some(b"not-a-path".to_vec()))),
            (JsonLengthPathSerdeNative, EvaluatedArgs::Bytes2(Some(b"[1,2]".to_vec()), Some(b"$[*]".to_vec()))),
            (JsonPredicateNullNative, EvaluatedArgs::NullWitness(Some(0))),
            (JsonPredicateMissingLegacy, EvaluatedArgs::NullWitness(None)),
        ] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns, || Ok(args), EvaluatedBytesResult::into_int_datum,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionRuntimeFailure(_))));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(observation.before_kernel_invocations, observation.after_kernel_invocations);
        }
        // The complete array is representation-validated before scanning: a
        // matching first element must not hide an invalid second child tag.
        let array = BinaryJSON::parse("[1,2]").unwrap();
        let mut malformed = array.value().to_vec();
        malformed[13] = 0xff; // Eight-byte header, then five-byte value entries.
        let (result, observation) = observe_wide_math(|| eval_legacy_json_member_of_in(
            LegacyBinaryArgs::Values(BinaryJSON::parse("1").unwrap(), BinaryJSON::from_encoded_parts(array.type_code(), malformed)), columns,
        ));
        assert!(matches!(result, Err(EvalError::ExpressionRuntimeFailure(_))));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(observation.before_kernel_invocations, observation.after_kernel_invocations);
        let (result, observation) = observe_wide_math(|| eval_legacy_json_member_of_in(LegacyBinaryArgs::NullWitness(Some(1)), columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert_eq!(observation.facade_entries, 0);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for args in [
            LegacyBinaryArgs::Values(BinaryJSON::parse("1").unwrap(), BinaryJSON::parse("[1]").unwrap()),
            LegacyBinaryArgs::NullWitness(None),
            LegacyBinaryArgs::Missing,
        ] {
            let (result, observation) = observe_wide_math(|| eval_legacy_json_member_of_in(args, columns));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let document = serde_json::json!([1, 2, 3]);
        let (result, observation) = observe_wide_math(|| evaluate_args_in(
            JsonLengthSerdeNative, columns,
            || super::super::prepare_json_serde_args(&document, None, None),
            EvaluatedBytesResult::into_int_datum,
        ));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn grouping_sdk_preserves_source_modes_unsigned_bits_and_wrapping_marks() {
    use EvaluatedBytesOp::{
        GroupingBitAndNative, GroupingNumericCmpNative, GroupingNumericSetNative,
    };

    // The checked shared helper transports actual id/counts/ascending marks,
    // never the recipe opcode or a precomputed GROUPING result.
    let prepare_marks = |operation, gid, groups: &[Vec<u64>]| {
        let mode = match operation {
            GroupingBitAndNative => tidb_query_expr::GroupingMode::BitAnd,
            GroupingNumericCmpNative => tidb_query_expr::GroupingMode::NumericCmp,
            GroupingNumericSetNative => tidb_query_expr::GroupingMode::NumericSet,
            _ => panic!("test requires a value grouping profile"),
        };
        let metadata = tidb_query_expr::GroupingMetadata::new(
            mode,
            groups
                .iter()
                .map(|group| group.iter().copied().collect())
                .collect(),
        )
        .unwrap();
        super::super::prepare_grouping_args(gid, &metadata)
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let mut wrapped = vec![Vec::new()];
    wrapped.extend(vec![vec![1]; 64]);
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, gid, marks, expected) in [
            // Original source mode rows composed in argument order: 001/0011/101.
            (
                GroupingBitAndNative,
                1_u64,
                vec![vec![1], vec![3], vec![6]],
                1_u64,
            ),
            (
                GroupingNumericCmpNative,
                2,
                vec![vec![0], vec![1], vec![2], vec![3]],
                3,
            ),
            (
                GroupingNumericSetNative,
                2,
                vec![vec![1, 3], vec![2, 3], Vec::new()],
                5,
            ),
            (GroupingNumericSetNative, 1, vec![Vec::new(); 64], u64::MAX),
            (GroupingNumericSetNative, 1, wrapped, 0),
            (
                GroupingNumericCmpNative,
                u64::MAX,
                vec![vec![u64::MAX - 1], vec![u64::MAX]],
                1,
            ),
            (
                GroupingBitAndNative,
                1_u64 << 63,
                vec![vec![1_u64 << 63], vec![1]],
                1,
            ),
            (
                GroupingNumericSetNative,
                u64::MAX,
                vec![vec![u64::MAX], Vec::new()],
                1,
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || prepare_marks(operation, gid, &marks),
                    EvaluatedBytesResult::into_uint_bits_datum,
                )
            });
            assert_eq!(result, Ok(Datum::UInt(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        let factories = owner.snapshot().unwrap().factory_successes;
        for (operation, expected, added_factories) in [
            (GroupingNumericCmpNative, 1, 1),
            (GroupingNumericCmpNative, 1, 1),
            (GroupingBitAndNative, 0, 2),
            (GroupingNumericCmpNative, 1, 3),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || prepare_marks(operation, 1, &[vec![1]]),
                    EvaluatedBytesResult::into_uint_bits_datum,
                )
            });
            assert_eq!(result, Ok(Datum::UInt(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(
                owner.snapshot().unwrap().factory_successes,
                factories + added_factories
            );
        }
        let (result, observation) = observe_wide_math(|| {
            evaluate_args_in(
                EvaluatedBytesOp::GroupingNullNative,
                columns,
                || Ok(EvaluatedArgs::NullWitness(None)),
                EvaluatedBytesResult::into_uint_bits_datum,
            )
        });
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn grouping_sdk_rejects_false_presence_and_preserves_zero_slot_refusals() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use EvaluatedBytesOp::{
        GroupingBitAndNative, GroupingNullNative, GroupingNumericCmpNative,
        GroupingNumericSetNative,
    };

    let typed_grouping = |value: Datum| {
        ScalarFunction::new(
            tidb_ast::CiString::new("grouping"),
            FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
            vec![Expression::Constant(Constant::new(
                value,
                FieldType::new(FieldTypeCode::LongLong),
            ))],
        )
    };
    let null_without_metadata = typed_grouping(Datum::Null);
    let value_without_metadata = typed_grouping(Datum::Int(1));
    let mut value_with_metadata = typed_grouping(Datum::Int(1));
    value_with_metadata
        .set_grouping_metadata(
            crate::grouping::GroupingMode::NumericSet,
            vec![BTreeSet::new(); 64],
        )
        .unwrap();

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (
                GroupingBitAndNative,
                EvaluatedArgs::Bytes2(None, Some(0_u64.to_le_bytes().to_vec())),
            ),
            (
                GroupingNumericCmpNative,
                EvaluatedArgs::Bytes2(Some(vec![0; 7]), Some(0_u64.to_le_bytes().to_vec())),
            ),
            (
                GroupingNumericSetNative,
                EvaluatedArgs::Bytes2(Some(vec![0; 8]), Some(Vec::new())),
            ),
            (GroupingNullNative, EvaluatedArgs::NullWitness(Some(0))),
            (GroupingNullNative, EvaluatedArgs::NoArgs),
        ] {
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(args),
                    EvaluatedBytesResult::into_uint_bits_datum,
                )
            });
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(_))
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.before_kernel_invocations,
                observation.after_kernel_invocations
            );
        }
        for (function, expected) in [
            (&null_without_metadata, Datum::Null),
            (&value_with_metadata, Datum::UInt(u64::MAX)),
        ] {
            let (result, observation) =
                observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| {
            value_without_metadata.eval(columns, tidb_chunk::row::Row::empty())
        });
        assert_eq!(
            result,
            Err(EvalError::Unsupported("Meta data is not initialized"))
        );
        assert_eq!(observation.facade_entries, 0);
        // A failing real child is demanded before metadata and remains the error.
        let overflow_child = ScalarFunction::new(
            tidb_ast::CiString::new("plus"),
            FieldType::new(FieldTypeCode::LongLong),
            [i64::MAX, 1]
                .into_iter()
                .map(|value| {
                    Expression::Constant(Constant::new(
                        Datum::Int(value),
                        FieldType::new(FieldTypeCode::LongLong),
                    ))
                })
                .collect(),
        );
        let child_before_metadata = ScalarFunction::new(
            tidb_ast::CiString::new("grouping"),
            FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
            vec![Expression::ScalarFunction(overflow_child)],
        );
        let (result, observation) = observe_wide_math(|| {
            child_before_metadata.eval(columns, tidb_chunk::row::Row::empty())
        });
        // ScalarFunction decorates the kernel's integer overflow with the
        // child's declared signed class and rendered operands before GROUPING.
        assert_eq!(
            result,
            Err(EvalError::DataOutOfRange {
                value: "BIGINT",
                expression: "(9223372036854775807 + 1)".to_owned(),
            })
        );
        assert_wide_math_c4(observation);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for function in [&null_without_metadata, &value_with_metadata] {
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
        }
        let (result, observation) = observe_wide_math(|| value_without_metadata.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Err(EvalError::Unsupported("Meta data is not initialized")));
        assert_eq!(observation.facade_entries, 0);
        for operation in [GroupingBitAndNative, GroupingNumericCmpNative, GroupingNumericSetNative, GroupingNullNative] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || Ok(if operation == GroupingNullNative {
                    EvaluatedArgs::NullWitness(None)
                } else {
                    EvaluatedArgs::Bytes2(Some(u64::MAX.to_le_bytes().to_vec()), Some(0_u64.to_le_bytes().to_vec()))
                }),
                EvaluatedBytesResult::into_uint_bits_datum,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn comparison_sdk_payload_identity_controls_scope_and_pool_reuse() {
    use tidb_query_expr::ComparisonOp::{Eq, Ne};

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (predicate, expected, factories) in [(Eq, 1, 1), (Eq, 1, 1), (Ne, 0, 2), (Eq, 1, 3)] {
            let operation = EvaluatedBytesOp::CompareIntSsNative(predicate);
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(EvaluatedArgs::Int2(Some(7), Some(7))),
                    EvaluatedBytesResult::into_boolean_datum,
                )
            });
            assert_eq!(result, Ok(Datum::Int(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
            assert_eq!(owner.snapshot().unwrap().factory_successes, factories);
        }
    });
    drop(scope);
    // Same full identity reuses the idle worker. A different payload of the
    // same enum variant evicts it; switching back cannot reuse the Ne kernel.
    for (predicate, expected, factories) in [(Eq, 1, 3), (Ne, 0, 4), (Eq, 1, 5)] {
        let scope = execution.scope();
        scope.with_columns(&crate::NoColumns, |columns| {
            let operation = EvaluatedBytesOp::CompareIntSsNative(predicate);
            let (result, observation) = observe_wide_math(|| {
                evaluate_args_in(
                    operation,
                    columns,
                    || Ok(EvaluatedArgs::Int2(Some(7), Some(7))),
                    EvaluatedBytesResult::into_boolean_datum,
                )
            });
            assert_eq!(result, Ok(Datum::Int(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
            assert_eq!(owner.snapshot().unwrap().factory_successes, factories);
        });
        assert!(!scope.poisoned.get());
        drop(scope);
    }
    execution.close();
}

#[test]
fn comparison_sdk_keeps_ieee_legacy_order_presence_and_infrastructure_distinct() {
    use tidb_query_expr::ComparisonOp::{Eq, Ge, Gt, Le, Lt, Ne};

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Fixed source-policy answers, not a host comparison used as an oracle.
        // Positive quiet NaN sorts after zero only in legacy total ordering;
        // native IEEE predicates are unordered, and native +/-0 compare equal.
        let nan = 0x7ff8_0000_0000_0001_u64;
        for (left, right, native, legacy) in [
            (nan, 0.0_f64.to_bits(), [0, 1, 0, 0, 0, 0], [0, 1, 0, 0, 1, 1]),
            (nan, nan, [0, 1, 0, 0, 0, 0], [1, 0, 0, 1, 0, 1]),
            ((-0.0_f64).to_bits(), 0.0_f64.to_bits(), [1, 0, 0, 1, 0, 1], [0, 1, 1, 1, 0, 0]),
        ] {
            for (index, predicate) in [Eq, Ne, Lt, Le, Gt, Ge].into_iter().enumerate() {
                for (operation, expected) in [
                    (EvaluatedBytesOp::CompareRealNative(predicate), native[index]),
                    (EvaluatedBytesOp::CompareRealLegacy(predicate), legacy[index]),
                ] {
                    let (result, observation) = observe_wide_math(|| evaluate_args_in(
                        operation, columns,
                        || Ok(EvaluatedArgs::Ieee754Bits2 {
                            left: super::super::ReadyIeee754Arg::Value(Some(left)),
                            right: super::super::ReadyIeee754Arg::Value(Some(right)),
                        }),
                        EvaluatedBytesResult::into_boolean_datum,
                    ));
                    assert_eq!(result, Ok(Datum::Int(expected)));
                    assert_wide_math_c4(observation);
                }
            }
        }
        let earlier = Time::new(CoreTime::from_date(2024, 1, 1, 0, 0, 0, 0), TimeType::DateTime, 0).unwrap();
        let later = Time::new(CoreTime::from_date(2024, 1, 2, 0, 0, 0, 0), TimeType::DateTime, 0).unwrap();
        for (result, observation) in [
            observe_wide_math(|| eval_legacy_integer_comparison_in(Lt,
                LegacyBinaryArgs::Values(1_i128 << 100, (1_i128 << 100) + 1), columns)),
            observe_wide_math(|| eval_legacy_real_comparison_in(Lt,
                LegacyBinaryArgs::Values(-0.0, 0.0), columns)),
            observe_wide_math(|| eval_legacy_bytes_comparison_in(Gt,
                LegacyBinaryArgs::Values(b"a ".to_vec(), b"a".to_vec()), 63, columns)),
            observe_wide_math(|| eval_legacy_decimal_comparison_in(Eq,
                LegacyBinaryArgs::Values(tidb_datatype::Decimal::from_literal("1.00"), tidb_datatype::Decimal::from_literal("1.0")), columns)),
            observe_wide_math(|| eval_legacy_time_comparison_in(Lt,
                LegacyBinaryArgs::Values(earlier, later), columns)),
        ] {
            assert_eq!(result, Ok(Some(1)));
            assert_wide_math_c4(observation);
        }
        for missing in [false, true] {
            for (result, observation) in [
                observe_wide_math(|| eval_legacy_integer_comparison_in(Eq,
                    if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns)),
                observe_wide_math(|| eval_legacy_real_comparison_in(Eq,
                    if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns)),
                observe_wide_math(|| eval_legacy_bytes_comparison_in(Eq,
                    if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, i32::MAX, columns)),
                observe_wide_math(|| eval_legacy_decimal_comparison_in(Eq,
                    if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns)),
                observe_wide_math(|| eval_legacy_time_comparison_in(Eq,
                    if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns)),
            ] {
                assert_eq!(result, Ok(None));
                assert_wide_math_c4(observation);
            }
        }
        let (result, observation) = observe_wide_math(|| eval_legacy_integer_comparison_in(
            Eq, LegacyBinaryArgs::NullWitness(Some(0)), columns,
        ));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert_eq!(observation.facade_entries, 0);
        for (operation, args) in [
            (EvaluatedBytesOp::CompareNullNative, EvaluatedArgs::NullWitness(None)),
            (EvaluatedBytesOp::CompareMissingLegacy, EvaluatedArgs::NoArgs),
        ] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns, || Ok(args), EvaluatedBytesResult::into_boolean_datum,
            ));
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
        for (operation, args) in [
            (EvaluatedBytesOp::CompareRealNative(Eq), EvaluatedArgs::Int2(Some(0), Some(0))),
            (EvaluatedBytesOp::CompareNullNative, EvaluatedArgs::NullWitness(Some(0))),
        ] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns, || Ok(args), EvaluatedBytesResult::into_boolean_datum,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionRuntimeFailure(_))));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(observation.before_kernel_invocations, observation.after_kernel_invocations);
        }
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (operation, args) in [
            (EvaluatedBytesOp::CompareIntSsNative(Eq), EvaluatedArgs::Int2(Some(7), Some(7))),
            (EvaluatedBytesOp::CompareIntSsNative(Ne), EvaluatedArgs::Int2(Some(7), Some(7))),
            (EvaluatedBytesOp::CompareNullNative, EvaluatedArgs::NullWitness(None)),
            (EvaluatedBytesOp::CompareMissingLegacy, EvaluatedArgs::NoArgs),
        ] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns, || Ok(args), EvaluatedBytesResult::into_boolean_datum,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn aes_sdk_all_profiles_match_original_vectors_and_typed_iv_failures() {
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let decode_hex = |text: &str| {
        text.as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>()
    };
    scope.with_columns(&crate::NoColumns, |columns| {
        // Fixed source vectors: pkg/expression/builtin_encryption_test.go
        // aesTests. Decrypt receives these constants, never our encrypt output.
        for (encrypt, decrypt, iv_required, block_mode, ciphertext) in [
            (AesEncrypt128EcbNative, AesDecrypt128EcbNative, false, true, "697BFE9B3F8C2F289DD82C88C7BC95C4"),
            (AesEncrypt192EcbNative, AesDecrypt192EcbNative, false, true, "9B139FD002E6496EA2D5C73A2265E661"),
            (AesEncrypt256EcbNative, AesDecrypt256EcbNative, false, true, "F80DCDEDDBE5663BDB68F74AEDDB8EE3"),
            (AesEncrypt128CbcNative, AesDecrypt128CbcNative, true, true, "2ECA0077C5EA5768A0485AA522774792"),
            (AesEncrypt192CbcNative, AesDecrypt192CbcNative, true, true, "516391DB38E908ECA93AAB22870EC787"),
            (AesEncrypt256CbcNative, AesDecrypt256CbcNative, true, true, "5D0E22C1E77523AEF5C3E10B65653C8F"),
            (AesEncrypt128OfbNative, AesDecrypt128OfbNative, true, false, "0515A36BBF3DE0"),
            (AesEncrypt192OfbNative, AesDecrypt192OfbNative, true, false, "FE09DCCF14D458"),
            (AesEncrypt256OfbNative, AesDecrypt256OfbNative, true, false, "2E70FCAC0C0834"),
            (AesEncrypt128CfbNative, AesDecrypt128CfbNative, true, false, "0515A36BBF3DE0"),
            (AesEncrypt192CfbNative, AesDecrypt192CfbNative, true, false, "FE09DCCF14D458"),
            (AesEncrypt256CfbNative, AesDecrypt256CfbNative, true, false, "2E70FCAC0C0834"),
        ] {
            for (operation, input, expected, function) in [
                (encrypt, b"pingcap".to_vec(), decode_hex(ciphertext), "aes_encrypt"),
                (decrypt, decode_hex(ciphertext), b"pingcap".to_vec(), "aes_decrypt"),
            ] {
                for long_iv in [false, true] {
                    if long_iv && !iv_required {
                        continue;
                    }
                    let (result, observation) = observe_wide_math(|| evaluate_args_in(
                        operation, columns,
                        || Ok(if iv_required {
                            EvaluatedArgs::Bytes3([
                                Some(input.clone()), Some(b"1234567890123456".to_vec()),
                                Some(if long_iv { b"1234567890123456ignored suffix".to_vec() } else { b"1234567890123456".to_vec() }),
                            ])
                        } else {
                            EvaluatedArgs::Bytes2(Some(input.clone()), Some(b"1234567890123456".to_vec()))
                        }),
                        EvaluatedBytesResult::into_bytes,
                    ));
                    assert_eq!(result, Ok(Some(expected.clone())));
                    assert_wide_math_c4(observation);
                }
                if iv_required {
                    let (result, observation) = observe_wide_math(|| evaluate_args_in(
                        operation, columns,
                        || Ok(EvaluatedArgs::Bytes3([
                            Some(input.clone()), Some(b"1234567890123456".to_vec()), Some(vec![0; 15]),
                        ])),
                        EvaluatedBytesResult::into_bytes,
                    ));
                    assert_eq!(result, Err(EvalError::IncorrectArguments(format!(
                        "The initialization vector supplied to {function} is too short. Must be at least 16 bytes long"
                    ))));
                    assert_wide_math_c4(observation);
                }
                if !block_mode {
                    let (result, observation) = observe_wide_math(|| evaluate_args_in(
                        operation, columns,
                        || Ok(EvaluatedArgs::Bytes3([
                            Some(Vec::new()), Some(b"1234567890123456".to_vec()), Some(b"1234567890123456".to_vec()),
                        ])),
                        EvaluatedBytesResult::into_bytes,
                    ));
                    assert_eq!(result, Ok(Some(Vec::new())));
                    assert_wide_math_c4(observation);
                }
            }
            if block_mode {
                for invalid_ciphertext in [Vec::new(), b"corrupt".to_vec()] {
                    let (result, observation) = observe_wide_math(|| evaluate_args_in(
                        decrypt, columns,
                        || Ok(if iv_required {
                            EvaluatedArgs::Bytes3([
                                Some(invalid_ciphertext), Some(b"1234567890123456".to_vec()), Some(b"1234567890123456".to_vec()),
                            ])
                        } else {
                            EvaluatedArgs::Bytes2(Some(invalid_ciphertext), Some(b"1234567890123456".to_vec()))
                        }),
                        EvaluatedBytesResult::into_bytes,
                    ));
                    assert_eq!(result, Ok(None));
                    assert_wide_math_c4(observation);
                }
            }
        }
        let (result, observation) = observe_wide_math(|| evaluate_args_in(
            AesNullNative, columns, || Ok(EvaluatedArgs::NullWitness(None)),
            EvaluatedBytesResult::into_bytes,
        ));
        assert_eq!(result, Ok(None));
        assert_wide_math_c4(observation);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn aes_sdk_zero_slots_suppress_values_short_iv_errors_and_genuine_null() {
    use EvaluatedBytesOp::*;

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [
            AesEncrypt128EcbNative, AesEncrypt192EcbNative, AesEncrypt256EcbNative,
            AesDecrypt128EcbNative, AesDecrypt192EcbNative, AesDecrypt256EcbNative,
        ] {
            let (result, observation) = observe_wide_math(|| evaluate_args_in(
                operation, columns,
                || Ok(EvaluatedArgs::Bytes2(Some(b"pingcap".to_vec()), Some(b"1234567890123456".to_vec()))),
                EvaluatedBytesResult::into_bytes,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
        }
        for operation in [
            AesEncrypt128CbcNative, AesEncrypt192CbcNative, AesEncrypt256CbcNative,
            AesDecrypt128CbcNative, AesDecrypt192CbcNative, AesDecrypt256CbcNative,
            AesEncrypt128OfbNative, AesEncrypt192OfbNative, AesEncrypt256OfbNative,
            AesDecrypt128OfbNative, AesDecrypt192OfbNative, AesDecrypt256OfbNative,
            AesEncrypt128CfbNative, AesEncrypt192CfbNative, AesEncrypt256CfbNative,
            AesDecrypt128CfbNative, AesDecrypt192CfbNative, AesDecrypt256CfbNative,
        ] {
            for iv in [b"short".as_slice(), b"1234567890123456".as_slice()] {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    operation, columns,
                    || Ok(EvaluatedArgs::Bytes3([
                        Some(b"pingcap".to_vec()), Some(b"1234567890123456".to_vec()), Some(iv.to_vec()),
                    ])),
                    EvaluatedBytesResult::into_bytes,
                ));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
        let (result, observation) = observe_wide_math(|| evaluate_args_in(
            AesNullNative, columns, || Ok(EvaluatedArgs::NullWitness(None)),
            EvaluatedBytesResult::into_bytes,
        ));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn division_sdk_preserves_kinds_dispositions_and_raw_legacy_precision() {
    use tidb_datatype::Decimal;
    use NativeDecimalDivisionDisposition::{Ok as Exact, Overflow, Truncated, ZeroDivisor};

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [EvaluatedBytesOp::DivRealNative, EvaluatedBytesOp::DivRealLegacy] {
            for (right, expected) in [(2.0_f64, Some(2.75_f64.to_bits())), (-0.0, None)] {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    operation, columns,
                    || Ok(EvaluatedArgs::Ieee754Bits2 {
                        left: super::super::ReadyIeee754Arg::Value(Some(5.5_f64.to_bits())),
                        right: super::super::ReadyIeee754Arg::Value(Some(right.to_bits())),
                    }),
                    EvaluatedBytesResult::into_ieee754_bits,
                ));
                assert_eq!(result, Ok(expected));
                assert_wide_math_c4(observation);
            }
        }
        let (result, observation) = observe_wide_math(|| evaluate_args_in(
            EvaluatedBytesOp::DivRealNative, columns,
            || Ok(EvaluatedArgs::Ieee754Bits2 {
                left: super::super::ReadyIeee754Arg::Value(Some(f64::MAX.to_bits())),
                right: super::super::ReadyIeee754Arg::Value(Some(0.5_f64.to_bits())),
            }),
            EvaluatedBytesResult::into_ieee754_bits,
        ));
        assert_eq!(result, Err(EvalError::FloatOverflow));
        assert_wide_math_c4(observation);
        for (left, right, expected) in [(5.5, 2.0, Some(2.75)), (5.5, -0.0, None), (f64::MAX, 0.5, Some(f64::INFINITY))] {
            let (result, observation) = observe_wide_math(|| eval_legacy_real_arithmetic_in(
                BinaryArithmeticOperation::Divide, LegacyBinaryArgs::Values(left, right), columns,
            ));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        let wide = format!("{}.1234", "9".repeat(74));
        let negative_wide = format!("-1{}", "0".repeat(80));
        let negative_maximum = format!("-{}", "9".repeat(81));
        for (operation, effective_increment, quotient) in [
            (EvaluatedBytesOp::DivDecimalNative, 4, "0.3333"),
            (EvaluatedBytesOp::DivDecimalLegacy, 0, "0"),
        ] {
            // Native packets already contain the frontend's effective 0 -> 4;
            // the legacy profile must retain the original raw zero increment.
            for (left, right, frac_increment, disposition, expected) in [
                ("1", "3", effective_increment, Exact, Some(quotient)),
                ("1.0", "0", u32::MAX, ZeroDivisor, None),
                (wide.as_str(), "1", 0, Truncated, Some(wide.as_str())),
                (negative_wide.as_str(), "0.01", 0, Overflow, Some(negative_maximum.as_str())),
            ] {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    operation, columns,
                    || Ok(EvaluatedArgs::DecimalDivision {
                        left: Some(super::super::prepare_math_decimal(&Decimal::from_literal(left))?),
                        right: Some(super::super::prepare_math_decimal(&Decimal::from_literal(right))?),
                        frac_increment,
                    }),
                    |computed| match computed {
                        EvaluatedBytesResult::DecimalDivision { value, disposition } => Ok((value, disposition)),
                        _ => Err(result_kind_error().into_eval_error()),
                    },
                ));
                let (value, actual_disposition) = result.unwrap();
                assert_eq!(actual_disposition, disposition);
                assert_eq!(value.map(|value| value.to_string()), expected.map(str::to_owned));
                assert_wide_math_c4(observation);
            }
        }
        for (left, right, frac_increment, expected) in [
            ("1", "3", 0, Some("0")),
            ("1", "3", 4, Some("0.3333")),
            ("1.0", "0", u32::MAX, None),
            (wide.as_str(), "1", 0, Some(wide.as_str())),
            (negative_wide.as_str(), "0.01", 0, Some(negative_maximum.as_str())),
        ] {
            let (result, observation) = observe_wide_math(|| eval_legacy_decimal_division_in(
                LegacyBinaryArgs::Values(Decimal::from_literal(left), Decimal::from_literal(right)),
                frac_increment, columns,
            ));
            assert_eq!(result.unwrap().map(|value| value.to_string()), expected.map(str::to_owned));
            assert_wide_math_c4(observation);
        }
        for missing in [false, true] {
            let operation = if missing { EvaluatedBytesOp::BinaryArithmeticMissingLegacy } else { EvaluatedBytesOp::BinaryArithmeticNullNative };
            let (result, observation) = observe_wide_math(|| eval_legacy_decimal_division_in(
                if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) },
                u32::MAX, columns,
            ));
            assert_eq!(result, Ok(None));
            assert_wide_math_c4(observation);
            assert_eq!(scope.lease.borrow().as_ref().unwrap().worker.as_ref().unwrap().operation(), operation);
            let (result, observation) = observe_wide_math(|| eval_legacy_real_arithmetic_in(
                BinaryArithmeticOperation::Divide,
                if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns,
            ));
            assert_eq!(result, Ok(None));
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| eval_legacy_decimal_division_in(
            LegacyBinaryArgs::NullWitness(Some(0)), 0, columns,
        ));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert_eq!(observation.facade_entries, 0);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn division_sdk_zero_budget_and_precisionless_contracts_do_not_fake_sql_results() {
    use tidb_datatype::Decimal;

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for zero in [false, true] {
            let right = if zero { 0.0_f64 } else { 0.5 };
            for operation in [EvaluatedBytesOp::DivRealNative, EvaluatedBytesOp::DivRealLegacy] {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    operation, columns,
                    || Ok(EvaluatedArgs::Ieee754Bits2 {
                        left: super::super::ReadyIeee754Arg::Value(Some(f64::MAX.to_bits())),
                        right: super::super::ReadyIeee754Arg::Value(Some(right.to_bits())),
                    }),
                    EvaluatedBytesResult::into_ieee754_bits,
                ));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                assert_eq!(observation.facade_entries, 0);
            }
            for operation in [EvaluatedBytesOp::DivDecimalNative, EvaluatedBytesOp::DivDecimalLegacy] {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    operation, columns,
                    || Ok(EvaluatedArgs::DecimalDivision {
                        left: Some(super::super::prepare_math_decimal(&Decimal::from_literal("1.0"))?),
                        right: Some(super::super::prepare_math_decimal(&Decimal::from_literal(if zero { "0" } else { "3" }))?),
                        frac_increment: u32::MAX,
                    }),
                    |_| Ok(()),
                ));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
        for args in [LegacyBinaryArgs::Missing, LegacyBinaryArgs::NullWitness(None), LegacyBinaryArgs::Values(Decimal::from_literal("1"), Decimal::from_literal("0"))] {
            let (result, observation) = observe_wide_math(|| eval_legacy_decimal_division_in(args, 0, columns));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
        }
        for args in [LegacyBinaryArgs::Missing, LegacyBinaryArgs::NullWitness(None), LegacyBinaryArgs::Values(Decimal::from_literal("1"), Decimal::from_literal("3"))] {
            let (result, observation) = observe_wide_math(|| eval_legacy_decimal_arithmetic_in(
                BinaryArithmeticOperation::Divide, args, columns,
            ));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
            assert_eq!(observation.facade_entries, 0);
        }
        let (result, observation) = observe_wide_math(|| eval_arithmetic_decimal_fast_in(
            BinaryArithmeticOperation::Divide, None, None, columns,
        ));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert_eq!(observation.facade_entries, 0);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn modulo_sdk_preserves_computed_kinds_zero_and_legacy_presence() {
    use tidb_datatype::Decimal;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for operation in [
            EvaluatedBytesOp::ModIntSsNative,
            EvaluatedBytesOp::ModIntSuNative,
            EvaluatedBytesOp::ModIntUsNative,
            EvaluatedBytesOp::ModIntUuNative,
        ] {
            for (right, expected) in [(5, Datum::Int(2)), (0, Datum::Null)] {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    operation, columns, || Ok(EvaluatedArgs::Int2(Some(17), Some(right))),
                    EvaluatedBytesResult::into_int_datum,
                ));
                assert_eq!(result, Ok(expected));
                assert_wide_math_c4(observation);
            }
        }
        for (left, right, expected) in [
            ((1_i128 << 100) + 3, 1_i128 << 110, Some((1_i128 << 100) + 3)),
            (i128::MIN, 2, Some(0)),
            (-17, 5, Some(-2)),
            (17, 0, None),
        ] {
            let (result, observation) = observe_wide_math(|| eval_legacy_integer_arithmetic_in(
                LegacyIntegerArithmetic::Modulo, LegacyBinaryArgs::Values(left, right), columns,
            ));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        for operation in [EvaluatedBytesOp::ModRealNative, EvaluatedBytesOp::ModRealLegacy] {
            for (right, expected) in [(2.0_f64, Some(1.5_f64.to_bits())), (-0.0, None)] {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    operation, columns,
                    || Ok(EvaluatedArgs::Ieee754Bits2 {
                        left: super::super::ReadyIeee754Arg::Value(Some(5.5_f64.to_bits())),
                        right: super::super::ReadyIeee754Arg::Value(Some(right.to_bits())),
                    }),
                    EvaluatedBytesResult::into_ieee754_bits,
                ));
                assert_eq!(result, Ok(expected));
                assert_wide_math_c4(observation);
            }
        }
        let (result, observation) = observe_wide_math(|| evaluate_args_in(
            EvaluatedBytesOp::ModRealNative, columns,
            || Ok(EvaluatedArgs::Ieee754Bits2 {
                left: super::super::ReadyIeee754Arg::Value(Some(f64::INFINITY.to_bits())),
                right: super::super::ReadyIeee754Arg::Value(Some(2.0_f64.to_bits())),
            }),
            EvaluatedBytesResult::into_ieee754_bits,
        ));
        assert_eq!(result, Err(EvalError::FloatOverflow));
        assert_wide_math_c4(observation);
        for (left, right, expected) in [(5.5, 2.0, Some(1.5)), (5.5, -0.0, None)] {
            let (result, observation) = observe_wide_math(|| eval_legacy_real_arithmetic_in(
                BinaryArithmeticOperation::Modulo, LegacyBinaryArgs::Values(left, right), columns,
            ));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| eval_legacy_real_arithmetic_in(
            BinaryArithmeticOperation::Modulo,
            LegacyBinaryArgs::Values(f64::INFINITY, 2.0), columns,
        ));
        assert!(result.unwrap().unwrap().is_nan());
        assert_wide_math_c4(observation);
        for (right, expected) in [("2.00", Some("1.50")), ("0.00", None)] {
            let (result, observation) = observe_wide_math(|| eval_legacy_decimal_arithmetic_in(
                BinaryArithmeticOperation::Modulo,
                LegacyBinaryArgs::Values(Decimal::from_literal("5.50"), Decimal::from_literal(right)), columns,
            ));
            assert_eq!(result.unwrap().map(|value| value.to_string()), expected.map(str::to_owned));
            assert_wide_math_c4(observation);
        }
        for missing in [false, true] {
            let operation = if missing { EvaluatedBytesOp::BinaryArithmeticMissingLegacy } else { EvaluatedBytesOp::BinaryArithmeticNullNative };
            let (result, observation) = observe_wide_math(|| eval_legacy_integer_arithmetic_in(
                LegacyIntegerArithmetic::Modulo,
                if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns,
            ));
            assert_eq!(result, Ok(None));
            assert_wide_math_c4(observation);
            assert_eq!(scope.lease.borrow().as_ref().unwrap().worker.as_ref().unwrap().operation(), operation);
            let (result, observation) = observe_wide_math(|| eval_legacy_real_arithmetic_in(
                BinaryArithmeticOperation::Modulo,
                if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns,
            ));
            assert_eq!(result, Ok(None));
            assert_wide_math_c4(observation);
            let (result, observation) = observe_wide_math(|| eval_legacy_decimal_arithmetic_in(
                BinaryArithmeticOperation::Modulo,
                if missing { LegacyBinaryArgs::Missing } else { LegacyBinaryArgs::NullWitness(None) }, columns,
            ));
            assert_eq!(result, Ok(None));
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| eval_legacy_integer_arithmetic_in(
            LegacyIntegerArithmetic::Modulo, LegacyBinaryArgs::NullWitness(Some(0)), columns,
        ));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert_eq!(observation.facade_entries, 0);
    });
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn modulo_sdk_zero_budget_never_fakes_sql_null_or_overflow() {
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for zero in [false, true] {
            let right = if zero { 0 } else { 2 };
            let decimal = super::super::prepare_math_decimal(&tidb_datatype::Decimal::from_literal("5")).unwrap();
            let decimal_right = super::super::prepare_math_decimal(&tidb_datatype::Decimal::from_literal(if zero { "0" } else { "2" })).unwrap();
            for (operation, args) in [
                (EvaluatedBytesOp::ModIntSsNative, EvaluatedArgs::Int2(Some(5), Some(right))),
                (EvaluatedBytesOp::ModIntSuNative, EvaluatedArgs::Int2(Some(5), Some(right))),
                (EvaluatedBytesOp::ModIntUsNative, EvaluatedArgs::Int2(Some(5), Some(right))),
                (EvaluatedBytesOp::ModIntUuNative, EvaluatedArgs::Int2(Some(5), Some(right))),
                (EvaluatedBytesOp::ModInt128Legacy, EvaluatedArgs::Int1282(Some(5), Some(i128::from(right)))),
                (EvaluatedBytesOp::ModRealNative, EvaluatedArgs::Ieee754Bits2 {
                    left: super::super::ReadyIeee754Arg::Value(Some(f64::INFINITY.to_bits())),
                    right: super::super::ReadyIeee754Arg::Value(Some((right as f64).to_bits())),
                }),
                (EvaluatedBytesOp::ModRealLegacy, EvaluatedArgs::Ieee754Bits2 {
                    left: super::super::ReadyIeee754Arg::Value(Some(5.0_f64.to_bits())),
                    right: super::super::ReadyIeee754Arg::Value(Some((right as f64).to_bits())),
                }),
                (EvaluatedBytesOp::ModDecimalNative, EvaluatedArgs::Decimal2 { left: Some(decimal), right: Some(decimal_right) }),
                (EvaluatedBytesOp::BinaryArithmeticNullNative, EvaluatedArgs::NullWitness(None)),
                (EvaluatedBytesOp::BinaryArithmeticMissingLegacy, EvaluatedArgs::NoArgs),
            ] {
                let (result, observation) = observe_wide_math(|| evaluate_args_in(
                    operation, columns, || Ok(args), |_| Ok(()),
                ));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
        let (result, observation) = observe_wide_math(|| eval_arithmetic_decimal_fast_in(
            BinaryArithmeticOperation::Modulo, None, None, columns,
        ));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
        assert_eq!(observation.facade_entries, 0);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn unary_dispatch_keeps_metadata_boundaries_and_scope_admission() {
    use crate::expression::Expression;
    use crate::ops::Operand;
    use tidb_ast::UnaryOp;
    use tidb_datatype::{Collation, Decimal};

    let signed = Expression::Column(crate::column::Column::new(
        1,
        FieldType::new(FieldTypeCode::LongLong),
    ));
    let unsigned = Expression::Column(crate::column::Column::new(
        2,
        FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
    ));
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let run = |op, value, operand| {
            let (result, observation) =
                observe_wide_math(|| crate::ops::eval_unary(op, value, operand, columns));
            assert_wide_math_c4(observation);
            result
        };
        // The original PLUS branches return these exact value kinds/metadata.
        for input in [
            Datum::Null,
            Datum::Int(i64::MIN),
            Datum::UInt(u64::MAX),
            Datum::Real(-0.0),
            Datum::new_bytes([0xff, b'a']),
            Datum::new_collation_string([0xff, b'A'], Collation::Utf8Mb4GeneralCi),
        ] {
            let result = run(UnaryOp::Plus, input.clone(), Operand::Literal).unwrap();
            assert_eq!(
                std::mem::discriminant(&result),
                std::mem::discriminant(&input)
            );
            assert_eq!(result, input);
            if let (Datum::String(result), Datum::String(input)) = (&result, &input) {
                assert_eq!(result.bytes(), input.bytes());
                assert_eq!(result.collation(), input.collation());
            }
        }
        // Float32 is tagged f64 storage: this low payload bit is lost by f32.
        for (op, expected_bits) in [
            (UnaryOp::Plus, 0x3ff0_0000_0000_0001_u64),
            (UnaryOp::Minus, 0xbff0_0000_0000_0001_u64),
        ] {
            let result = run(
                op,
                Datum::Float32(f64::from_bits(0x3ff0_0000_0000_0001)),
                Operand::Literal,
            )
            .unwrap();
            let Datum::Float32(value) = result else {
                panic!("unary Float32 lost its original value kind")
            };
            assert_eq!(value.to_bits(), expected_bits);
        }
        let stamped = Decimal::from_literal("1.20").with_declared_shape(12, 2);
        for (op, text, shape) in [
            (UnaryOp::Plus, "1.20", Some((12, 2))),
            (UnaryOp::Minus, "-1.20", None),
        ] {
            let result = run(op, Datum::Decimal(stamped.clone()), Operand::Literal).unwrap();
            let Datum::Decimal(value) = result else {
                panic!("unary decimal lost its original value kind")
            };
            assert_eq!(value.to_string(), text);
            assert_eq!(
                (value.scale(), value.storage_scale()),
                (stamped.scale(), stamped.storage_scale())
            );
            assert_eq!(value.declared_shape(), shape);
        }
        // Source constant promotion versus the original column overflow policy.
        for (value, operand, expected) in [
            (Datum::Int(7), Operand::Literal, Ok(Datum::Int(-7))),
            (
                Datum::Int(i64::MIN),
                Operand::Literal,
                Ok(Datum::Decimal(Decimal::from_literal("9223372036854775808"))),
            ),
            (
                Datum::UInt(1_u64 << 63),
                Operand::Literal,
                Ok(Datum::Int(i64::MIN)),
            ),
            (
                Datum::UInt(u64::MAX),
                Operand::Literal,
                Ok(Datum::Decimal(Decimal::from_literal(
                    "-18446744073709551615",
                ))),
            ),
            (
                Datum::Int(i64::MIN),
                Operand::Expr(&signed),
                Err(EvalError::DataOutOfRange {
                    value: "BIGINT",
                    expression: "--9223372036854775808".to_owned(),
                }),
            ),
            (Datum::Int(7), Operand::Expr(&signed), Ok(Datum::Int(-7))),
            (
                Datum::UInt(u64::MAX),
                Operand::Expr(&unsigned),
                Err(EvalError::DataOutOfRange {
                    value: "BIGINT",
                    expression: "-18446744073709551615".to_owned(),
                }),
            ),
            (
                Datum::Int(i64::MIN),
                Operand::Expr(&unsigned),
                Ok(Datum::Int(i64::MIN)),
            ),
        ] {
            assert_eq!(run(UnaryOp::Minus, value, operand), expected);
        }
    });
    let current = scope_worker_observation(&scope);
    assert_eq!(current.2 + current.3, current.4);
    assert!(current.4 <= TEST_WORKER_CAP);
    assert!(!scope.busy.get());
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for op in [UnaryOp::Plus, UnaryOp::Minus] {
            let (result, observation) = observe_wide_math(|| {
                crate::ops::eval_unary(op, Datum::Null, Operand::Literal, columns)
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn regexp_dispatch_reuses_workers_and_actual_context_cache_owners() {
    use tidb_query_expr::{
        NativeCachedRegexp, NativeContextCache, NativeRegexpInvocation, NativeReplacementPart,
    };
    let patterns = NativeContextCache::<NativeCachedRegexp>::default();
    let replacements = NativeContextCache::<Vec<NativeReplacementPart>>::default();
    let invocation = NativeRegexpInvocation::new(&patterns, &replacements, 11, true, true);
    let shared = invocation.clone();
    assert!(patterns.get_cache(11).is_none());
    assert!(
        replacements.get_cache(11).is_none(),
        "binding and cloning handles do not initialize owners"
    );
    let replace = |invocation: NativeRegexpInvocation, columns: &dyn Columns| {
        evaluate_regexp_in(RegexpFunction::Replace, columns, || {
            Ok(EvaluatedArgs::RegexpReplace {
                invocation,
                text: b"fool food foo".to_vec(),
                pattern: b"foo(.?)".to_vec(),
                replacement: br"\0+\1".to_vec(),
                pos: 1,
                occurrence: 0,
                match_type: Vec::new(),
            })
        })
    };
    let like = |invocation: NativeRegexpInvocation,
                pattern: &[u8],
                match_type: &[u8],
                columns: &dyn Columns| {
        evaluate_regexp_in(RegexpFunction::Like, columns, || {
            Ok(EvaluatedArgs::RegexpLike {
                invocation,
                text: b"abc".to_vec(),
                pattern: pattern.to_vec(),
                match_type: match_type.to_vec(),
            })
        })
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Call the invocation clone first: it must fill the original owners.
        let (result, observation) = observe_wide_math(|| replace(shared, columns));
        assert_eq!(result, Ok(Datum::new_string("fool+l food+d foo+"))); // Old capture-replacement fixture.
        assert_wide_math_c4(observation);
        let pattern = patterns.get_cache(11).unwrap();
        let replacement = replacements.get_cache(11).unwrap();
        assert!(pattern.result.is_ok());
        let (result, observation) = observe_wide_math(|| replace(invocation, columns));
        assert_eq!(result, Ok(Datum::new_string("fool+l food+d foo+")));
        assert_wide_math_c4(observation);
        assert_eq!(observation.before_kernel_invocations, Some(1));
        assert_eq!(observation.after_kernel_invocations, Some(2));
        assert!(Arc::ptr_eq(&pattern, &patterns.get_cache(11).unwrap()));
        assert!(Arc::ptr_eq(
            &replacement,
            &replacements.get_cache(11).unwrap()
        ));

        let cloned_patterns = patterns.clone();
        let cloned_replacements = replacements.clone();
        assert!(cloned_patterns.get_cache(11).is_none());
        assert!(cloned_replacements.get_cache(11).is_none());
        let cloned_owner =
            NativeRegexpInvocation::new(&cloned_patterns, &cloned_replacements, 11, true, true);
        let (result, observation) = observe_wide_math(|| replace(cloned_owner, columns));
        assert_eq!(result, Ok(Datum::new_string("fool+l food+d foo+")));
        assert_wide_math_c4(observation);
        assert!(!Arc::ptr_eq(
            &pattern,
            &cloned_patterns.get_cache(11).unwrap()
        ));
        assert!(!Arc::ptr_eq(
            &replacement,
            &cloned_replacements.get_cache(11).unwrap()
        ));

        let switched = NativeRegexpInvocation::new(&patterns, &replacements, 12, true, true);
        assert!(
            Arc::ptr_eq(&pattern, &patterns.get_cache(11).unwrap()),
            "new-context binding does not evict before the worker"
        );
        assert!(patterns.get_cache(12).is_none());
        let (result, observation) = observe_wide_math(|| replace(switched, columns));
        assert_eq!(result, Ok(Datum::new_string("fool+l food+d foo+")));
        assert_wide_math_c4(observation);
        assert!(patterns.get_cache(11).is_none());
        assert!(replacements.get_cache(11).is_none());
        assert!(!Arc::ptr_eq(&pattern, &patterns.get_cache(12).unwrap()));
        assert!(!Arc::ptr_eq(
            &replacement,
            &replacements.get_cache(12).unwrap()
        ));
        let current = scope_worker_observation(&scope);
        assert_eq!(current.1, 4);
        assert_eq!(current.2 + current.3, current.4);
        assert!(current.4 <= TEST_WORKER_CAP);
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 1);

        let failed = NativeRegexpInvocation::new(&patterns, &replacements, 13, true, false);
        let (result, observation) = observe_wide_math(|| like(failed.clone(), b"(", b"", columns));
        assert_eq!(
            result,
            Err(EvalError::Unsupported("invalid regular expression pattern"))
        );
        assert_wide_math_c4(observation);
        let failure = patterns.get_cache(13).unwrap();
        assert!(failure.result.is_err());
        let (result, observation) = observe_wide_math(|| like(failed, b"(", b"", columns));
        assert_eq!(
            result,
            Err(EvalError::Unsupported("invalid regular expression pattern"))
        );
        assert_wide_math_c4(observation);
        assert!(
            Arc::ptr_eq(&failure, &patterns.get_cache(13).unwrap()),
            "ordinary compile failure is a genuine warm cache hit"
        );
        let bypass = NativeRegexpInvocation::new(&patterns, &replacements, 13, false, false);
        let (result, observation) = observe_wide_math(|| like(bypass, b"AbC", b"i", columns));
        assert_eq!(result, Ok(Datum::Int(1))); // Old native regexp_like scalar row.
        assert_wide_math_c4(observation);
        assert!(
            Arc::ptr_eq(&failure, &patterns.get_cache(13).unwrap()),
            "disabled caching neither reads the cached failure nor overwrites it"
        );
        assert_eq!(scope_worker_observation(&scope).1, 3);
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 2);
    });
    assert!(!scope.busy.get());
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
}

#[test]
fn regexp_dispatch_keeps_demand_and_refusal_ahead_of_cache_writes() {
    use tidb_query_expr::{NativeCachedRegexp, NativeContextCache, NativeReplacementPart};
    let patterns = NativeContextCache::<NativeCachedRegexp>::default();
    let replacements = NativeContextCache::<Vec<NativeReplacementPart>>::default();
    let call = |name: &str, values: &[Datum], context_id: u64, columns: &dyn Columns| {
        crate::builtin_ext::regexp::dispatch_with_cache_in(
            name,
            values,
            context_id,
            true,
            true,
            &patterns,
            &replacements,
            columns,
        )
        .unwrap()
    };
    let invalid_option = vec![
        Datum::new_string("abc"),
        Datum::new_string("("),
        Datum::Int(1),
        Datum::Int(1),
        Datum::Int(2),
        Datum::new_bytes([0xff]),
    ];
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_wide_math(|| call("REGEXP_INSTR", &invalid_option, 77, columns));
        assert_eq!(result, Err(EvalError::Unsupported("Incorrect arguments to regexp_instr: return_option must be 1 or 0")));
        assert_wide_math_c4(observation);
        assert!(patterns.get_cache(77).is_none(), "invalid return_option precedes compilation and does not coerce the bad UTF8 flag suffix");
        for (name, operation, values) in [
            ("REGEXP_INSTR", EvaluatedBytesOp::RegexpNullIntNative, vec![Datum::new_string("abc"), Datum::new_string("("), Datum::Int(1), Datum::Int(1), Datum::Null, Datum::MinNotNull]),
            ("REGEXP_SUBSTR", EvaluatedBytesOp::RegexpNullBytesNative, vec![Datum::new_string("abc"), Datum::new_string("("), Datum::Null, Datum::MinNotNull]),
        ] {
            let (result, observation) = observe_wide_math(|| call(name, &values, 77, columns));
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
            assert_eq!(scope.lease.borrow().as_ref().unwrap().worker.as_ref().unwrap().operation(), operation);
            assert!(patterns.get_cache(77).is_none());
            assert!(replacements.get_cache(77).is_none());
        }
        // Old source literals; only the real shared worker fills owners.
        let values = [Datum::new_string("abc"), Datum::new_string("bc")];
        let (result, observation) = observe_wide_math(|| call("REGEXP_SUBSTR", &values, 77, columns));
        assert_eq!(result, Ok(Datum::new_string("bc")));
        assert_wide_math_c4(observation);
        let cached = patterns.get_cache(77).unwrap();
        assert!(cached.result.is_ok());
        let (result, observation) = observe_wide_math(|| call("REGEXP_INSTR", &values, 77, columns));
        assert_eq!(result, Ok(Datum::Int(2)));
        assert_wide_math_c4(observation);
        assert!(Arc::ptr_eq(&cached, &patterns.get_cache(77).unwrap()));
        assert!(replacements.get_cache(77).is_none());
        for (case_insensitive, text, pattern, operation, expected) in [
            (Some(false), Some(b"abc".to_vec()), Some(b"AbC".to_vec()), EvaluatedBytesOp::RegexpLikeLegacyBinNative, Datum::Int(0)),
            (Some(true), Some(b"abc".to_vec()), Some(b"AbC".to_vec()), EvaluatedBytesOp::RegexpLikeLegacyCiNative, Datum::Int(1)),
            (None, None, Some(b"(".to_vec()), EvaluatedBytesOp::RegexpNullIntNative, Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| crate::eval_regexp_legacy_ready_in(columns, crate::RegexpLegacyInput::Values { text, pattern, case_insensitive }));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
            assert_eq!(scope.lease.borrow().as_ref().unwrap().worker.as_ref().unwrap().operation(), operation, "legacy SQL NULL uses its real witness without a collation decision");
        }
    });
    scope_worker_observation(&scope);
    drop(scope);
    execution.close();
    let cached = patterns.get_cache(77).unwrap();
    for closed in [false, true] {
        let slots = usize::from(closed);
        let owner = AsciiPoolOwner::new(test_policy(slots, slots)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        if closed {
            execution.close();
        }
        let class = if closed {
            crate::ExpressionAdapterFailureClass::PoolClosed
        } else {
            crate::ExpressionAdapterFailureClass::PoolResource
        };
        scope.with_columns(&crate::NoColumns, |columns| {
            for (name, values) in [
                ("REGEXP_REPLACE", vec![Datum::new_string("abc abd abe"), Datum::new_string("ab."), Datum::new_string("cz")]),
                ("REGEXP_INSTR", invalid_option.clone()),
                ("REGEXP_SUBSTR", vec![Datum::new_string("abc"), Datum::new_string("("), Datum::Null]),
            ] {
                let (result, observation) = observe_wide_math(|| call(name, &values, 88, columns));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == class));
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
                assert!(Arc::ptr_eq(&cached, &patterns.get_cache(77).unwrap()), "binding a refused new context must not evict the live cache entry");
                assert!(patterns.get_cache(88).is_none());
                assert!(replacements.get_cache(88).is_none());
            }
        });
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
        drop(scope);
        execution.close();
    }
}

#[test]
fn vector_dispatch_keeps_owned_vectors_text_metadata_and_routes() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let retained = scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_wide_math(|| {
            dispatch_bytes_family(
                EvaluatedBytesOp::VecFromTextNative,
                &Datum::new_string("[1,2]"),
                columns,
            )
        });
        let Datum::VectorFloat32(retained) = result.unwrap() else {
            panic!("FROM_TEXT must return an owned native vector, not transport bytes")
        };
        assert_eq!(retained.elements(), [1.0, 2.0]);
        assert_eq!(
            (retained.elements().as_ptr() as usize) % std::mem::align_of::<f32>(),
            0
        );
        assert_wide_math_c4(observation);
        let input = Datum::VectorFloat32(retained.clone());
        let (result, observation) = observe_wide_math(|| {
            dispatch_bytes_family(EvaluatedBytesOp::VecAsTextNative, &input, columns)
        });
        let Datum::String(text) = result.unwrap() else {
            panic!("AS_TEXT preserves String metadata")
        };
        assert_eq!(text.bytes(), b"[1,2]");
        assert_eq!(text.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
        assert_wide_math_c4(observation);
        let (result, observation) = observe_wide_math(|| {
            dispatch_bytes_family(EvaluatedBytesOp::VecDimsNative, &input, columns)
        });
        assert_eq!(result, Ok(Datum::Int(2)));
        assert_wide_math_c4(observation);
        let function = crate::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new("VEC_FROM_TEXT"),
            FieldType::new(FieldTypeCode::VectorFloat32).with_flen(2),
            vec![crate::expression::Expression::Constant(Constant::new(
                Datum::new_string("[1.1,2.2]"),
                FieldType::new(FieldTypeCode::VarString),
            ))],
        );
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        let Datum::VectorFloat32(value) = result.unwrap() else {
            panic!("typed FROM_TEXT retains VectorFloat32")
        };
        // Fixed raw f32 values from the original vector_endianess fixture.
        assert_eq!(
            value
                .elements()
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            [0x3f8c_cccd, 0x400c_cccd]
        );
        assert_wide_math_c4(observation);
        let (result, observation) =
            observe_wide_math(|| calendar_fields_ast("VEC_FROM_TEXT('[1,2]')", columns));
        let Datum::VectorFloat32(value) = result.unwrap() else {
            panic!("AST FROM_TEXT retains VectorFloat32")
        };
        assert_eq!(value.elements(), [1.0, 2.0]);
        assert_wide_math_c4(observation);
        assert_eq!(
            scope_worker_observation(&scope).1,
            2,
            "typed and AST calls share the actual current worker"
        );
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 4);
        for operation in [
            EvaluatedBytesOp::VecFromTextNative,
            EvaluatedBytesOp::VecAsTextNative,
            EvaluatedBytesOp::VecDimsNative,
        ] {
            let (result, observation) =
                observe_wide_math(|| dispatch_bytes_family(operation, &Datum::Null, columns));
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
        retained
    });
    let current = scope_worker_observation(&scope);
    assert_eq!(current.2 + current.3, current.4);
    assert!(current.4 <= TEST_WORKER_CAP);
    assert!(!scope.busy.get());
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
    assert_eq!(
        retained.elements(),
        [1.0, 2.0],
        "the returned aligned value owns its elements after worker replacement and close"
    );
}

#[test]
fn vector_dispatch_keeps_metric_values_raw_bits_and_null_demand() {
    use tidb_datatype::VectorFloat32;
    let vector = |values: Vec<f32>| Datum::VectorFloat32(VectorFloat32::must_create(values));
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Original builtin_ext/vec.rs fixed metrics, including zero-norm NaN.
        for (name, operation, values, expected) in [
            (
                "VEC_L1_DISTANCE",
                EvaluatedBytesOp::VecL1DistanceNative,
                vec![vector(vec![1.0, 2.0]), vector(vec![3.0, 5.0])],
                Datum::Real(5.0),
            ),
            (
                "VEC_L2_DISTANCE",
                EvaluatedBytesOp::VecL2DistanceNative,
                vec![vector(vec![0.0, 0.0]), vector(vec![3.0, 4.0])],
                Datum::Real(5.0),
            ),
            (
                "VEC_NEGATIVE_INNER_PRODUCT",
                EvaluatedBytesOp::VecNegativeInnerProductNative,
                vec![vector(vec![1.0, 2.0]), vector(vec![3.0, 4.0])],
                Datum::Real(-11.0),
            ),
            (
                "VEC_COSINE_DISTANCE",
                EvaluatedBytesOp::VecCosineDistanceNative,
                vec![vector(vec![0.0]), vector(vec![1.0])],
                Datum::Null,
            ),
            (
                "VEC_L2_NORM",
                EvaluatedBytesOp::VecL2NormNative,
                vec![vector(vec![3.0, 4.0])],
                Datum::Real(5.0),
            ),
            (
                "VEC_L2_NORM",
                EvaluatedBytesOp::VecL2NormNative,
                vec![Datum::Null],
                Datum::Null,
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values(name, &values, columns).unwrap()
            });
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        // Raw mutation is a source-supported domain; create/must_create would
        // reject nonfinite components before they could exercise transport.
        let mut raw = VectorFloat32::init(1);
        raw.elements_mut()[0] = f32::from_bits(0x7fc0_1234);
        let (result, observation) = observe_wide_math(|| {
            crate::func::eval_func_values(
                "VEC_L1_DISTANCE",
                &[Datum::VectorFloat32(raw.clone()), vector(vec![0.0])],
                columns,
            )
            .unwrap()
        });
        assert_eq!(
            result,
            Ok(Datum::Null),
            "raw NaN becomes NULL in the actual real-metric worker"
        );
        assert_wide_math_c4(observation);
        raw.elements_mut()[0] = f32::INFINITY;
        let (result, observation) = observe_wide_math(|| {
            dispatch_bytes_family(
                EvaluatedBytesOp::VecL2NormNative,
                &Datum::VectorFloat32(raw.clone()),
                columns,
            )
        });
        assert_eq!(
            result,
            Ok(Datum::Real(f64::INFINITY)),
            "infinity is not rejected or folded into NULL"
        );
        assert_wide_math_c4(observation);
        raw.elements_mut()[0] = f32::from_bits(0x8000_0000);
        let (result, observation) = observe_wide_math(|| {
            crate::func::eval_func_values(
                "VEC_NEGATIVE_INNER_PRODUCT",
                &[Datum::VectorFloat32(raw), vector(vec![1.0])],
                columns,
            )
            .unwrap()
        });
        let Datum::Real(value) = result.unwrap() else {
            panic!("negative inner product keeps its real carrier")
        };
        assert_eq!(
            value.to_bits(),
            0x8000_0000_0000_0000,
            "the original zero accumulation followed by negation returns -0"
        );
        assert_wide_math_c4(observation);
        for (name, values) in [
            ("VEC_L1_DISTANCE", [Datum::Null, Datum::MinNotNull]),
            ("VEC_L2_DISTANCE", [vector(vec![1.0]), Datum::Null]),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values(name, &values, columns).unwrap()
            });
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                EvaluatedBytesOp::VecRealNullNative
            );
        }
        let (result, observation) = observe_wide_math(|| {
            crate::func::eval_func_values(
                "VEC_L2_DISTANCE",
                &[Datum::new_string("[-1e39,1e39]"), Datum::Null],
                columns,
            )
            .unwrap()
        });
        assert_eq!(
            result,
            Err(EvalError::Vector(
                "value -1e+39 out of range for float32".to_owned()
            )),
            "actual right NULL does not skip left coercion"
        );
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    scope_worker_observation(&scope);
    drop(scope);
    execution.close();
}

#[test]
fn vector_dispatch_keeps_actual_vector_errors_behind_admission() {
    use tidb_datatype::VectorFloat32;
    let left = Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0]));
    let right = Datum::VectorFloat32(VectorFloat32::must_create(vec![1.0, 2.0]));
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_wide_math(|| {
            dispatch_bytes_family(
                EvaluatedBytesOp::VecFromTextNative,
                &Datum::new_string("[-1e39,1e39]"),
                columns,
            )
        });
        assert_eq!(
            result,
            Err(EvalError::Vector(
                "value -1e+39 out of range for float32".to_owned()
            ))
        );
        assert_wide_math_c4(observation);
        let (result, observation) = observe_wide_math(|| {
            dispatch_bytes_family(
                EvaluatedBytesOp::VecFromTextNative,
                &Datum::new_bytes([0xff]),
                columns,
            )
        });
        assert!(
            matches!(result, Err(EvalError::Vector(_))),
            "strict UTF8 validation belongs to the FROM_TEXT worker, not its ETString guard"
        );
        assert_wide_math_c4(observation);
        for name in [
            "VEC_L1_DISTANCE",
            "VEC_L2_DISTANCE",
            "VEC_NEGATIVE_INNER_PRODUCT",
            "VEC_COSINE_DISTANCE",
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values(name, &[left.clone(), right.clone()], columns)
                    .unwrap()
            });
            assert_eq!(
                result,
                Err(EvalError::Vector(
                    "vectors have different dimensions: 1 and 2".to_owned()
                ))
            );
            assert_wide_math_c4(observation);
            scope_worker_observation(&scope); // Genuine SQL causes leave a healthy worker.
        }
    });
    drop(scope);
    execution.close();
    for closed in [false, true] {
        let slots = usize::from(closed);
        let owner = AsciiPoolOwner::new(test_policy(slots, slots)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        if closed {
            execution.close();
        }
        let class = if closed {
            crate::ExpressionAdapterFailureClass::PoolClosed
        } else {
            crate::ExpressionAdapterFailureClass::PoolResource
        };
        scope.with_columns(&crate::NoColumns, |columns| {
            for (name, values) in [
                ("VEC_FROM_TEXT", vec![Datum::new_string("[-1e39,1e39]")]),
                ("VEC_L2_DISTANCE", vec![left.clone(), right.clone()]),
                ("VEC_L1_DISTANCE", vec![Datum::Null, Datum::MinNotNull]),
            ] {
                let (result, observation) = observe_wide_math(|| crate::func::eval_func_values(name, &values, columns).unwrap());
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == class), "a source-shaped vector error cannot mask infrastructure refusal");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
            let (result, observation) = observe_wide_math(|| crate::func::eval_func_values("VEC_L2_DISTANCE", &[Datum::new_string("[-1e39,1e39]"), Datum::Null], columns).unwrap());
            assert_eq!(result, Err(EvalError::Vector("value -1e+39 out of range for float32".to_owned())), "the original frontend coercion error still precedes admission");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        });
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
        drop(scope);
        execution.close();
    }
}

#[test]
fn crypt_hash_format_dispatch_keeps_crypt_bytes_metadata_and_null_demand() {
    // Original encrypt/crypt.rs and crypto.rs fixture, not a round-trip oracle.
    let crypt = [0x2c_u8, 0x35, 0xb5, 0xa4, 0xad, 0xf3, 0x91];
    let password = Datum::new_string("1234567890123456");
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        assert_eq!(
            columns.connection_charset_info(),
            ("utf8mb4", "utf8mb4_bin")
        );
        for (data, password, expected) in [
            (
                Datum::new_string("pingcap"),
                password.clone(),
                crypt.to_vec(),
            ),
            (Datum::new_string(""), Datum::new_string(""), Vec::new()),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values("DECODE", &[data, password], columns).unwrap()
            });
            let Datum::String(value) = result.unwrap() else {
                panic!("SQL crypt packs raw bytes as a connection string, not binary Datum")
            };
            assert_eq!(value.bytes(), expected);
            assert_eq!(value.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                EvaluatedBytesOp::SqlDecodeNative
            );
        }
        let (result, observation) = observe_wide_math(|| {
            calendar_fields_ast("DECODE('pingcap', '1234567890123456')", columns)
        });
        assert_eq!(result, Ok(Datum::new_string(crypt.to_vec())));
        assert_wide_math_c4(observation);
        let current = scope_worker_observation(&scope);
        assert_eq!(current.1, 3);
        assert_eq!(current.2 + current.3, current.4);
        assert!(current.4 <= TEST_WORKER_CAP);
        assert_eq!(
            owner.snapshot().unwrap().factory_attempts,
            1,
            "AST and values reuse the actual scoped worker"
        );
        let function = crate::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new("ENCODE"),
            FieldType::new(FieldTypeCode::VarString),
            vec![
                crate::expression::Expression::Constant(Constant::new(
                    Datum::new_bytes(crypt),
                    FieldType::new(FieldTypeCode::VarString)
                        .with_collation(tidb_datatype::Collation::Binary),
                )),
                crate::expression::Expression::Constant(Constant::new(
                    password.clone(),
                    FieldType::new(FieldTypeCode::VarString),
                )),
            ],
        );
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::new_string("pingcap")));
        assert_wide_math_c4(observation);
        assert_eq!(
            scope
                .lease
                .borrow()
                .as_ref()
                .unwrap()
                .worker
                .as_ref()
                .unwrap()
                .operation(),
            EvaluatedBytesOp::SqlEncodeNative
        );
        for (name, values) in [
            ("DECODE", [Datum::Null, Datum::MinNotNull]),
            ("ENCODE", [Datum::new_bytes(crypt), Datum::Null]),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values(name, &values, columns).unwrap()
            });
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                EvaluatedBytesOp::SqlCryptNullNative
            );
        }
        for values in [
            [Datum::MinNotNull, Datum::Null],
            [Datum::new_bytes(crypt), Datum::MinNotNull],
        ] {
            let before = owner.snapshot().unwrap();
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values("ENCODE", &values, columns).unwrap()
            });
            assert_eq!(
                result,
                Err(EvalError::Unsupported("range sentinel string argument"))
            );
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert_eq!(owner.snapshot().unwrap(), before);
        }
    });
    assert!(!scope.busy.get());
    assert!(!scope.poisoned.get());
    drop(scope);
    execution.close();
    for closed in [false, true] {
        let slots = usize::from(closed);
        let owner = AsciiPoolOwner::new(test_policy(slots, slots)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        if closed {
            execution.close();
        }
        let class = if closed {
            crate::ExpressionAdapterFailureClass::PoolClosed
        } else {
            crate::ExpressionAdapterFailureClass::PoolResource
        };
        scope.with_columns(&crate::NoColumns, |columns| {
            let (result, observation) = observe_wide_math(|| crate::func::eval_func_values("DECODE", &[Datum::Null, Datum::MinNotNull], columns).unwrap());
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == class), "actual NULL skips password coercion, not admission");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            let (result, observation) = observe_wide_math(|| crate::func::eval_func_values("DECODE", &[Datum::MinNotNull, Datum::Null], columns).unwrap());
            assert_eq!(result, Err(EvalError::Unsupported("range sentinel string argument")), "data preparation fails before admission despite the later NULL");
            assert_eq!(observation.facade_entries, 0);
        });
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
        drop(scope);
        execution.close();
    }
}

#[test]
fn crypt_hash_format_dispatch_keeps_unsigned_hashes_and_cast_warnings() {
    let native = ConstructTimeWarnings::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        // Old native misc/Vitess literals; no new hash provider computes wants.
        for (operation, input, expected) in [
            (
                EvaluatedBytesOp::TidbShardNative,
                Datum::UInt(u64::MAX),
                81_u64,
            ),
            (EvaluatedBytesOp::TidbShardNative, Datum::Real(1.9), 143),
            (
                EvaluatedBytesOp::VitessHashNative,
                Datum::Int(0),
                10_134_873_677_816_210_343,
            ),
        ] {
            let (result, observation) =
                observe_wide_math(|| dispatch_bytes_family(operation, &input, columns));
            let Datum::UInt(value) = result.unwrap() else {
                panic!("hash carrier bits must retain the original unsigned result")
            };
            assert_eq!(value, expected);
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        assert!(native.0.borrow().is_empty());
        for (name, input, expected, warning) in [
            (
                "TIDB_SHARD",
                "1.9",
                214_u64,
                "Truncated incorrect INTEGER value: '1.9'",
            ),
            (
                "VITESS_HASH",
                "18446744073709551616",
                3_843_066_582_818_235_473,
                "Truncated incorrect INTEGER value: '18446744073709551616'",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values(name, &[Datum::new_string(input)], columns).unwrap()
            });
            assert_eq!(result, Ok(Datum::UInt(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(
                native.0.replace(Vec::new()),
                [(1292, warning.to_owned(), true)]
            );
        }
        for operation in [
            EvaluatedBytesOp::TidbShardNative,
            EvaluatedBytesOp::VitessHashNative,
        ] {
            let (result, observation) =
                observe_wide_math(|| dispatch_bytes_family(operation, &Datum::Null, columns));
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
        let (result, observation) =
            observe_wide_math(|| calendar_fields_ast("TIDB_SHARD(1)", columns));
        assert_eq!(result, Ok(Datum::UInt(214)));
        assert_wide_math_c4(observation);
        let function = crate::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new("VITESS_HASH"),
            FieldType::new(FieldTypeCode::LongLong).with_unsigned(true),
            vec![crate::expression::Expression::Constant(Constant::new(
                Datum::Int(0),
                FieldType::new(FieldTypeCode::LongLong),
            ))],
        );
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(
            result,
            Ok(Datum::UInt(10_134_873_677_816_210_343)),
            "a high result bit is not signed SQL overflow"
        );
        assert_wide_math_c4(observation);
        assert!(native.0.borrow().is_empty());
    });
    scope_worker_observation(&scope);
    drop(scope);
    execution.close();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for name in ["TIDB_SHARD", "VITESS_HASH"] {
            let (result, observation) = observe_wide_math(|| crate::func::eval_func_values(name, &[Datum::new_string("18446744073709551614")], columns).unwrap());
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert_eq!(native.0.replace(Vec::new()), [(8030, "Cast to signed converted positive out-of-range integer to its negative complement".to_owned(), true)]);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn crypt_hash_format_dispatch_keeps_format_bits_and_source_units() {
    let function = crate::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new("FORMAT_NANO_TIME"),
        FieldType::new(FieldTypeCode::VarString),
        vec![crate::expression::Expression::Constant(Constant::new(
            Datum::Real(-0.0),
            FieldType::new(FieldTypeCode::Double),
        ))],
    );
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Exact old info.rs unit/scientific/-0 literals, never provider output.
        for (operation, input, expected) in [
            (EvaluatedBytesOp::FormatBytesNative, -0.0, "0 bytes"),
            (EvaluatedBytesOp::FormatBytesNative, 2048.0, "2.00 KiB"),
            (
                EvaluatedBytesOp::FormatBytesNative,
                287_952_852_482_075_252_752_429_875.0,
                "2.50e+08 EiB",
            ),
            (EvaluatedBytesOp::FormatNanoTimeNative, -0.0, "0 ns"),
            (EvaluatedBytesOp::FormatNanoTimeNative, 2000.0, "2.00 us"),
            (
                EvaluatedBytesOp::FormatNanoTimeNative,
                4_827_524_825_702_572_425_242_552.0,
                "5.59e+10 d",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                dispatch_bytes_family(operation, &Datum::Real(input), columns)
            });
            let Datum::String(value) = result.unwrap() else {
                panic!("formatted units keep their String result domain")
            };
            assert_eq!(value.bytes(), expected.as_bytes());
            assert_eq!(value.collation(), tidb_datatype::Collation::Utf8Mb4Bin);
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        for operation in [
            EvaluatedBytesOp::FormatBytesNative,
            EvaluatedBytesOp::FormatNanoTimeNative,
        ] {
            let (result, observation) =
                observe_wide_math(|| dispatch_bytes_family(operation, &Datum::Null, columns));
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
        let (result, observation) =
            observe_wide_math(|| calendar_fields_ast("FORMAT_BYTES(2048)", columns));
        assert_eq!(result, Ok(Datum::new_string("2.00 KiB")));
        assert_wide_math_c4(observation);
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::new_string("0 ns")));
        assert_wide_math_c4(observation);
    });
    scope_worker_observation(&scope);
    execution.close();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolClosed));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    drop(scope);
}

#[test]
fn uuid_translate_dispatch_preserves_uuid_spellings_and_timestamp_carriers() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Original misc.rs fixtures: raw 36/32/45/38-byte spellings. The last
        // wrapper is deliberately not accepted by a narrower braced parser.
        for text in [
            "6ccd780c-baba-1026-9564-5b8c656024db",
            "6ccd780cbaba102695645b8c656024db",
            "urn:uuid:6ccd780c-baba-1026-9564-5b8c656024db",
            "{99a9ad03-5298-11ec-8f5c-00ff90147ac3*",
        ] {
            for operation in [EvaluatedBytesOp::IsUuidNative, EvaluatedBytesOp::UuidVersionNative] {
                let (result, observation) = observe_wide_math(|| {
                    dispatch_bytes_family(operation, &Datum::new_string(text), columns)
                });
                assert_eq!(result, Ok(Datum::Int(1)), "{operation:?}: {text}");
                assert_wide_math_c4(observation);
            }
        }
        let mut raw_wrapper = vec![0xff];
        raw_wrapper.extend_from_slice(b"99a9ad03-5298-11ec-8f5c-00ff90147ac3*");
        for (input, expected) in [
            (Datum::Bytes(raw_wrapper.clone()), 1),
            (Datum::new_string(raw_wrapper.clone()), 1),
            (Datum::new_bytes([0xff]), 0),
            (Datum::new_bytes(b" 99a9ad03-5298-11ec-8f5c-00ff90147ac3\xff"), 0),
            (Datum::new_string(" 6ccd780c-baba-1026-9564-5b8c656024db "), 0),
        ] {
            let (result, observation) = observe_wide_math(|| {
                dispatch_bytes_family(EvaluatedBytesOp::IsUuidNative, &input, columns)
            });
            assert_eq!(result, Ok(Datum::Int(expected)), "raw parsing follows the lossy trim check, not lossy replacement of the parsed bytes");
            assert_wide_math_c4(observation);
        }
        for operation in [EvaluatedBytesOp::UuidVersionNative, EvaluatedBytesOp::UuidTimestampNative] {
            for (input, message) in [
                (Datum::Bytes(raw_wrapper.clone()), "invalid UTF-8 byte datum"),
                (Datum::new_string(raw_wrapper.clone()), "invalid UTF-8 string datum"),
            ] {
                let before = owner.snapshot().unwrap();
                let (result, observation) = observe_wide_math(|| {
                    dispatch_bytes_family(operation, &input, columns)
                });
                assert_eq!(result, Err(EvalError::Unsupported(message)));
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
                assert_eq!(owner.snapshot().unwrap(), before);
            }
        }
        // Fixed source timestamp expectations, never computed by a provider.
        for (text, expected) in [
            ("5f13f854-d74a-11f0-9b7a-0ae0156bd76b", "1765537487.118139"),
            ("1f0e48c1-7860-69cc-9b3f-35f89c103d4d", "1766995078.970004"),
            ("019b1440-87b7-7380-ab00-ce413e795004", "1765571332.023000"),
            ("6ccd780cbaba102695645b8c656024db", "-11129156903.290674"),
        ] {
            let (result, observation) = observe_wide_math(|| {
                dispatch_bytes_family(EvaluatedBytesOp::UuidTimestampNative, &Datum::new_string(text), columns)
            });
            let Datum::Decimal(value) = result.unwrap() else {
                panic!("UUID_TIMESTAMP must retain its Decimal carrier")
            };
            assert_eq!(value.to_string(), expected);
            assert_eq!(value.scale(), 6);
            assert_wide_math_c4(observation);
        }
        for text in [
            "a3e3b4a1-ea6d-471e-9860-8303a8b261f6",
            "00000000-0000-0000-0000-000000000000",
        ] {
            let (result, observation) = observe_wide_math(|| {
                dispatch_bytes_family(EvaluatedBytesOp::UuidTimestampNative, &Datum::new_string(text), columns)
            });
            assert_eq!(result, Ok(Datum::Null), "valid non-timestamp UUIDs return worker NULL");
            assert_wide_math_c4(observation);
        }
        for operation in [
            EvaluatedBytesOp::IsUuidNative,
            EvaluatedBytesOp::UuidVersionNative,
            EvaluatedBytesOp::UuidTimestampNative,
        ] {
            let (result, observation) = observe_wide_math(|| {
                dispatch_bytes_family(operation, &Datum::Null, columns)
            });
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| {
            calendar_fields_ast("UUID_VERSION('{99a9ad03-5298-11ec-8f5c-00ff90147ac3*')", columns)
        });
        assert_eq!(result, Ok(Datum::Int(1)));
        assert_wide_math_c4(observation);
        let function = crate::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new("UUID_TIMESTAMP"),
            FieldType::new(FieldTypeCode::NewDecimal).with_flen(18).with_decimal(6),
            vec![crate::expression::Expression::Constant(Constant::new(
                Datum::new_string("6ccd780cbaba102695645b8c656024db"),
                FieldType::new(FieldTypeCode::VarString),
            ))],
        );
        let (result, observation) = observe_wide_math(|| {
            function.eval(columns, tidb_chunk::row::Row::empty())
        });
        let Datum::Decimal(value) = result.unwrap() else {
            panic!("typed UUID_TIMESTAMP must retain Decimal")
        };
        assert_eq!(value.to_string(), "-11129156903.290674");
        assert_eq!(value.scale(), 6);
        assert_wide_math_c4(observation);
    });
    assert!(!scope.busy.get());
    assert!(!scope.poisoned.get());
    scope_worker_observation(&scope); // Checks the actual current worker's health/storage.
    drop(scope);
    execution.close();
}

#[test]
fn uuid_translate_dispatch_keeps_parse_swap_sequential_and_reuses_real_workers() {
    let canonical = "6ccd780c-baba-1026-9564-5b8c656024db";
    // Original UUID_TO_BIN/BIN_TO_UUID fixture bytes, not a round-trip oracle.
    let normal = vec![
        0x6c, 0xcd, 0x78, 0x0c, 0xba, 0xba, 0x10, 0x26, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24,
        0xdb,
    ];
    let swapped = vec![
        0x10, 0x26, 0xba, 0xba, 0x6c, 0xcd, 0x78, 0x0c, 0x95, 0x64, 0x5b, 0x8c, 0x65, 0x60, 0x24,
        0xdb,
    ];
    let native = ConstructTimeWarnings::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    scope.with_columns(&native, |columns| {
        arm_eval_one_observation();
        let parsed = evaluate_args_in(
            EvaluatedBytesOp::UuidToBinParseNative,
            columns,
            || Ok(EvaluatedArgs::Bytes(Some(canonical.as_bytes().to_vec()))),
            EvaluatedBytesResult::into_bytes,
        );
        let observation = take_eval_one_observation();
        assert_eq!(parsed, Ok(Some(normal.clone())));
        assert_wide_math_c4(observation);
        assert_eq!(observation.before_kernel_invocations, Some(0));
        assert_eq!(observation.after_kernel_invocations, Some(1));
    });
    assert!(
        !scope.busy.get(),
        "the parse call has completely returned before the next call"
    );
    let first = scope_worker_observation(&scope);
    assert_eq!(first.2 + first.3, first.4);
    assert!(first.4 <= TEST_WORKER_CAP);
    drop(scope);
    assert_eq!(owner.snapshot().unwrap().idle, 1);

    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        arm_eval_one_observation();
        let parsed = evaluate_args_in(
            EvaluatedBytesOp::UuidToBinParseNative,
            columns,
            || Ok(EvaluatedArgs::Bytes(Some(canonical.as_bytes().to_vec()))),
            EvaluatedBytesResult::into_bytes,
        )
        .unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(parsed, Some(normal.clone()));
        assert_wide_math_c4(observation);
        assert_eq!(observation.before_kernel_invocations, Some(1));
        assert_eq!(observation.after_kernel_invocations, Some(2));
        let reused = scope_worker_observation(&scope);
        assert_eq!(
            (reused.0, reused.2, reused.3, reused.4),
            (first.0, first.2, first.3, first.4)
        );
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 1);
        assert!(!scope.busy.get());
        // Feed the actual parsed Some(16 bytes), not a fabricated marker, into
        // a second complete invocation of the different swap recipe.
        let (result, observation) = observe_wide_math(|| {
            evaluate_args_in(
                EvaluatedBytesOp::UuidToBinSwapNative,
                columns,
                || Ok(EvaluatedArgs::BytesInt(parsed, Some(1))),
                |computed| Ok(computed.into_bytes()?.map_or(Datum::Null, Datum::Bytes)),
            )
        });
        assert_eq!(result, Ok(Datum::Bytes(swapped.clone())));
        assert_wide_math_c4(observation);
        assert_eq!(observation.before_kernel_invocations, Some(0));
        assert_eq!(observation.after_kernel_invocations, Some(1));
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 2);
        assert_eq!(scope_worker_observation(&scope).1, 1);

        for (values, expected) in [
            (vec![Datum::new_string(canonical)], normal.clone()),
            (
                vec![Datum::new_string(canonical), Datum::Int(1)],
                swapped.clone(),
            ),
            (
                vec![Datum::new_string(canonical), Datum::Null],
                normal.clone(),
            ),
            (
                vec![Datum::new_string(canonical), Datum::new_string("a")],
                normal.clone(),
            ),
        ] {
            let before = owner.snapshot().unwrap().factory_attempts;
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values("UUID_TO_BIN", &values, columns).unwrap()
            });
            assert_eq!(result, Ok(Datum::Bytes(expected)));
            assert_week_auth_two_calls(observation);
            // FIRST-before is the new parse worker; LAST-after is the new swap
            // worker. Neither is a fictional summed per-worker count of two.
            assert_eq!(observation.before_kernel_invocations, Some(0));
            assert_eq!(observation.after_kernel_invocations, Some(1));
            assert_eq!(owner.snapshot().unwrap().factory_attempts, before + 2);
            assert_eq!(scope_worker_observation(&scope).1, 1);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                EvaluatedBytesOp::UuidToBinSwapNative
            );
            assert!(!scope.busy.get());
            assert!(
                native.0.borrow().is_empty(),
                "UUID_TO_BIN keeps its quiet flag conversion"
            );
        }
        let (result, observation) = observe_wide_math(|| {
            crate::func::eval_func_values(
                "UUID_TO_BIN",
                &[Datum::Null, Datum::new_string("a")],
                columns,
            )
            .unwrap()
        });
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation); // No swap call after the parse worker's NULL.
        assert!(native.0.borrow().is_empty());
        for (values, expected) in [
            (vec![Datum::Bytes(normal.clone())], canonical),
            (
                vec![Datum::Bytes(normal.clone()), Datum::Int(1)],
                "baba1026-780c-6ccd-9564-5b8c656024db",
            ),
            (
                vec![Datum::Bytes(swapped.clone())],
                "1026baba-6ccd-780c-9564-5b8c656024db",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values("BIN_TO_UUID", &values, columns).unwrap()
            });
            assert_eq!(result, Ok(Datum::new_string(expected)));
            assert_wide_math_c4(observation);
        }
        let function = crate::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new("BIN_TO_UUID"),
            FieldType::new(FieldTypeCode::VarString),
            vec![
                crate::expression::Expression::Constant(Constant::new(
                    Datum::Bytes(swapped.clone()),
                    FieldType::new(FieldTypeCode::VarString)
                        .with_collation(tidb_datatype::Collation::Binary),
                )),
                crate::expression::Expression::Constant(Constant::new(
                    Datum::Int(1),
                    FieldType::new(FieldTypeCode::LongLong),
                )),
            ],
        );
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::new_string(canonical)));
        assert_wide_math_c4(observation);
        assert!(native.0.borrow().is_empty());
    });
    assert!(!scope.busy.get());
    assert!(!scope.poisoned.get());
    scope_worker_observation(&scope);
    drop(scope);
    let idle = owner.snapshot().unwrap();
    assert_eq!((idle.live, idle.idle, idle.creating), (0, 1, 0));
    assert_eq!(idle.factory_attempts, idle.factory_successes);
    execution.close();
}

#[test]
fn uuid_translate_dispatch_preserves_error_receipts_and_flag_warning_order() {
    // The four original Unsupported literals are intentionally distinct. BIN
    // length failure instead carries its actual input, rendered lossily once.
    let invalid = [
        (
            "UUID_TO_BIN",
            vec![
                Datum::new_string(" 6ccd780c-baba-1026-9564-5b8c656024db"),
                Datum::new_string("a"),
            ],
            EvalError::Unsupported("invalid UUID_TO_BIN whitespace"),
        ),
        (
            "UUID_TO_BIN",
            vec![
                Datum::new_string("6ccd780c-baba-1026-9564-5b8c6560"),
                Datum::new_string("a"),
            ],
            EvalError::Unsupported("invalid UUID for UUID_TO_BIN"),
        ),
        (
            "UUID_VERSION",
            vec![Datum::new_string("abc")],
            EvalError::Unsupported("invalid UUID for UUID_VERSION"),
        ),
        (
            "UUID_TIMESTAMP",
            vec![Datum::new_string("abc")],
            EvalError::Unsupported("invalid UUID for UUID_TIMESTAMP"),
        ),
        (
            "BIN_TO_UUID",
            vec![Datum::new_bytes(b"short\xff"), Datum::new_string("a")],
            EvalError::WrongValueForType {
                value_class: "string",
                value: "short\u{fffd}".to_owned(),
                function: "bin_to_uuid",
            },
        ),
    ];
    let native = ConstructTimeWarnings::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (name, values, expected) in &invalid {
            let (result, observation) =
                observe_wide_math(|| crate::func::eval_func_values(name, values, columns).unwrap());
            assert_eq!(
                result,
                Err(expected.clone()),
                "{name}: actual sealed worker cause only"
            );
            assert_wide_math_c4(observation);
            if *name == "BIN_TO_UUID" {
                assert_eq!(
                    native.0.replace(Vec::new()),
                    [(
                        1292,
                        "Truncated incorrect INTEGER value: 'a'".to_owned(),
                        true
                    )]
                );
            } else {
                assert!(native.0.borrow().is_empty());
            }
            assert!(!scope.busy.get());
            assert!(!scope.poisoned.get());
            scope_worker_observation(&scope);
        }
        let (result, observation) = observe_wide_math(|| {
            crate::func::eval_func_values(
                "BIN_TO_UUID",
                &[Datum::Null, Datum::new_string("a")],
                columns,
            )
            .unwrap()
        });
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation);
        assert_eq!(
            native.0.replace(Vec::new()),
            [(
                1292,
                "Truncated incorrect INTEGER value: 'a'".to_owned(),
                true
            )],
            "flag warning precedes even a NULL payload's admission"
        );
    });
    drop(scope);
    execution.close();

    for closed in [false, true] {
        let slots = usize::from(closed);
        let owner = AsciiPoolOwner::new(test_policy(slots, slots)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        if closed {
            execution.close();
        }
        let expected_class = if closed {
            crate::ExpressionAdapterFailureClass::PoolClosed
        } else {
            crate::ExpressionAdapterFailureClass::PoolResource
        };
        scope.with_columns(&native, |columns| {
            for (name, values, _) in &invalid {
                let (result, observation) = observe_wide_math(|| {
                    crate::func::eval_func_values(name, values, columns).unwrap()
                });
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == expected_class), "{name}: malformed input must not mask infrastructure refusal as SQL failure");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
                if *name == "BIN_TO_UUID" {
                    assert_eq!(native.0.replace(Vec::new()), [(1292, "Truncated incorrect INTEGER value: 'a'".to_owned(), true)]);
                } else {
                    assert!(native.0.borrow().is_empty());
                }
            }
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values("BIN_TO_UUID", &[Datum::Null, Datum::new_string("a")], columns).unwrap()
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == expected_class));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert_eq!(native.0.replace(Vec::new()), [(1292, "Truncated incorrect INTEGER value: 'a'".to_owned(), true)]);
        });
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
        drop(scope);
        execution.close();
    }
}

#[test]
fn uuid_translate_dispatch_keeps_translate_modes_and_null_coercion_demand() {
    let function = crate::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new("TRANSLATE"),
        FieldType::new(FieldTypeCode::VarString),
        ["hello", "lo", "L"]
            .into_iter()
            .map(|text| {
                crate::expression::Expression::Constant(Constant::new(
                    Datum::new_string(text),
                    FieldType::new(FieldTypeCode::VarString),
                ))
            })
            .collect(),
    );
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Existing string2.rs literals: rune substitution, first duplicate,
        // and deletion past the end of `to` all come from the real worker.
        for (src, from, to, expected) in [
            ("中文测试", "中试", "XY", "X文测Y"),
            ("aaa", "aa", "xy", "xxx"),
            ("hello", "lo", "L", "heLL"),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values(
                    "TRANSLATE",
                    &[
                        Datum::new_string(src),
                        Datum::new_string(from),
                        Datum::new_string(to),
                    ],
                    columns,
                )
                .unwrap()
            });
            assert_eq!(result, Ok(Datum::new_string(expected)));
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                EvaluatedBytesOp::TranslateUtf8Native
            );
        }
        // Fixed TestTranslate binary duplicate row: FFFF/FFFF/FEFD -> FEFE.
        // Move only the binary charset among the three arguments. A binary
        // suffix must select byte mode without changing arg0's result charset.
        for binary_argument in 0..3 {
            let mut values = [
                Datum::new_string(vec![0xff, 0xff]),
                Datum::new_string(vec![0xff, 0xff]),
                Datum::new_string(vec![0xfe, 0xfd]),
            ];
            values[binary_argument] = Datum::new_bytes(if binary_argument == 2 {
                vec![0xfe, 0xfd]
            } else {
                vec![0xff, 0xff]
            });
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values("TRANSLATE", &values, columns).unwrap()
            });
            let result = result.unwrap();
            let expected = if binary_argument == 0 {
                Datum::new_bytes([0xfe, 0xfe])
            } else {
                Datum::new_string(vec![0xfe, 0xfe])
            };
            assert_eq!(
                std::mem::discriminant(&result),
                std::mem::discriminant(&expected)
            );
            assert_eq!(result, expected);
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                EvaluatedBytesOp::TranslateBinaryNative
            );
        }
        let (result, observation) = observe_wide_math(|| {
            crate::func::eval_func_values(
                "TRANSLATE",
                &[
                    Datum::new_bytes([0xff, 0xfe, 0xfd, 0xfc, 0xfb]),
                    Datum::new_bytes([0xfd, 0xfc, 0xfb]),
                    Datum::new_bytes([0xfe, 0xfd]),
                ],
                columns,
            )
            .unwrap()
        });
        assert_eq!(result, Ok(Datum::new_bytes([0xff, 0xfe, 0xfe, 0xfd])));
        assert_wide_math_c4(observation);
        for values in [
            [
                Datum::Null,
                Datum::new_string(vec![0xff]),
                Datum::new_string(vec![0xff]),
            ],
            [
                Datum::new_string("x"),
                Datum::Null,
                Datum::new_string(vec![0xff]),
            ],
            [Datum::new_string("x"), Datum::new_string("a"), Datum::Null],
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values("TRANSLATE", &values, columns).unwrap()
            });
            assert_eq!(
                result,
                Ok(Datum::Null),
                "an actual first NULL stops suffix coercion, not worker admission"
            );
            assert_wide_math_c4(observation);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                EvaluatedBytesOp::TranslateNullNative
            );
        }
        for values in [
            [
                Datum::new_string(vec![0xff]),
                Datum::Null,
                Datum::new_string("x"),
            ],
            [
                Datum::new_string("x"),
                Datum::new_string(vec![0xff]),
                Datum::Null,
            ],
        ] {
            let before = owner.snapshot().unwrap();
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values("TRANSLATE", &values, columns).unwrap()
            });
            assert_eq!(
                result,
                Err(EvalError::Unsupported("invalid UTF-8 string datum")),
                "a later NULL cannot hide a demanded prefix coercion error"
            );
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert_eq!(owner.snapshot().unwrap(), before);
        }
        let (result, observation) = observe_wide_math(|| {
            calendar_fields_ast("TRANSLATE('中文测试', '中试', 'XY')", columns)
        });
        assert_eq!(result, Ok(Datum::new_string("X文测Y")));
        assert_wide_math_c4(observation);
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::new_string("heLL")));
        assert_wide_math_c4(observation);
    });
    assert!(!scope.busy.get());
    assert!(!scope.poisoned.get());
    scope_worker_observation(&scope);
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_wide_math(|| {
            crate::func::eval_func_values("TRANSLATE", &[
                Datum::new_string("x"), Datum::Null, Datum::new_string(vec![0xff]),
            ], columns).unwrap()
        });
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "NULL witness still requires admission; its invalid UTF-8 suffix remains undemanded");
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn format_time_dispatch_keeps_sql_date_format_and_strict_last_day_distinct() {
    let native = ConstructTimeWarnings::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        // The first row is the original date_format_source_vectors literal.
        // The remaining formatting expectations pin the small original source
        // rules: bad clock/fraction becomes midnight, empty stays empty, and
        // SQL text keeps a trailing percent. No provider computes expectations.
        for (date, layout, expected) in [
            (Datum::new_string("2010-01-07 23:12:34.12345"), Datum::new_string("%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%"), Datum::new_string("Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %")),
            (Datum::new_string("2007-10-07 23:59:61"), Datum::new_string("%Y-%m-%d %T.%f"), Datum::new_string("2007-10-07 00:00:00.000000")),
            (Datum::new_string("2007-10-07 11:22:33.bad"), Datum::new_string("%T"), Datum::new_string("00:00:00")),
            (Datum::new_string("2010-01-07"), Datum::new_string(""), Datum::new_string("")),
            (Datum::new_string("2010-01-07"), Datum::new_string("trailing%"), Datum::new_string("trailing%")),
            (Datum::Null, Datum::new_string("%Y"), Datum::Null),
            (Datum::new_string("2010-01-07"), Datum::Null, Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| crate::func::eval_func_values("DATE_FORMAT", &[date, layout], columns).unwrap());
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        for (input, expected) in [
            (Datum::new_string("2003-02-05"), Datum::new_string("2003-02-28")),
            (Datum::new_string("2004-02-05"), Datum::new_string("2004-02-29")),
            (Datum::Int(950501), Datum::new_string("1995-05-31")),
            (Datum::new_string("2007-10-07 23:59:61"), Datum::Null),
            (Datum::new_string("2007-10-07 11:22:33.bad"), Datum::Null),
            (Datum::Null, Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("LAST_DAY", &[input], columns).unwrap());
            assert_eq!(result, Ok(expected), "LAST_DAY keeps full datetime validation rather than DATE_FORMAT's midnight fallback");
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("DATE_FORMAT('2010-01-07', '%Y')", columns));
        assert_eq!(result, Ok(Datum::new_string("2010")));
        assert_wide_math_c4(observation);
        let function = construct_time_function("LAST_DAY", vec![Datum::Int(950501)], FieldTypeCode::Date);
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::Time(Time::new(CoreTime::from_date(1995, 5, 31, 0, 0, 0, 0), TimeType::Date, 0).unwrap())));
        assert_wide_math_c4(observation);
        assert!(native.0.borrow().is_empty());
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for date in [Datum::new_string("2010-01-07"), Datum::Null, Datum::new_string("not-a-date")] {
            let (result, observation) = observe_wide_math(|| crate::func::eval_func_values("DATE_FORMAT", &[date, Datum::new_string("%Y")], columns).unwrap());
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for values in [
            vec![Datum::Null, Datum::new_bytes([0xff])],
            vec![Datum::new_bytes([0xff]), Datum::MinNotNull],
        ] {
            let (result, observation) = observe_wide_math(|| crate::func::eval_func_values("DATE_FORMAT", &values, columns).unwrap());
            assert_eq!(result, Err(EvalError::Unsupported("invalid UTF-8 byte datum")), "SQL coerces the layout after a NULL date, but a left coercion error stops first");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for input in [Datum::new_string("2004-02-05"), Datum::Null, Datum::new_string("2007-10-07 23:59:61")] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("LAST_DAY", &[input], columns).unwrap());
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (values, expected) in [
            (Vec::new(), EvalError::Unsupported("bad function arity")),
            (vec![Datum::new_bytes([0xff])], EvalError::Unsupported("invalid UTF-8 byte datum")),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("LAST_DAY", &values, columns).unwrap());
            assert_eq!(result, Err(expected));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let function = construct_time_function("LAST_DAY", vec![Datum::Int(950501)], FieldTypeCode::Date);
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("LAST_DAY('0000-00-00')", columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        assert_eq!(native.0.replace(Vec::new()), [(1292, "Incorrect datetime value: '0000-00-00 00:00:00.000000'".to_owned(), true)], "the existing upstream datetime cast still warns before admission");
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn format_time_dispatch_keeps_duration_probe_before_layout_demand() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Original duration-scale and time_format_hour_family source literals.
        for (time, layout, expected) in [
            (
                Datum::new_string("1990-05-07 19:30:10"),
                Datum::new_string("%H %i %s"),
                Datum::new_string("19 30 10"),
            ),
            (
                Datum::new_string("23:00:00"),
                Datum::new_string("%H %k %h %I %l"),
                Datum::new_string("23 23 11 11 11"),
            ),
            (
                Datum::new_string("07:42:03.000001"),
                Datum::new_string("%f"),
                Datum::new_string("000001"),
            ),
            (Datum::new_string("12:34:56"), Datum::Null, Datum::Null),
            (
                Datum::new_string("12:34:56"),
                Datum::new_string(""),
                Datum::Null,
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::time_fn::dispatch("TIME_FORMAT", &[time, layout], columns).unwrap()
            });
            assert_eq!(result, Ok(expected));
            assert_week_auth_two_calls(observation); // FIRST-before/LAST-after may belong to different workers.
        }
        for time in [Datum::Null, Datum::new_string("900:00:00")] {
            let (result, observation) = observe_wide_math(|| {
                crate::time_fn::dispatch("TIME_FORMAT", &[time, Datum::new_bytes([0xff])], columns)
                    .unwrap()
            });
            assert_eq!(
                result,
                Ok(Datum::Null),
                "a NULL or invalid duration never coerces its layout"
            );
            assert_wide_math_c4(observation);
        }
        // The original wide parser treats a non-colon string with no leading
        // digits as zero, not an invalid duration. Its successful probe must
        // still demand the layout; this expectation follows that original rule.
        for time in ["12:34:56", "not-a-time"] {
            let (result, observation) = observe_wide_math(|| {
                crate::time_fn::dispatch(
                    "TIME_FORMAT",
                    &[Datum::new_string(time), Datum::new_bytes([0xff])],
                    columns,
                )
                .unwrap()
            });
            assert_eq!(
                result,
                Err(EvalError::Unsupported("invalid UTF-8 byte datum"))
            );
            assert_wide_math_c4(observation); // The whole probe returned before layout coercion failed.
        }
        let (result, observation) = observe_wide_math(|| {
            calendar_fields_ast("TIME_FORMAT('23:00:00', '%H %k %h %I %l')", columns)
        });
        assert_eq!(result, Ok(Datum::new_string("23 23 11 11 11")));
        assert_week_auth_two_calls(observation);
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for time in [Datum::new_string("12:34:56"), Datum::Null, Datum::new_string("not-a-time")] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("TIME_FORMAT", &[time, Datum::new_bytes([0xff])], columns).unwrap());
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "the first admission precedes the still-undemanded layout error");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (values, expected) in [
            (Vec::new(), EvalError::Unsupported("bad function arity")),
            (vec![Datum::new_bytes([0xff]), Datum::MinNotNull], EvalError::Unsupported("invalid UTF-8 byte datum")),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("TIME_FORMAT", &values, columns).unwrap());
            assert_eq!(result, Err(expected));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("TIME_FORMAT('23:00:00', '%H')", columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn format_time_dispatch_preserves_pb_null_demand_and_public_raw_missing_domains() {
    let normal = format_time_pb(
        vec![
            Datum::new_string("2007-10-07 23:59:61"),
            Datum::new_string("%T"),
        ],
        false,
    );
    let nulls = [
        format_time_pb(vec![Datum::Null], false),
        format_time_pb(vec![Datum::Null, Datum::new_bytes([0xff])], true),
        format_time_pb(vec![Datum::new_bytes([0xff]), Datum::Null], true),
    ];
    let core = CoreTime::from_date(2010, 1, 7, 23, 12, 34, 123_450);
    let invalid_month = CoreTime::from_date(2010, 0, 1, 0, 0, 0, 0);
    // The raw %M error and discarded trailing percent are old datatype fixture
    // literals. The single %Y field below is its original four-digit source
    // rule, not a call to a migrated public formatter to produce an oracle.
    let raw = [
        (Some((core, Some("%Y"))), Datum::new_string("2010")),
        (Some((invalid_month, Some("%M"))), Datum::Null),
        (
            Some((invalid_month, Some("trailing%"))),
            Datum::new_string("trailing"),
        ),
        (None, Datum::Null),
        (Some((core, None)), Datum::Null),
    ];
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_wide_math(|| normal.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::new_string("00:00:00")), "PB Values retains the SQL text midnight fallback, not raw clock fields");
        assert_wide_math_c4(observation);
        for function in &nulls {
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(Datum::Null), "only the observed PB NULL is demanded, without prefix coercion, suffix evaluation, or new arity restriction");
            assert_wide_math_c4(observation);
        }
        for (input, expected) in &raw {
            let (result, observation) = observe_wide_math(|| crate::eval_legacy_date_format_in(*input, columns).map(|value| value.map_or(Datum::Null, Datum::new_string)));
            assert_eq!(result, Ok(expected.clone()));
            assert_wide_math_c4(observation);
        }
        arm_eval_one_observation();
        let missing = crate::eval_legacy_date_format_missing_in(columns);
        let observation = take_eval_one_observation();
        assert_eq!(missing, Ok(Some(0_i128)), "genuinely missing first child retains its original boolean-domain zero");
        assert_wide_math_c4(observation);
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for function in std::iter::once(&normal).chain(nulls.iter()) {
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (values, expected) in [
            (vec![Datum::new_string("2010-01-07"), Datum::new_string("%Y"), Datum::new_string("extra")], EvalError::WrongParameterCount("date_format")),
            (vec![Datum::new_bytes([0xff]), Datum::new_string("%Y")], EvalError::Unsupported("invalid UTF-8 byte datum")),
        ] {
            let function = format_time_pb(values, false);
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Err(expected), "non-NULL PB Values keeps its exact original guard and strict coercion");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (input, _) in &raw {
            let (result, observation) = observe_wide_math(|| crate::eval_legacy_date_format_in(*input, columns).map(|value| value.map_or(Datum::Null, Datum::new_string)));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "raw formatter failure may become NULL, but actual driver errors must not be swallowed by .ok()");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        arm_eval_one_observation();
        let missing = crate::eval_legacy_date_format_missing_in(columns);
        let observation = take_eval_one_observation();
        assert!(matches!(missing, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn construct_time_dispatch_keeps_date_source_values_and_real_zero_time_carriers() {
    let native = ConstructTimeWarnings::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let zero = Datum::Time(Time::new(CoreTime::default(), TimeType::Date, 0).unwrap());
    scope.with_columns(&native, |columns| {
        // Original go_test_makedate/from_days_source_vectors. The exception
        // band's final point and following zero are fixed old source rules,
        // not expectations computed by the migrated date provider.
        for (name, values, expected) in [
            ("MAKEDATE", vec![Datum::Int(69), Datum::Int(1)], Datum::new_string("2069-01-01")),
            ("MAKEDATE", vec![Datum::Int(70), Datum::Int(1)], Datum::new_string("1970-01-01")),
            ("MAKEDATE", vec![Datum::Real(71.1), Datum::Real(1.89)], Datum::new_string("1971-01-02")),
            ("MAKEDATE", vec![Datum::Int(2060), Datum::Int(2_900_026)], Datum::Null),
            ("MAKEDATE", vec![Datum::Null, Datum::Int(1)], Datum::Null),
            ("FROM_DAYS", vec![Datum::Int(735_000)], Datum::new_string("2012-05-12")),
            ("FROM_DAYS", vec![Datum::new_string("6500z")], Datum::new_string("0017-10-18")),
            ("FROM_DAYS", vec![Datum::Int(-140)], zero.clone()),
            ("FROM_DAYS", vec![Datum::Int(3_652_425)], Datum::Null),
            ("FROM_DAYS", vec![Datum::Int(3_652_499)], Datum::Null),
            ("FROM_DAYS", vec![Datum::Int(3_652_500)], zero.clone()),
            ("FROM_DAYS", vec![Datum::Null], Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
            assert_eq!(result, Ok(expected), "{name}: FROM_DAYS keeps integer-prefix coercion and String/NULL/actual Time distinct");
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| crate::func::eval_func_values("FROM_DAYS", &[Datum::Int(735_000)], columns).unwrap());
        assert_eq!(result, Ok(Datum::new_string("2012-05-12")));
        assert_wide_math_c4(observation);
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("FROM_DAYS(-140)", columns));
        assert_eq!(result, Ok(zero.clone()));
        // Unary minus and FROM_DAYS now execute independent worker recipes.
        assert_eq!(observation.facade_entries, 2);
        assert!(observation.before_kernel_invocations.is_some());
        assert!(observation.after_kernel_invocations.is_some());
        for (name, values, code, expected) in [
            ("FROM_DAYS", vec![Datum::Int(735_000)], FieldTypeCode::Date, Datum::Time(Time::new(CoreTime::from_date(2012, 5, 12, 0, 0, 0, 0), TimeType::Date, 0).unwrap())),
            ("FROM_DAYS", vec![Datum::Int(-140)], FieldTypeCode::Date, zero.clone()),
            ("MAKEDATE", vec![Datum::Int(71), Datum::Int(1)], FieldTypeCode::Datetime, Datum::Time(Time::new(CoreTime::from_date(1971, 1, 1, 0, 0, 0, 0), TimeType::DateTime, 0).unwrap())),
        ] {
            let function = construct_time_function(name, values, code);
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(expected), "the unchanged temporal return-type parser receives the actual worker value");
            assert_wide_math_c4(observation);
        }
        assert!(native.0.borrow().is_empty(), "a real zero Time does not acquire the string zero-date warning");
    });
    drop(scope);
    execution.close();
}

#[test]
fn construct_time_dispatch_keeps_duration_source_values_and_sequential_fsp_demand() {
    let native = ConstructTimeWarnings::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        // Original MakeTime and SecToTime literals, including the UInt type
        // signal and the shortest-decimal half-up result, not a new oracle.
        for (name, values, expected, calls) in [
            (
                "MAKETIME",
                vec![Datum::Int(12), Datum::Int(15), Datum::Int(30)],
                Datum::new_string("12:15:30"),
                2,
            ),
            (
                "MAKETIME",
                vec![Datum::Int(-25), Datum::Int(15), Datum::Int(30)],
                Datum::new_string("-25:15:30"),
                2,
            ),
            (
                "MAKETIME",
                vec![Datum::UInt(u64::MAX), Datum::Int(0), Datum::Int(0)],
                Datum::new_string("838:59:59"),
                2,
            ),
            (
                "MAKETIME",
                vec![
                    Datum::Int(1000),
                    Datum::Int(1),
                    Datum::Decimal(crate::Decimal::from_literal("1.0")),
                ],
                Datum::new_string("838:59:59.0"),
                2,
            ),
            (
                "MAKETIME",
                vec![Datum::Int(12), Datum::Int(15), Datum::Real(30.000_000_5)],
                Datum::new_string("12:15:30.000001"),
                2,
            ),
            (
                "MAKETIME",
                vec![Datum::Int(12), Datum::Int(15), Datum::new_string("30.10")],
                Datum::new_string("12:15:30.100000"),
                2,
            ),
            (
                "MAKETIME",
                vec![Datum::Null, Datum::Int(15), Datum::Int(0)],
                Datum::Null,
                1,
            ),
            (
                "MAKETIME",
                vec![Datum::Int(12), Datum::Int(60), Datum::Int(0)],
                Datum::Null,
                1,
            ),
            (
                "SEC_TO_TIME",
                vec![Datum::Int(2378)],
                Datum::new_string("00:39:38"),
                1,
            ),
            (
                "SEC_TO_TIME",
                vec![Datum::Decimal(crate::Decimal::from_literal("86401.4"))],
                Datum::new_string("24:00:01.4"),
                1,
            ),
            (
                "SEC_TO_TIME",
                vec![Datum::new_string("123.4")],
                Datum::new_string("00:02:03.400000"),
                1,
            ),
            ("SEC_TO_TIME", vec![Datum::Null], Datum::Null, 1),
            // Tiny original source rules: NaN fails MakeTime's seconds range;
            // the formatter's saturating cast yields zero, Inf clamps, and
            // its seconds<0 sign check does not render a minus for negative zero.
            (
                "MAKETIME",
                vec![Datum::Int(0), Datum::Int(0), Datum::Real(f64::NAN)],
                Datum::Null,
                1,
            ),
            (
                "SEC_TO_TIME",
                vec![Datum::Real(f64::NAN)],
                Datum::new_string("00:00:00.000000"),
                1,
            ),
            (
                "SEC_TO_TIME",
                vec![Datum::Real(f64::INFINITY)],
                Datum::new_string("838:59:59.000000"),
                1,
            ),
            (
                "SEC_TO_TIME",
                vec![Datum::Real(-0.0)],
                Datum::new_string("00:00:00.000000"),
                1,
            ),
        ] {
            let (result, observation) =
                observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
            assert_eq!(
                result,
                Ok(expected),
                "{name}: retain raw IEEE values rather than introducing SQL-Real validation"
            );
            if calls == 2 {
                assert_week_auth_two_calls(observation);
            } else {
                assert_wide_math_c4(observation);
            }
        }
        // Parent-locked policy-derived literal, NOT an existing fixture row:
        // public raw metadata 1111 is zero Timestamp/FSP7; zero to_number is
        // safe, and the original formatter retains all seven requested digits.
        let raw_zero_fsp7 = Datum::Time(Time::from_go_raw_like_go(0b1111));
        let (result, observation) = observe_wide_math(|| {
            crate::time_fn::dispatch("SEC_TO_TIME", &[raw_zero_fsp7], columns).unwrap()
        });
        assert_eq!(result, Ok(Datum::new_string("00:00:00.0000000")));
        assert_wide_math_c4(observation);
        assert!(native.0.borrow().is_empty());
        for (hour, expected, calls) in [
            (Datum::Null, Datum::Null, 1),
            (Datum::Int(0), Datum::new_string("00:00:00.000000"), 2),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::time_fn::dispatch(
                    "MAKETIME",
                    &[hour, Datum::Int(0), Datum::new_string("x")],
                    columns,
                )
                .unwrap()
            });
            assert_eq!(result, Ok(expected));
            if calls == 2 {
                assert_week_auth_two_calls(observation);
            } else {
                assert_wide_math_c4(observation);
            }
            assert_eq!(
                native.0.replace(Vec::new()),
                [(
                    1292,
                    "Truncated incorrect DOUBLE value: 'x'".to_owned(),
                    true
                )],
                "the third conversion still warns after an earlier NULL, before the first worker"
            );
        }
        let function = construct_time_function(
            "MAKETIME",
            vec![Datum::Int(12), Datum::Int(15), Datum::Int(30)],
            FieldTypeCode::Duration,
        );
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(
            result,
            Ok(Datum::Duration(
                tidb_datatype::MySqlDuration::new(12, 15, 30, 0, 0).unwrap()
            ))
        );
        assert_week_auth_two_calls(observation);
        assert!(native.0.borrow().is_empty());
    });
    drop(scope);
    execution.close();
}

#[test]
fn construct_time_dispatch_keeps_preparation_errors_and_warnings_before_refusal() {
    let native = ConstructTimeWarnings::default();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (name, normal, nulls, invalid) in [
            ("MAKEDATE", vec![Datum::Int(71), Datum::Int(1)], vec![Datum::Null, Datum::Int(1)], vec![Datum::Int(10_000), Datum::Int(1)]),
            ("FROM_DAYS", vec![Datum::Int(735_000)], vec![Datum::Null], vec![Datum::new_bytes([0xff])]),
            ("MAKETIME", vec![Datum::Int(12), Datum::Int(15), Datum::Int(30)], vec![Datum::Null, Datum::Int(15), Datum::Int(0)], vec![Datum::Int(12), Datum::Int(60), Datum::Int(0)]),
            ("SEC_TO_TIME", vec![Datum::Int(2378)], vec![Datum::Null], vec![Datum::new_bytes([0xff])]),
        ] {
            for values in [normal, nulls, invalid] {
                let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "normal/NULL/invalid results require admission; FROM_DAYS is not int_arg and SEC_TO_TIME invalid UTF8 is silently numeric zero");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &[], columns).unwrap());
            assert_eq!(result, Err(EvalError::Unsupported("bad function arity")));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (name, values, expected) in [
            ("MAKEDATE", vec![Datum::Null, Datum::MinNotNull], EvalError::Unsupported("range sentinel time argument")),
            ("MAKEDATE", vec![Datum::new_bytes([0xff]), Datum::MinNotNull], EvalError::Unsupported("invalid UTF-8 byte datum")),
            ("MAKETIME", vec![Datum::Null, Datum::MinNotNull, Datum::new_string("x")], EvalError::Unsupported("range sentinel time argument")),
            ("MAKETIME", vec![Datum::Null, Datum::Int(0), Datum::MinNotNull], EvalError::Unsupported("range sentinel numeric argument")),
            ("SEC_TO_TIME", vec![Datum::MinNotNull], EvalError::Unsupported("range sentinel numeric argument")),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
            assert_eq!(result, Err(expected), "tuple coercions continue after NULL but stop at the first actual error");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert!(native.0.borrow().is_empty(), "a minute coercion error prevents the third argument's warning");
        }
        for (name, values, message) in [
            ("MAKETIME", vec![Datum::Null, Datum::Int(0), Datum::new_string("x")], "Truncated incorrect DOUBLE value: 'x'"),
            ("SEC_TO_TIME", vec![Datum::new_string("abc")], "Truncated incorrect DOUBLE value: 'abc'"),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert_eq!(native.0.replace(Vec::new()), [(1292, message.to_owned(), true)]);
        }
        let (result, observation) = observe_wide_math(|| crate::func::eval_func_values("FROM_DAYS", &[Datum::Int(3_652_425)], columns).unwrap());
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        for (name, values, code) in [
            ("FROM_DAYS", vec![Datum::Int(-140)], FieldTypeCode::Date),
            ("MAKETIME", vec![Datum::Int(12), Datum::Int(15), Datum::Int(30)], FieldTypeCode::Duration),
        ] {
            let function = construct_time_function(name, values, code);
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        assert!(native.0.borrow().is_empty());
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn week_auth_dispatch_keeps_week_mode_demand_and_two_sequential_workers() {
    let native = WeekAuthMode {
        mode: Cell::new(1),
        reads: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        // Existing week/yearweek/weekofyear date/mode result literals. The
        // native parser still ignores a bad clock suffix. NULL mode is zero,
        // not the caller's default and not a nullable result.
        for (name, values, expected, calls) in [
            (
                "WEEK",
                vec![Datum::new_string("2008-02-20 23:59:61"), Datum::Int(0)],
                7,
                2,
            ),
            (
                "WEEK",
                vec![Datum::new_string("2008-02-20"), Datum::Int(1)],
                8,
                2,
            ),
            (
                "WEEK",
                vec![Datum::new_string("2023-01-01"), Datum::Null],
                1,
                2,
            ),
            (
                "YEARWEEK",
                vec![Datum::new_string("2000-01-01")],
                199_952,
                2,
            ),
            (
                "YEARWEEK",
                vec![Datum::new_string("2000-01-01"), Datum::Null],
                199_952,
                2,
            ),
            ("WEEKOFYEAR", vec![Datum::new_string("2024-03-15")], 11, 1),
        ] {
            let (result, observation) =
                observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
            assert_eq!(result, Ok(Datum::Int(expected)));
            if calls == 2 {
                assert_week_auth_two_calls(observation);
            } else {
                assert_wide_math_c4(observation);
            }
            assert_eq!(
                native.reads.replace(Vec::new()),
                if name == "WEEK" {
                    vec![true]
                } else {
                    Vec::new()
                }
            );
        }
        let function =
            date_diff_days_function("WEEK", vec![Datum::new_string("2000-12-31")], false, false);
        for (mode, expected) in [(0, 53), (6, 1)] {
            native.mode.set(mode);
            let (result, observation) =
                observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(
                result,
                Ok(Datum::Int(expected)),
                "the original default is read again after the function was built"
            );
            assert_week_auth_two_calls(observation);
            assert_eq!(native.reads.replace(Vec::new()), [true]);
        }
        let (result, observation) =
            observe_wide_math(|| calendar_fields_ast("WEEK('2008-02-20', 1)", columns));
        assert_eq!(result, Ok(Datum::Int(8)));
        assert_week_auth_two_calls(observation);
        assert_eq!(
            native.reads.replace(Vec::new()),
            [true],
            "explicit SQL mode still reads the default before the date probe"
        );
        for name in ["WEEK", "YEARWEEK"] {
            for date in [Datum::Null, Datum::new_string("2008-15-31")] {
                let (result, observation) = observe_wide_math(|| {
                    crate::time_fn::dispatch(name, &[date, Datum::MinNotNull], columns).unwrap()
                });
                assert_eq!(
                    result,
                    Ok(Datum::Null),
                    "a NULL/bad date never demands the mode"
                );
                assert_wide_math_c4(observation);
                assert_eq!(
                    native.reads.replace(Vec::new()),
                    if name == "WEEK" {
                        vec![true]
                    } else {
                        Vec::new()
                    }
                );
            }
            let (result, observation) = observe_wide_math(|| {
                crate::time_fn::dispatch(
                    name,
                    &[Datum::new_string("2008-02-20"), Datum::MinNotNull],
                    columns,
                )
                .unwrap()
            });
            assert_eq!(
                result,
                Err(EvalError::Unsupported("range sentinel time argument"))
            );
            assert_wide_math_c4(observation); // The successful probe preceded the mode error; no second worker entered.
            assert_eq!(
                native.reads.replace(Vec::new()),
                if name == "WEEK" {
                    vec![true]
                } else {
                    Vec::new()
                }
            );
        }
        for input in [Datum::Null, Datum::new_string("0000-00-00")] {
            let (result, observation) = observe_wide_math(|| {
                crate::time_fn::dispatch("WEEKOFYEAR", &[input], columns).unwrap()
            });
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
            assert!(native.reads.borrow().is_empty());
        }
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for name in ["WEEK", "YEARWEEK", "WEEKOFYEAR"] {
            for date in [Datum::new_string("2008-02-20"), Datum::Null, Datum::new_string("not-a-date")] {
                let mut values = vec![date];
                if name != "WEEKOFYEAR" { values.push(Datum::MinNotNull); }
                let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "refusal of the first call precedes even a valid date's unobserved mode error");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
                assert_eq!(native.reads.replace(Vec::new()), if name == "WEEK" { vec![true] } else { Vec::new() });
            }
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &[], columns).unwrap());
            assert_eq!(result, Err(EvalError::Unsupported("bad function arity")));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert_eq!(native.reads.replace(Vec::new()), if name == "WEEK" { vec![true] } else { Vec::new() }, "SQL WEEK's default getter also precedes its arity guard");
        }
        let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("WEEK", &[Datum::new_bytes([0xff])], columns).unwrap());
        assert_eq!(result, Err(EvalError::Unsupported("invalid UTF-8 byte datum")));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        assert_eq!(native.reads.replace(Vec::new()), [true]);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn week_auth_dispatch_preserves_pb_null_prefix_and_legacy_raw_week_domain() {
    let native = WeekAuthMode {
        mode: Cell::new(1),
        reads: RefCell::new(Vec::new()),
    };
    let normal = week_auth_pb(vec![Datum::new_string("2008-02-20")], false);
    let nulls = [
        week_auth_pb(vec![Datum::Null], true),
        week_auth_pb(vec![Datum::new_string("2023-01-01"), Datum::Null], true),
        week_auth_pb(vec![Datum::new_bytes([0xff]), Datum::Null], true),
    ];
    let core = CoreTime::from_date(2008, 2, 20, 0, 0, 0, 0);
    let invalid = CoreTime::from_date(0, 15, 31, 23, 59, 59, 999_999);
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let (result, observation) = observe_wide_math(|| normal.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::Int(8)));
        assert_week_auth_two_calls(observation);
        assert_eq!(native.reads.replace(Vec::new()), [true]);
        for function in &nulls {
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(Datum::Null), "PB's observed NULL mode differs from SQL mode zero: no prefix coercion, extra suffix demand, or new arity gate");
            assert_wide_math_c4(observation);
            assert!(native.reads.borrow().is_empty(), "the PB NULL branch never reads the default");
        }
        // CoreTime's original 2008-02-20 mode-zero fixture and the locked old
        // zero-core literal. No migrated week getter supplies these expectations.
        for (input, expected) in [(Some(core), Datum::Int(7)), (Some(CoreTime::default()), Datum::Int(0)), (None, Datum::Null)] {
            let (result, observation) = observe_wide_math(|| crate::eval_legacy_week_in(input, columns));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
            assert!(native.reads.borrow().is_empty(), "legacy is always raw-core mode zero, not the session default");
        }
        let (result, observation) = observe_wide_math(|| crate::eval_legacy_week_in(Some(invalid), columns));
        assert!(matches!(result, Ok(Datum::Int(_))), "invalid packed date fields retain the raw integer domain; this representative does not pin their numerical week");
        assert_wide_math_c4(observation);
        assert!(native.reads.borrow().is_empty());
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let (result, observation) = observe_wide_math(|| normal.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        assert_eq!(native.reads.replace(Vec::new()), [true]);
        for function in &nulls {
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert!(native.reads.borrow().is_empty());
        }
        for (values, expected) in [
            (vec![Datum::new_string("2008-02-20"), Datum::Int(1), Datum::Int(2)], EvalError::Unsupported("bad function arity")),
            (vec![Datum::new_bytes([0xff])], EvalError::Unsupported("invalid UTF-8 byte datum")),
        ] {
            let function = week_auth_pb(values, false);
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Err(expected));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert_eq!(native.reads.replace(Vec::new()), [true], "non-NULL PB Values keeps the original eager default and guards");
        }
        for input in [Some(core), Some(CoreTime::default()), Some(invalid), None] {
            let (result, observation) = observe_wide_math(|| crate::eval_legacy_week_in(input, columns));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert!(native.reads.borrow().is_empty());
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn week_auth_dispatch_keeps_native_auth_bytes_and_password_warning_order() {
    #[derive(Default)]
    struct Warnings(RefCell<Vec<(u16, String, bool)>>);
    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            let before_admission = EVAL_ONE_OBSERVATION.with(|slot| {
                slot.borrow().as_ref().is_some_and(|value| {
                    value.facade_entries == 0
                        && value.before_kernel_invocations.is_none()
                        && value.after_kernel_invocations.is_none()
                })
            });
            self.0
                .borrow_mut()
                .push((code, message.to_owned(), before_admission));
        }
    }
    let native = Warnings::default();
    let warning = (
        1681,
        "PASSWORD is deprecated and will be removed in a future release.".to_owned(),
        true,
    );
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        // Native crypto's old fixtures, not wire PASSWORD's double-hex spelling
        // and not expectations computed with the migrated hash provider.
        for (name, input, expected) in [
            (
                "PASSWORD",
                Datum::new_string("abc"),
                Datum::new_string("*0D3CED9BEC10A777AEC23CCC353A8C08A633045E"),
            ),
            (
                "PASSWORD",
                Datum::Int(123),
                Datum::new_string("*23AE809DDACAF96AF0FD78ED04B6A265E05AA257"),
            ),
            ("PASSWORD", Datum::new_string(""), Datum::new_string("")),
            ("PASSWORD", Datum::Null, Datum::Null),
            (
                "PASSWORD",
                Datum::new_bytes([0xff, 0x00, b'a']),
                Datum::new_string("*F5A241511384DB827F22D2A2188A456E87F7D4F2"),
            ),
            (
                "SM3",
                Datum::new_bytes(b"abc"),
                Datum::new_string(
                    "66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0",
                ),
            ),
            ("SM3", Datum::Null, Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values(name, &[input], columns).unwrap()
            });
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
            assert_eq!(
                native.0.replace(Vec::new()),
                if name == "PASSWORD" {
                    vec![warning.clone()]
                } else {
                    Vec::new()
                }
            );
        }
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for name in ["PASSWORD", "SM3"] {
            // Empty/non-UTF8 SM3 has no new digest oracle here: this pins only
            // its original raw-byte preparation and real resource admission.
            for input in [Datum::new_string("abc"), Datum::Null, Datum::new_string(""), Datum::new_bytes([0xff, 0x00, b'a']), Datum::new_string([0xff])] {
                let (result, observation) = observe_wide_math(|| crate::func::eval_func_values(name, &[input], columns).unwrap());
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
                assert_eq!(native.0.replace(Vec::new()), if name == "PASSWORD" { vec![warning.clone()] } else { Vec::new() }, "PASSWORD warns even before NULL or refused admission; SM3 adds no warning");
            }
            let (result, observation) = observe_wide_math(|| crate::func::eval_func_values(name, &[Datum::MinNotNull], columns).unwrap());
            assert_eq!(result, Err(EvalError::Unsupported("range sentinel hash argument")));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            assert_eq!(native.0.replace(Vec::new()), if name == "PASSWORD" { vec![warning.clone()] } else { Vec::new() }, "the original PASSWORD warning precedes its coercion error too");
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn date_diff_days_dispatch_keeps_source_values_nulls_and_distinct_suffix_rules() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Fixed date_diff/to_days/to_seconds source vectors and the original
        // builtin_time_calendars_source TSO literals; no new provider oracle.
        for (name, values, expected) in [
            (
                "DATEDIFF",
                vec![
                    Datum::new_string("2004-05-21"),
                    Datum::new_string("2004:01:02"),
                ],
                Datum::Int(140),
            ),
            (
                "DATEDIFF",
                vec![
                    Datum::new_string("2008-12-31 23:59:59.000001"),
                    Datum::new_string("2008-12-30 01:01:01.000002"),
                ],
                Datum::Int(1),
            ),
            (
                "DATEDIFF",
                vec![
                    Datum::new_string("1010-11-30 23:59:59"),
                    Datum::new_string("2010-12-31"),
                ],
                Datum::Int(-365_274),
            ),
            (
                "DATEDIFF",
                vec![
                    Datum::new_string("2007-10-07 23:59:61"),
                    Datum::new_string("2007-10-07"),
                ],
                Datum::Int(0),
            ),
            (
                "DATEDIFF",
                vec![
                    Datum::new_string("2004-05-21"),
                    Datum::new_string("abcdefg"),
                ],
                Datum::Null,
            ),
            (
                "DATEDIFF",
                vec![Datum::Null, Datum::new_string("2004-01-01")],
                Datum::Null,
            ),
            (
                "DATEDIFF",
                vec![Datum::new_string("2004-01-01"), Datum::Null],
                Datum::Null,
            ),
            ("TO_DAYS", vec![Datum::Int(950501)], Datum::Int(728_779)),
            (
                "TO_DAYS",
                vec![Datum::new_string("0000-01-01")],
                Datum::Int(1),
            ),
            (
                "TO_DAYS",
                vec![Datum::new_string("2007-10-07 00:00:59")],
                Datum::Int(733_321),
            ),
            (
                "TO_SECONDS",
                vec![Datum::Int(950501)],
                Datum::Int(62_966_505_600),
            ),
            (
                "TO_SECONDS",
                vec![Datum::new_string("2009-11-29 13:43:32")],
                Datum::Int(63_426_721_412),
            ),
            (
                "TO_SECONDS",
                vec![Datum::new_string("99-11-29 13:43:32")],
                Datum::Int(63_111_102_212),
            ),
            (
                "TO_DAYS",
                vec![Datum::new_string("2007-10-07 23:59:61")],
                Datum::Null,
            ),
            (
                "TO_SECONDS",
                vec![Datum::new_string("2007-10-07 23:59:61")],
                Datum::Null,
            ),
            (
                "TO_SECONDS",
                vec![Datum::new_string("2007-10-07 00:00:00.bad")],
                Datum::Null,
            ),
            (
                "TIDB_PARSE_TSO_LOGICAL",
                vec![Datum::Int(404_411_537_129_996_288)],
                Datum::Int(0),
            ),
            (
                "TIDB_PARSE_TSO_LOGICAL",
                vec![Datum::Int(404_411_537_129_996_289)],
                Datum::Int(1),
            ),
            (
                "TIDB_PARSE_TSO_LOGICAL",
                vec![Datum::Int(404_411_537_129_996_290)],
                Datum::Int(2),
            ),
            ("TIDB_PARSE_TSO_LOGICAL", vec![Datum::Int(0)], Datum::Null),
            ("TIDB_PARSE_TSO_LOGICAL", vec![Datum::Int(-1)], Datum::Null),
            (
                "TIDB_PARSE_TSO_LOGICAL",
                vec![Datum::new_string("-1")],
                Datum::Null,
            ),
        ] {
            let (result, observation) =
                observe_wide_math(|| date_diff_days_native(name, &values, columns));
            assert_eq!(
                result,
                Ok(expected),
                "{name}: DATEDIFF ignores the time suffix, but day-number functions validate it"
            );
            assert_wide_math_c4(observation);
        }
        for name in ["TO_DAYS", "TO_SECONDS", "TIDB_PARSE_TSO_LOGICAL"] {
            let (result, observation) =
                observe_wide_math(|| date_diff_days_native(name, &[Datum::Null], columns));
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| {
            calendar_fields_ast(
                "DATEDIFF('2008-12-31 23:59:59.000001', '2008-12-30 01:01:01.000002')",
                columns,
            )
        });
        assert_eq!(result, Ok(Datum::Int(1)));
        assert_wide_math_c4(observation);
        for (name, value, expected) in [
            ("TO_DAYS", Datum::new_string("2007-10-07"), 733_321),
            (
                "TIDB_PARSE_TSO_LOGICAL",
                Datum::Int(404_411_537_129_996_290),
                2,
            ),
        ] {
            let function = date_diff_days_function(name, vec![value], false, false);
            let (result, observation) =
                observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(Datum::Int(expected)));
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn date_diff_days_dispatch_refuses_after_original_coercion_and_datetime_casts() {
    let native = CompressionWarningProbe::default();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (name, normal, nulls, bad) in [
            ("DATEDIFF", vec![Datum::new_string("2004-05-21"), Datum::new_string("2004:01:02")], vec![Datum::Null, Datum::new_string("2004-01-01")], vec![Datum::new_string("2004-05-21"), Datum::new_string("abcdefg")]),
            ("TO_DAYS", vec![Datum::new_string("2007-10-07")], vec![Datum::Null], vec![Datum::new_string("2007-10-07 23:59:61")]),
            ("TO_SECONDS", vec![Datum::new_string("2009-11-29 13:43:32")], vec![Datum::Null], vec![Datum::new_string("2007-10-07 00:00:00.bad")]),
            // int_arg's ordinary bad text becomes zero, not a preparation error.
            ("TIDB_PARSE_TSO_LOGICAL", vec![Datum::Int(404_411_537_129_996_290)], vec![Datum::Null], vec![Datum::new_string("bad")]),
        ] {
            for values in [normal, nulls, bad] {
                let (result, observation) = observe_wide_math(|| date_diff_days_native(name, &values, columns));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "NULL, invalid text, and the nonpositive TSO predicate cannot bypass admission");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
            let (result, observation) = observe_wide_math(|| date_diff_days_native(name, &[], columns));
            assert_eq!(result, Err(EvalError::Unsupported("bad function arity")), "native body guard, not func's DATEDIFF len gate");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (name, values, expected) in [
            ("DATEDIFF", vec![Datum::Null, Datum::new_bytes([0xff])], EvalError::Unsupported("invalid UTF-8 byte datum")),
            ("DATEDIFF", vec![Datum::new_bytes([0xff]), Datum::MinNotNull], EvalError::Unsupported("invalid UTF-8 byte datum")),
            ("TO_DAYS", vec![Datum::new_bytes([0xff])], EvalError::Unsupported("invalid UTF-8 byte datum")),
            ("TO_SECONDS", vec![Datum::new_string([0xff])], EvalError::Unsupported("invalid UTF-8 string datum")),
            ("TIDB_PARSE_TSO_LOGICAL", vec![Datum::MinNotNull], EvalError::Unsupported("range sentinel time argument")),
        ] {
            let (result, observation) = observe_wide_math(|| date_diff_days_native(name, &values, columns));
            assert_eq!(result, Err(expected), "DATEDIFF still coerces the right operand after left NULL, but left error stops first");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("DATEDIFF('2004-05-21', '2004-01-02')", columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        let function = date_diff_days_function("TO_DAYS", vec![Datum::new_string("2007-10-07")], false, false);
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        native.0.borrow_mut().clear();
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("DATEDIFF('0000-00-00', 'not-a-date')", columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        assert_eq!(native.0.borrow().as_slice(), [
            (1292, "Incorrect datetime value: '0000-00-00 00:00:00.000000'".to_owned(), false),
            (1292, "Incorrect datetime value: 'not-a-date'".to_owned(), false),
        ], "both existing ETDatetime casts warn in argument order before worker admission");
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn date_diff_days_dispatch_preserves_pb_null_demand_and_public_raw_core_domain() {
    let pb_normal = date_diff_days_function(
        "DATEDIFF",
        vec![
            Datum::new_string("2004-05-21"),
            Datum::new_string("2004:01:02"),
        ],
        true,
        false,
    );
    let pb_nulls = [
        date_diff_days_function("DATEDIFF", vec![Datum::Null], true, false),
        date_diff_days_function(
            "DATEDIFF",
            vec![Datum::Null, Datum::new_bytes([0xff])],
            true,
            true,
        ),
        date_diff_days_function(
            "DATEDIFF",
            vec![Datum::new_bytes([0xff]), Datum::Null],
            true,
            true,
        ),
    ];
    // temporal_extraction_follows_go's fixed two-day pair, despite clocks.
    let left = CoreTime::from_date(2024, 3, 5, 14, 30, 45, 123_456);
    let right = CoreTime::from_date(2024, 3, 3, 23, 0, 0, 0);
    let invalid = CoreTime::from_date(16_383, 15, 31, 31, 63, 63, 1_048_575);
    let invalid_other_clock = CoreTime::from_date(16_383, 15, 31, 0, 0, 0, 0);
    let raw_pairs = [
        (Some(left), Some(right), Some(2)),
        (
            Some(CoreTime::default()),
            Some(CoreTime::default()),
            Some(0),
        ),
        (Some(invalid), Some(invalid_other_clock), Some(0)),
        (None, Some(invalid), None),
        (Some(left), None, None),
    ];
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_wide_math(|| pb_normal.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::Int(140)));
        assert_wide_math_c4(observation);
        let pb_suffix = date_diff_days_function("DATEDIFF", vec![Datum::new_string("2007-10-07 23:59:61"), Datum::new_string("2007-10-07")], true, false);
        let (result, observation) = observe_wide_math(|| pb_suffix.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::Int(0)), "PB Values remains the old SQL text algorithm, not raw CoreTime or a stricter timestamp cast");
        assert_wide_math_c4(observation);
        for function in &pb_nulls {
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(Datum::Null), "only the observed PB NULL is demanded: no arity gate, prefix coercion, or suffix evaluation");
            assert_wide_math_c4(observation);
        }
        for &(left, right, expected) in &raw_pairs {
            let (result, observation) = observe_wide_math(|| crate::eval_legacy_date_diff_in(left, right, columns).map(|value| value.map_or(Datum::Null, Datum::Int)));
            assert_eq!(result, Ok(expected.map_or(Datum::Null, Datum::Int)), "raw date fields are not SQL text: zero/invalid equal dates still differ by zero and clock bits do not affect days");
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for function in std::iter::once(&pb_normal).chain(pb_nulls.iter()) {
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "PB Values and its actual NULL witness both retain the caller's scope");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (values, expected) in [
            (vec![Datum::new_string("2004-05-21"), Datum::new_string("2004-01-02"), Datum::new_string("2004-01-01")], EvalError::Unsupported("bad function arity")),
            (vec![Datum::new_bytes([0xff]), Datum::new_string("2004-01-02")], EvalError::Unsupported("invalid UTF-8 byte datum")),
        ] {
            let function = date_diff_days_function("DATEDIFF", values, true, false);
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Err(expected), "non-NULL PB inputs retain calendar arity and text preparation errors");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for &(left, right, _) in &raw_pairs {
            let (result, observation) = observe_wide_math(|| crate::eval_legacy_date_diff_in(left, right, columns).map(|value| value.map_or(Datum::Null, Datum::Int)));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

fn weekday_fields_function(name: &str, input: Datum) -> crate::scalar_function::ScalarFunction {
    let result_type = FieldType::new(if name == "DAYNAME" {
        FieldTypeCode::VarString
    } else {
        FieldTypeCode::LongLong
    });
    crate::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new(name),
        result_type,
        vec![crate::expression::Expression::Constant(Constant::new(
            input,
            FieldType::new(FieldTypeCode::VarString),
        ))],
    )
}

#[test]
fn weekday_fields_dispatch_keeps_source_calendar_fields_and_typed_reparse() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Original calendar_part_source_vectors/dayname_source_vectors plus
        // the locked year-zero Saturday literal; never derive an expectation
        // from a migrated parser, weekday primitive, name table, or getter.
        for (name, input, expected) in [
            ("DAYOFWEEK", Datum::new_string("2017-12-01"), Datum::Int(6)),
            (
                "DAYOFYEAR",
                Datum::new_string("2017-12-01"),
                Datum::Int(335),
            ),
            (
                "DAYNAME",
                Datum::new_string("2017-12-01"),
                Datum::new_string("Friday"),
            ),
            ("DAYOFWEEK", Datum::new_string("2000-01-01"), Datum::Int(7)),
            ("WEEKDAY", Datum::new_string("2000-01-01"), Datum::Int(5)),
            ("DAYOFYEAR", Datum::new_string("2000-01-01"), Datum::Int(1)),
            ("DAYOFWEEK", Datum::new_string("0000-01-01"), Datum::Int(7)),
            ("WEEKDAY", Datum::new_string("0000-01-01"), Datum::Int(5)),
            ("DAYOFYEAR", Datum::new_string("0000-01-01"), Datum::Int(1)),
            (
                "DAYNAME",
                Datum::new_string("0000-01-01"),
                Datum::new_string("Saturday"),
            ),
            ("DAYOFWEEK", Datum::Int(20_240_315), Datum::Int(6)),
            ("WEEKDAY", Datum::Int(20_240_315), Datum::Int(4)),
            ("DAYOFYEAR", Datum::Int(20_240_315), Datum::Int(75)),
            (
                "DAYNAME",
                Datum::Int(20_171_201),
                Datum::new_string("Friday"),
            ),
            (
                "DAYNAME",
                Datum::Time(
                    Time::new(
                        CoreTime::from_date(2017, 1, 0, 0, 0, 0, 0),
                        TimeType::Date,
                        0,
                    )
                    .unwrap(),
                ),
                Datum::Null,
            ),
        ] {
            let (result, observation) =
                observe_wide_math(|| crate::time_fn::dispatch(name, &[input], columns).unwrap());
            assert_eq!(
                result,
                Ok(expected),
                "{name}: valid year zero is not a blanket NULL"
            );
            assert_wide_math_c4(observation);
        }
        let invalid_time = Datum::Time(
            Time::new(
                CoreTime::from_date(2024, 15, 1, 0, 0, 0, 0),
                TimeType::Date,
                0,
            )
            .unwrap(),
        );
        for name in ["DAYOFWEEK", "WEEKDAY", "DAYOFYEAR", "DAYNAME"] {
            for input in [
                Datum::Null,
                Datum::new_string("2017-00-01"),
                invalid_time.clone(),
            ] {
                let (result, observation) = observe_wide_math(|| {
                    crate::time_fn::dispatch(name, &[input], columns).unwrap()
                });
                assert_eq!(
                    result,
                    Ok(Datum::Null),
                    "typed Time still Display/reparses; do not substitute wide raw-core extraction"
                );
                assert_wide_math_c4(observation);
            }
        }
        let (result, observation) =
            observe_wide_math(|| calendar_fields_ast("DAYOFWEEK('2017-12-01')", columns));
        assert_eq!(result, Ok(Datum::Int(6)));
        assert_wide_math_c4(observation);
        for (name, input, expected) in [
            ("DAYNAME", "2017-12-01", Datum::new_string("Friday")),
            ("WEEKDAY", "2000-01-01", Datum::Int(5)),
        ] {
            let function = weekday_fields_function(name, Datum::new_string(input));
            let (result, observation) =
                observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn weekday_fields_dispatch_keeps_admission_after_preparation_and_original_cast_context() {
    struct Probe {
        reject_zero: Cell<bool>,
        events: RefCell<Vec<String>>,
    }
    impl Columns for Probe {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn date_modes(&self) -> DateModes {
            self.events.borrow_mut().push("modes".to_owned());
            DateModes {
                no_zero_date: self.reject_zero.get(),
                ..DateModes::TIDB_DEFAULT_SQL_MODE
            }
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push("zone".to_owned());
            SessionTimeZone::Fixed {
                name: "weekday-fields-context".to_owned(),
                offset_secs: 0,
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            let before = EVAL_ONE_OBSERVATION.with(|slot| {
                slot.borrow().as_ref().is_some_and(|value| {
                    value.facade_entries == 0
                        && value.before_kernel_invocations.is_none()
                        && value.after_kernel_invocations.is_none()
                })
            });
            self.events
                .borrow_mut()
                .push(format!("warn:{code}:{message}:before:{before}"));
        }
    }
    let native = Probe {
        reject_zero: Cell::new(true),
        events: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for name in ["DAYOFWEEK", "WEEKDAY", "DAYOFYEAR", "DAYNAME"] {
            for input in [Datum::new_string("2017-12-01"), Datum::Null, Datum::new_string("2017-00-01")] {
                let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &[input], columns).unwrap());
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "the worker decides bad-date NULL only after admission");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
        for (name, values, expected) in [
            ("DAYOFWEEK", vec![Datum::new_bytes([0xff])], EvalError::Unsupported("invalid UTF-8 byte datum")),
            ("DAYNAME", vec![Datum::new_string([0xff])], EvalError::Unsupported("invalid UTF-8 string datum")),
            ("WEEKDAY", Vec::new(), EvalError::Unsupported("bad function arity")),
            ("DAYOFYEAR", vec![Datum::Null, Datum::new_string("2017-12-01")], EvalError::Unsupported("bad function arity")),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
            assert_eq!(result, Err(expected));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("DAYOFYEAR('2017-12-01')", columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        let function = weekday_fields_function("DAYNAME", Datum::new_string("2017-12-01"));
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        native.events.borrow_mut().clear();
        for reject_zero in [true, false] {
            native.reject_zero.set(reject_zero);
            let function = weekday_fields_function("DAYNAME", Datum::new_string("0000-00-00"));
            let (result, observation) = observe_wide_math(|| {
                if reject_zero {
                    calendar_fields_ast("DAYNAME('0000-00-00')", columns)
                } else {
                    function.eval(columns, tidb_chunk::row::Row::empty())
                }
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            let events = native.events.replace(Vec::new());
            assert!(events.iter().any(|event| event == "modes"));
            assert!(events.iter().any(|event| event == "zone"));
            let warnings = events.iter().filter(|event| event.starts_with("warn:")).map(String::as_str).collect::<Vec<_>>();
            if reject_zero {
                assert_eq!(warnings, ["warn:1292:Incorrect datetime value: '0000-00-00 00:00:00.000000':before:true"]);
            } else {
                assert!(warnings.is_empty(), "the existing ETDatetime cast keeps the caller's date mode");
            }
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

fn period_format_function(
    name: &str,
    values: Vec<Datum>,
) -> crate::scalar_function::ScalarFunction {
    let field = FieldType::new(if name == "GET_FORMAT" {
        FieldTypeCode::VarString
    } else {
        FieldTypeCode::LongLong
    });
    let args = values
        .into_iter()
        .map(|value| crate::expression::Expression::Constant(Constant::new(value, field.clone())))
        .collect();
    crate::scalar_function::ScalarFunction::new(tidb_ast::CiString::new(name), field, args)
}

#[test]
fn period_format_dispatch_keeps_period_source_vectors_wrapping_and_nulls() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Fixed period_arithmetic_* fixtures, including the recorded Go uint64
        // wrapping boundary. No migrated helper computes these expectations.
        for (name, left, right, expected) in [
            ("PERIOD_ADD", 201611, 2, 201701),
            ("PERIOD_ADD", 1611, 3, 201702),
            ("PERIOD_ADD", 7011, 3, 197102),
            ("PERIOD_DIFF", 200802, 200703, 11),
            ("PERIOD_DIFF", 201510, 201611, -13),
            ("PERIOD_ADD", i64::MAX, 1, i64::MIN),
            ("PERIOD_DIFF", i64::MAX, 197001, 1_106_804_644_422_549_462),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::time_fn::dispatch(name, &[Datum::Int(left), Datum::Int(right)], columns)
                    .unwrap()
            });
            assert_eq!(result, Ok(Datum::Int(expected)), "{name}");
            assert_wide_math_c4(observation);
        }
        for (name, values) in [
            ("PERIOD_ADD", vec![Datum::Int(0), Datum::Null]),
            ("PERIOD_DIFF", vec![Datum::Null, Datum::Int(201611)]),
        ] {
            let (result, observation) =
                observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
        let (result, observation) =
            observe_wide_math(|| calendar_fields_ast("PERIOD_ADD(201611, 2)", columns));
        assert_eq!(result, Ok(Datum::Int(201701)));
        assert_wide_math_c4(observation);
        let function =
            period_format_function("PERIOD_DIFF", vec![Datum::Int(200802), Datum::Int(200703)]);
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::Int(11)));
        assert_wide_math_c4(observation);
    });
    drop(scope);
    execution.close();
}

#[test]
fn period_format_dispatch_keeps_period_sql_failures_behind_admission_and_coercion_first() {
    let invalid = [
        ("PERIOD_ADD", 0, 3, "Incorrect arguments to period_add"),
        ("PERIOD_ADD", -1, 3, "Incorrect arguments to period_add"),
        (
            "PERIOD_DIFF",
            201600,
            201611,
            "Incorrect arguments to period_diff",
        ),
        (
            "PERIOD_DIFF",
            201611,
            201613,
            "Incorrect arguments to period_diff",
        ),
    ];
    let nullable = [
        ("PERIOD_ADD", vec![Datum::Int(-1), Datum::Null]),
        ("PERIOD_DIFF", vec![Datum::Null, Datum::Int(201613)]),
    ];
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for &(name, left, right, message) in &invalid {
            let (result, observation) = observe_wide_math(|| {
                crate::time_fn::dispatch(name, &[Datum::Int(left), Datum::Int(right)], columns)
                    .unwrap()
            });
            // Preserve the original 1210 carrier and exact function-specific
            // message; these are actual worker failures, not fabricated receipts.
            assert_eq!(
                result,
                Err(EvalError::IncorrectArguments(message.to_owned()))
            );
            assert_wide_math_c4(observation);
        }
        for (name, values) in &nullable {
            let (result, observation) =
                observe_wide_math(|| crate::time_fn::dispatch(name, values, columns).unwrap());
            assert_eq!(
                result,
                Ok(Datum::Null),
                "NULL precedes period validity, but not the worker"
            );
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for &(name, left, right, _) in &invalid {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &[Datum::Int(left), Datum::Int(right)], columns).unwrap());
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "identical invalid periods must not become SQL errors before admission");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (name, values) in &nullable {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, values, columns).unwrap());
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for name in ["PERIOD_ADD", "PERIOD_DIFF"] {
            for (values, expected) in [
                (vec![Datum::Null], EvalError::Unsupported("bad function arity")),
                (vec![Datum::Null, Datum::MinNotNull], EvalError::Unsupported("range sentinel time argument")),
                (vec![Datum::new_bytes([0xff]), Datum::MinNotNull], EvalError::Unsupported("invalid UTF-8 byte datum")),
            ] {
                let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
                assert_eq!(result, Err(expected), "both coercions are demanded after left NULL; left error still stops first");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("PERIOD_ADD(201611, 2)", columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        let function = period_format_function("PERIOD_DIFF", vec![Datum::Int(200802), Datum::Int(200703)]);
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn period_format_dispatch_keeps_get_format_bytes_null_demand_and_strict_ast_child() {
    struct Location {
        value: RefCell<Datum>,
        reads: Cell<usize>,
    }
    impl Columns for Location {
        fn get(&self, _: &[String]) -> Option<Datum> {
            self.reads.set(self.reads.get() + 1);
            Some(self.value.borrow().clone())
        }
    }
    let native = Location {
        value: RefCell::new(Datum::new_string("USA")),
        reads: Cell::new(0),
    };
    let typed = period_format_function(
        "GET_FORMAT",
        vec![Datum::new_string("DATE"), Datum::new_string("USA")],
    );
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        // Original get_format_table literals: kind is case-sensitive, while
        // location is ASCII case-insensitive. Unknown is an empty String.
        for (kind, location, expected) in [
            (
                Datum::new_string("DATE"),
                Datum::new_string("USA"),
                Datum::new_string("%m.%d.%Y"),
            ),
            (
                Datum::new_string("TIMESTAMP"),
                Datum::new_string("eur"),
                Datum::new_string("%Y-%m-%d %H.%i.%s"),
            ),
            (
                Datum::new_string("TIME"),
                Datum::new_string("usa"),
                Datum::new_string("%h:%i:%s %p"),
            ),
            (
                Datum::new_string("DATE"),
                Datum::new_string("unknown"),
                Datum::new_string(""),
            ),
            (
                Datum::new_string("YEAR"),
                Datum::new_string("USA"),
                Datum::new_string(""),
            ),
            (
                Datum::new_string("date"),
                Datum::new_string("USA"),
                Datum::new_string(""),
            ),
            (
                Datum::new_bytes([0xff]),
                Datum::new_string("USA"),
                Datum::new_string(""),
            ),
            (
                Datum::new_string("DATE"),
                Datum::new_bytes([0xff]),
                Datum::new_string(""),
            ),
            (Datum::Null, Datum::MinNotNull, Datum::Null),
            (Datum::new_string("DATE"), Datum::Null, Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::time_fn::dispatch("GET_FORMAT", &[kind, location], columns).unwrap()
            });
            assert_eq!(
                result,
                Ok(expected),
                "scalar eval_string keeps raw bytes and skips location after actual first NULL"
            );
            assert_wide_math_c4(observation);
        }
        let (result, observation) =
            observe_wide_math(|| typed.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Ok(Datum::new_string("%m.%d.%Y")));
        assert_wide_math_c4(observation);
        for (location, expected) in [
            (Datum::new_string("USA"), Datum::new_string("%m.%d.%Y")),
            (Datum::Null, Datum::Null),
        ] {
            native.value.replace(location);
            native.reads.set(0);
            let (result, observation) = observe_wide_math(|| {
                calendar_fields_ast("GET_FORMAT(DATE, format_location)", columns)
            });
            assert_eq!(result, Ok(expected));
            assert_eq!(
                native.reads.get(),
                1,
                "the grammar location child is evaluated exactly once"
            );
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for values in [
            vec![Datum::new_string("DATE"), Datum::new_string("USA")],
            vec![Datum::Null, Datum::MinNotNull],
            vec![Datum::new_string("DATE"), Datum::Null],
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("GET_FORMAT", &values, columns).unwrap());
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (values, expected) in [
            (vec![Datum::Null], EvalError::Unsupported("bad function arity")),
            (vec![Datum::MinNotNull, Datum::Null], EvalError::Unsupported("un-cast types.ETString argument")),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("GET_FORMAT", &values, columns).unwrap());
            assert_eq!(result, Err(expected), "arity and actual eval_string errors remain in preparation");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let (result, observation) = observe_wide_math(|| typed.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        for location in [Datum::new_string("USA"), Datum::Null] {
            native.value.replace(location);
            native.reads.set(0);
            let (result, observation) = observe_wide_math(|| calendar_fields_ast("GET_FORMAT(DATE, format_location)", columns));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(native.reads.get(), 1);
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        native.value.replace(Datum::new_bytes([0xff]));
        native.reads.set(0);
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("GET_FORMAT(DATE, format_location)", columns));
        assert_eq!(result, Err(EvalError::Unsupported("invalid UTF-8 byte datum")), "AST keeps its original strict coerce_str boundary");
        assert_eq!(native.reads.get(), 1);
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

fn month_seconds_function(name: &str, input: Datum) -> crate::scalar_function::ScalarFunction {
    if name == "TIME_TO_SEC" {
        return hms_fields_function(name, vec![input], false, false);
    }
    assert_eq!(name, "MONTHNAME");
    let field = FieldType::new(FieldTypeCode::VarString);
    crate::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new(name),
        field.clone(),
        vec![crate::expression::Expression::Constant(Constant::new(
            input, field,
        ))],
    )
}

#[test]
fn month_seconds_dispatch_keeps_month_reparse_and_seconds_text_policy() {
    use tidb_datatype::MySqlDuration;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Fixed month_and_monthname_source_vectors values, not the new table
        // or getters as an oracle. Native typed dates still Display/reparse.
        for (input, expected) in [
            (Datum::new_string("2017-12-01"), Datum::new_string("December")),
            (Datum::new_string("2000-01-01"), Datum::new_string("January")),
            (Datum::new_string("2011-11-11"), Datum::new_string("November")),
            (Datum::new_string("0000-01-01"), Datum::new_string("January")),
            (Datum::new_string("2017-00-01"), Datum::Null),
            (Datum::new_string("0000-00-00"), Datum::Null),
            (Datum::new_string("2008-13-01"), Datum::Null),
            (Datum::Int(20_240_315), Datum::new_string("March")),
            (Datum::Time(Time::new(CoreTime::from_date(2024, 15, 1, 0, 0, 0, 0), TimeType::Date, 0).unwrap()), Datum::Null),
            (Datum::Time(Time::new(CoreTime::from_date(2024, 2, 0, 0, 0, 0, 0), TimeType::Date, 0).unwrap()), Datum::Null),
            (Datum::Null, Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("MONTHNAME", &[input], columns).unwrap());
            assert_eq!(result, Ok(expected), "MONTHNAME must retain its old date parser, not wide raw-core MONTH policy");
            assert_wide_math_c4(observation);
        }
        // First four are time_to_sec_source_vectors. The remaining literals
        // pin the original duration text policy, distinct from HMS clamping.
        for (input, expected) in [
            (Datum::new_string("22:23:00"), Datum::Int(80_580)),
            (Datum::new_string("00:39:38"), Datum::Int(2_378)),
            (Datum::new_string("-02:00:05"), Datum::Int(-7_205)),
            (Datum::new_string("020005"), Datum::Int(7_205)),
            (Datum::Int(20_005), Datum::Int(7_205)),
            (Datum::new_string("2010-10-10 02:00:05.123456"), Datum::Int(7_205)),
            (Datum::new_string("02:00:05.not-digits"), Datum::Int(7_205)),
            (Datum::new_string("900:30:15"), Datum::Null),
            (Datum::new_string("02:60:05"), Datum::Null),
            (Datum::new_string("02:00:60"), Datum::Null),
            (Datum::new_string("not-a-time"), Datum::Int(0)),
            (Datum::Duration(MySqlDuration::new(2, 0, 5, 999_999, 3).unwrap()), Datum::Int(7_205)),
            (Datum::Duration(MySqlDuration::new(900, 30, 15, 0, 0).unwrap()), Datum::Null),
            (Datum::Null, Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch("TIME_TO_SEC", &[input], columns).unwrap());
            assert_eq!(result, Ok(expected), "native Duration uses Display/FSP; fractions neither round seconds nor acquire new validation");
            assert_wide_math_c4(observation);
        }
        for (sql, expected) in [
            ("MONTHNAME('0000-01-01')", Datum::new_string("January")),
            ("TIME_TO_SEC('-02:00:05')", Datum::Int(-7_205)),
        ] {
            let (result, observation) = observe_wide_math(|| calendar_fields_ast(sql, columns));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        for (name, input, expected) in [
            ("MONTHNAME", "2011-11-11", Datum::new_string("November")),
            ("TIME_TO_SEC", "00:39:38", Datum::Int(2_378)),
        ] {
            let function = month_seconds_function(name, Datum::new_string(input));
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn month_seconds_dispatch_preserves_preparation_order_context_and_unwind() {
    struct Probe {
        reject_zero: Cell<bool>,
        events: RefCell<Vec<String>>,
    }
    impl Columns for Probe {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn date_modes(&self) -> DateModes {
            self.events.borrow_mut().push("modes".to_owned());
            DateModes {
                no_zero_date: self.reject_zero.get(),
                ..DateModes::TIDB_DEFAULT_SQL_MODE
            }
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push("zone".to_owned());
            SessionTimeZone::Fixed {
                name: "monthname-context".to_owned(),
                offset_secs: 0,
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            let before = EVAL_ONE_OBSERVATION.with(|slot| {
                slot.borrow().as_ref().is_some_and(|value| {
                    value.facade_entries == 0
                        && value.before_kernel_invocations.is_none()
                        && value.after_kernel_invocations.is_none()
                })
            });
            self.events
                .borrow_mut()
                .push(format!("warn:{code}:{message}:before:{before}"));
        }
    }
    let native = Probe {
        reject_zero: Cell::new(true),
        events: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (name, ordinary, bad) in [("MONTHNAME", "2017-12-01", "2017-00-01"), ("TIME_TO_SEC", "22:23:00", "900:30:15")] {
            for input in [Datum::new_string(ordinary), Datum::Null, Datum::new_string(bad)] {
                let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &[input], columns).unwrap());
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "bad valid text and SQL NULL still require worker admission");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
        for (name, values, expected) in [
            ("MONTHNAME", vec![Datum::new_bytes([0xff])], EvalError::Unsupported("invalid UTF-8 byte datum")),
            ("TIME_TO_SEC", vec![Datum::new_string([0xff])], EvalError::Unsupported("invalid UTF-8 string datum")),
            ("MONTHNAME", Vec::new(), EvalError::Unsupported("bad function arity")),
            ("TIME_TO_SEC", vec![Datum::new_string("22:23:00"), Datum::Null], EvalError::Unsupported("bad function arity")),
        ] {
            let (result, observation) = observe_wide_math(|| crate::time_fn::dispatch(name, &values, columns).unwrap());
            assert_eq!(result, Err(expected));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("TIME_TO_SEC('22:23:00')", columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        let function = month_seconds_function("MONTHNAME", Datum::new_string("2011-11-11"));
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        native.events.borrow_mut().clear();
        for reject_zero in [true, false] {
            native.reject_zero.set(reject_zero);
            let function = month_seconds_function("MONTHNAME", Datum::new_string("0000-00-00"));
            let (result, observation) = observe_wide_math(|| {
                if reject_zero {
                    calendar_fields_ast("MONTHNAME('0000-00-00')", columns)
                } else {
                    function.eval(columns, tidb_chunk::row::Row::empty())
                }
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
            let events = native.events.replace(Vec::new());
            assert!(events.iter().any(|event| event == "modes"));
            assert!(events.iter().any(|event| event == "zone"));
            let warnings = events.iter().filter(|event| event.starts_with("warn:")).map(String::as_str).collect::<Vec<_>>();
            if reject_zero {
                assert_eq!(warnings, ["warn:1292:Incorrect datetime value: '0000-00-00 00:00:00.000000':before:true"]);
            } else {
                assert!(warnings.is_empty(), "the original ETDatetime cast still observes the caller's no_zero_date mode");
            }
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();

    // Preserve the old unchecked multiply's unwind in the current test profile;
    // this does not promise release-profile behavior or an EvalError conversion.
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let payload = catch_unwind(AssertUnwindSafe(|| {
        scope.with_columns(&crate::NoColumns, |columns| {
            crate::time_fn::dispatch(
                "TIME_TO_SEC",
                &[Datum::new_string("--9223372036854775808:00")],
                columns,
            )
            .unwrap()
        })
    }))
    .expect_err("the original duration multiply must still unwind in the current test profile");
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
    assert_eq!(message, Some("attempt to multiply with overflow"));
    // A panicked invocation poisons that scope. Do not reuse it or leave an
    // armed observation behind; a fresh scope must rebuild the actual worker.
    drop(scope);
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_wide_math(|| {
            crate::time_fn::dispatch("TIME_TO_SEC", &[Datum::new_string("-02:00:05")], columns)
                .unwrap()
        });
        assert_eq!(result, Ok(Datum::Int(-7_205)));
        assert_wide_math_c4(observation);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 2);
    drop(scope);
    execution.close();
}

fn hms_fields_function(
    name: &str,
    values: Vec<Datum>,
    pb: bool,
    unreadable_tail: bool,
) -> crate::scalar_function::ScalarFunction {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;
    let field = FieldType::new(FieldTypeCode::VarString);
    let mut args = values
        .into_iter()
        .map(|value| Expression::Constant(Constant::new(value, field.clone())))
        .collect::<Vec<_>>();
    if unreadable_tail {
        args.push(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("__undemanded_hms_suffix__"),
            field,
            Vec::new(),
        )));
    }
    let result_type = FieldType::new(FieldTypeCode::LongLong);
    if pb {
        let signature = match name {
            "HOUR" => ScalarFuncSig::Hour,
            "MINUTE" => ScalarFuncSig::Minute,
            "SECOND" => ScalarFuncSig::Second,
            _ => panic!("HMS fixture name"),
        };
        ScalarFunction::from_pb(PbBuiltin::new(signature).unwrap(), result_type, args)
    } else {
        ScalarFunction::new(tidb_ast::CiString::new(name), result_type, args)
    }
}

#[test]
fn hms_fields_dispatch_keeps_source_text_parsing_and_duration_display_lane() {
    use tidb_datatype::MySqlDuration;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // test_clock's fixed literals and the old calendar parser's documented
        // clamp/bare-date rules. No migrated getter or parser computes expected.
        for (input, expected) in [
            (Datum::new_string("10:10:10.123456"), [10, 10, 10]),
            (Datum::new_string("2010-10-10 11:11:11.11"), [11, 11, 11]),
            (Datum::new_string("900:30:15"), [838, 59, 59]),
            (Datum::new_string("2024-01-15"), [0, 20, 24]),
            // This is a native SQL Duration datum, not raw legacy nanoseconds:
            // Display/FSP precedes text parsing, which still clamps all fields.
            (
                Datum::Duration(MySqlDuration::new(900, 30, 15, 123_456, 3).unwrap()),
                [838, 59, 59],
            ),
        ] {
            for (name, expected) in ["HOUR", "MINUTE", "SECOND"].into_iter().zip(expected) {
                let (result, observation) = observe_wide_math(|| {
                    crate::func::eval_func_values(name, std::slice::from_ref(&input), columns)
                        .unwrap()
                });
                assert_eq!(
                    result,
                    Ok(Datum::Int(expected)),
                    "{name} source text policy"
                );
                assert_wide_math_c4(observation);
            }
        }
        for (name, input, expected) in [
            (
                "HOUR",
                Datum::new_string("-12:34:56.123456"),
                Datum::Int(12),
            ),
            ("MINUTE", Datum::new_string("900:60:15"), Datum::Null),
            (
                "SECOND",
                Datum::Duration(MySqlDuration::new(12, 34, 56, 123_456, 3).unwrap()),
                Datum::Int(56),
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values(name, &[input], columns).unwrap()
            });
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        for name in ["HOUR", "MINUTE", "SECOND"] {
            let (result, observation) = observe_wide_math(|| {
                crate::func::eval_func_values(name, &[Datum::Null], columns).unwrap()
            });
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn hms_fields_dispatch_keeps_ast_typed_pb_and_first_null_child_demand() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (name, expected) in [("HOUR", 0), ("MINUTE", 20), ("SECOND", 24)] {
            let (result, observation) = observe_wide_math(|| calendar_fields_ast(&format!("{name}('2024-01-15')"), columns));
            assert_eq!(result, Ok(Datum::Int(expected)));
            assert_wide_math_c4(observation);
            for pb in [false, true] {
                let function = hms_fields_function(name, vec![Datum::new_string("2024-01-15")], pb, false);
                let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
                assert_eq!(result, Ok(Datum::Int(expected)), "no ETDuration cast may replace the native text domain");
                assert_wide_math_c4(observation);
            }
            let function = hms_fields_function(name, vec![Datum::MinNotNull, Datum::Null], true, true);
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(Datum::Null), "PB must neither coerce the bad prefix nor read the suffix before forwarding the observed NULL");
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("HOUR(NULL)", columns));
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation);
        for (name, pb) in [("MINUTE", false), ("SECOND", true)] {
            let function = hms_fields_function(name, vec![Datum::Null], pb, false);
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(result, Ok(Datum::Null));
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn hms_fields_dispatch_admits_before_bad_text_parse_but_after_native_preparation() {
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for name in ["HOUR", "MINUTE", "SECOND"] {
            for input in [Datum::new_string("10:10:10.123456"), Datum::Null, Datum::new_string("12:60:00")] {
                let (result, observation) = observe_wide_math(|| crate::func::eval_func_values(name, &[input], columns).unwrap());
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
                    "valid UTF-8 bad time text must reach admission before the worker can decide SQL NULL");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
        for (name, values, expected) in [
            ("HOUR", vec![Datum::new_bytes([0xff])], EvalError::Unsupported("invalid UTF-8 byte datum")),
            ("MINUTE", vec![Datum::new_string([0xff])], EvalError::Unsupported("invalid UTF-8 string datum")),
            ("SECOND", Vec::new(), EvalError::Unsupported("bad function arity")),
        ] {
            let (result, observation) = observe_wide_math(|| crate::func::eval_func_values(name, &values, columns).unwrap());
            assert_eq!(result, Err(expected));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let (result, observation) = observe_wide_math(|| calendar_fields_ast("HOUR('10:10:10.123456')", columns));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        for (name, pb) in [("MINUTE", false), ("SECOND", true)] {
            let function = hms_fields_function(name, vec![Datum::new_string("10:10:10.123456")], pb, false);
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "the actual context must reach the worker from each public route");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let function = hms_fields_function("HOUR", vec![Datum::MinNotNull, Datum::Null], true, true);
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "PB first-NULL demand skips prefix coercion and the suffix, not worker admission");
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        let function = hms_fields_function("HOUR", vec![Datum::new_string("1:02:03"), Datum::new_string("4:05:06")], true, false);
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(result, Err(EvalError::Unsupported("bad function arity")), "non-NULL PB multi-argument calls retain the original arity error");
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

fn calendar_fields_native(
    name: &str,
    values: &[Datum],
    columns: &dyn Columns,
) -> Result<Datum, EvalError> {
    if name == "YEAR" {
        crate::func::eval_func_values(name, values, columns).unwrap()
    } else {
        crate::time_fn::dispatch(name, values, columns).unwrap()
    }
}

fn calendar_fields_ast(sql: &str, columns: &dyn Columns) -> Result<Datum, EvalError> {
    let tidb_ast::Stmt::Query(query) = tidb_parser::parse(&format!("SELECT {sql}")).unwrap() else {
        panic!("query")
    };
    let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
        panic!("SELECT")
    };
    let tidb_ast::SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("expression")
    };
    crate::eval_in(expr, columns)
}

fn calendar_fields_month(input: Datum, pb: bool) -> crate::scalar_function::ScalarFunction {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    let args = vec![Expression::Constant(Constant::new(
        input,
        FieldType::new(FieldTypeCode::Date),
    ))];
    let result_type = FieldType::new(FieldTypeCode::LongLong);
    if pb {
        ScalarFunction::from_pb(
            PbBuiltin::new(tidb_proto::tipb::ScalarFuncSig::Month).unwrap(),
            result_type,
            args,
        )
    } else {
        ScalarFunction::new(tidb_ast::CiString::new("month"), result_type, args)
    }
}

#[test]
fn calendar_fields_dispatch_preserves_raw_fields_zero_and_temporal_metadata() {
    // Public constructors retain these stored fields; expected values are
    // locked source fields, never computed through the migrated getters.
    let maximum = Datum::Time(
        Time::new(
            CoreTime::from_date(16_383, 15, 31, 17, 18, 19, 123_456),
            TimeType::DateTime,
            6,
        )
        .unwrap(),
    );
    let zero = Datum::Time(Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap());
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (name, maximum_field) in [
            ("YEAR", 16_383),
            ("MONTH", 15),
            ("DAYOFMONTH", 31),
            ("QUARTER", 5),
        ] {
            for (input, expected) in [
                (maximum.clone(), Datum::Int(maximum_field)),
                (zero.clone(), Datum::Int(0)),
                (Datum::Null, Datum::Null),
            ] {
                let (result, observation) = observe_wide_math(|| {
                    calendar_fields_native(name, std::slice::from_ref(&input), columns)
                });
                assert_eq!(
                    result,
                    Ok(expected),
                    "{name} must use the stored core without validating or reparsing it"
                );
                assert_wide_math_c4(observation);
            }
        }
        let core = CoreTime::from_date(2024, 3, 15, 17, 18, 19, 123_456);
        // Date forces FSP zero in Time::new but retains its clock bits;
        // Timestamp keeps FSP six. Neither changes the selected date field.
        for (name, kind, expected) in [
            ("DAYOFMONTH", TimeType::Date, 15),
            ("MONTH", TimeType::Timestamp, 3),
        ] {
            let input = Datum::Time(Time::new(core, kind, 6).unwrap());
            let (result, observation) =
                observe_wide_math(|| calendar_fields_native(name, &[input], columns));
            assert_eq!(result, Ok(Datum::Int(expected)));
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn calendar_fields_dispatch_month_routes_keep_cast_context_and_nullable_worker() {
    struct Probe {
        reject_zero: Cell<bool>,
        events: RefCell<Vec<String>>,
    }
    impl Columns for Probe {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn date_modes(&self) -> DateModes {
            self.events.borrow_mut().push("modes".to_owned());
            DateModes {
                no_zero_date: self.reject_zero.get(),
                ..DateModes::TIDB_DEFAULT_SQL_MODE
            }
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.events.borrow_mut().push("zone".to_owned());
            SessionTimeZone::Fixed {
                name: "calendar-context".to_owned(),
                offset_secs: 0,
            }
        }
        fn append_warning(&self, code: u16, message: &str) {
            let before = EVAL_ONE_OBSERVATION.with(|slot| {
                slot.borrow().as_ref().is_some_and(|value| {
                    value.facade_entries == 0
                        && value.before_kernel_invocations.is_none()
                        && value.after_kernel_invocations.is_none()
                })
            });
            self.events
                .borrow_mut()
                .push(format!("warn:{code}:{message}:before:{before}"));
        }
    }
    let native = Probe {
        reject_zero: Cell::new(true),
        events: RefCell::new(Vec::new()),
    };
    let ordinary = Datum::Time(
        Time::new(
            CoreTime::from_date(2011, 11, 11, 12, 13, 14, 0),
            TimeType::Date,
            0,
        )
        .unwrap(),
    );
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (sql, expected) in [
            ("MONTH('2011-11-11')", Datum::Int(11)),
            ("MONTH(NULL)", Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| calendar_fields_ast(sql, columns));
            assert_eq!(result, Ok(expected));
            assert_wide_math_c4(observation);
        }
        native.events.borrow_mut().clear();
        for pb in [false, true] {
            // Normal one-argument MONTH requests on the typed and PB paths.
            for (input, expected) in [
                (ordinary.clone(), Datum::Int(11)),
                (Datum::Null, Datum::Null),
            ] {
                let function = calendar_fields_month(input, pb);
                let (result, observation) =
                    observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
                assert_eq!(result, Ok(expected));
                assert!(
                    native.events.borrow().is_empty(),
                    "typed temporal values must not stringify/reparse through session casting"
                );
                assert_wide_math_c4(observation);
            }
        }
        let (result, observation) =
            observe_wide_math(|| calendar_fields_ast("MONTH('0000-00-00')", columns));
        assert_eq!(result, Ok(Datum::Null));
        assert_wide_math_c4(observation);
        let events = native.events.replace(Vec::new());
        assert!(events.iter().any(|event| event == "modes"));
        assert!(events.iter().any(|event| event == "zone"));
        assert_eq!(
            events
                .iter()
                .filter(|event| event.starts_with("warn:"))
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["warn:1292:Incorrect datetime value: '0000-00-00 00:00:00.000000':before:true"]
        );
        native.reject_zero.set(false);
        let (result, observation) =
            observe_wide_math(|| calendar_fields_ast("MONTH('0000-00-00')", columns));
        assert_eq!(
            result,
            Ok(Datum::Int(0)),
            "the caller's original date mode still controls the cast"
        );
        assert_wide_math_c4(observation);
        let events = native.events.replace(Vec::new());
        assert!(events.iter().any(|event| event == "modes"));
        assert!(events.iter().any(|event| event == "zone"));
        assert!(!events.iter().any(|event| event.starts_with("warn:")));
    });
    drop(scope);
    execution.close();
}

#[test]
fn calendar_fields_dispatch_refuses_without_fallback_but_keeps_preparation_errors() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    let field = FieldType::new(FieldTypeCode::LongLong);
    // The old PB first-NULL shortcut skips this unreadable suffix, even at
    // extra arity; only the observed NULL is sent to the real MONTH worker.
    let null_prefix = ScalarFunction::from_pb(
        PbBuiltin::new(tidb_proto::tipb::ScalarFuncSig::Month).unwrap(),
        field.clone(),
        vec![
            Expression::Constant(Constant::new(
                Datum::Null,
                FieldType::new(FieldTypeCode::Date),
            )),
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("__undemanded_calendar_tail__"),
                field,
                Vec::new(),
            )),
        ],
    );
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) =
            observe_wide_math(|| null_prefix.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(
            result,
            Ok(Datum::Null),
            "PB NULL must skip the suffix rather than add an arity gate"
        );
        assert_wide_math_c4(observation);
    });
    drop(scope);
    execution.close();

    let ordinary = Datum::Time(
        Time::new(
            CoreTime::from_date(2011, 11, 11, 12, 13, 14, 0),
            TimeType::Date,
            0,
        )
        .unwrap(),
    );
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for name in ["YEAR", "MONTH", "DAYOFMONTH", "QUARTER"] {
            for input in [ordinary.clone(), Datum::Null] {
                let (result, observation) = observe_wide_math(|| calendar_fields_native(name, &[input], columns));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
        for (name, values, expected) in [
            ("MONTH", vec![Datum::new_bytes([0u8; 8])], EvalError::Unsupported("a date-part argument reached the signature without its ETDatetime cast")),
            ("YEAR", Vec::new(), EvalError::Unsupported("bad function arity")),
        ] {
            let (result, observation) = observe_wide_math(|| calendar_fields_native(name, &values, columns));
            assert_eq!(result, Err(expected), "wrong native type (including eight bytes) and arity remain preparation errors");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for sql in ["MONTH('2011-11-11')", "MONTH(NULL)"] {
            let (result, observation) = observe_wide_math(|| calendar_fields_ast(sql, columns));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for pb in [false, true] {
            for input in [ordinary.clone(), Datum::Null] {
                let function = calendar_fields_month(input, pb);
                let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
                assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
                    "ordinary and PB MONTH must retain the actual context even for SQL NULL");
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }
        let (result, observation) = observe_wide_math(|| null_prefix.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
            "PB NULL prefix still skips the suffix but cannot bypass the real worker's admission");
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn json_storage_quote_dispatch_keeps_native_storage_and_quote_conventions() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Original json2.rs storage tables and the locked old inline-size
        // formula; neither wire JSON nor the migrated algorithm is an oracle.
        for (name, input, expected) in [
            ("JSON_STORAGE_FREE", Datum::Null, Ok(Datum::Null)),
            ("JSON_STORAGE_SIZE", Datum::Null, Ok(Datum::Null)),
            ("JSON_QUOTE", Datum::Null, Ok(Datum::Null)),
            ("JSON_STORAGE_FREE", Datum::Int(1), Ok(Datum::Int(0))),
            ("JSON_STORAGE_SIZE", Datum::Int(1), Ok(Datum::Int(9))),
            (
                "JSON_STORAGE_SIZE",
                Datum::new_string("true"),
                Ok(Datum::Int(2)),
            ),
            (
                "JSON_STORAGE_SIZE",
                Datum::new_string("[null,true,false]"),
                Ok(Datum::Int(24)),
            ),
            (
                "JSON_STORAGE_SIZE",
                Datum::new_string("{}"),
                Ok(Datum::Int(9)),
            ),
            (
                "JSON_STORAGE_SIZE",
                Datum::new_string(r#"{"a":1}"#),
                Ok(Datum::Int(29)),
            ),
            (
                "JSON_STORAGE_SIZE",
                Datum::new_string(r#"[{"a":{"a":1},"b":2}]"#),
                Ok(Datum::Int(82)),
            ),
            (
                "JSON_STORAGE_FREE",
                Datum::new_string("a"),
                Err(EvalError::Json(crate::JsonError::InvalidText)),
            ),
            (
                "JSON_QUOTE",
                Datum::new_string(""),
                Ok(Datum::new_string("\"\"")),
            ),
            // Native serde quoting, not wire-Go quoting: controls use JSON
            // escapes, but HTML and the two literal separators are untouched.
            (
                "JSON_QUOTE",
                Datum::new_string("\u{7}\u{b}\0<>&\u{2028}\u{2029}"),
                Ok(Datum::new_string(
                    "\"\\u0007\\u000b\\u0000<>&\u{2028}\u{2029}\"",
                )),
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::dispatch(name, std::slice::from_ref(&input), columns).unwrap()
            });
            if let Ok(Datum::String(expected)) = &expected {
                let Ok(Datum::String(actual)) = &result else {
                    panic!("{name} lost its String carrier")
                };
                assert_eq!(actual.bytes(), expected.bytes());
            }
            assert_eq!(result, expected, "{name}({input:?})");
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();

    // Only this long-key fixture gets more per-call room for ready/column
    // storage; global policy, worker/pool caps, and ordinary cases stay put.
    // This configured allowance is not evidence of an allocator peak.
    let policy = AsciiPoolPolicy::checked(
        1,
        1,
        TEST_POOL_BYTES,
        TEST_WORKER_CAP,
        TEST_CREATION_RESERVATION,
        64,
        8,
        4 * TEST_CALL_BYTES,
    )
    .unwrap();
    let owner = AsciiPoolOwner::new(policy).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let document = Datum::new_string(format!("{{\"{}\":null}}", "k".repeat(65_536)));
    scope.with_columns(&crate::NoColumns, |columns| {
        // Locked old formula: 1 root + 8 header + 11 entry + 65536 key
        // bytes + 0 inline-literal payload. No u16 wire-key restriction.
        for (name, expected) in [("JSON_STORAGE_SIZE", 65_556), ("JSON_STORAGE_FREE", 0)] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::dispatch(name, std::slice::from_ref(&document), columns)
                    .unwrap()
            });
            assert_eq!(result, Ok(Datum::Int(expected)));
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn json_storage_quote_dispatch_preserves_scope_and_preparation_precedence() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Storage parsing now belongs to C4: refused admission wins over bad
        // JSON, deliberately continuing the new JSON scope-priority policy.
        for (name, input) in [
            ("JSON_STORAGE_FREE", Datum::Null),
            ("JSON_STORAGE_SIZE", Datum::Null),
            ("JSON_QUOTE", Datum::Null),
            ("JSON_STORAGE_FREE", Datum::new_string("a")),
            ("JSON_STORAGE_SIZE", Datum::new_string("a")),
            ("JSON_STORAGE_FREE", Datum::Int(1)),
            ("JSON_STORAGE_SIZE", Datum::Int(1)),
            ("JSON_QUOTE", Datum::new_string("native")),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::dispatch(name, std::slice::from_ref(&input), columns).unwrap()
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
                "{name} must not preparse, return SQL NULL, or fall back");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (name, input, expected) in [
            ("JSON_QUOTE", Datum::new_bytes([0xff]), EvalError::Unsupported("invalid UTF-8 string datum")),
            ("JSON_QUOTE", Datum::Int(1), EvalError::Json(crate::JsonError::IncorrectType {
                argument: 1, function: "json_quote", // Original SQL error 3064.
            })),
            ("JSON_STORAGE_SIZE", Datum::new_bytes([0xff]), EvalError::Unsupported("invalid UTF-8 string datum")),
            ("JSON_STORAGE_FREE", Datum::Real(f64::NAN), EvalError::Unsupported("datum JSON conversion")),
            ("JSON_STORAGE_FREE", Datum::Float32(1.0), EvalError::Unsupported("JSON document requires JSON or string")),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::dispatch(name, std::slice::from_ref(&input), columns).unwrap()
            });
            assert_eq!(result, Err(expected), "{name} retains its original preparation error before admission");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        let field = FieldType::new(FieldTypeCode::VarString);
        let function = ScalarFunction::new(
            tidb_ast::CiString::new("json_quote"), field.clone(),
            vec![Expression::Constant(Constant::new(Datum::new_string("native"), field))],
        );
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
            "the normal typed fallback must retain the caller's actual context");
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

fn json_introspection_raw_empty(type_code: u8) -> Datum {
    // Public persisted-JSON constructor, not a private C4 result envelope.
    Datum::Json(tidb_datatype::BinaryJSON::from_encoded_parts(
        type_code,
        Vec::<u8>::new(),
    ))
}

#[test]
fn json_introspection_dispatch_keeps_text_other_null_and_depth_semantics() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (name, input, expected) in [
            ("JSON_VALID", Datum::new_string("[]"), Ok(Datum::Int(1))),
            ("JSON_VALID", Datum::new_bytes([0xff]), Ok(Datum::Int(0))),
            ("JSON_VALID", Datum::Int(3), Ok(Datum::Int(0))),
            ("JSON_VALID", Datum::Null, Ok(Datum::Null)),
            ("JSON_TYPE", Datum::Null, Ok(Datum::Null)),
            ("JSON_DEPTH", Datum::Null, Ok(Datum::Null)),
            // Preserve the original serde document boundary, not the wire parser.
            (
                "JSON_TYPE",
                Datum::new_string("9223372036854775807"),
                Ok(Datum::new_string("INTEGER")),
            ),
            ("JSON_DEPTH", Datum::Int(1), Ok(Datum::Int(1))),
            // The later duplicate replaces the deeper value before depth is taken.
            (
                "JSON_DEPTH",
                Datum::new_string(r#"{"a":[[1]],"a":0}"#),
                Ok(Datum::Int(2)),
            ),
            (
                "JSON_TYPE",
                Datum::new_string("a"),
                Err(EvalError::Json(crate::JsonError::InvalidText)),
            ),
            (
                "JSON_DEPTH",
                Datum::new_string(""),
                Err(EvalError::Json(crate::JsonError::EmptyText)),
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::dispatch(name, std::slice::from_ref(&input), columns).unwrap()
            });
            if matches!(&expected, Ok(Datum::String(_))) {
                assert!(matches!(&result, Ok(Datum::String(_))));
            }
            assert_eq!(result, expected, "{name}({input:?})");
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn json_introspection_dispatch_preserves_typed_tags_and_literal_nonvalidation() {
    use tidb_datatype::{BinaryJSON, MySqlDuration, Opaque, JSON_TYPE_CODE_LITERAL};
    // These native accessor guards are not runtime evidence or envelope tests.
    assert!(
        matches!(EvaluatedBytesResult::Bytes(None).into_json_report(),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
    );
    for result in [
        EvaluatedBytesResult::JsonReport(crate::tikv::JsonReportOutcome::Null)
            .into_int_datum()
            .map(|_| ()),
        EvaluatedBytesResult::JsonReport(crate::tikv::JsonReportOutcome::Null)
            .into_bytes()
            .map(|_| ()),
    ] {
        assert!(
            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
        );
    }

    let date = Time::new(
        CoreTime::from_date(2024, 3, 15, 0, 0, 0, 0),
        TimeType::Date,
        0,
    )
    .unwrap();
    let duration = MySqlDuration::new(1, 2, 3, 0, 0).unwrap();
    let opaque = Opaque {
        type_code: 0,
        bytes: vec![0, 1, 2, 3],
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (name, input, expected) in [
            (
                "JSON_TYPE",
                Datum::Json(BinaryJSON::from_time(date)),
                Ok(Datum::new_string("DATE")),
            ),
            (
                "JSON_TYPE",
                Datum::Json(BinaryJSON::from_duration(duration)),
                Ok(Datum::new_string("TIME")),
            ),
            (
                "JSON_TYPE",
                Datum::Json(BinaryJSON::from_opaque(opaque)),
                Ok(Datum::new_string("OPAQUE")),
            ),
            (
                "JSON_VALID",
                json_introspection_raw_empty(JSON_TYPE_CODE_LITERAL),
                Ok(Datum::Int(1)),
            ),
            // Frozen pre-migration type_name: every non-NULL literal is BOOLEAN,
            // even an empty literal; do not add general binary validation here.
            (
                "JSON_TYPE",
                json_introspection_raw_empty(JSON_TYPE_CODE_LITERAL),
                Ok(Datum::new_string("BOOLEAN")),
            ),
            (
                "JSON_TYPE",
                json_introspection_raw_empty(0xff),
                Err(EvalError::Json(crate::JsonError::InvalidText)),
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::dispatch(name, std::slice::from_ref(&input), columns).unwrap()
            });
            if matches!(&expected, Ok(Datum::String(_))) {
                assert!(matches!(&result, Ok(Datum::String(_))));
            }
            assert_eq!(result, expected, "{name}({input:?})");
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn json_introspection_dispatch_admission_now_precedes_parse_but_not_preparation() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // New scope priority: bad JSON and typed type-name failures occur in
        // the worker, so refusal wins. This is not the old host-parse ordering.
        for (name, input) in [
            ("JSON_VALID", Datum::new_string("[]")),
            ("JSON_VALID", Datum::Null),
            ("JSON_VALID", Datum::Real(f64::INFINITY)), // Others ignores its payload.
            ("JSON_VALID", Datum::new_bytes([0xff])), // Healthy text answer was 0.
            ("JSON_VALID", json_introspection_raw_empty(0xff)), // Binary never validates.
            ("JSON_TYPE", Datum::Null),
            ("JSON_TYPE", Datum::new_string("a")),
            ("JSON_TYPE", json_introspection_raw_empty(0xff)),
            ("JSON_DEPTH", Datum::new_string("[1]")),
            ("JSON_DEPTH", Datum::Null),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::dispatch(name, std::slice::from_ref(&input), columns).unwrap()
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
                "{name} must not preparse, prevalidate, return a constant, or fall back");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for (name, input, expected) in [
            ("JSON_TYPE", Datum::new_bytes([0xff]), EvalError::Unsupported("invalid UTF-8 string datum")),
            ("JSON_DEPTH", Datum::new_bytes([0xff]), EvalError::Unsupported("invalid UTF-8 string datum")),
            ("JSON_VALID", Datum::MinNotNull, EvalError::Unsupported("range sentinel JSON_VALID argument")),
            ("JSON_TYPE", Datum::Int(3), EvalError::Json(crate::JsonError::InvalidTypeForJson {
                argument: 1, function: "json_type",
            })),
            ("JSON_DEPTH", Datum::Real(f64::INFINITY), EvalError::Unsupported("datum JSON conversion")),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::dispatch(name, std::slice::from_ref(&input), columns).unwrap()
            });
            assert_eq!(result, Err(expected), "{name} retains its original preparation error before admission");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        // The normal typed ScalarFunction falls through json_dispatch_typed to
        // eval_func_values_in and the actual-context builtin_ext dispatcher.
        let field = FieldType::new(FieldTypeCode::VarString);
        let function = ScalarFunction::new(
            tidb_ast::CiString::new("json_type"), field.clone(),
            vec![Expression::Constant(Constant::new(Datum::new_string("[]"), field))],
        );
        let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

// Existing SQL frames from builtin_ext/crypto.rs, not C4 result envelopes.
const COMPRESSION_HELLO_FRAME: &[u8] = &[
    5, 0, 0, 0, 0x78, 0x9c, 0xca, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x04, 0, 0, 0xff, 0xff, 0x06, 0x2c,
    0x02, 0x15,
];
const COMPRESSION_LIMIT_FRAME: &[u8] = &[
    2, 0, 0, 0, 0x78, 0x9c, 0xca, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x04, 0, 0, 0xff, 0xff, 0x06, 0x2c,
    0x02, 0x15,
];

#[derive(Default)]
struct CompressionWarningProbe(RefCell<Vec<(u16, String, bool)>>);

impl Columns for CompressionWarningProbe {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn append_warning(&self, code: u16, message: &str) {
        let invoked = EVAL_ONE_OBSERVATION.with(|slot| {
            slot.borrow().as_ref().is_some_and(|observation| {
                matches!((observation.before_kernel_invocations, observation.after_kernel_invocations),
                    (Some(before), Some(after)) if after > before)
            })
        });
        self.0
            .borrow_mut()
            .push((code, message.to_owned(), invoked));
    }
}

#[test]
fn compression_dispatch_compress_keeps_go_bytes_string_carrier_and_nulls() {
    // go_flate.rs::compresses_like_go_zlib's fixed "aaaaaaaaaa" stream,
    // with the original four-byte LE(10) SQL frame; no encoder oracle.
    let golden = vec![
        10u8, 0, 0, 0, 0x78, 0x9c, 0x4a, 0x84, 0x03, 0x40, 0, 0, 0, 0xff, 0xff, 0x14, 0xe1, 0x03,
        0xcb,
    ];
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (input, expected) in [
            (Datum::new_string("aaaaaaaaaa"), Some(golden)),
            (Datum::Null, None),
            (Datum::new_string(""), Some(Vec::new())),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::crypto::dispatch(
                    "COMPRESS",
                    std::slice::from_ref(&input),
                    columns,
                )
                .unwrap()
            });
            match (result.unwrap(), expected) {
                (Datum::Null, None) => {}
                (Datum::String(value), Some(expected)) => {
                    assert_eq!(value.bytes(), expected.as_slice())
                }
                (actual, expected) => {
                    panic!("COMPRESS carrier/value mismatch: {actual:?}, {expected:?}")
                }
            }
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn compression_dispatch_uncompress_keeps_outcomes_and_actual_warning_contexts() {
    // Native accessor isolation only: these are not C4 invocation evidence
    // and do not fabricate a private computed value or a wire envelope.
    assert!(
        matches!(EvaluatedBytesResult::Bytes(None).into_uncompress(),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
    );
    assert!(
        matches!(EvaluatedBytesResult::Uncompress(crate::tikv::UncompressOutcome::Null).into_bytes(),
        Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
    );

    let first = CompressionWarningProbe::default();
    let second = CompressionWarningProbe::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&first, |columns| {
        for (input, expected) in [
            (Datum::Null, Datum::Null),
            (Datum::new_string(""), Datum::new_string("")),
            (
                Datum::new_bytes(COMPRESSION_HELLO_FRAME),
                Datum::new_string("hello"),
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::crypto::dispatch(
                    "UNCOMPRESS",
                    std::slice::from_ref(&input),
                    columns,
                )
                .unwrap()
            });
            match (result.unwrap(), expected) {
                (Datum::Null, Datum::Null) => {}
                (Datum::String(actual), Datum::String(expected)) => {
                    assert_eq!(actual.bytes(), expected.bytes())
                }
                (actual, expected) => {
                    panic!("UNCOMPRESS carrier/value mismatch: {actual:?}, {expected:?}")
                }
            }
            assert!(first.0.borrow().is_empty());
            assert_wide_math_c4(observation);
        }
    });
    let (result, corrupt_call) = scope.with_columns(&first, |columns| {
        observe_wide_math(|| {
            crate::builtin_ext::crypto::dispatch(
                "UNCOMPRESS",
                &[Datum::new_string("12345")],
                columns,
            )
            .unwrap()
        })
    });
    assert_eq!(result, Ok(Datum::Null));
    assert_wide_math_c4(corrupt_call);
    let corrupt_warning = vec![(1259, "ZLIB: Input data corrupted".to_owned(), true)];
    assert_eq!(*first.0.borrow(), corrupt_warning);
    assert!(second.0.borrow().is_empty());

    let (result, limit_call) = scope.with_columns(&second, |columns| {
        observe_wide_math(|| {
            crate::builtin_ext::crypto::dispatch(
                "UNCOMPRESS",
                &[Datum::new_bytes(COMPRESSION_LIMIT_FRAME)],
                columns,
            )
            .unwrap()
        })
    });
    assert_eq!(result, Ok(Datum::Null));
    assert_wide_math_c4(limit_call);
    assert_eq!(
        limit_call.before_kernel_invocations, corrupt_call.after_kernel_invocations,
        "the same operation stays on this scope's worker across native contexts"
    );
    assert_eq!(*second.0.borrow(), vec![(1258,
        "ZLIB: Not enough room in the output buffer (probably, length of uncompressed data was corrupted)".to_owned(), true)]);
    assert_eq!(
        *first.0.borrow(),
        corrupt_warning,
        "the later context must not contaminate the first warning sink"
    );
    drop(scope);
    execution.close();
}

#[test]
fn compression_dispatch_refusal_precedes_sql_outcomes_but_not_string_coercion() {
    let native = CompressionWarningProbe::default();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (name, input) in [
            ("COMPRESS", Datum::new_string("aaaaaaaaaa")),
            ("COMPRESS", Datum::Null),
            ("COMPRESS", Datum::new_string("")),
            ("UNCOMPRESS", Datum::new_bytes(COMPRESSION_HELLO_FRAME)),
            ("UNCOMPRESS", Datum::Null),
            ("UNCOMPRESS", Datum::new_string("")),
            ("UNCOMPRESS", Datum::new_string("12345")),
            ("UNCOMPRESS", Datum::new_bytes(COMPRESSION_LIMIT_FRAME)),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::crypto::dispatch(name, std::slice::from_ref(&input), columns).unwrap()
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
                "{name} must not fall back or predict SQL NULL from the input");
            assert!(native.0.borrow().is_empty(), "corruption/output-limit warnings belong after actual C4");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        for name in ["COMPRESS", "UNCOMPRESS"] {
            let (result, observation) = observe_wide_math(|| {
                crate::builtin_ext::crypto::dispatch(name, &[Datum::MinNotNull], columns).unwrap()
            });
            assert_eq!(result, Err(EvalError::Unsupported("range sentinel string argument")),
                "sql_string_bytes keeps its original error before worker admission");
            assert!(native.0.borrow().is_empty());
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn exp_log_dispatch_preserves_native_bits_ieee_policies_and_null_workers() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (name, input, expected) in [
            // Locked old native-Go EXP fixture (not libm's .0645 neighbor),
            // and tests/math.rs::log10_source_vectors. No algorithm oracle.
            (
                "EXP",
                Datum::Real(1.5),
                Ok(Datum::Real(4.481689070338065_f64)),
            ),
            ("LOG10", Datum::Real(100.0), Ok(Datum::Real(2.0_f64))),
            ("EXP", Datum::Null, Ok(Datum::Null)),
            ("LOG10", Datum::Null, Ok(Datum::Null)),
            ("EXP", Datum::Real(f64::NEG_INFINITY), Ok(Datum::Real(0.0))),
            (
                "EXP",
                Datum::Real(f64::NAN),
                Err(EvalError::DataOutOfRange {
                    value: "DOUBLE",
                    expression: "exp(NaN)".to_owned(),
                }),
            ),
            (
                "EXP",
                Datum::Real(f64::INFINITY),
                Err(EvalError::DataOutOfRange {
                    value: "DOUBLE",
                    expression: "exp(inf)".to_owned(),
                }),
            ),
            ("LOG10", Datum::Real(f64::NAN), Ok(Datum::Real(f64::NAN))),
            (
                "LOG10",
                Datum::Real(f64::INFINITY),
                Ok(Datum::Real(f64::INFINITY)),
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::math_fn::dispatch_values(name, std::slice::from_ref(&input), columns)
                    .unwrap()
            });
            match expected {
                Ok(Datum::Real(expected)) => {
                    let Datum::Real(actual) = result.unwrap() else {
                        panic!("{name} lost its Real carrier")
                    };
                    if expected.is_nan() {
                        assert!(actual.is_nan());
                    } else {
                        assert_eq!(actual.to_bits(), expected.to_bits(), "{name}({input:?})");
                    }
                }
                expected => assert_eq!(result, expected, "{name}({input:?})"),
            }
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn exp_log_dispatch_keeps_diagnostics_before_admission_and_overflow_packing() {
    struct Probe {
        level: Cell<ErrorLevel>,
        events: RefCell<Vec<String>>,
    }
    impl Columns for Probe {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> ErrorLevel {
            self.events.borrow_mut().push("truncate".to_owned());
            self.level.get()
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.events
                .borrow_mut()
                .push(format!("warn:{code}:{message}"));
        }
    }
    let native = Probe {
        level: Cell::new(ErrorLevel::Warn),
        events: RefCell::new(Vec::new()),
    };
    let domain_events = "truncate|warn:1292:Truncated incorrect DOUBLE value: '0junk'|warn:3020:Invalid argument for logarithm";
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (name, input, expected, events) in [
            ("LOG10", "0junk", Ok(Datum::Null), domain_events),
            (
                "EXP",
                "1000junk",
                Err(EvalError::DataOutOfRange {
                    value: "DOUBLE",
                    expression: "exp(1000)".to_owned(),
                }),
                "truncate|warn:1292:Truncated incorrect DOUBLE value: '1000junk'",
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::math_fn::dispatch_values(name, &[Datum::new_string(input)], columns).unwrap()
            });
            assert_eq!(
                result, expected,
                "EXP formats the coerced argument, not source text or its infinite result"
            );
            assert_eq!(native.events.replace(Vec::new()).join("|"), events);
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (name, input, events) in [
            ("EXP", Datum::Real(1000.0), ""),
            ("LOG10", Datum::new_string("0junk"), domain_events),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::math_fn::dispatch_values(name, std::slice::from_ref(&input), columns).unwrap()
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
                "resource refusal must not become EXP's 1690 or LOG10's domain NULL");
            assert_eq!(native.events.replace(Vec::new()).join("|"), events,
                "coercion 1292 and domain 3020 precede even refused worker admission");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        native.level.set(ErrorLevel::Error);
        let (result, observation) = observe_wide_math(|| {
            crate::math_fn::dispatch_values("EXP", &[Datum::new_string("1000junk")], columns).unwrap()
        });
        assert_eq!(result, Err(EvalError::TruncatedWrongValue(
            "Truncated incorrect DOUBLE value: '1000junk'".to_owned())),
            "strict coercion still wins over both admission refusal and EXP overflow");
        assert_eq!(native.events.replace(Vec::new()), ["truncate"]);
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn trig_dispatch_native_go_bits_preserve_reduction_and_signed_zero() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Frozen Go 1.25 vectors and the cot(1) regression from the original
        // math_fn/go_trig.rs tests, not host libm or the moved pure functions.
        for (name, values, expected_bits) in [
            ("SIN", vec![Datum::Real(1e9)], 0x3fe1778cae83c69a),
            ("COS", vec![Datum::Real(1.0)], 0x3fe14a280fb5068c),
            ("TAN", vec![Datum::Real(1.0)], 0x3ff8eb245cbee3a5),
            (
                "COT",
                vec![Datum::Real(1.0)],
                0.6420926159343308_f64.to_bits(),
            ),
            // The original Go zero branches retain y's sign, including the
            // negative-x atan2 quadrant; reversing y/x would produce -pi/2.
            ("ATAN", vec![Datum::Real(-0.0)], 0x8000000000000000),
            (
                "ATAN2",
                vec![Datum::Real(-0.0), Datum::Real(-1.0)],
                0xc00921fb54442d18,
            ),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::math_fn::dispatch_values(name, &values, columns).unwrap()
            });
            let Datum::Real(value) = result.unwrap() else {
                panic!("{name} must retain its Real result carrier")
            };
            assert_eq!(value.to_bits(), expected_bits, "{name}");
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn trig_dispatch_atan2_null_left_still_coerces_right_unlike_pb() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;

    struct Probe {
        values: RefCell<Vec<Datum>>,
        events: RefCell<Vec<String>>,
        level: Cell<ErrorLevel>,
    }
    impl Columns for Probe {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.events.borrow_mut().push(format!("eval:{order}"));
            Ok(self.values.borrow()[order].clone())
        }
        fn truncate_level(&self) -> ErrorLevel {
            self.events.borrow_mut().push("truncate".to_owned());
            self.level.get()
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.events
                .borrow_mut()
                .push(format!("warn:{code}:{message}"));
        }
    }
    let native = Probe {
        values: RefCell::new(Vec::new()),
        events: RefCell::new(Vec::new()),
        level: Cell::new(ErrorLevel::Warn),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (level, expected, events, invoked) in [
            (
                ErrorLevel::Warn,
                Ok(Datum::Null),
                "truncate|warn:1292:Truncated incorrect DOUBLE value: '12x'",
                true,
            ),
            (
                ErrorLevel::Error,
                Err(EvalError::TruncatedWrongValue(
                    "Truncated incorrect DOUBLE value: '12x'".to_owned(),
                )),
                "truncate",
                false,
            ),
        ] {
            native.level.set(level);
            let (result, observation) = observe_wide_math(|| {
                crate::math_fn::atan2(&[Datum::Null, Datum::new_string("12x")], columns)
            });
            assert_eq!(
                result, expected,
                "ordinary atan2 still demands numeric coercion of x after NULL y"
            );
            assert_eq!(native.events.replace(Vec::new()).join("|"), events);
            if invoked {
                assert_wide_math_c4(observation);
            } else {
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        }

        let field = FieldType::new(FieldTypeCode::Double);
        for (signature, values, expected_reads) in [
            (ScalarFuncSig::Atan1Arg, vec![Datum::Null], "eval:0"),
            (
                ScalarFuncSig::Atan2Args,
                vec![Datum::MinNotNull, Datum::Null],
                "eval:0|eval:1",
            ),
        ] {
            *native.values.borrow_mut() = values;
            let mut args: Vec<_> = (0..native.values.borrow().len())
                .map(|order| {
                    let mut constant = Constant::new(Datum::Null, field.clone());
                    constant.param_marker = Some(crate::constant::ParamMarker {
                        order: order as i64,
                    });
                    Expression::Constant(constant)
                })
                .collect();
            args.push(Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("__undemanded_trig_tail__"),
                field.clone(),
                Vec::new(),
            )));
            let function =
                ScalarFunction::from_pb(PbBuiltin::new(signature).unwrap(), field.clone(), args);
            let (result, observation) =
                observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(
                result,
                Ok(Datum::Null),
                "PB NULL neither coerces its earlier sentinel nor checks the extra arity"
            );
            assert_eq!(native.events.replace(Vec::new()).join("|"), expected_reads);
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn trig_dispatch_cot_keeps_overflow_pack_and_pb_nonnull_forwards_context() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;

    let field = FieldType::new(FieldTypeCode::Double);
    let function = ScalarFunction::new(
        tidb_ast::CiString::new("cot"),
        field.clone(),
        vec![Expression::Constant(Constant::new(
            Datum::Real(0.0),
            field.clone(),
        ))],
    );
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) =
            observe_wide_math(|| crate::math_fn::cot(&[Datum::Real(0.0)], columns));
        assert_eq!(result, Err(EvalError::FloatOverflow));
        assert_wide_math_c4(observation);
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(
            result,
            Err(EvalError::DataOutOfRange {
                value: "DOUBLE",
                expression: "cot(0)".to_owned(),
            })
        );
        assert_wide_math_c4(observation);
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // Normal non-NULL calls exercise the PB values callbacks, not the
        // dedicated early-NULL witness. One unary and one binary representative.
        for (signature, values) in [
            (ScalarFuncSig::Sin, vec![Datum::Real(1.0)]),
            (ScalarFuncSig::Atan2Args, vec![Datum::Real(1.0), Datum::Real(2.0)]),
        ] {
            let args = values.into_iter().map(|value| {
                Expression::Constant(Constant::new(value, field.clone()))
            }).collect();
            let function = ScalarFunction::from_pb(PbBuiltin::new(signature).unwrap(), field.clone(), args);
            let (result, observation) = observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
                "PB values callbacks must pass the explicit root context rather than use a one-shot fallback");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[derive(Default)]
struct CharConvProbe {
    values: RefCell<Vec<Datum>>,
    events: RefCell<Vec<String>>,
    strict: Cell<bool>,
}

impl Columns for CharConvProbe {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
        self.events.borrow_mut().push(format!("eval:{order}"));
        Ok(self.values.borrow()[order].clone())
    }
    fn truncate_level(&self) -> ErrorLevel {
        self.events.borrow_mut().push("truncate".to_owned());
        ErrorLevel::Warn
    }
    fn append_warning(&self, code: u16, message: &str) {
        // Observe the existing actual-C4 getter snapshots, not the host decoder.
        let invoked = EVAL_ONE_OBSERVATION.with(|slot| {
            slot.borrow().as_ref().is_some_and(|observation| {
                matches!((observation.before_kernel_invocations, observation.after_kernel_invocations),
                    (Some(before), Some(after)) if after > before)
            })
        });
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:c4={invoked}:{message}"));
    }
    fn strict_sql_mode(&self) -> bool {
        self.events.borrow_mut().push("strict".to_owned());
        self.strict.get()
    }
}

fn observe_char_conv(
    evaluate: impl FnOnce() -> Result<Datum, EvalError>,
) -> (Result<Datum, EvalError>, EvalOneObservation) {
    arm_eval_one_observation();
    let result = evaluate();
    (result, take_eval_one_observation())
}

fn assert_char_conv_c4(observation: EvalOneObservation) {
    assert_eq!(observation.facade_entries, 1);
    assert!(
        observation.after_kernel_invocations.unwrap()
            > observation.before_kernel_invocations.unwrap()
    );
}

#[test]
fn char_conv_dispatch_char_empty_null_numbers_and_four_byte_values() {
    // Decoder refusals are not kernel-entry witnesses or empty CHAR results.
    for computed in [
        EvaluatedBytesResult::Bytes(None),
        EvaluatedBytesResult::Int(Datum::Null),
    ] {
        assert!(matches!(computed.into_nonnull_bytes(),
            Err(EvalError::ExpressionAdapterFailure(failure))
                if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract));
    }
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        // The final NULL is the charset sentinel, including the zero-number
        // value-domain call; it is not a claim about SQL registry admission.
        for (values, expected) in [
            (vec![Datum::Null], Vec::new()),
            (vec![Datum::Null, Datum::Null, Datum::Null], Vec::new()),
            (
                vec![
                    Datum::Int(-1),
                    Datum::Null,
                    Datum::Int(0),
                    Datum::Int(0x0102030405),
                    Datum::Null,
                ],
                vec![0xff, 0xff, 0xff, 0xff, 0, 2, 3, 4, 5],
            ),
        ] {
            let (result, observation) =
                observe_char_conv(|| crate::string_fn::char_func_with_context(&values, columns));
            let Datum::Bytes(bytes) = result.unwrap() else {
                panic!("CHAR without USING must keep its Bytes carrier")
            };
            assert_eq!(bytes, expected);
            assert_char_conv_c4(observation);
        }
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_char_conv(|| {
            crate::string_fn::char_func_with_context(&[Datum::Int(65), Datum::new_string("ascii")], columns)
        });
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn char_conv_dispatch_char_keeps_coercion_charset_and_decode_warning_order() {
    let native = CharConvProbe::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let (result, observation) = observe_char_conv(|| {
            crate::string_fn::char_func_with_context(&[
                Datum::new_string("65x"), Datum::new_bytes(b"66x".to_vec()),
                Datum::Json(tidb_datatype::BinaryJSON::parse("false").unwrap()),
                Datum::new_string("CHAR_TEST_MISSING"),
            ], columns)
        });
        assert_eq!(result, Err(EvalError::Unsupported("Unknown charset char_test_missing")));
        assert_eq!(native.events.replace(Vec::new()), [
            "truncate", "warn:1292:c4=false:Truncated incorrect INTEGER value: '65x'",
            "truncate", "warn:1292:c4=false:Truncated incorrect INTEGER value: '66x'",
            "truncate", "warn:1292:c4=false:Truncated incorrect INTEGER value: 'false'",
        ], "all numeric coercions, including JSON rendering, precede charset lookup failure");
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);

        for strict in [true, false] {
            native.strict.set(strict);
            let (result, observation) = observe_char_conv(|| {
                crate::string_fn::char_func_with_context(&[
                    Datum::Int(65), Datum::Int(255), Datum::new_string("ascii"),
                ], columns)
            });
            let result = result.unwrap();
            if strict {
                assert_eq!(result, Datum::Null);
            } else {
                assert!(matches!(&result, Datum::String(_)));
                assert_eq!(result, Datum::new_collation_string("A", tidb_datatype::Collation::AsciiBin));
                assert_eq!(result.collation(), Some(tidb_datatype::Collation::AsciiBin));
            }
            assert_eq!(native.events.replace(Vec::new()), [
                "warn:1300:c4=true:Invalid ascii character string: 'FF'", "strict",
            ], "decode is host packing after actual C4; its warning precedes the strict-mode getter");
            assert_char_conv_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn char_conv_dispatch_conv_keeps_literal_payload_overflow_receipts_and_pb_demand() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_datatype::BinaryLiteral;
    use tidb_proto::tipb::ScalarFuncSig;

    let native = CharConvProbe::default();
    let field = FieldType::new(FieldTypeCode::VarString);
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (values, expected) in [
            (
                vec![
                    Datum::BinaryLiteral(BinaryLiteral::from(vec![0, 0x41])),
                    Datum::Int(16),
                    Datum::Int(10),
                ],
                Ok(Datum::new_string("65")),
            ),
            (
                vec![
                    Datum::new_string("18446744073709551615"),
                    Datum::Int(-10),
                    Datum::Int(16),
                ],
                Ok(Datum::new_string("7FFFFFFFFFFFFFFF")),
            ),
            // The first 2 -> from-base leg sees the full 65-bit payload, not a
            // narrowed/truncated u64. The receipt owns the signless bit digits.
            (
                vec![
                    Datum::BinaryLiteral(BinaryLiteral::from(vec![1, 0, 0, 0, 0, 0, 0, 0, 0])),
                    Datum::Int(16),
                    Datum::Int(10),
                ],
                Err(EvalError::DataOutOfRange {
                    value: "BIGINT UNSIGNED",
                    expression: format!("1{}", "0".repeat(64)),
                }),
            ),
            (
                vec![
                    Datum::new_string("  -18446744073709551616tail  "),
                    Datum::Int(10),
                    Datum::Int(16),
                ],
                Err(EvalError::DataOutOfRange {
                    value: "BIGINT UNSIGNED",
                    expression: "18446744073709551616".to_owned(),
                }),
            ),
        ] {
            let (result, observation) =
                observe_char_conv(|| crate::math_fn::conv_in(&values, columns));
            assert_eq!(result, expected);
            assert_char_conv_c4(observation);
        }
        assert!(native.events.borrow().is_empty());

        for (values, expected_reads) in [
            (vec![Datum::Null], "eval:0"),
            (vec![Datum::MinNotNull, Datum::Null], "eval:0|eval:1"),
        ] {
            *native.values.borrow_mut() = values;
            let mut args: Vec<_> = (0..native.values.borrow().len())
                .map(|order| {
                    let mut constant = Constant::new(Datum::Null, field.clone());
                    constant.param_marker = Some(crate::constant::ParamMarker {
                        order: order as i64,
                    });
                    Expression::Constant(constant)
                })
                .collect();
            args.push(Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new("__undemanded_conv_tail__"),
                field.clone(),
                Vec::new(),
            )));
            let function = ScalarFunction::from_pb(
                PbBuiltin::new(ScalarFuncSig::Conv).unwrap(),
                field.clone(),
                args,
            );
            let (result, observation) =
                observe_char_conv(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(
                result,
                Ok(Datum::Null),
                "PB stops at NULL before coercing prior values or checking arity"
            );
            assert_eq!(native.events.replace(Vec::new()).join("|"), expected_reads);
            assert_char_conv_c4(observation);
        }
        let (result, observation) = observe_char_conv(|| {
            crate::math_fn::conv_in(&[Datum::MinNotNull, Datum::Null, Datum::Int(16)], columns)
        });
        assert_eq!(
            result,
            Err(EvalError::Unsupported("range sentinel CONV argument")),
            "ordinary CONV still scans sentinels before checking NULL bases"
        );
        assert_eq!(observation.facade_entries, 0);
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let args = vec![
            Constant::new(Datum::new_string("10"), field.clone()),
            Constant::new(Datum::Int(10), FieldType::new(FieldTypeCode::LongLong)),
            Constant::new(Datum::Int(16), FieldType::new(FieldTypeCode::LongLong)),
        ].into_iter().map(Expression::Constant).collect();
        let function = ScalarFunction::from_pb(PbBuiltin::new(ScalarFuncSig::Conv).unwrap(), field.clone(), args);
        let (result, observation) = observe_char_conv(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
            "non-NULL PB CONV must forward ctx through its values callback, with no fallback or fake SQL overflow");
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

fn observe_wide_math<T>(
    evaluate: impl FnOnce() -> Result<T, EvalError>,
) -> (Result<T, EvalError>, EvalOneObservation) {
    arm_eval_one_observation();
    let result = evaluate();
    (result, take_eval_one_observation())
}

fn assert_wide_math_c4(observation: EvalOneObservation) {
    assert_eq!(observation.facade_entries, 1);
    assert!(
        observation.after_kernel_invocations.unwrap()
            > observation.before_kernel_invocations.unwrap()
    );
}

#[test]
fn wide_math_dispatch_preserves_value_kinds_wide_storage_and_typed_decimal() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use tidb_datatype::Decimal;

    // Only public constructors/arithmetic build this >nine-word value; its
    // hidden division fraction must not pass through Display or i128.
    let hidden = Decimal::from_int(-1)
        .div_mysql(&Decimal::from_int(100_000), 4)
        .unwrap();
    let wide = Decimal::max_or_min(true, 90, 0)
        .add(&hidden)
        .with_declared_shape(100, 4);
    assert!(wide.coefficient_digits().len() > 81);
    assert!(wide.coefficient_i128().is_none());
    assert!(wide.storage_scale() > wide.scale());
    assert!(wide.is_negative());
    assert_eq!(wide.declared_shape(), Some((100, 4)));

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for (name, values, expected) in [
            ("ABS", vec![Datum::Float32(-1.25)], Datum::Real(1.25)),
            ("FLOOR", vec![Datum::Float32(1.75)], Datum::Float32(1.0)),
            (
                "TRUNCATE",
                vec![Datum::Float32(1.75), Datum::Int(1)],
                Datum::Float32(1.7),
            ),
            ("ROUND", vec![Datum::UInt(u64::MAX)], Datum::UInt(u64::MAX)),
        ] {
            let (result, observation) = observe_wide_math(|| {
                crate::math_fn::dispatch_values(name, &values, columns).unwrap()
            });
            let result = result.unwrap();
            assert_eq!(
                std::mem::discriminant(&result),
                std::mem::discriminant(&expected),
                "{name}"
            );
            assert_eq!(result, expected, "{name}");
            assert_wide_math_c4(observation);
        }

        // The generic native pack return must not detour through a 64-bit Datum.
        arm_eval_one_observation();
        let integer = crate::eval_legacy_round_int_in(Some(i128::MIN), columns);
        assert_eq!(integer, Ok(Some(i128::MIN)));
        assert_wide_math_c4(take_eval_one_observation());

        let (result, observation) = observe_wide_math(|| {
            crate::math_fn::dispatch_values("ABS", &[Datum::Decimal(wide.clone())], columns)
                .unwrap()
        });
        let Datum::Decimal(result) = result.unwrap() else {
            panic!("wide ABS must return Decimal")
        };
        assert!(!result.is_negative());
        assert_eq!(result.coefficient_digits(), wide.coefficient_digits());
        assert_eq!(
            (result.scale(), result.storage_scale()),
            (wide.scale(), wide.storage_scale())
        );
        assert_eq!(result.declared_shape(), None);
        assert_wide_math_c4(observation);

        let decimal = |text| {
            Expression::Constant(Constant::new(
                Datum::Decimal(Decimal::from_literal(text)),
                FieldType::new(FieldTypeCode::NewDecimal),
            ))
        };
        for (name, args, result_type, expected) in [
            (
                "round",
                vec![
                    decimal("1.2567"),
                    Expression::Constant(Constant::new(
                        Datum::Int(4),
                        FieldType::new(FieldTypeCode::LongLong),
                    )),
                ],
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(20)
                    .with_decimal(2),
                "1.26",
            ),
            (
                "ceil",
                vec![decimal("1.2")],
                FieldType::new(FieldTypeCode::NewDecimal)
                    .with_flen(20)
                    .with_decimal(0),
                "2",
            ),
        ] {
            let function = ScalarFunction::new(tidb_ast::CiString::new(name), result_type, args);
            let (result, observation) =
                observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            let Datum::Decimal(result) = result.unwrap() else {
                panic!("typed {name} lost its Decimal result domain")
            };
            assert_eq!(
                result.to_string(),
                expected,
                "typed result scale caps ROUND's requested scale"
            );
            assert_eq!(
                result.declared_shape(),
                None,
                "CEIL must not collapse to Int and recast through the return column shape"
            );
            assert_wide_math_c4(observation);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn wide_math_dispatch_pb_round_null_keeps_child_demand_and_native_precedence() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;

    #[derive(Default)]
    struct Params {
        values: RefCell<Vec<Datum>>,
        reads: RefCell<Vec<usize>>,
    }
    impl Columns for Params {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.reads.borrow_mut().push(order);
            Ok(self.values.borrow()[order].clone())
        }
    }
    let native = Params::default();
    let build = |signature, result_code| {
        let mut args: Vec<_> = (0..native.values.borrow().len())
            .map(|order| {
                let mut constant =
                    Constant::new(Datum::Null, FieldType::new(FieldTypeCode::Double));
                constant.param_marker = Some(crate::constant::ParamMarker {
                    order: order as i64,
                });
                Expression::Constant(constant)
            })
            .collect();
        args.push(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("__undemanded_round_tail__"),
            FieldType::new(FieldTypeCode::Double),
            Vec::new(),
        )));
        ScalarFunction::from_pb(
            PbBuiltin::new(signature).unwrap(),
            FieldType::new(result_code),
            args,
        )
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (signature, result_code, values, reads) in [
            (
                ScalarFuncSig::RoundInt,
                FieldTypeCode::LongLong,
                vec![Datum::Null],
                vec![0],
            ),
            (
                ScalarFuncSig::RoundDec,
                FieldTypeCode::NewDecimal,
                vec![Datum::MinNotNull, Datum::Null],
                vec![0, 1],
            ),
        ] {
            *native.values.borrow_mut() = values;
            let function = build(signature, result_code);
            let (result, observation) =
                observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
            assert_eq!(
                result,
                Ok(Datum::Null),
                "PB NULL precedes coercion and malformed-arity rejection"
            );
            assert_eq!(native.reads.replace(Vec::new()), reads);
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| {
            crate::math_fn::dispatch_values(
                "ROUND",
                &[Datum::MinNotNull, Datum::Null, Datum::Int(0)],
                columns,
            )
            .unwrap()
        });
        assert_eq!(
            result,
            Err(EvalError::Unsupported(
                "range sentinel ROUND/TRUNCATE argument"
            )),
            "ordinary math keeps sentinel-before-NULL-before-arity, unlike PB's child loop"
        );
        assert_eq!(observation.facade_entries, 0);
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        *native.values.borrow_mut() = vec![Datum::MinNotNull, Datum::Null];
        let function = build(ScalarFuncSig::RoundReal, FieldTypeCode::Double);
        let (result, observation) = observe_wide_math(|| {
            function.eval(columns, tidb_chunk::row::Row::empty())
        });
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(native.reads.replace(Vec::new()), [0, 1]);
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn wide_math_dispatch_abs_overflow_requires_actual_c4_receipt() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;

    let function = ScalarFunction::new(
        tidb_ast::CiString::new("abs"),
        FieldType::new(FieldTypeCode::LongLong),
        vec![Expression::Constant(Constant::new(
            Datum::Int(i64::MIN),
            FieldType::new(FieldTypeCode::LongLong),
        ))],
    );
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let (result, observation) = observe_wide_math(|| {
            crate::math_fn::dispatch_values("ABS", &[Datum::Int(i64::MIN)], columns).unwrap()
        });
        assert_eq!(result, Err(EvalError::IntOverflow));
        assert_wide_math_c4(observation);
        let (result, observation) =
            observe_wide_math(|| function.eval(columns, tidb_chunk::row::Row::empty()));
        assert_eq!(
            result,
            Err(EvalError::DataOutOfRange {
                value: "BIGINT",
                expression: "abs(-9223372036854775808)".to_owned(),
            })
        );
        assert_wide_math_c4(observation);
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        for typed in [false, true] {
            let (result, observation) = observe_wide_math(|| {
                if typed {
                    function.eval(columns, tidb_chunk::row::Row::empty())
                } else {
                    crate::math_fn::dispatch_values("ABS", &[Datum::Int(i64::MIN)], columns).unwrap()
                }
            });
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "refused ABS must not invent IntOverflow or its 1690 expression wrapper");
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[derive(Default)]
struct SetFieldProbe {
    values: RefCell<Vec<Datum>>,
    events: RefCell<Vec<String>>,
}

impl Columns for SetFieldProbe {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
        self.events.borrow_mut().push(format!("eval:{order}"));
        Ok(self.values.borrow()[order].clone())
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
}

impl SetFieldProbe {
    fn typed(
        &self,
        name: &str,
        values: Vec<Datum>,
        return_type: FieldType,
        columns: &dyn Columns,
    ) -> (Result<Datum, EvalError>, EvalOneObservation, String) {
        *self.values.borrow_mut() = values;
        let args = (0..self.values.borrow().len())
            .map(|order| {
                let mut constant =
                    Constant::new(Datum::Null, FieldType::new(FieldTypeCode::VarString));
                constant.param_marker = Some(crate::constant::ParamMarker {
                    order: order as i64,
                });
                crate::expression::Expression::Constant(constant)
            })
            .collect();
        let function = crate::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new(name),
            return_type,
            args,
        );
        arm_eval_one_observation();
        let result = function.eval(columns, tidb_chunk::row::Row::empty());
        let observation = take_eval_one_observation();
        (
            result,
            observation,
            self.events.replace(Vec::new()).join("|"),
        )
    }
}

fn assert_set_field_c4(observation: EvalOneObservation) {
    assert_eq!(observation.facade_entries, 1);
    assert!(
        observation.after_kernel_invocations.unwrap()
            > observation.before_kernel_invocations.unwrap()
    );
}

#[test]
fn set_field_dispatch_keeps_typed_mode_eager_children_and_coercion_cutoff() {
    let native = SetFieldProbe::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let (result, observation, trace) = native.typed(
            "field",
            vec![Datum::new_string("12abc"), Datum::new_string("1"), Datum::Null,
                Datum::new_string("12"), Datum::Int(0), Datum::new_string("bad_tail")],
            FieldType::new(FieldTypeCode::LongLong),
            columns,
        );
        assert_eq!(result, Ok(Datum::Int(3)));
        assert_eq!(trace, "eval:0|eval:1|eval:2|eval:3|eval:4|eval:5|warn:1292:Truncated incorrect DOUBLE value: '12abc'",
            "the later Int selects Real for the whole list; all children precede one needle warning, but matching stops candidate coercion");
        assert_set_field_c4(observation);

        let (result, observation, trace) = native.typed(
            "field", vec![Datum::UInt(u64::MAX), Datum::Int(-1), Datum::UInt(u64::MAX)],
            FieldType::new(FieldTypeCode::LongLong), columns,
        );
        assert_eq!(result, Ok(Datum::Int(2)), "equal bits do not erase mixed integer signedness");
        assert_eq!(trace, "eval:0|eval:1|eval:2");
        assert_set_field_c4(observation);

        let (result, observation, trace) = native.typed(
            "field", vec![Datum::new_string("ABC"), Datum::new_string("abc")],
            FieldType::new(FieldTypeCode::LongLong).with_collation(tidb_datatype::Collation::Utf8Mb4GeneralCi),
            columns,
        );
        assert_eq!(result, Ok(Datum::Int(1)), "derived collation is not the plain datum's collation");
        assert_eq!(trace, "eval:0|eval:1");
        assert_set_field_c4(observation);
    });
    drop(scope);
    execution.close();
}

#[test]
fn set_field_dispatch_export_keeps_distinct_frontend_policies() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let check = |coercing, values: &[Datum], expected: Result<Datum, EvalError>| {
            arm_eval_one_observation();
            let result = if coercing {
                crate::builtin_ext::dispatch("EXPORT_SET", values, columns).unwrap()
            } else {
                crate::string_fn::export_set_in(values, columns)
            };
            let observation = take_eval_one_observation();
            assert_eq!(result, expected);
            if expected.is_ok() {
                assert_set_field_c4(observation);
            } else {
                assert_eq!(observation.facade_entries, 0);
                assert_eq!(observation.before_kernel_invocations, None);
                assert_eq!(observation.after_kernel_invocations, None);
            }
        };
        // A coerces both on/off before count=0; B checks any NULL first.
        let nullable = [
            Datum::Int(1),
            Datum::Null,
            Datum::MinNotNull,
            Datum::new_string(""),
            Datum::Int(0),
        ];
        check(
            true,
            &nullable,
            Err(EvalError::Unsupported("range sentinel string coercion")),
        );
        check(false, &nullable, Ok(Datum::Null));

        let invalid_utf8 = [
            Datum::Int(1),
            Datum::new_bytes([0xff]),
            Datum::new_string("N"),
            Datum::new_bytes([0xfe]),
            Datum::Int(2),
        ];
        check(
            true,
            &invalid_utf8,
            Err(EvalError::Unsupported("invalid UTF-8 byte datum")),
        );
        check(
            false,
            &invalid_utf8,
            Ok(Datum::new_string("\u{fffd}\u{fffd}N")),
        );

        // B's strict reader still runs when zero count will emit no bytes.
        let uncast_bits = [
            Datum::new_string("1"),
            Datum::new_string("Y"),
            Datum::new_string("N"),
            Datum::new_string(""),
            Datum::Int(0),
        ];
        check(true, &uncast_bits, Ok(Datum::new_string("")));
        check(
            false,
            &uncast_bits,
            Err(EvalError::Unsupported("un-cast types.ETInt argument")),
        );

        // Both original signed >0 tests reject the top bit, but retain bit 0.
        let high_bit = [
            Datum::Int(i64::MIN | 1),
            Datum::new_string("Y"),
            Datum::new_string(""),
            Datum::new_string(""),
            Datum::Int(64),
        ];
        check(true, &high_bit, Ok(Datum::new_string("Y")));
        check(false, &high_bit, Ok(Datum::new_string("Y")));
    });
    drop(scope);
    execution.close();
}

#[test]
fn set_field_dispatch_make_set_demand_and_all_family_root_refusal() {
    let native = SetFieldProbe::default();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        // Only small masks: no assertion about unchecked shifts at index >=64
        // or about overflow-check configuration in any compilation profile.
        for (mask, expected) in [
            (Datum::Int(5), Ok(Datum::new_string(""))),
            (
                Datum::Int(2),
                Err(EvalError::Unsupported("range sentinel string coercion")),
            ),
            (Datum::Null, Ok(Datum::Null)),
        ] {
            let (result, observation, trace) = native.typed(
                "make_set",
                vec![mask, Datum::Null, Datum::MinNotNull, Datum::new_string("")],
                FieldType::new(FieldTypeCode::VarString),
                columns,
            );
            if matches!(&expected, Ok(Datum::String(_))) {
                assert!(matches!(&result, Ok(Datum::String(_))));
            }
            assert_eq!(result, expected);
            assert_eq!(
                trace, "eval:0|eval:1|eval:2|eval:3",
                "all children are eager, only selected candidates are coerced"
            );
            if expected.is_ok() {
                assert_set_field_c4(observation);
            } else {
                assert_eq!(observation.facade_entries, 0);
            }
        }
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let refused = |result: Result<Datum, EvalError>, observation: EvalOneObservation| {
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        };
        let (result, observation, trace) = native.typed(
            "field", vec![Datum::Null, Datum::MinNotNull],
            FieldType::new(FieldTypeCode::LongLong), columns,
        );
        assert_eq!(trace, "eval:0|eval:1", "NULL needle still demands the SQL children");
        refused(result, observation);

        // Mask-only is an existing value-helper boundary, not SQL admission.
        arm_eval_one_observation();
        let result = crate::string_fn::make_set_in(&[Datum::Int(0)], columns);
        refused(result, take_eval_one_observation());

        arm_eval_one_observation();
        let result = crate::builtin_ext::dispatch(
            "EXPORT_SET", &[Datum::Null, Datum::MinNotNull, Datum::MinNotNull], columns,
        ).unwrap();
        refused(result, take_eval_one_observation());
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

// Native argument/policy probe shared by the three streaming-concat tests.
struct ConcatStreamProbe {
    values: RefCell<Vec<Datum>>,
    limits: RefCell<std::collections::VecDeque<u64>>,
    level: Cell<ErrorLevel>,
    events: RefCell<Vec<String>>,
}

impl ConcatStreamProbe {
    fn new() -> Self {
        Self {
            values: RefCell::new(Vec::new()),
            limits: RefCell::new(std::collections::VecDeque::new()),
            level: Cell::new(ErrorLevel::Warn),
            events: RefCell::new(Vec::new()),
        }
    }

    fn evaluate(
        &self,
        name: &str,
        values: Vec<Datum>,
        limits: &[u64],
        columns: &dyn Columns,
    ) -> (Result<Datum, EvalError>, EvalOneObservation, String) {
        *self.values.borrow_mut() = values;
        *self.limits.borrow_mut() = limits.iter().copied().collect();
        let args = (0..self.values.borrow().len())
            .map(|order| {
                let mut constant =
                    Constant::new(Datum::Null, FieldType::new(FieldTypeCode::VarString));
                constant.param_marker = Some(crate::constant::ParamMarker {
                    order: order as i64,
                });
                crate::expression::Expression::Constant(constant)
            })
            .collect();
        let function = crate::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new(name),
            FieldType::new(FieldTypeCode::VarString),
            args,
        );
        arm_eval_one_observation();
        let result = function.eval(columns, tidb_chunk::row::Row::empty());
        let observation = take_eval_one_observation();
        assert!(self.limits.borrow().is_empty(), "missing packet getter");
        (
            result,
            observation,
            self.events.replace(Vec::new()).join("|"),
        )
    }
}

impl Columns for ConcatStreamProbe {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
        self.events.borrow_mut().push(format!("eval:{order}"));
        Ok(self.values.borrow()[order].clone())
    }
    fn max_allowed_packet(&self) -> u64 {
        let limit = self
            .limits
            .borrow_mut()
            .pop_front()
            .expect("extra packet getter");
        self.events.borrow_mut().push(format!("max:{limit}"));
        limit
    }
    fn truncate_level(&self) -> ErrorLevel {
        self.events.borrow_mut().push("level".to_owned());
        self.level.get()
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warn:{code}:{message}"));
    }
}

fn assert_concat_stream_c4(observation: EvalOneObservation) {
    assert_eq!(observation.facade_entries, 1);
    assert!(
        observation.after_kernel_invocations.unwrap()
            > observation.before_kernel_invocations.unwrap()
    );
}

#[test]
fn concat_stream_dispatch_preserves_getters_diagnostics_and_stop() {
    let native = ConcatStreamProbe::new();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let values = || {
            vec![
                Datum::new_string(""),
                Datum::new_string("x"),
                Datum::MinNotNull,
            ]
        };
        let warning = "Result of concat() was larger than max_allowed_packet (7) - truncated";
        let (result, observation, trace) = native.evaluate("concat", values(), &[0, 0, 7], columns);
        assert_eq!(result, Ok(Datum::Null));
        assert_eq!(
            trace,
            format!("eval:0|max:0|eval:1|max:0|max:7|level|warn:1301:{warning}")
        );
        assert_concat_stream_c4(observation);

        // The default handler rereads the limit for its message, without
        // undoing the earlier overflow or demanding the sentinel child.
        native.level.set(ErrorLevel::Error);
        let (result, observation, trace) = native.evaluate("concat", values(), &[0, 0, 7], columns);
        assert_eq!(
            result,
            Err(EvalError::AllowedPacketOverflowed(warning.to_owned()))
        );
        assert_eq!(trace, "eval:0|max:0|eval:1|max:0|max:7|level");
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);

        let (result, observation, trace) = native.evaluate(
            "concat",
            vec![Datum::new_string(""), Datum::Null, Datum::MinNotNull],
            &[0],
            columns,
        );
        assert_eq!(result, Ok(Datum::Null));
        assert_eq!(trace, "eval:0|max:0|eval:1", "NULL has no packet getter");
        assert_concat_stream_c4(observation);
    });
    drop(scope);
    execution.close();
}

#[test]
fn concat_stream_dispatch_ws_preserves_separator_and_original_index_budget() {
    let native = ConcatStreamProbe::new();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let (result, observation, trace) = native.evaluate(
            "concat_ws",
            vec![Datum::new_string(","), Datum::Null, Datum::new_string("a"), Datum::MinNotNull],
            &[1, 9],
            columns,
        );
        assert_eq!(result, Ok(Datum::Null), "the original data index charges the separator even before the first survivor");
        assert_eq!(trace, "eval:0|eval:1|eval:2|max:1|max:9|level|warn:1301:Result of concat_ws() was larger than max_allowed_packet (9) - truncated");
        assert_concat_stream_c4(observation);

        let (result, observation, trace) = native.evaluate(
            "concat_ws", vec![Datum::Null, Datum::MinNotNull], &[], columns,
        );
        assert_eq!(result, Ok(Datum::Null));
        assert_eq!(trace, "eval:0", "NULL separator stops before any data child");
        assert_concat_stream_c4(observation);

        let (result, observation, trace) = native.evaluate(
            "concat_ws", vec![Datum::new_string(","), Datum::Null, Datum::Null], &[], columns,
        );
        assert!(matches!(&result, Ok(Datum::String(_))));
        assert_eq!(result, Ok(Datum::new_string("")));
        assert_eq!(trace, "eval:0|eval:1|eval:2", "separator and NULL data never read packet policy");
        assert_concat_stream_c4(observation);
    });
    drop(scope);
    execution.close();
}

#[test]
fn concat_stream_dispatch_wide_arity_and_terminal_results_keep_explicit_root() {
    let native = ConcatStreamProbe::new();
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let (result, observation, trace) = native.evaluate(
            "concat",
            vec![
                Datum::new_bytes([0xff]),
                Datum::new_string("b"),
                Datum::new_string(""),
                Datum::Int(7),
                Datum::new_string("e"),
            ],
            &[16; 5],
            columns,
        );
        assert!(matches!(&result, Ok(Datum::String(_))));
        assert_eq!(result, Ok(Datum::new_string(vec![0xff, b'b', b'7', b'e'])));
        assert_eq!(
            trace,
            "eval:0|max:16|eval:1|max:16|eval:2|max:16|eval:3|max:16|eval:4|max:16"
        );
        assert_concat_stream_c4(observation);

        let (result, observation, trace) = native.evaluate(
            "concat_ws",
            vec![
                Datum::Int(7),
                Datum::new_string(""),
                Datum::Null,
                Datum::new_string("b"),
                Datum::new_string(""),
                Datum::new_string("d"),
            ],
            &[16; 4],
            columns,
        );
        assert!(matches!(&result, Ok(Datum::String(_))));
        assert_eq!(result, Ok(Datum::new_string("7b77d")));
        assert_eq!(
            trace, "eval:0|eval:1|max:16|eval:2|eval:3|max:16|eval:4|max:16|eval:5|max:16",
            "empty data reads the limit, numeric separator and NULL data do not"
        );
        assert_concat_stream_c4(observation);
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        // Three terminal classes, not a cross-product over names and arities.
        for (name, values, limits) in [
            ("concat", vec![Datum::new_string("")], vec![0]),
            ("concat_ws", vec![Datum::Null, Datum::MinNotNull], vec![]),
            ("concat", vec![Datum::new_string("x"), Datum::MinNotNull], vec![0, 7]),
        ] {
            let (result, observation, _) = native.evaluate(name, values, &limits, columns);
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn collation_search_dispatch_preserves_typed_order_and_values() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use tidb_datatype::Collation;

    struct Params {
        values: RefCell<Vec<Datum>>,
        reads: RefCell<Vec<usize>>,
    }
    impl Columns for Params {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.reads.borrow_mut().push(order);
            Ok(self.values.borrow()[order].clone())
        }
        fn truncate_level(&self) -> ErrorLevel {
            ErrorLevel::Error
        }
    }
    let parameter = |order| {
        let mut constant = Constant::new(Datum::Null, FieldType::new(FieldTypeCode::VarString));
        constant.param_marker = Some(crate::constant::ParamMarker { order });
        Expression::Constant(constant)
    };
    let function = |name: &str, arity| {
        ScalarFunction::new(
            tidb_ast::CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong).with_collation(Collation::Utf8Mb4GeneralCi),
            (0..arity).map(parameter).collect(),
        )
    };
    let native = Params {
        values: RefCell::new(vec![
            Datum::Null,
            Datum::MinNotNull,
            Datum::new_string("bad_pos"),
        ]),
        reads: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let locate3 = function("locate", 3);
        arm_eval_one_observation();
        let result = locate3.eval(columns, tidb_chunk::row::Row::empty());
        let observation = take_eval_one_observation();
        assert!(matches!(result, Err(EvalError::TruncatedWrongValue(message)) if message.contains("bad_pos")), "typed position cast precedes both string coercions, even with a NULL needle");
        assert_eq!(native.reads.replace(Vec::new()), [0, 1, 2]);
        assert_eq!(observation.facade_entries, 0);

        native.values.borrow_mut()[2] = Datum::Int(1);
        arm_eval_one_observation();
        let result = locate3.eval(columns, tidb_chunk::row::Row::empty());
        let observation = take_eval_one_observation();
        assert_eq!(result, Err(EvalError::Unsupported("range sentinel string coercion")));
        assert_eq!(native.reads.replace(Vec::new()), [0, 1, 2]);
        assert_eq!(observation.facade_entries, 0, "a NULL needle does not suppress source coercion");

        // Fixed derived policies, not a change to the process-wide collation mode.
        for (name, values, expected) in [
            ("strcmp", vec![Datum::new_string("A"), Datum::new_string("a")], 0),
            ("locate", vec![Datum::new_string("b"), Datum::new_string("一Ab")], 3),
            ("instr", vec![Datum::new_string("一Ab"), Datum::new_string("b")], 3),
            ("locate", vec![Datum::new_string("b"), Datum::new_string("b一b"), Datum::new_string("2")], 3),
        ] {
            let arity = values.len();
            *native.values.borrow_mut() = values;
            arm_eval_one_observation();
            let result = function(name, arity as i64).eval(columns, tidb_chunk::row::Row::empty());
            let observation = take_eval_one_observation();
            assert_eq!(result, Ok(Datum::Int(expected)), "{name}");
            assert_eq!(native.reads.replace(Vec::new()), (0..arity).collect::<Vec<_>>(), "INSTR evaluates source then needle before swapping");
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(observation.after_kernel_invocations, observation.before_kernel_invocations.map(|before| before + 1));
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn collation_search_dispatch_find_cache_lifecycle_and_nopad() {
    use crate::expression::{ConstLevel, Expression};
    use crate::scalar_function::ScalarFunction;
    use tidb_datatype::Collation;

    struct Params {
        context: Cell<u64>,
        values: RefCell<[Datum; 2]>,
        reads: RefCell<Vec<usize>>,
    }
    impl Columns for Params {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn context_id(&self) -> u64 {
            self.context.get()
        }
        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.reads.borrow_mut().push(order);
            Ok(self.values.borrow()[order].clone())
        }
    }
    let parameter = |order| {
        let mut constant = Constant::new(Datum::Null, FieldType::new(FieldTypeCode::VarString));
        constant.param_marker = Some(crate::constant::ParamMarker { order });
        Expression::Constant(constant)
    };
    let mut function = ScalarFunction::new(
        tidb_ast::CiString::new("find_in_set"),
        FieldType::new(FieldTypeCode::LongLong).with_collation(Collation::Utf8Mb4GeneralCi),
        vec![parameter(0), parameter(1)],
    );
    assert_eq!(function.args[1].const_level(), ConstLevel::ONLY_IN_CONTEXT);
    let native = Params {
        context: Cell::new(71),
        values: RefCell::new([Datum::Null, Datum::Null]),
        reads: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let check = |function: &ScalarFunction, expected, reads: &[usize]| {
            arm_eval_one_observation();
            let result = function.eval(columns, tidb_chunk::row::Row::empty());
            let observation = take_eval_one_observation();
            assert_eq!(result, Ok(expected));
            assert_eq!(native.reads.replace(Vec::new()), reads);
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
        };
        check(&function, Datum::Null, &[0, 1]); // NULL needle still initializes list.
        native.values.borrow_mut()[0] = Datum::MinNotNull;
        check(&function, Datum::Null, &[0]); // Cached NULL skips cast, not needle eval.

        native.context.set(72);
        *native.values.borrow_mut() = [Datum::new_string("a "), Datum::new_string("a,a ,a")];
        check(&function, Datum::Int(2), &[0, 1]); // NoPad keeps the trailing space.
        *native.values.borrow_mut() = [Datum::new_string("a"), Datum::new_string("other")];
        check(&function, Datum::Int(1), &[0]); // Cached first duplicate still wins.

        native.context.set(71);
        native.values.borrow_mut()[1] = Datum::new_string("a ,a");
        check(&function, Datum::Int(2), &[0, 1]); // Context 72 replaced the old NULL slot.
        native.values.borrow_mut()[1] = Datum::new_string("other");
        let cloned = function.clone();
        check(&cloned, Datum::Int(0), &[0, 1]); // Clone has an empty cache.
        check(&function, Datum::Int(2), &[0]);
        function.invalidate_cached_arguments();
        check(&function, Datum::Int(0), &[0, 1]);
    });
    drop(scope);
    execution.close();
}

#[test]
fn collation_search_dispatch_find_constructor_errors_and_root_refusal() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use tidb_datatype::Collation;

    struct Params {
        context: Cell<u64>,
        values: RefCell<[Datum; 2]>,
        fail_list: Cell<bool>,
        reads: RefCell<Vec<usize>>,
    }
    impl Columns for Params {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn context_id(&self) -> u64 {
            self.context.get()
        }
        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.reads.borrow_mut().push(order);
            if order == 1 && self.fail_list.get() {
                return Err(EvalError::Unsupported("find list expression failed"));
            }
            Ok(self.values.borrow()[order].clone())
        }
    }
    let parameter = |order| {
        let mut constant = Constant::new(Datum::Null, FieldType::new(FieldTypeCode::VarString));
        constant.param_marker = Some(crate::constant::ParamMarker { order });
        Expression::Constant(constant)
    };
    let function = ScalarFunction::new(
        tidb_ast::CiString::new("find_in_set"),
        FieldType::new(FieldTypeCode::LongLong).with_collation(Collation::Utf8Mb4GeneralCi),
        vec![parameter(0), parameter(1)],
    );
    let null_function = function.clone();
    let native = Params {
        context: Cell::new(81),
        values: RefCell::new([Datum::Null, Datum::MinNotNull]),
        fail_list: Cell::new(true),
        reads: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let check =
            |function: &ScalarFunction, expected: Result<Datum, EvalError>, reads: &[usize]| {
                arm_eval_one_observation();
                let result = function.eval(columns, tidb_chunk::row::Row::empty());
                let observation = take_eval_one_observation();
                assert_eq!(result, expected);
                assert_eq!(native.reads.replace(Vec::new()), reads);
                assert_eq!(observation.facade_entries, usize::from(expected.is_ok()));
                if expected.is_ok() {
                    assert_eq!(
                        observation.after_kernel_invocations,
                        observation
                            .before_kernel_invocations
                            .map(|before| before + 1)
                    );
                } else {
                    assert_eq!(observation.before_kernel_invocations, None);
                    assert_eq!(observation.after_kernel_invocations, None);
                }
            };
        // Constructor expression/coercion errors precede NULL propagation;
        // repeated attempts in this same context must not cache either error.
        for _ in 0..2 {
            check(
                &function,
                Err(EvalError::Unsupported("find list expression failed")),
                &[0, 1],
            );
        }
        native.fail_list.set(false);
        for _ in 0..2 {
            check(
                &function,
                Err(EvalError::Unsupported("range sentinel byte coercion")),
                &[0, 1],
            );
        }
        native.values.borrow_mut()[1] = Datum::new_string("a,b,a");
        check(&function, Ok(Datum::Null), &[0, 1]);

        native.values.borrow_mut()[0] = Datum::new_string("a");
        native.context.set(82);
        native.fail_list.set(true);
        check(
            &function,
            Err(EvalError::Unsupported("find list expression failed")),
            &[0, 1],
        );
        native.context.set(81);
        check(&function, Ok(Datum::Int(1)), &[0]); // Failed replacement kept the old slot.

        native.fail_list.set(false);
        *native.values.borrow_mut() = [Datum::MinNotNull, Datum::Null];
        check(&null_function, Ok(Datum::Null), &[0, 1]);
    });
    drop(scope);
    execution.close();

    // Reuse the actual typed caches under an explicit exhausted root. Any
    // list reevaluation would now fail; neither cached hits, misses nor NULL
    // may return a native answer or create an alternate one-shot pool.
    native.fail_list.set(true);
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (function, needle) in [
            (&function, Datum::new_string("a")),
            (&function, Datum::new_string("missing")),
            (&null_function, Datum::MinNotNull),
        ] {
            native.values.borrow_mut()[0] = needle;
            arm_eval_one_observation();
            let result = function.eval(columns, tidb_chunk::row::Row::empty());
            let observation = take_eval_one_observation();
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(native.reads.replace(Vec::new()), [0]);
            assert_eq!(observation.facade_entries, 0);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn substring_dispatch_preserves_arity_bytes_and_null_precedence() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (name, values, expected) in [
        (
            "SUBSTRING",
            vec![Datum::new_string("abcd"), Datum::Int(2)],
            Datum::new_string("bcd"),
        ),
        (
            "SUBSTRING",
            vec![
                Datum::new_string("abcd"),
                Datum::Int(2),
                Datum::Int(i64::MAX),
            ],
            Datum::new_string(""),
        ),
        (
            "SUBSTR",
            vec![
                Datum::new_string(vec![0xe2, 0x82, b'z']),
                Datum::Int(1),
                Datum::Int(2),
            ],
            Datum::new_string("\u{fffd}\u{fffd}"),
        ),
        (
            "MID",
            vec![
                Datum::new_bytes([0xe2, 0x82, b'z']),
                Datum::Int(1),
                Datum::Int(2),
            ],
            Datum::new_bytes([0xe2, 0x82]),
        ),
        (
            "SUBSTRING",
            vec![Datum::Null, Datum::MinNotNull],
            Datum::Null,
        ),
        (
            "SUBSTRING",
            vec![Datum::MinNotNull, Datum::Null, Datum::new_string("bad_len")],
            Datum::Null,
        ),
        (
            "SUBSTRING",
            vec![Datum::MinNotNull, Datum::new_string("bad_pos"), Datum::Null],
            Datum::Null,
        ),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in(name, &values, &columns).unwrap();
        let observation = take_eval_one_observation();
        assert!(matches!(
            (&result, &expected),
            (Ok(Datum::Null), Datum::Null)
                | (Ok(Datum::String(_)), Datum::String(_))
                | (Ok(Datum::Bytes(_)), Datum::Bytes(_))
        ));
        assert_eq!(result, Ok(expected), "{name}");
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    for (values, expected) in [
        (vec![Datum::Null], Ok(Datum::Null)),
        (
            vec![Datum::new_string("abcd")],
            Err(EvalError::Unsupported("bad SUBSTRING arguments")),
        ),
    ] {
        arm_eval_one_observation();
        let result = crate::string_fn::substring(&values, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(
            result, expected,
            "malformed helper calls retain NULL before arity"
        );
        assert_eq!(observation.facade_entries, 0);
    }
    drop(scope);
    execution.close();
}

#[test]
fn substring_dispatch_preserves_pb_demand_reader_and_actual_arity() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;
    struct Strict;
    impl Columns for Strict {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> ErrorLevel {
            ErrorLevel::Error
        }
    }
    let field = FieldType::new(FieldTypeCode::VarString);
    let constant = |value| Expression::Constant(Constant::new(value, field.clone()));
    let missing = || {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("__undemanded_substring_child__"),
            field.clone(),
            Vec::new(),
        ))
    };
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&Strict, |columns| {
        for (signature, args, expected) in [
            (
                ScalarFuncSig::Substring2Args,
                vec![constant(Datum::Null), missing()],
                Datum::Null,
            ),
            (
                ScalarFuncSig::Substring2ArgsUtf8,
                vec![constant(Datum::MinNotNull), constant(Datum::Null)],
                Datum::Null,
            ),
            (
                ScalarFuncSig::Substring3Args,
                vec![constant(Datum::Null), missing(), missing()],
                Datum::Null,
            ),
            (
                ScalarFuncSig::Substring3ArgsUtf8,
                vec![
                    constant(Datum::MinNotNull),
                    constant(Datum::new_string("bad_pos")),
                    constant(Datum::Null),
                ],
                Datum::Null,
            ),
            (
                ScalarFuncSig::Substring2ArgsUtf8,
                vec![
                    constant(Datum::new_string("abcd")),
                    constant(Datum::Int(2)),
                    constant(Datum::Int(i64::MAX)),
                ],
                Datum::new_string(""),
            ),
            (
                ScalarFuncSig::Substring3Args,
                vec![constant(Datum::new_string("abcd")), constant(Datum::Int(2))],
                Datum::new_bytes(b"bcd"),
            ),
        ] {
            let function =
                ScalarFunction::from_pb(PbBuiltin::new(signature).unwrap(), field.clone(), args);
            arm_eval_one_observation();
            let result = function.eval(columns, row.to_row());
            let observation = take_eval_one_observation();
            assert_eq!(result, Ok(expected), "{signature:?}");
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
        }
        let function = ScalarFunction::from_pb(
            PbBuiltin::new(ScalarFuncSig::Substring2ArgsUtf8).unwrap(),
            field.clone(),
            vec![
                constant(Datum::Int(1)),
                constant(Datum::new_string("bad_pos")),
            ],
        );
        arm_eval_one_observation();
        let result = function.eval(columns, row.to_row());
        let observation = take_eval_one_observation();
        assert_eq!(
            result,
            Err(EvalError::Unsupported("un-cast types.ETString argument")),
            "PB source reader precedes numeric conversion"
        );
        assert_eq!(observation.facade_entries, 0);
        for (input, expected) in [
            (Datum::Null, Ok(Datum::Null)),
            (
                Datum::new_string("abcd"),
                Err(EvalError::Unsupported("bad SUBSTRING arguments")),
            ),
        ] {
            let function = ScalarFunction::from_pb(
                PbBuiltin::new(ScalarFuncSig::Substring2ArgsUtf8).unwrap(),
                field.clone(),
                vec![constant(input)],
            );
            arm_eval_one_observation();
            let result = function.eval(columns, row.to_row());
            let observation = take_eval_one_observation();
            assert_eq!(result, expected);
            assert_eq!(
                observation.facade_entries, 0,
                "other malformed arities keep their old path"
            );
        }
    });
    drop(scope);
    execution.close();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&Strict, |columns| {
        let function = ScalarFunction::from_pb(PbBuiltin::new(ScalarFuncSig::Substring3ArgsUtf8).unwrap(), field.clone(),
            vec![constant(Datum::MinNotNull), constant(Datum::new_string("bad_pos")), constant(Datum::Null)]);
        assert!(matches!(function.eval(columns, row.to_row()), Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn substring_dispatch_separates_cast_policy_from_execution_context() {
    struct Policy {
        levels: Cell<usize>,
        zones: Cell<usize>,
        warnings: RefCell<Vec<u16>>,
    }
    impl Columns for Policy {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> ErrorLevel {
            self.levels.set(self.levels.get() + 1);
            ErrorLevel::Error
        }
        fn time_zone(&self) -> SessionTimeZone {
            self.zones.set(self.zones.get() + 1);
            crate::NoColumns.time_zone()
        }
        fn append_warning(&self, code: u16, _: &str) {
            self.warnings.borrow_mut().push(code);
        }
    }
    let native = Policy {
        levels: Cell::new(0),
        zones: Cell::new(0),
        warnings: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in("MID", &[Datum::new_string("abcd"), Datum::new_string("bad_pos")], columns).unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::new_string("")));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(observation.after_kernel_invocations, observation.before_kernel_invocations.map(|before| before + 1));
        assert_eq!((native.levels.get(), native.zones.get()), (0, 0), "two-argument casts retain complete NoColumns policy");
        assert!(native.warnings.borrow().is_empty());
        for values in [
            vec![Datum::new_string("abcd"), Datum::Int(99), Datum::new_string("bad_len")],
            vec![Datum::MinNotNull, Datum::Int(1), Datum::new_string("bad_len")],
        ] {
            arm_eval_one_observation();
            let result = crate::string_fn::substring(&values, columns);
            let observation = take_eval_one_observation();
            assert!(matches!(result, Err(EvalError::TruncatedWrongValue(message)) if message.contains("bad_len")), "native length conversion precedes source/bounds");
            assert_eq!(observation.facade_entries, 0);
        }
        assert!(native.levels.get() > 0 && native.zones.get() > 0, "three-argument coercion still uses the real context");
    });
    drop(scope);
    execution.close();
    native.levels.set(0);
    native.zones.set(0);
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in("MID", &[Datum::new_string("abcd"), Datum::new_string("bad_pos")], columns).unwrap();
        let observation = take_eval_one_observation();
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(observation.facade_entries, 0, "NoColumns casts must not create a fallback execution scope");
        let result = crate::string_fn::substring(&[Datum::MinNotNull, Datum::Null, Datum::new_string("bad_len")], columns);
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!((native.levels.get(), native.zones.get()), (0, 0));
        assert!(native.warnings.borrow().is_empty());
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn log_pow_ulength_insert_dispatch_keeps_math_ieee_and_policy() {
    struct Warnings(RefCell<Vec<u16>>);
    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> ErrorLevel {
            ErrorLevel::Warn
        }
        fn append_warning(&self, code: u16, _: &str) {
            self.0.borrow_mut().push(code);
        }
    }
    let native = Warnings(RefCell::new(Vec::new()));
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (name, values, expected, warnings) in [
            ("LN", vec![Datum::Int(1)], Datum::Real(0.0), vec![]),
            ("LOG", vec![Datum::Int(1)], Datum::Real(0.0), vec![]),
            (
                "LOG",
                vec![Datum::Int(2), Datum::Int(2)],
                Datum::Real(1.0),
                vec![],
            ),
            ("LOG2", vec![Datum::Int(8)], Datum::Real(3.0), vec![]),
            (
                "POWER",
                vec![Datum::Int(-2), Datum::Int(3)],
                Datum::Real(-8.0),
                vec![],
            ),
            ("LN", vec![Datum::Real(-0.0)], Datum::Null, vec![3020]),
            (
                "LOG",
                vec![Datum::Int(1), Datum::Int(2)],
                Datum::Null,
                vec![3020],
            ),
            ("LOG2", vec![Datum::Int(0)], Datum::Null, vec![3020]),
            (
                "LOG",
                vec![Datum::Null, Datum::Int(-1)],
                Datum::Null,
                vec![],
            ),
            (
                "LN",
                vec![Datum::new_string("bad")],
                Datum::Null,
                vec![1292, 3020],
            ),
            (
                "LN",
                vec![Datum::Real(f64::NAN)],
                Datum::Real(f64::NAN),
                vec![],
            ),
            (
                "LOG2",
                vec![Datum::Real(f64::INFINITY)],
                Datum::Real(f64::INFINITY),
                vec![],
            ),
        ] {
            native.0.borrow_mut().clear();
            arm_eval_one_observation();
            let result = crate::func::eval_func_values_in(name, &values, columns).unwrap();
            let observation = take_eval_one_observation();
            match (result, expected) {
                (Ok(Datum::Real(actual)), Datum::Real(expected)) if expected.is_nan() => {
                    assert!(actual.is_nan())
                }
                (Ok(Datum::Real(actual)), Datum::Real(expected)) => {
                    assert_eq!(actual.to_bits(), expected.to_bits(), "{name}")
                }
                (actual, expected) => assert_eq!(actual, Ok(expected), "{name}"),
            }
            assert_eq!(*native.0.borrow(), warnings, "{name}");
            assert_eq!(
                observation.facade_entries, 1,
                "NULL/domain policy still invokes C4"
            );
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
        }
        for name in ["POW", "POWER"] {
            arm_eval_one_observation();
            let result = crate::func::eval_func_values_in(
                name,
                &[Datum::Real(1e308), Datum::Int(2)],
                columns,
            )
            .unwrap();
            let observation = take_eval_one_observation();
            assert_eq!(result, Err(EvalError::FloatOverflow));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
        }
        for name in ["LOG", "POW"] {
            arm_eval_one_observation();
            let result =
                crate::func::eval_func_values_in(name, &[Datum::Null, Datum::MinNotNull], columns)
                    .unwrap();
            let observation = take_eval_one_observation();
            assert_eq!(
                result,
                Err(EvalError::Unsupported("range sentinel numeric argument"))
            );
            assert_eq!(
                observation.facade_entries, 0,
                "native NULL left still coerces right"
            );
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn log_pow_ulength_insert_dispatch_keeps_pb_pow_demand_and_arity() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;
    let field = FieldType::new(FieldTypeCode::Double);
    let constant = |value| Expression::Constant(Constant::new(value, field.clone()));
    let missing = Expression::ScalarFunction(ScalarFunction::new(
        tidb_ast::CiString::new("__undemanded_pow_child__"),
        field.clone(),
        Vec::new(),
    ));
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (args, expected) in [
        (vec![constant(Datum::Null), missing], Datum::Null),
        (
            vec![constant(Datum::MinNotNull), constant(Datum::Null)],
            Datum::Null,
        ),
        (
            vec![constant(Datum::Real(2.0)), constant(Datum::Real(3.0))],
            Datum::Real(8.0),
        ),
    ] {
        let function = ScalarFunction::from_pb(
            PbBuiltin::new(ScalarFuncSig::Pow).unwrap(),
            field.clone(),
            args,
        );
        arm_eval_one_observation();
        let result = function.eval(&columns, row.to_row());
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    for (input, expected) in [
        (Datum::Null, Ok(Datum::Null)),
        (
            Datum::Real(2.0),
            Err(EvalError::Unsupported("bad function arity")),
        ),
    ] {
        let function = ScalarFunction::from_pb(
            PbBuiltin::new(ScalarFuncSig::Pow).unwrap(),
            field.clone(),
            vec![constant(input)],
        );
        arm_eval_one_observation();
        let result = function.eval(&columns, row.to_row());
        let observation = take_eval_one_observation();
        assert_eq!(
            result, expected,
            "malformed arity retains the old loop boundary"
        );
        assert_eq!(observation.facade_entries, 0);
    }
    drop(scope);
    execution.close();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for args in [
        vec![constant(Datum::Null), constant(Datum::MinNotNull)],
        vec![constant(Datum::MinNotNull), constant(Datum::Null)],
    ] {
        let function = ScalarFunction::from_pb(
            PbBuiltin::new(ScalarFuncSig::Pow).unwrap(),
            field.clone(),
            args,
        );
        assert!(
            matches!(function.eval(&columns, row.to_row()), Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
        );
    }
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn log_pow_ulength_insert_dispatch_keeps_length_warnings_and_refusal() {
    struct Warnings(RefCell<Vec<u16>>);
    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, _: &str) {
            self.0.borrow_mut().push(code);
        }
        fn max_allowed_packet(&self) -> u64 {
            panic!("UNCOMPRESSED_LENGTH has no packet policy; refused INSERT has no result yet")
        }
    }
    let native = Warnings(RefCell::new(Vec::new()));
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (input, expected, warnings) in [
            (Datum::Null, Datum::Null, vec![]),
            (Datum::new_bytes(Vec::new()), Datum::Int(0), vec![]),
            (Datum::new_bytes([1, 2, 3, 4]), Datum::Int(0), vec![1259]),
            (
                Datum::new_bytes([0xff, 0xff, 0xff, 0xff, 0]),
                Datum::Int(4_294_967_295),
                vec![],
            ),
        ] {
            native.0.borrow_mut().clear();
            arm_eval_one_observation();
            let result =
                crate::func::eval_func_values_in("UNCOMPRESSED_LENGTH", &[input], columns).unwrap();
            let observation = take_eval_one_observation();
            assert_eq!(result, Ok(expected));
            assert_eq!(*native.0.borrow(), warnings);
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
        }
    });
    drop(scope);
    execution.close();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (name, values, warnings) in [
            ("LN", vec![Datum::Int(0)], vec![3020]),
            ("LOG", vec![Datum::Null, Datum::Null], vec![]),
            ("LOG2", vec![Datum::Null], vec![]),
            ("POW", vec![Datum::Null, Datum::Null], vec![]),
            ("UNCOMPRESSED_LENGTH", vec![Datum::new_bytes([1])], vec![1259]),
            ("INSERT_FUNC", vec![Datum::Null, Datum::Null, Datum::Null, Datum::Null], vec![]),
        ] {
            native.0.borrow_mut().clear();
            arm_eval_one_observation();
            let result = crate::func::eval_func_values_in(name, &values, columns).unwrap();
            let observation = take_eval_one_observation();
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "{name}");
            assert_eq!(*native.0.borrow(), warnings, "frontend diagnostic precedes C4 refusal");
            assert_eq!(observation.facade_entries, 0);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn log_pow_ulength_insert_dispatch_keeps_insert_bytes_and_post_packet() {
    struct Packet {
        limit: Cell<u64>,
        getters: Cell<usize>,
        warnings: RefCell<Vec<u16>>,
    }
    impl Columns for Packet {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> ErrorLevel {
            ErrorLevel::Warn
        }
        fn append_warning(&self, code: u16, _: &str) {
            self.warnings.borrow_mut().push(code);
        }
        fn max_allowed_packet(&self) -> u64 {
            self.getters.set(self.getters.get() + 1);
            self.limit.get()
        }
    }
    let native = Packet {
        limit: Cell::new(1024),
        getters: Cell::new(0),
        warnings: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (source, replacement, expected) in [
            (
                Datum::new_bytes("中a".as_bytes()),
                Datum::new_string("X"),
                Datum::new_bytes([0xe4, b'X', 0xad, b'a']),
            ),
            (
                Datum::new_string("中a"),
                Datum::new_string("X"),
                Datum::new_string("中X"),
            ),
            (
                Datum::new_string("中a"),
                Datum::new_string(vec![0xff]),
                Datum::new_string(vec![0xe4, 0xb8, 0xad, 0xff]),
            ),
            (
                Datum::new_string(vec![b'a', 0xe2, 0x82, b'z']),
                Datum::new_string("X"),
                Datum::new_string("aX\u{fffd}z"),
            ),
        ] {
            native.getters.set(0);
            arm_eval_one_observation();
            let result = crate::func::eval_func_values_in(
                "INSERT_FUNC",
                &[source, Datum::Int(2), Datum::Int(1), replacement],
                columns,
            )
            .unwrap();
            let observation = take_eval_one_observation();
            assert!(matches!(
                (&result, &expected),
                (Ok(Datum::String(_)), Datum::String(_)) | (Ok(Datum::Bytes(_)), Datum::Bytes(_))
            ));
            assert_eq!(result, Ok(expected));
            assert_eq!(native.getters.get(), 1);
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
        }
        native.limit.set(1);
        native.getters.set(0);
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in(
            "INSERT_FUNC",
            &[
                Datum::new_string("中a"),
                Datum::Int(2),
                Datum::Int(1),
                Datum::new_string("X"),
            ],
            columns,
        )
        .unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::Null));
        assert_eq!(*native.warnings.borrow(), vec![1301]);
        // The existing overflow handler reads the limit again for its message.
        assert_eq!(native.getters.get(), 2);
        assert_eq!(
            observation.facade_entries, 1,
            "packet policy follows actual C4 computation"
        );
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
        native.getters.set(0);
        native.warnings.borrow_mut().clear();
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in(
            "INSERT_FUNC",
            &[
                Datum::Null,
                Datum::Int(2),
                Datum::Int(1),
                Datum::new_string("X"),
            ],
            columns,
        )
        .unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::Null));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            native.getters.get(),
            0,
            "computed NULL never reads packet policy"
        );
        assert!(native.warnings.borrow().is_empty());
        arm_eval_one_observation();
        let result = crate::string_fn::str_insert(
            &[Datum::Null, Datum::Null, Datum::Null, Datum::MinNotNull],
            columns,
        );
        let observation = take_eval_one_observation();
        assert_eq!(
            result,
            Err(EvalError::Unsupported("range sentinel byte coercion"))
        );
        assert_eq!(
            observation.facade_entries, 0,
            "the original full tuple still demands replacement"
        );
        assert_eq!(native.getters.get(), 0);
    });
    drop(scope);
    execution.close();
}

#[test]
fn trim_subidx_pad_dispatch_preserves_results_and_demand_markers() {
    use tidb_ast::TrimDirection;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (direction, input, removal, binary, expected) in [
        (
            TrimDirection::Both,
            Some(b"ababa".to_vec()),
            Some(b"aba".to_vec()),
            false,
            Datum::new_string("ba"),
        ),
        (
            TrimDirection::Leading,
            Some(b"ababa".to_vec()),
            Some(b"aba".to_vec()),
            false,
            Datum::new_string("ba"),
        ),
        (
            TrimDirection::Trailing,
            Some(b"ababa".to_vec()),
            Some(b"aba".to_vec()),
            false,
            Datum::new_string("ab"),
        ),
        (
            TrimDirection::Both,
            Some(vec![0xff]),
            Some(Vec::new()),
            true,
            Datum::new_bytes([0xff]),
        ),
        (TrimDirection::Both, None, None, false, Datum::Null),
    ] {
        arm_eval_one_observation();
        let result = crate::string_fn::trim_value_in(input, removal, direction, binary, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    for (name, values, expected) in [
        (
            "SUBSTRING_INDEX",
            vec![
                Datum::new_string("aaa"),
                Datum::new_string("aa"),
                Datum::Int(-1),
            ],
            Datum::new_string("a"),
        ),
        (
            "SUBSTRING_INDEX",
            vec![
                Datum::new_bytes(b"a.b"),
                Datum::new_string("."),
                Datum::Int(i64::MIN),
            ],
            Datum::new_bytes(b"a.b"),
        ),
        (
            "SUBSTRING_INDEX",
            vec![
                Datum::new_string("a.b"),
                Datum::new_string("."),
                Datum::UInt(u64::MAX),
            ],
            Datum::new_string("a.b"),
        ),
        (
            "SUBSTRING_INDEX",
            vec![
                Datum::new_string("abc"),
                Datum::new_string(""),
                Datum::MinNotNull,
            ],
            Datum::new_string(""),
        ),
        (
            "SUBSTRING_INDEX",
            vec![Datum::new_string("abc"), Datum::new_string(""), Datum::Null],
            Datum::Null,
        ),
        (
            "SUBSTRING_INDEX",
            vec![Datum::Null, Datum::Null, Datum::Null],
            Datum::Null,
        ),
        (
            "LPAD",
            vec![
                Datum::new_string("ab"),
                Datum::Int(4),
                Datum::new_string("你"),
            ],
            Datum::new_string("你你ab"),
        ),
        (
            "RPAD",
            vec![
                Datum::new_bytes(b"ab"),
                Datum::Int(4),
                Datum::new_bytes([0xff]),
            ],
            Datum::new_bytes([b'a', b'b', 0xff, 0xff]),
        ),
        (
            "LPAD",
            vec![Datum::MinNotNull, Datum::Null, Datum::MinNotNull],
            Datum::Null,
        ),
        (
            "RPAD",
            vec![Datum::Null, Datum::Null, Datum::Null],
            Datum::Null,
        ),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in(name, &values, &columns).unwrap();
        let observation = take_eval_one_observation();
        assert!(
            matches!(
                (&result, &expected),
                (Ok(Datum::Null), Datum::Null)
                    | (Ok(Datum::String(_)), Datum::String(_))
                    | (Ok(Datum::Bytes(_)), Datum::Bytes(_))
            ),
            "{name} result tag"
        );
        assert_eq!(result, Ok(expected), "{name}");
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    drop(scope);
    execution.close();
}

#[test]
fn trim_subidx_pad_dispatch_preserves_order_and_explicit_refusal() {
    use crate::scalar_function::ScalarFunction;
    use tidb_ast::{Expr, TrimDirection};
    struct Order {
        source: RefCell<Datum>,
        reads: RefCell<Vec<String>>,
        packet_getters: Cell<usize>,
    }
    impl Columns for Order {
        fn get(&self, path: &[String]) -> Option<Datum> {
            self.reads.borrow_mut().push(path.join("."));
            (path.len() == 1 && path[0] == "source").then(|| self.source.borrow().clone())
        }
        fn truncate_level(&self) -> ErrorLevel {
            ErrorLevel::Error
        }
        fn max_allowed_packet(&self) -> u64 {
            self.packet_getters.set(self.packet_getters.get() + 1);
            64 << 20
        }
    }
    let native = Order {
        source: RefCell::new(Datum::MinNotNull),
        reads: RefCell::new(Vec::new()),
        packet_getters: Cell::new(0),
    };
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        let ast = Expr::Trim {
            expr: Box::new(Expr::Column(vec!["source".into()])),
            remstr: Some(Box::new(Expr::Column(vec!["remove".into()]))),
            direction: Some(TrimDirection::Both),
        };
        assert_eq!(crate::eval_in(&ast, columns), Err(EvalError::Unsupported("range sentinel byte coercion")));
        assert_eq!(native.reads.borrow().as_slice(), &["source"]);
        *native.source.borrow_mut() = Datum::Null;
        native.reads.borrow_mut().clear();
        assert_eq!(crate::eval_in(&ast, columns), Err(EvalError::Unsupported("unknown column")));
        assert_eq!(native.reads.borrow().as_slice(), &["source", "remove"], "AST NULL source still evaluates removal");

        let field = FieldType::new(FieldTypeCode::VarString);
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        let missing = ScalarFunction::new(tidb_ast::CiString::new("__missing_trim_remove__"), field.clone(), Vec::new());
        let removal_error = missing.eval(columns, row.to_row());
        assert!(removal_error.is_err());
        assert_ne!(removal_error, Err(EvalError::Unsupported("range sentinel byte coercion")));
        let typed = ScalarFunction::new(tidb_ast::CiString::new("trim"), field.clone(), vec![
            crate::expression::Expression::Constant(Constant::new(Datum::MinNotNull, field)),
            crate::expression::Expression::ScalarFunction(missing),
        ]);
        assert_eq!(typed.eval(columns, row.to_row()), removal_error, "typed TRIM evaluates both expressions before coercion");

        assert_eq!(crate::string_fn::substring_index_in(&[Datum::Null, Datum::MinNotNull, Datum::Null], columns), Err(EvalError::Unsupported("range sentinel byte coercion")));
        assert_eq!(crate::string_packet::pad(&[Datum::Null, Datum::Int(0), Datum::new_string(vec![0xff])], true, columns), Err(EvalError::Unsupported("invalid UTF-8 string datum")), "NULL source and zero width still demand pad coercion");
        native.packet_getters.set(0);
        let result = crate::string_packet::pad(&[Datum::MinNotNull, Datum::new_string("bad"), Datum::MinNotNull], true, columns);
        assert!(matches!(result, Err(EvalError::TruncatedWrongValue(_))));
        assert_eq!(native.packet_getters.get(), 0, "length warning/error precedes packet and strings");

        for direction in [TrimDirection::Both, TrimDirection::Leading, TrimDirection::Trailing] {
            let result = crate::string_fn::trim_value_in(None, None, direction, false, columns);
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        }
        for name in ["SUBSTRING_INDEX", "LPAD", "RPAD"] {
            arm_eval_one_observation();
            let result = crate::func::eval_func_values_in(name, &[Datum::Null, Datum::Null, Datum::Null], columns).unwrap();
            let observation = take_eval_one_observation();
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource), "{name}");
            assert_eq!(observation.facade_entries, 0);
        }
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn trim_subidx_pad_dispatch_native_text_pad_exceeds_wire_character_limit() {
    // One native-only successful size, with the unchanged isolated policy and
    // the existing default packet allowance. Do not store a giant expected row.
    let width = 4_194_305_usize;
    arm_eval_one_observation();
    let result = crate::string_packet::pad(
        &[
            Datum::new_string("尾"),
            Datum::Int(width as i64),
            Datum::new_string("你"),
        ],
        true,
        &crate::NoColumns,
    );
    let observation = take_eval_one_observation();
    let Datum::String(value) = result.unwrap() else {
        panic!("native text LPAD must remain text")
    };
    let text = value.as_utf8().unwrap();
    assert_eq!(text.len(), width * 3);
    assert_eq!(text.chars().count(), width);
    assert!(text.starts_with("你你"));
    assert!(text.ends_with("尾"));
    assert_eq!(observation.facade_entries, 1);
    assert_eq!(
        observation.after_kernel_invocations,
        observation
            .before_kernel_invocations
            .map(|before| before + 1)
    );
}

#[test]
fn case_sha2_ord_dispatch_preserves_case_aliases_pb_and_nulls() {
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (name, input, expected) in [
        ("LCASE", Datum::new_string("İA"), Datum::new_string("ia")),
        ("UCASE", Datum::new_string("aßﬁ"), Datum::new_string("Aßﬁ")),
        (
            "LOWER",
            Datum::new_bytes([b'A', 0xff]),
            Datum::new_bytes([b'A', 0xff]),
        ),
        (
            "UPPER",
            Datum::new_string(vec![b'a', 0xe2, 0x82]),
            Datum::new_string("A\u{fffd}\u{fffd}"),
        ),
        ("LOWER", Datum::Null, Datum::Null),
        ("UPPER", Datum::Null, Datum::Null),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values(name, &[input], &columns).unwrap();
        let observation = take_eval_one_observation();
        assert!(matches!(
            (&result, &expected),
            (Ok(Datum::Null), Datum::Null)
                | (Ok(Datum::String(_)), Datum::String(_))
                | (Ok(Datum::Bytes(_)), Datum::Bytes(_))
        ));
        assert_eq!(result, Ok(expected), "{name}");
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    let field = FieldType::new(FieldTypeCode::VarString);
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    for (signature, expected) in [
        (ScalarFuncSig::Lower, Datum::new_bytes([b'A', 0xe2, 0x82])),
        (ScalarFuncSig::Upper, Datum::new_bytes([b'A', 0xe2, 0x82])),
        (
            ScalarFuncSig::LowerUtf8,
            Datum::new_string("a\u{fffd}\u{fffd}"),
        ),
        (
            ScalarFuncSig::UpperUtf8,
            Datum::new_string("A\u{fffd}\u{fffd}"),
        ),
    ] {
        for input in [Datum::new_string(vec![b'A', 0xe2, 0x82]), Datum::Null] {
            let expected = if input.is_null() {
                Datum::Null
            } else {
                expected.clone()
            };
            let function = ScalarFunction::from_pb(
                PbBuiltin::new(signature).unwrap(),
                field.clone(),
                vec![crate::expression::Expression::Constant(Constant::new(
                    input,
                    field.clone(),
                ))],
            );
            arm_eval_one_observation();
            let result = function.eval(&columns, row.to_row());
            let observation = take_eval_one_observation();
            assert_eq!(result, Ok(expected), "{signature:?}");
            assert_eq!(
                observation.facade_entries, 1,
                "PB NULL/no-op cannot bypass C4"
            );
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
        }
    }
    drop(scope);
    execution.close();
}

#[test]
fn case_sha2_ord_dispatch_preserves_selector_and_charset_coercion() {
    use crate::scalar_function::ScalarFunction;
    use tidb_datatype::Decimal;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    let digest =
        Datum::new_string("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    for (input, length, expected) in [
        (
            Datum::new_string("abc"),
            Datum::Decimal(Decimal::from_scaled_i128(2555, 1)),
            digest.clone(),
        ),
        (Datum::new_string("abc"), Datum::Real(256.5), digest),
        (
            Datum::new_string("abc"),
            Datum::Decimal(Decimal::from_scaled_i128(2565, 1)),
            Datum::Null,
        ),
        (Datum::Null, Datum::new_bytes([0xff]), Datum::Null),
        (Datum::new_string("abc"), Datum::Null, Datum::Null),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in("SHA2", &[input, length], &columns).unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    for (input, expected) in [
        (Datum::new_bytes([0xe4, 0xbd, 0xa0]), Datum::Int(228)),
        (Datum::new_string("你"), Datum::Int(14_990_752)),
        (Datum::new_string(""), Datum::Int(0)),
        (Datum::Null, Datum::Null),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values("ORD", &[input], &columns).unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    for (charset, input, expected) in [
        ("ascii", "你", 228),
        ("latin1", "你", 228),
        ("latin1", "é", 195),
    ] {
        let argument_type = FieldType::new(FieldTypeCode::VarString).with_charset_name(charset);
        let function = ScalarFunction::new(
            tidb_ast::CiString::new("ord"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![crate::expression::Expression::Constant(Constant::new(
                Datum::new_string(input),
                argument_type,
            ))],
        );
        arm_eval_one_observation();
        let result = function.eval(&columns, row.to_row());
        let observation = take_eval_one_observation();
        assert_eq!(
            result,
            Ok(Datum::Int(expected)),
            "preserve existing {charset} preparation"
        );
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    drop(scope);
    execution.close();
}

#[test]
fn case_sha2_ord_dispatch_keeps_explicit_refusal_and_error_precedence() {
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (name, values) in [
        ("LOWER", vec![Datum::new_bytes([b'A', 0xff])]),
        ("UPPER", vec![Datum::Null]),
        ("SHA2", vec![Datum::Null, Datum::new_bytes([0xff])]),
        ("ORD", vec![Datum::new_string("")]),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in(name, &values, &columns).unwrap();
        let observation = take_eval_one_observation();
        assert!(
            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource),
            "{name}"
        );
        assert_eq!(observation.facade_entries, 0);
    }
    for (name, values, error) in [
        (
            "LOWER",
            vec![Datum::MinNotNull],
            "range sentinel byte coercion",
        ),
        (
            "SHA2",
            vec![Datum::new_string("abc"), Datum::new_bytes([0xff])],
            "invalid UTF-8 SHA2 length",
        ),
        (
            "SHA2",
            vec![Datum::MinNotNull, Datum::Null],
            "range sentinel hash argument",
        ),
        (
            "ORD",
            vec![Datum::MinNotNull],
            "range sentinel byte coercion",
        ),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in(name, &values, &columns).unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(result, Err(EvalError::Unsupported(error)));
        assert_eq!(observation.facade_entries, 0);
    }
    let field = FieldType::new(FieldTypeCode::VarString);
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    for signature in [
        ScalarFuncSig::Lower,
        ScalarFuncSig::Upper,
        ScalarFuncSig::LowerUtf8,
        ScalarFuncSig::UpperUtf8,
    ] {
        let function = ScalarFunction::from_pb(
            PbBuiltin::new(signature).unwrap(),
            field.clone(),
            vec![crate::expression::Expression::Constant(Constant::new(
                Datum::Null,
                field.clone(),
            ))],
        );
        assert!(
            matches!(function.eval(&columns, row.to_row()), Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
        );
    }
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn packet_string_dispatch_preserves_results_nulls_and_count_demand() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (name, values, expected) in [
        ("SPACE", vec![Datum::Null], Datum::Null),
        ("SPACE", vec![Datum::Int(16_777_217)], Datum::Null),
        ("REPEAT", vec![Datum::Null, Datum::MinNotNull], Datum::Null),
        (
            "REPEAT",
            vec![Datum::new_string("x"), Datum::Null],
            Datum::Null,
        ),
        (
            "REPEAT",
            vec![Datum::new_bytes([0xff, 0]), Datum::Int(2)],
            Datum::new_string(vec![0xff, 0, 0xff, 0]),
        ),
        (
            "REPEAT",
            vec![Datum::new_string(""), Datum::Int(i64::MAX)],
            Datum::new_string(""),
        ),
        (
            "REPEAT",
            vec![Datum::new_string("x"), Datum::UInt(u64::MAX)],
            Datum::new_string(""),
        ),
        ("TO_BASE64", vec![Datum::Null], Datum::Null),
        (
            "TO_BASE64",
            vec![Datum::new_bytes([0xff, 0])],
            Datum::new_string("/wA="),
        ),
        ("FROM_BASE64", vec![Datum::Null], Datum::Null),
        ("FROM_BASE64", vec![Datum::new_string("YQ")], Datum::Null),
        (
            "FROM_BASE64",
            vec![Datum::new_bytes(b"YQ==\x0b")],
            Datum::Null,
        ),
        (
            "FROM_BASE64",
            vec![Datum::new_string("/wA=")],
            Datum::new_bytes([0xff, 0]),
        ),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values_in(name, &values, &columns).unwrap();
        let observation = take_eval_one_observation();
        assert!(
            matches!(
                (&result, &expected),
                (Ok(Datum::Null), Datum::Null)
                    | (Ok(Datum::String(_)), Datum::String(_))
                    | (Ok(Datum::Bytes(_)), Datum::Bytes(_))
            ),
            "{name} retains its result tag"
        );
        assert_eq!(result, Ok(expected), "{name}");
        assert_eq!(
            observation.facade_entries, 1,
            "NULL and empty answers still enter C4"
        );
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    arm_eval_one_observation();
    let result = crate::string_packet::repeat(&[Datum::MinNotNull, Datum::Null], &columns);
    let observation = take_eval_one_observation();
    assert_eq!(
        result,
        Err(EvalError::Unsupported("range sentinel byte coercion"))
    );
    assert_eq!(
        observation.facade_entries, 0,
        "left coercion precedes right NULL"
    );
    drop(scope);
    execution.close();
}

#[test]
fn packet_string_dispatch_to_base64_above_wire_blob_limit() {
    // Native TO_BASE64 permits this input; the public wire signature's
    // MaxBlobWidth guard returns empty instead. Use the unchanged isolated
    // policy, not an enlarged test policy or a recorded expected payload.
    let input_len = 16_777_217_usize;
    arm_eval_one_observation();
    let result = crate::string_packet::to_base64(
        &[Datum::new_bytes(vec![b'x'; input_len])],
        &crate::NoColumns,
    );
    let observation = take_eval_one_observation();
    let Datum::String(encoded) = result.unwrap() else {
        panic!("TO_BASE64 must retain its text tag")
    };
    let bytes = encoded.bytes();
    let plain_len = input_len.div_ceil(3) * 4;
    let newlines = (plain_len - 1) / 76;
    assert_eq!(bytes.len(), plain_len + newlines);
    assert!(bytes.starts_with(b"eHh4"));
    assert!(bytes.ends_with(b"eHg="));
    assert_eq!(bytes[76], b'\n');
    assert_eq!(
        bytes.iter().filter(|&&byte| byte == b'\n').count(),
        newlines
    );
    let mut lines = bytes.split(|&byte| byte == b'\n').peekable();
    while let Some(line) = lines.next() {
        let expected_len = if lines.peek().is_some() {
            76
        } else {
            (plain_len - 1) % 76 + 1
        };
        assert_eq!(line.len(), expected_len);
    }
    assert_eq!(observation.facade_entries, 1);
    assert_eq!(
        observation.after_kernel_invocations,
        observation
            .before_kernel_invocations
            .map(|before| before + 1)
    );
}

#[test]
fn packet_string_dispatch_keeps_policy_order_and_value_only_context() {
    struct Packet {
        level: Cell<ErrorLevel>,
        getters: Cell<usize>,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Packet {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> ErrorLevel {
            self.level.get()
        }
        fn max_allowed_packet(&self) -> u64 {
            self.getters.set(self.getters.get() + 1);
            2
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }
    let native = Packet {
        level: Cell::new(ErrorLevel::Warn),
        getters: Cell::new(0),
        warnings: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        arm_eval_one_observation();
        let result =
            crate::func::eval_func_values("FROM_BASE64", &[Datum::new_string("YQ==")], columns)
                .unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::new_bytes(b"a")));
        assert_eq!(native.getters.get(), 0, "value-only has no packet policy");
        assert!(native.warnings.borrow().is_empty());
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );

        for level in [ErrorLevel::Warn, ErrorLevel::Ignore] {
            native.level.set(level);
            native.getters.set(0);
            native.warnings.borrow_mut().clear();
            arm_eval_one_observation();
            let result = crate::func::eval_func_values_in(
                "FROM_BASE64",
                &[Datum::new_string("    ")],
                columns,
            )
            .unwrap();
            let observation = take_eval_one_observation();
            assert_eq!(
                result,
                Ok(Datum::Null),
                "raw length is checked before stripping whitespace"
            );
            assert_eq!(
                native.getters.get(),
                2,
                "comparison then diagnostic formatting"
            );
            assert_eq!(
                native.warnings.borrow().as_slice(),
                &[(
                    1301,
                    "Result of from_base64() was larger than max_allowed_packet (2) - truncated"
                        .to_owned()
                )]
            );
            assert_eq!(
                observation.facade_entries, 1,
                "packet suppression is a real kernel NULL"
            );
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
        }
        native.getters.set(0);
        native.warnings.borrow_mut().clear();
        arm_eval_one_observation();
        let result =
            crate::string_packet::repeat(&[Datum::new_string(""), Datum::Int(i64::MAX)], columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::new_string("")));
        assert_eq!(
            native.getters.get(),
            0,
            "empty REPEAT keeps its original packet bypass"
        );
        assert!(native.warnings.borrow().is_empty());
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    });
    drop(scope);
    execution.close();

    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    native.level.set(ErrorLevel::Error);
    scope.with_columns(&native, |columns| {
        native.getters.set(0);
        arm_eval_one_observation();
        let result = crate::string_packet::space(&[Datum::Int(16_777_217)], columns);
        let observation = take_eval_one_observation();
        assert!(matches!(result, Err(EvalError::AllowedPacketOverflowed(_))), "packet Error precedes domain NULL and pool refusal");
        assert_eq!(native.getters.get(), 2);
        assert!(native.warnings.borrow().is_empty());
        assert_eq!(observation.facade_entries, 0);
        native.getters.set(0);
        arm_eval_one_observation();
        let result = crate::func::eval_func_values("FROM_BASE64", &[Datum::new_string("YQ==")], columns).unwrap();
        let observation = take_eval_one_observation();
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(native.getters.get(), 0, "value-only retains execution scope, not packet policy");
        assert_eq!(observation.facade_entries, 0);
    });
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn ip_predicate_dispatch_preserves_spelling_raw_bytes_and_nulls() {
    use EvaluatedBytesOp::{
        IsIpv4CompatNullable, IsIpv4MappedNullable, IsIpv4Nullable, IsIpv6Nullable,
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    let mapped = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4];
    for (operation, input, expected) in [
        (IsIpv4Nullable, Datum::new_string("001.002.0003.000"), 1),
        (IsIpv4Nullable, Datum::new_string("0000.000.00.0"), 1),
        (IsIpv4Nullable, Datum::new_string("000256.2.3.4"), 0),
        (IsIpv4Nullable, Datum::new_string("1..2.3"), 0),
        (IsIpv4Nullable, Datum::new_string("1.2.3.4."), 0),
        (IsIpv4Nullable, Datum::new_string("01 .2.3.4"), 0),
        (IsIpv4Nullable, Datum::new_string("000"), 0),
        (IsIpv4Nullable, Datum::new_string(""), 0),
        (IsIpv6Nullable, Datum::new_string("::ffff:1.2.3.4"), 1),
        (IsIpv6Nullable, Datum::new_string("::ffff:001.2.3.4"), 0),
        (IsIpv6Nullable, Datum::new_string("1.2.3.4"), 0),
        (IsIpv4CompatNullable, Datum::new_bytes([0; 16]), 1),
        (IsIpv4CompatNullable, Datum::new_bytes([0; 4]), 0),
        (IsIpv4MappedNullable, Datum::new_bytes(mapped), 1),
        (IsIpv4MappedNullable, Datum::new_string(mapped.to_vec()), 1),
        (IsIpv4MappedNullable, Datum::new_string("::ffff:1.2.3.4"), 0),
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &input, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::Int(expected)), "{operation:?}");
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    for operation in [
        IsIpv4Nullable,
        IsIpv6Nullable,
        IsIpv4CompatNullable,
        IsIpv4MappedNullable,
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &Datum::Null, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::Null));
        assert_eq!(
            observation.facade_entries, 1,
            "NULL enters the real nullable wrapper"
        );
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    drop(scope);
    execution.close();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for operation in [IsIpv4Nullable, IsIpv6Nullable] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &Datum::new_bytes([0xff]), &columns);
        let observation = take_eval_one_observation();
        assert_eq!(
            result,
            Err(EvalError::Unsupported("invalid UTF-8 byte datum"))
        );
        assert_eq!(
            observation.facade_entries, 0,
            "original coercion error precedes refusal"
        );
    }
    for operation in [
        IsIpv4Nullable,
        IsIpv6Nullable,
        IsIpv4CompatNullable,
        IsIpv4MappedNullable,
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &Datum::Null, &columns);
        let observation = take_eval_one_observation();
        assert!(
            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
        );
        assert_eq!(observation.facade_entries, 0);
    }
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
}

#[test]
fn pi_dispatch_uses_noargs_and_preserves_explicit_context() {
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    let field = FieldType::new(FieldTypeCode::Double);
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let protobuf = ScalarFunction::from_pb(
        PbBuiltin::new(tidb_proto::tipb::ScalarFuncSig::Pi).unwrap(),
        field.clone(),
        Vec::new(),
    );
    let typed = ScalarFunction::new(tidb_ast::CiString::new("pi"), field, Vec::new());
    for entry in 0..4 {
        arm_eval_one_observation();
        let result = match entry {
            0 => crate::eval_pi_in(&columns),
            1 => crate::math_fn::dispatch_values("PI", &[], &columns).unwrap(),
            2 => protobuf.eval(&columns, row.to_row()),
            _ => typed.eval(&columns, row.to_row()),
        };
        let observation = take_eval_one_observation();
        let Datum::Real(value) = result.unwrap() else {
            panic!("PI must keep its Real tag")
        };
        assert_eq!(value.to_bits(), 0x4009_21fb_5444_2d18);
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    drop(scope);
    execution.close();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    // Test the explicit helper, not an optimizer-folded SQL constant.
    arm_eval_one_observation();
    let result = crate::eval_pi_in(&columns);
    let observation = take_eval_one_observation();
    assert!(
        matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
    );
    assert_eq!(observation.facade_entries, 0);
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    assert_eq!(
        crate::math_fn::pi(&[Datum::Null], &columns),
        Err(EvalError::Unsupported("bad function arity"))
    );
    drop(scope);
    execution.close();
}

#[test]
fn math_dispatch_preserves_raw_results_and_nullable_policies() {
    use EvaluatedBytesOp::{AcosRaw, AsinRaw, DegreesRaw, RadiansRaw, SignRaw, SqrtRaw};
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (operation, input, expected) in [
        (AsinRaw, f64::NAN, Datum::Null),
        (AsinRaw, f64::INFINITY, Datum::Null),
        (AcosRaw, 2.0, Datum::Null),
        (AsinRaw, -0.0, Datum::Real(-0.0)),
        (AcosRaw, 1.0, Datum::Real(0.0)),
        (SqrtRaw, -1.0, Datum::Null),
        (SqrtRaw, f64::NAN, Datum::Real(f64::NAN)),
        (SqrtRaw, f64::INFINITY, Datum::Real(f64::INFINITY)),
        (SqrtRaw, -0.0, Datum::Real(-0.0)),
        (RadiansRaw, 180.0, Datum::Real(std::f64::consts::PI)),
        (RadiansRaw, -0.0, Datum::Real(-0.0)),
        (DegreesRaw, std::f64::consts::PI, Datum::Real(180.0)),
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &Datum::Real(input), &columns);
        let observation = take_eval_one_observation();
        match (result.unwrap(), expected) {
            (Datum::Real(actual), Datum::Real(expected)) if expected.is_nan() => {
                assert!(actual.is_nan())
            }
            (Datum::Real(actual), Datum::Real(expected)) => {
                assert_eq!(actual.to_bits(), expected.to_bits(), "{operation:?}")
            }
            (actual, expected) => assert_eq!(actual, expected, "{operation:?}"),
        }
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    for (operation, input) in [
        (RadiansRaw, f64::NAN),
        (RadiansRaw, f64::INFINITY),
        (DegreesRaw, f64::MAX),
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &Datum::Real(input), &columns);
        let observation = take_eval_one_observation();
        assert_eq!(
            result,
            Err(EvalError::FloatOverflow),
            "retain native 1690 carrier"
        );
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    for operation in [AsinRaw, AcosRaw, SqrtRaw, SignRaw, RadiansRaw, DegreesRaw] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &Datum::Null, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::Null));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    // The raw public seam deliberately differs from ordinary SQL domain packing.
    for function in [
        crate::RawInverseTrigFunction::Asin,
        crate::RawInverseTrigFunction::Acos,
    ] {
        let result = crate::eval_raw_inverse_trig_ready_in(function, Some(2.0), &columns).unwrap();
        assert!(matches!(result, Datum::Real(value) if value.is_nan()));
    }
    assert!(matches!(
        EvaluatedBytesResult::Ieee754Bits(Some(0)).into_bytes(),
        Err(EvalError::ExpressionAdapterFailure(_))
    ));
    assert!(matches!(
        EvaluatedBytesResult::Int(Datum::Int(0)).into_ieee754_bits(),
        Err(EvalError::ExpressionAdapterFailure(_))
    ));
    drop(scope);
    execution.close();
}

#[test]
fn math_dispatch_sign_retains_hidden_precision_and_unbounded_scale_classes() {
    use tidb_datatype::Decimal;
    let hidden = Decimal::from_int(1)
        .div_mysql(&Decimal::from_int(100_000), 4)
        .unwrap();
    // This valid storage value is nonzero even though the old general f64
    // conversion would parse its rounded SQL display as zero.
    assert_eq!(hidden.to_string(), "0.0000");
    assert_eq!(hidden.to_f64(), 0.0);
    let tiny = Decimal::from_scaled_i128(1, 400);
    let huge = Decimal::max_or_min(false, 400, 0);
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (input, expected) in [
        (Datum::Decimal(hidden.clone()), 1),
        (Datum::Decimal(hidden.negate()), -1),
        (Datum::Decimal(tiny.clone()), 1),
        (Datum::Decimal(tiny.negate()), -1),
        (Datum::Decimal(huge.clone()), 1),
        (Datum::Decimal(huge.negate()), -1),
        (Datum::Decimal(Decimal::from_scaled_i128(0, 400)), 0),
        (Datum::Int(i64::MIN), -1),
        (Datum::Int(i64::MAX), 1),
        (Datum::UInt(u64::MAX), 1),
        (Datum::UInt(0), 0),
        (Datum::Real(-0.0), 0),
        (Datum::Real(f64::NAN), 0),
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(EvaluatedBytesOp::SignRaw, &input, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::Int(expected)));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    drop(scope);
    execution.close();
}

#[test]
fn math_dispatch_keeps_coercion_precedence_pb_null_and_explicit_refusal() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;
    use EvaluatedBytesOp::{AcosRaw, AsinRaw, DegreesRaw, RadiansRaw, SignRaw, SqrtRaw};
    struct Diagnostics {
        level: Cell<ErrorLevel>,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Diagnostics {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> ErrorLevel {
            self.level.get()
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }
    let native = Diagnostics {
        level: Cell::new(ErrorLevel::Warn),
        warnings: RefCell::new(Vec::new()),
    };
    let input = Datum::new_string("4x");
    let message = "Truncated incorrect DOUBLE value: '4x'";
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for level in [ErrorLevel::Warn, ErrorLevel::Error] {
            native.level.set(level);
            native.warnings.borrow_mut().clear();
            arm_eval_one_observation();
            let result = dispatch_bytes_family(SqrtRaw, &input, columns);
            let observation = take_eval_one_observation();
            if level == ErrorLevel::Warn {
                assert_eq!(result, Ok(Datum::Real(2.0)));
                assert_eq!(*native.warnings.borrow(), vec![(1292, message.to_owned())]);
                assert_eq!(observation.facade_entries, 1);
            } else {
                assert_eq!(
                    result,
                    Err(EvalError::TruncatedWrongValue(message.to_owned()))
                );
                assert!(native.warnings.borrow().is_empty());
                assert_eq!(observation.facade_entries, 0);
            }
        }
        let field = FieldType::new(FieldTypeCode::Double);
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        for signature in [ScalarFuncSig::Asin, ScalarFuncSig::Acos] {
            let function = ScalarFunction::from_pb(
                PbBuiltin::new(signature).unwrap(),
                field.clone(),
                vec![Expression::Constant(Constant::new(
                    Datum::Null,
                    field.clone(),
                ))],
            );
            arm_eval_one_observation();
            let result = function.eval(columns, row.to_row());
            let observation = take_eval_one_observation();
            assert_eq!(result, Ok(Datum::Null));
            assert_eq!(observation.facade_entries, 1, "PB NULL must not bypass C4");
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
        }
    });
    drop(scope);
    execution.close();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(SqrtRaw, &input, columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Err(EvalError::TruncatedWrongValue(message.to_owned())));
        assert_eq!(observation.facade_entries, 0);
        for operation in [AsinRaw, AcosRaw, SqrtRaw, SignRaw, RadiansRaw, DegreesRaw] {
            arm_eval_one_observation();
            let result = dispatch_bytes_family(operation, &Datum::MinNotNull, columns);
            let observation = take_eval_one_observation();
            assert_eq!(result, Err(EvalError::Unsupported("range sentinel numeric argument")));
            assert_eq!(observation.facade_entries, 0);
            arm_eval_one_observation();
            let result = dispatch_bytes_family(operation, &Datum::Null, columns);
            let observation = take_eval_one_observation();
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0);
        }
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    });
    drop(scope);
    execution.close();
}

#[test]
fn inet_dispatch_preserves_unsigned_binary_text_and_null_results() {
    use EvaluatedBytesOp::{Inet6Aton, Inet6Ntoa, InetAton, InetNtoa};
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    let mapped = Datum::new_bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]);
    for (operation, input, expected) in [
        (InetAton, Datum::new_string("127"), Datum::UInt(127)),
        (
            InetAton,
            Datum::new_string("127.255"),
            Datum::UInt(2_130_706_687),
        ),
        (
            InetAton,
            Datum::new_string("255.255.255.255"),
            Datum::UInt(4_294_967_295),
        ),
        (
            InetNtoa,
            Datum::UInt(4_294_967_295),
            Datum::new_string("255.255.255.255"),
        ),
        (InetNtoa, Datum::UInt(u64::MAX), Datum::Null),
        (InetNtoa, Datum::Int(-1), Datum::Null),
        (
            Inet6Aton,
            Datum::new_bytes(b"10.0.5.9"),
            Datum::new_bytes([10, 0, 5, 9]),
        ),
        (
            Inet6Aton,
            Datum::new_string("::ffff:1.2.3.4"),
            mapped.clone(),
        ),
        (Inet6Aton, Datum::new_bytes([0xff]), Datum::Null),
        (Inet6Aton, Datum::new_string(vec![0xff]), Datum::Null),
        (Inet6Ntoa, mapped, Datum::new_string("::ffff:1.2.3.4")),
        (
            Inet6Ntoa,
            Datum::new_bytes([0xff, 0, 0, 1]),
            Datum::new_string("255.0.0.1"),
        ),
        (Inet6Ntoa, Datum::new_bytes([1, 2, 3]), Datum::Null),
        (
            Inet6Ntoa,
            Datum::Int(1234),
            Datum::new_string("49.50.51.52"),
        ),
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &input, &columns);
        let observation = take_eval_one_observation();
        let result = result.unwrap();
        assert_eq!(result.kind(), expected.kind(), "{operation:?}");
        assert_eq!(result, expected, "{operation:?}");
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    for operation in [InetAton, InetNtoa, Inet6Aton, Inet6Ntoa] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &Datum::Null, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::Null));
        assert_eq!(
            observation.facade_entries, 1,
            "NULL also reaches the real C4 call"
        );
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    drop(scope);
    execution.close();
}

#[test]
fn inet_dispatch_preserves_conversion_errors_warnings_and_root_refusal() {
    use EvaluatedBytesOp::{Inet6Aton, Inet6Ntoa, InetAton, InetNtoa};
    struct Diagnostics {
        level: Cell<ErrorLevel>,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Diagnostics {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn truncate_level(&self) -> ErrorLevel {
            self.level.get()
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }
    let native = Diagnostics {
        level: Cell::new(ErrorLevel::Warn),
        warnings: RefCell::new(Vec::new()),
    };
    let input = Datum::new_string("16909060x");
    let message = "Truncated incorrect INTEGER value: '16909060x'";
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for level in [ErrorLevel::Warn, ErrorLevel::Error] {
            native.level.set(level);
            native.warnings.borrow_mut().clear();
            arm_eval_one_observation();
            let result = dispatch_bytes_family(InetNtoa, &input, columns);
            let observation = take_eval_one_observation();
            if level == ErrorLevel::Warn {
                assert_eq!(result, Ok(Datum::new_string("1.2.3.4")));
                assert_eq!(*native.warnings.borrow(), vec![(1292, message.to_owned())]);
                assert_eq!(observation.facade_entries, 1);
                assert_eq!(
                    observation.after_kernel_invocations,
                    observation
                        .before_kernel_invocations
                        .map(|before| before + 1)
                );
            } else {
                assert_eq!(
                    result,
                    Err(EvalError::TruncatedWrongValue(message.to_owned()))
                );
                assert!(native.warnings.borrow().is_empty());
                assert_eq!(observation.facade_entries, 0);
            }
        }
    });
    drop(scope);
    execution.close();
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&native, |columns| {
        for (operation, input, message) in [
            (InetAton, Datum::new_bytes([0xff]), "invalid UTF-8 byte datum"),
            (InetAton, Datum::new_string(vec![0xff]), "invalid UTF-8 string datum"),
            (Inet6Aton, Datum::Raw(vec![0xff]), "invalid UTF-8 raw datum"),
            (Inet6Ntoa, Datum::Raw(vec![0xff]), "invalid UTF-8 raw datum"),
        ] {
            arm_eval_one_observation();
            let result = dispatch_bytes_family(operation, &input, columns);
            let observation = take_eval_one_observation();
            assert_eq!(result, Err(EvalError::Unsupported(message)));
            assert_eq!(observation.facade_entries, 0, "conversion precedes admission");
        }
        arm_eval_one_observation();
        let result = dispatch_bytes_family(InetNtoa, &input, columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Err(EvalError::TruncatedWrongValue(message.to_owned())));
        assert_eq!(observation.facade_entries, 0);
        for operation in [InetAton, InetNtoa, Inet6Aton, Inet6Ntoa] {
            arm_eval_one_observation();
            let result = dispatch_bytes_family(operation, &Datum::Null, columns);
            let observation = take_eval_one_observation();
            assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
            assert_eq!(observation.facade_entries, 0, "no NULL bypass of an explicit root");
        }
        native.level.set(ErrorLevel::Warn);
        arm_eval_one_observation();
        let result = dispatch_bytes_family(InetNtoa, &input, columns);
        let observation = take_eval_one_observation();
        assert!(matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource));
        assert_eq!(*native.warnings.borrow(), vec![(1292, message.to_owned())]);
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    });
    drop(scope);
    execution.close();
}

#[test]
fn logical_dispatch_checks_demand_markers_before_admission() {
    use crate::{LogicalArgs::*, LogicalFunction::*};
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (function, left) in [
        (And, Some(true)),
        (And, None),
        (Or, Some(false)),
        (Or, None),
        (Xor, Some(false)),
        (Xor, Some(true)),
        (Xor, None),
    ] {
        arm_eval_one_observation();
        let result = crate::eval_logical_ready_in(function, UndemandedRight { left }, &columns);
        let observation = take_eval_one_observation();
        assert!(
            matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::ScopeContract)
        );
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(observation.after_kernel_invocations, None);
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    }
    for (function, arguments, expected) in [
        (And, UndemandedRight { left: Some(false) }, Datum::Int(0)),
        (Or, UndemandedRight { left: Some(true) }, Datum::Int(1)),
        (And, Both(None, Some(false)), Datum::Int(0)),
        (Or, Both(None, Some(true)), Datum::Int(1)),
        (Xor, Both(None, Some(false)), Datum::Null),
        (Xor, Both(Some(true), Some(false)), Datum::Int(1)),
    ] {
        arm_eval_one_observation();
        let result = crate::eval_logical_ready_in(function, arguments, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    drop(scope);
    execution.close();
    // Even a legitimate short-circuit marker cannot bypass an explicit root.
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    arm_eval_one_observation();
    let result = crate::eval_logical_ready_in(And, UndemandedRight { left: Some(false) }, &columns);
    let observation = take_eval_one_observation();
    assert!(
        matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
    );
    assert_eq!(observation.facade_entries, 0);
    drop(scope);
    execution.close();
}

#[test]
fn logical_dispatch_preserves_typed_pb_lazy_and_ast_eager_demand() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    use tidb_proto::tipb::ScalarFuncSig;
    struct Demand(Cell<usize>);
    impl Columns for Demand {
        fn get(&self, _: &[String]) -> Option<Datum> {
            self.0.set(self.0.get() + 1);
            None
        }
        fn param_value(&self, _: usize) -> Result<Datum, EvalError> {
            self.0.set(self.0.get() + 1);
            Err(EvalError::Unsupported("logical right child failed"))
        }
    }
    let field = FieldType::new(FieldTypeCode::LongLong);
    let literal = |value| Expression::Constant(Constant::new(value, field.clone()));
    let mut parameter = Constant::new(Datum::Int(99), field.clone());
    parameter.param_marker = Some(crate::constant::ParamMarker { order: 0 });
    let rhs = Expression::Constant(parameter);
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let native = Demand(Cell::new(0));
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    scope.with_columns(&native, |columns| {
        for (name, signature, dominant) in [
            ("and", ScalarFuncSig::LogicalAnd, 0),
            ("or", ScalarFuncSig::LogicalOr, 1),
        ] {
            for protobuf in [false, true] {
                let args = vec![literal(Datum::Int(dominant)), rhs.clone()];
                let mut function = if protobuf {
                    // The real admitted PB implementation, not a native-name alias.
                    ScalarFunction::from_pb(PbBuiltin::new(signature).unwrap(), field.clone(), args)
                } else {
                    ScalarFunction::new(tidb_ast::CiString::new(name), field.clone(), args)
                };
                native.0.set(0);
                arm_eval_one_observation();
                let result = function.eval(columns, row.to_row());
                let observation = take_eval_one_observation();
                assert_eq!(result, Ok(Datum::Int(dominant)));
                assert_eq!(native.0.get(), 0, "the failing RHS was never evaluated");
                assert_eq!(observation.facade_entries, 1);
                assert_eq!(
                    observation.after_kernel_invocations,
                    observation
                        .before_kernel_invocations
                        .map(|before| before + 1)
                );
                function.args[0] = literal(Datum::Null);
                arm_eval_one_observation();
                let result = function.eval(columns, row.to_row());
                let observation = take_eval_one_observation();
                assert_eq!(
                    result,
                    Err(EvalError::Unsupported("logical right child failed"))
                );
                assert_eq!(native.0.get(), 1, "NULL left still demands the right child");
                assert_eq!(observation.facade_entries, 0);
            }
        }
        let xor = ScalarFunction::new(
            tidb_ast::CiString::new("xor"),
            field.clone(),
            vec![literal(Datum::Int(0)), rhs.clone()],
        );
        native.0.set(0);
        assert_eq!(
            xor.eval(columns, row.to_row()),
            Err(EvalError::Unsupported("logical right child failed"))
        );
        assert_eq!(native.0.get(), 1);
        assert!(
            PbBuiltin::new(ScalarFuncSig::LogicalXor).is_none(),
            "no new PB admission"
        );
        for sql in ["0 AND rhs", "1 OR rhs", "0 XOR rhs"] {
            let tidb_ast::Stmt::Query(query) =
                tidb_parser::parse(&format!("SELECT {sql}")).unwrap()
            else {
                panic!("query")
            };
            let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
                panic!("SELECT")
            };
            let tidb_ast::SelectField::Expr { expr, .. } = &select.fields[0] else {
                panic!("expression")
            };
            native.0.set(0);
            arm_eval_one_observation();
            let result = crate::eval_in(expr, columns);
            let observation = take_eval_one_observation();
            assert!(result.is_err(), "AST remains eager: {sql}");
            assert_eq!(native.0.get(), 1);
            assert_eq!(observation.facade_entries, 0);
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn logical_dispatch_preserves_eager_probes_and_lazy_truncation_policy() {
    use crate::expression::Expression;
    use crate::scalar_function::{PbBuiltin, ScalarFunction};
    struct Diagnostics {
        reject: Cell<bool>,
        probes: RefCell<Vec<String>>,
        warnings: RefCell<Vec<(u16, String)>>,
    }
    impl Columns for Diagnostics {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn handle_truncate(&self, message: &str) -> Result<(), EvalError> {
            self.probes.borrow_mut().push(message.to_owned());
            if self.reject.get() {
                Err(EvalError::Unsupported("original logical truncation policy"))
            } else {
                self.warnings.borrow_mut().push((1292, message.to_owned()));
                Ok(())
            }
        }
    }
    let native = Diagnostics {
        reject: Cell::new(false),
        probes: RefCell::new(Vec::new()),
        warnings: RefCell::new(Vec::new()),
    };
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    scope.with_columns(&native, |columns| {
        for reject in [false, true] {
            native.reject.set(reject);
            for frontend in ["eager", "typed", "pb"] {
                native.probes.borrow_mut().clear();
                native.warnings.borrow_mut().clear();
                let left = Datum::new_string("0x");
                let right = Datum::new_string("1x");
                arm_eval_one_observation();
                let result = if frontend == "eager" {
                    crate::ops::logic_and(left, right, columns)
                } else {
                    let args = vec![left, right]
                        .into_iter()
                        .map(|value| {
                            Expression::Constant(Constant::new(
                                value,
                                FieldType::new(FieldTypeCode::VarString),
                            ))
                        })
                        .collect();
                    let field = FieldType::new(FieldTypeCode::LongLong);
                    let function = if frontend == "typed" {
                        ScalarFunction::new(tidb_ast::CiString::new("and"), field, args)
                    } else {
                        ScalarFunction::from_pb(
                            PbBuiltin::new(tidb_proto::tipb::ScalarFuncSig::LogicalAnd).unwrap(),
                            field,
                            args,
                        )
                    };
                    function.eval(columns, row.to_row())
                };
                let observation = take_eval_one_observation();
                let expected_probes = (if frontend == "eager" {
                    vec!["0x", "1x"]
                } else {
                    vec!["0x"]
                })
                .into_iter()
                .map(|text| format!("Truncated incorrect DOUBLE value: '{text}'"))
                .collect::<Vec<_>>();
                assert_eq!(*native.probes.borrow(), expected_probes);
                let expected_warnings: Vec<(u16, String)> = if reject {
                    vec![]
                } else {
                    expected_probes
                        .into_iter()
                        .map(|message| (1292, message))
                        .collect()
                };
                assert_eq!(*native.warnings.borrow(), expected_warnings);
                if reject && frontend != "eager" {
                    assert_eq!(
                        result,
                        Err(EvalError::Unsupported("original logical truncation policy"))
                    );
                    assert_eq!(observation.facade_entries, 0);
                } else {
                    // Eager helper ignores both probe errors, then calls truthy_of;
                    // lazy typed/PB propagate their one probe's error instead.
                    assert_eq!(result, Ok(Datum::Int(0)));
                    assert_eq!(observation.facade_entries, 1);
                    assert_eq!(
                        observation.after_kernel_invocations,
                        observation
                            .before_kernel_invocations
                            .map(|before| before + 1)
                    );
                }
            }
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn hash_dispatch_preserves_raw_bytes_aliases_and_text_results() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    // Existing crypto vectors, retaining their text result tag even for bytes.
    let gbk = Datum::new_bytes([0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd]);
    for (name, input, expected) in [
        (
            "MD5",
            Datum::new_string("abc"),
            Datum::new_string("900150983cd24fb0d6963f7d28e17f72"),
        ),
        (
            "MD5",
            Datum::new_bytes([0xff, 0, b'a']),
            Datum::new_string("310e56cdb9dccaf757dbcab30054500e"),
        ),
        (
            "MD5",
            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("123.123").0),
            Datum::new_string("46ddc40585caa8abc07c460b3485781e"),
        ),
        (
            "SHA1",
            gbk.clone(),
            Datum::new_string("30cda4eed59a2ff592f2881f39d42fed6e10cad8"),
        ),
        (
            "SHA",
            gbk,
            Datum::new_string("30cda4eed59a2ff592f2881f39d42fed6e10cad8"),
        ),
        (
            "SHA",
            Datum::new_string(""),
            Datum::new_string("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
        ),
        ("MD5", Datum::Null, Datum::Null),
        ("SHA1", Datum::Null, Datum::Null),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values(name, &[input], &columns).unwrap();
        let observation = take_eval_one_observation();
        assert!(matches!(&result, Ok(Datum::String(_) | Datum::Null)));
        assert_eq!(result, Ok(expected), "{name}");
        assert_eq!(
            observation.facade_entries, 1,
            "NULL also enters the real worker"
        );
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    drop(scope);
    execution.close();
}

#[test]
fn hash_dispatch_retains_coercion_errors_before_explicit_root_refusal() {
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for operation in [EvaluatedBytesOp::Md5, EvaluatedBytesOp::Sha1] {
        for input in [Datum::Null, Datum::new_string("abc")] {
            arm_eval_one_observation();
            let result = dispatch_bytes_family(operation, &input, &columns);
            let observation = take_eval_one_observation();
            assert!(
                matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
            );
            assert_eq!(observation.facade_entries, 0);
            assert_eq!(observation.before_kernel_invocations, None);
            assert_eq!(observation.after_kernel_invocations, None);
        }
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &Datum::MaxValue, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(
            result,
            Err(EvalError::Unsupported("range sentinel hash argument"))
        );
        assert_eq!(
            observation.facade_entries, 0,
            "frontend rejection precedes C admission"
        );
    }
    drop(scope);
    execution.close();
}

#[test]
fn boolean_dispatch_preserves_three_values_and_composed_aliases() {
    use crate::BooleanFunction::*;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (function, expected, calls) in [
        (UnaryNot, [None, Some(1), Some(0)], 1),
        (IsNull, [Some(1), Some(0), Some(0)], 1),
        (IsTrue, [Some(0), Some(0), Some(1)], 1),
        (IsFalse, [Some(0), Some(1), Some(0)], 1),
        (IsTrueWithNull, [None, Some(0), Some(1)], 1),
        (IsNotNull, [Some(0), Some(1), Some(1)], 2),
        (IsNotTrue, [Some(1), Some(1), Some(0)], 2),
        (IsNotFalse, [Some(1), Some(0), Some(1)], 2),
    ] {
        for (ready, expected) in [None, Some(false), Some(true)].into_iter().zip(expected) {
            arm_eval_one_observation();
            let result = crate::eval_boolean_ready_in(function, ready, &columns);
            let observation = take_eval_one_observation();
            assert_eq!(
                result,
                Ok(expected.map_or(Datum::Null, Datum::Int)),
                "{function:?} {ready:?}"
            );
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + calls)
            );
        }
    }
    assert!(EvaluatedBytesResult::Int(Datum::Int(2))
        .into_boolean_datum()
        .is_err());
    drop(scope);
    execution.close();
}

#[test]
fn boolean_dispatch_preserves_pb_warnings_and_native_predicate_wrappers() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use tidb_proto::tipb;
    struct Warnings(RefCell<Vec<(u16, String)>>);
    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            Some(Datum::MaxValue)
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let native = Warnings(RefCell::new(Vec::new()));
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    scope.with_columns(&native, |columns| {
        assert_eq!(
            crate::apply_unary(tidb_ast::UnaryOp::Not, Datum::new_string("1x"), columns),
            Ok(Datum::Int(0))
        );
        assert!(
            native.0.borrow().is_empty(),
            "ordinary NOT retains warning-free truthy_of"
        );
        for (signature, input, expected, warned) in [
            (
                tipb::ScalarFuncSig::UnaryNotInt,
                Some(b"1x".to_vec()),
                Datum::Int(0),
                true,
            ),
            (
                tipb::ScalarFuncSig::IntIsTrueWithNull,
                None,
                Datum::Null,
                false,
            ),
            (
                tipb::ScalarFuncSig::StringIsNull,
                Some(b"1x".to_vec()),
                Datum::Int(0),
                false,
            ),
        ] {
            native.0.borrow_mut().clear();
            let child = tipb::Expr {
                tp: Some(
                    (if input.is_none() {
                        tipb::ExprType::Null
                    } else {
                        tipb::ExprType::String
                    }) as i32,
                ),
                val: input,
                field_type: Some(tipb::FieldType {
                    tp: Some(253),
                    charset: Some("utf8mb4".into()),
                    collate: Some(46),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let pb = tipb::Expr {
                tp: Some(tipb::ExprType::ScalarFunc as i32),
                sig: Some(signature as i32),
                children: vec![child],
                field_type: Some(tipb::FieldType {
                    tp: Some(8),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let expression = crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
            arm_eval_one_observation();
            let result = expression.eval(columns, row.to_row());
            let observation = take_eval_one_observation();
            assert_eq!(result, Ok(expected));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(
                observation.after_kernel_invocations,
                observation
                    .before_kernel_invocations
                    .map(|before| before + 1)
            );
            assert_eq!(
                *native.0.borrow(),
                if warned {
                    vec![(1292, "Truncated incorrect DOUBLE value: '1x'".to_owned())]
                } else {
                    vec![]
                }
            );
        }
        // AST UNKNOWN is presence-only; the typed alias deliberately retains
        // its preexisting truth-coercion rejection of this same sentinel.
        let unknown = tidb_ast::Expr::Is {
            expr: Box::new(tidb_ast::Expr::Column(vec!["x".into()])),
            target: tidb_ast::IsTarget::Unknown,
            not: false,
        };
        assert_eq!(crate::eval_in(&unknown, columns), Ok(Datum::Int(0)));
        let typed = ScalarFunction::new(
            tidb_ast::CiString::new("isunknown"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Constant(Constant::new(
                Datum::MaxValue,
                FieldType::new(FieldTypeCode::LongLong),
            ))],
        );
        arm_eval_one_observation();
        let result = typed.eval(columns, row.to_row());
        let observation = take_eval_one_observation();
        assert_eq!(
            result,
            Err(EvalError::Unsupported("truth coercion of a non-SQL datum"))
        );
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(
            crate::apply_unary(tidb_ast::UnaryOp::Not, Datum::MaxValue, columns),
            Err(EvalError::Unsupported("range sentinel expression operand"))
        );
        for (sql, expected, facades, single_worker_calls) in [
            // eval_in_list visits both items even after a match: Eq, Eq, NOT.
            // The two Eq calls share a worker, but NOT replaces that worker;
            // first/last getter snapshots are not a cumulative kernel delta.
            ("1 NOT IN (1, 2)", Datum::Int(0), 3, None),
            // AST BETWEEN eagerly evaluates Ge and Le, then AND and NOT.
            ("1 NOT BETWEEN 0 AND 2", Datum::Int(0), 4, None),
            // LIKE's real NULL witness now precedes the independent NOT worker.
            ("NULL NOT LIKE '%'", Datum::Null, 2, None),
            // REGEXP now dispatches too, before the independent NOT worker.
            ("'a' NOT REGEXP 'b'", Datum::Int(1), 2, None),
            ("NULL IS NOT TRUE", Datum::Int(1), 1, Some(2)),
        ] {
            let tidb_ast::Stmt::Query(query) =
                tidb_parser::parse(&format!("SELECT {sql}")).unwrap()
            else {
                panic!("query")
            };
            let tidb_ast::QueryStmt::Select(select) = query.into_inner() else {
                panic!("SELECT")
            };
            let tidb_ast::SelectField::Expr { expr, .. } = &select.fields[0] else {
                panic!("expression")
            };
            arm_eval_one_observation();
            let result = crate::eval_in(expr, columns);
            let observation = take_eval_one_observation();
            assert_eq!(result, Ok(expected), "{sql}");
            assert_eq!(observation.facade_entries, facades, "{sql}");
            if let Some(calls) = single_worker_calls {
                assert_eq!(
                    observation.after_kernel_invocations,
                    observation
                        .before_kernel_invocations
                        .map(|before| before + calls)
                );
            } else {
                // Independent snapshots from the predicate and NOT workers.
                assert_eq!(observation.before_kernel_invocations, Some(0));
                assert_eq!(observation.after_kernel_invocations, Some(1));
            }
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn boolean_dispatch_vector_fallback_keeps_the_explicit_root() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    let field = FieldType::new(FieldTypeCode::LongLong);
    let mut column = crate::column::Column::new(1, field.clone());
    column.index = 0;
    let isnull = ScalarFunction::new(
        tidb_ast::CiString::new("isnull"),
        field.clone(),
        vec![Expression::Column(column)],
    );
    let not = ScalarFunction::new(
        tidb_ast::CiString::new("not"),
        field.clone(),
        vec![Expression::ScalarFunction(isnull.clone())],
    );
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&[field], 2);
    chunk.append_null(0);
    chunk.append_int64(0, 0);
    let ctx = crate::NoColumns;
    for function in [&isnull, &not] {
        let mut untouched = vec![42];
        arm_eval_one_observation();
        assert_eq!(
            function.vec_eval_bool(&chunk, &[0, 1], &mut untouched, &ctx),
            Ok(false)
        );
        let observation = take_eval_one_observation();
        assert_eq!(untouched, vec![42]);
        assert_eq!(
            observation.facade_entries, 0,
            "decline before child evaluation"
        );
    }
    let filters = [Expression::ScalarFunction(not)];
    for slots in [0, 1] {
        let owner = AsciiPoolOwner::new(test_policy(slots, slots)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        let columns = AdvertisedAsciiColumns {
            scope: Some(&scope),
            execution: &execution,
        };
        arm_eval_one_observation();
        let result = crate::evaluator::vectorized_filter_consider_null(
            &columns,
            true,
            &filters,
            &chunk,
            Vec::new(),
            Vec::new(),
        );
        let observation = take_eval_one_observation();
        if slots == 0 {
            assert!(
                matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
            );
            assert_eq!(observation.facade_entries, 0);
        } else {
            assert_eq!(result, Ok((vec![false, true], vec![false, false])));
            assert_eq!(observation.facade_entries, 4);
            // The alternating operations replace a one-slot worker, so its
            // per-worker counter is not a root-wide aggregate counter.
            assert!(observation.before_kernel_invocations.is_some());
            assert!(observation.after_kernel_invocations.is_some());
        }
        drop(scope);
        execution.close();
    }
}

#[test]
fn bit_dispatch_preserves_full_width_results_and_conversion_domains() {
    use tidb_ast::BinaryOp;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (operation, input, expected) in [
        (
            EvaluatedBytesOp::BitCount,
            Datum::new_string("18446744073709551615"),
            Datum::Int(64),
        ),
        (EvaluatedBytesOp::BitCount, Datum::Null, Datum::Null),
        (
            EvaluatedBytesOp::BitNeg,
            Datum::UInt(u64::MAX),
            Datum::UInt(0),
        ),
        (
            EvaluatedBytesOp::BitNeg,
            Datum::Real(2.5),
            Datum::UInt(u64::MAX - 2),
        ),
        (EvaluatedBytesOp::BitNeg, Datum::Null, Datum::Null),
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &input, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    for (operation, left, right, expected) in [
        (
            BinaryOp::BitAnd,
            Datum::UInt(u64::MAX),
            Datum::Int(i64::MIN),
            1_u64 << 63,
        ),
        (BinaryOp::BitOr, Datum::Int(0), Datum::Int(-1), u64::MAX),
        (BinaryOp::BitXor, Datum::UInt(u64::MAX), Datum::Int(-1), 0),
        (
            BinaryOp::LeftShift,
            Datum::UInt(1),
            Datum::Int(63),
            1_u64 << 63,
        ),
        (BinaryOp::LeftShift, Datum::UInt(1), Datum::Int(64), 0),
        (BinaryOp::RightShift, Datum::Int(-1), Datum::Int(63), 1),
        (BinaryOp::RightShift, Datum::UInt(1), Datum::Int(-1), 0),
        (BinaryOp::BitOr, Datum::Real(2.5), Datum::Int(0), 2),
        // A hybrid partner retains the existing decimal-kernel fallback.
        (
            BinaryOp::BitOr,
            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("2.5").0),
            Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![0])),
            3,
        ),
    ] {
        arm_eval_one_observation();
        let result = crate::ops::eval_binary_in(operation, left, right, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::UInt(expected)), "{operation:?}");
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
    }
    drop(scope);
    execution.close();
}

#[test]
fn bit_dispatch_preserves_null_and_diagnostic_demand_order() {
    struct Warnings(RefCell<Vec<(u16, String)>>);
    impl Columns for Warnings {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let native = Warnings(RefCell::new(Vec::new()));
    for (left, right, warning) in [
        (
            Datum::new_string("1x"),
            Datum::Null,
            Some("Truncated incorrect INTEGER value: '1x'"),
        ),
        (
            Datum::Null,
            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("10000000000000000000").0),
            Some("Truncated incorrect DECIMAL value: '10000000000000000000'"),
        ),
        (Datum::Real(f64::NAN), Datum::Null, None),
    ] {
        native.0.borrow_mut().clear();
        arm_eval_one_observation();
        let result = scope.with_columns(&native, |columns| {
            crate::ops::eval_binary_in(tidb_ast::BinaryOp::BitOr, left, right, columns)
        });
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(Datum::Null));
        assert_eq!(observation.facade_entries, 1, "NULL must still enter C4");
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
        assert_eq!(
            *native.0.borrow(),
            warning
                .into_iter()
                .map(|message| (1292, message.to_owned()))
                .collect::<Vec<_>>()
        );
    }
    arm_eval_one_observation();
    let result = scope.with_columns(&native, |columns| {
        crate::ops::eval_binary_in(
            tidb_ast::BinaryOp::BitOr,
            Datum::Real(f64::NAN),
            Datum::Int(0),
            columns,
        )
    });
    let observation = take_eval_one_observation();
    assert_eq!(result, Err(EvalError::IntOverflow));
    assert_eq!(
        observation.facade_entries, 0,
        "Real overflow precedes admission"
    );

    // The existing Option<Result> tuple evaluates the right Decimal's policy
    // callback even when the left callback already returned an error.
    let rejecting = ForwardingSentinel::new(true, None);
    arm_eval_one_observation();
    let result = scope.with_columns(&rejecting, |columns| {
        crate::ops::eval_binary_in(
            tidb_ast::BinaryOp::BitOr,
            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("10000000000000000000").0),
            Datum::Decimal(tidb_datatype::Decimal::parse_mysql("-10000000000000000000").0),
            columns,
        )
    });
    let observation = take_eval_one_observation();
    assert_eq!(
        result,
        Err(EvalError::UnknownColumn(
            "sentinel original error: handle_truncate".to_owned()
        ))
    );
    assert_eq!(observation.facade_entries, 0);
    assert_eq!(
        rejecting.effects.borrow().warnings.as_slice(),
        &[
            (42000, "preexisting warning".to_owned()),
            (
                42101,
                "truncate:Truncated incorrect DECIMAL value: '10000000000000000000'".to_owned()
            ),
            (
                42101,
                "truncate:Truncated incorrect DECIMAL value: '-10000000000000000000'".to_owned()
            ),
        ]
    );
    drop(scope);
    execution.close();
}

#[test]
fn bit_dispatch_typed_and_vector_routes_retain_the_explicit_root() {
    use crate::expression::Expression;
    use crate::scalar_function::ScalarFunction;
    use tidb_ast::CiString;
    let signed = FieldType::new(FieldTypeCode::LongLong);
    let unsigned = signed
        .clone()
        .with_added_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&[], 2);
    chunk.set_num_virtual_rows(2);
    for slots in [0, 1] {
        let owner = AsciiPoolOwner::new(test_policy(slots, slots)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        let columns = AdvertisedAsciiColumns {
            scope: Some(&scope),
            execution: &execution,
        };
        for left in [Datum::Null, Datum::UInt(u64::MAX)] {
            let expected = left.clone();
            let expression = Expression::ScalarFunction(ScalarFunction::new(
                CiString::new("bitxor"),
                unsigned.clone(),
                vec![
                    Expression::Constant(Constant::new(left, unsigned.clone())),
                    Expression::Constant(Constant::new(Datum::Int(0), signed.clone())),
                ],
            ));
            arm_eval_one_observation();
            let result = expression.eval(&columns, row.to_row());
            let observation = take_eval_one_observation();
            if slots == 0 {
                assert!(
                    matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
                );
                assert_eq!(observation.facade_entries, 0);
            } else {
                assert_eq!(result, Ok(expected));
                assert_eq!(observation.facade_entries, 1);
            }
            arm_eval_one_observation();
            let result = crate::evaluator::vectorized_filter_consider_null(
                &columns,
                true,
                &[expression],
                &chunk,
                Vec::new(),
                Vec::new(),
            );
            let observation = take_eval_one_observation();
            if slots == 0 {
                assert!(
                    matches!(result, Err(EvalError::ExpressionAdapterFailure(failure)) if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
                );
                assert_eq!(observation.facade_entries, 0);
            } else {
                assert!(result.is_ok());
                assert_eq!(observation.facade_entries, 2);
                assert_eq!(
                    observation.after_kernel_invocations,
                    observation
                        .before_kernel_invocations
                        .map(|before| before + 2)
                );
            }
        }
        drop(scope);
        execution.close();
    }
}

#[test]
fn args_dispatch_shares_one_scope_for_all_fixed_shapes() {
    use EvaluatedBytesOp::*;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (name, operation, input, expected) in [
        ("ASCII", Ascii, vec![Datum::new_string("A")], Datum::Int(65)),
        (
            "HEX",
            HexInt,
            vec![Datum::UInt(u64::MAX)],
            Datum::new_string("FFFFFFFFFFFFFFFF"),
        ),
        (
            "HEX",
            HexStr,
            vec![Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(
                vec![0, 0x41],
            ))],
            Datum::new_string("0041"),
        ),
        (
            "BIN",
            Bin,
            vec![Datum::Int(-1)],
            Datum::new_string("1111111111111111111111111111111111111111111111111111111111111111"),
        ),
        (
            "LEFT",
            Left,
            vec![Datum::new_bytes(vec![0xe2, 0x82, b'a']), Datum::Int(2)],
            Datum::new_bytes(vec![0xe2, 0x82]),
        ),
        (
            "LEFT",
            LeftUtf8,
            vec![Datum::new_string(vec![0xe2, 0x82, b'a']), Datum::Int(2)],
            Datum::new_string("\u{fffd}\u{fffd}"),
        ),
        (
            "RIGHT",
            Right,
            vec![Datum::new_bytes(vec![0xe2, 0x82, b'a']), Datum::Int(2)],
            Datum::new_bytes(vec![0x82, b'a']),
        ),
        (
            "RIGHT",
            RightUtf8,
            vec![Datum::new_string(vec![0xe2, 0x82, b'a']), Datum::Int(2)],
            Datum::new_string("\u{fffd}a"),
        ),
        (
            "REPLACE",
            Replace,
            vec![
                Datum::new_bytes("aaaaa"),
                Datum::new_string("aa"),
                Datum::new_string("b"),
            ],
            Datum::new_bytes("bba"),
        ),
        // Original REPLACE packing recognizes Bytes, not every binary collation.
        (
            "REPLACE",
            Replace,
            vec![
                Datum::new_collation_string(vec![0xff], tidb_datatype::Collation::Binary),
                Datum::new_string(""),
                Datum::new_string("x"),
            ],
            Datum::new_string(vec![0xff]),
        ),
    ] {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values(name, &input, &columns).unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected), "{name}");
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
        assert_eq!(
            scope
                .lease
                .borrow()
                .as_ref()
                .unwrap()
                .worker
                .as_ref()
                .unwrap()
                .operation(),
            operation
        );
    }
    drop(scope);
    execution.close();
}

#[test]
fn args_dispatch_preserves_count_first_and_replace_coercion_demand() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for name in ["LEFT", "RIGHT"] {
        arm_eval_one_observation();
        let result =
            crate::func::eval_func_values(name, &[Datum::MaxValue, Datum::Null], &columns).unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(
            result,
            Ok(Datum::Null),
            "NULL count must not coerce subject"
        );
        assert_eq!(observation.facade_entries, 1, "NULL still comes from C4");
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
        arm_eval_one_observation();
        let result =
            crate::func::eval_func_values(name, &[Datum::MaxValue, Datum::Int(0)], &columns)
                .unwrap();
        let observation = take_eval_one_observation();
        assert_eq!(
            result,
            Err(EvalError::Unsupported("range sentinel byte coercion"))
        );
        assert_eq!(
            observation.facade_entries, 0,
            "zero still coerces subject first"
        );
    }
    arm_eval_one_observation();
    let result = crate::func::eval_func_values(
        "REPLACE",
        &[Datum::Null, Datum::new_string(""), Datum::MaxValue],
        &columns,
    )
    .unwrap();
    let observation = take_eval_one_observation();
    assert_eq!(
        result,
        Err(EvalError::Unsupported("range sentinel byte coercion"))
    );
    assert_eq!(
        observation.facade_entries, 0,
        "earlier NULL must not skip later coercion"
    );
    arm_eval_one_observation();
    let result = crate::func::eval_func_values(
        "REPLACE",
        &[Datum::Null, Datum::new_string(""), Datum::new_string("x")],
        &columns,
    )
    .unwrap();
    let observation = take_eval_one_observation();
    assert_eq!(result, Ok(Datum::Null));
    assert_eq!(observation.facade_entries, 1);
    assert_eq!(
        observation.after_kernel_invocations,
        observation
            .before_kernel_invocations
            .map(|before| before + 1)
    );
    drop(scope);
    execution.close();
}

#[test]
fn args_dispatch_preserves_typed_hex_and_bin_warning_before_admission() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (kind, input, operation, expected) in [
        (
            FieldTypeCode::VarString,
            Datum::UInt(26),
            EvaluatedBytesOp::HexStr,
            Datum::new_string("3236"),
        ),
        (
            FieldTypeCode::Bit,
            Datum::Bit(tidb_datatype::BinaryLiteral::from(vec![0, 0x41])),
            EvaluatedBytesOp::HexInt,
            Datum::new_string("41"),
        ),
        (
            FieldTypeCode::LongLong,
            Datum::Null,
            EvaluatedBytesOp::HexInt,
            Datum::Null,
        ),
        (
            FieldTypeCode::VarString,
            Datum::Null,
            EvaluatedBytesOp::HexStr,
            Datum::Null,
        ),
    ] {
        arm_eval_one_observation();
        let result =
            crate::string_fn::hex_with_type(&[input], Some(&FieldType::new(kind)), &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
        assert_eq!(
            scope
                .lease
                .borrow()
                .as_ref()
                .unwrap()
                .worker
                .as_ref()
                .unwrap()
                .operation(),
            operation
        );
    }
    drop(scope);
    execution.close();

    // A new Int shape must not obtain a fallback pool; its original warning
    // still precedes the explicit owner's ordinary admission failure.
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let native = ForwardingSentinel::new(false, None);
    arm_eval_one_observation();
    let result = scope.with_columns(&native, |columns| {
        crate::string_fn::bin(&[Datum::new_string("1x")], columns)
    });
    let observation = take_eval_one_observation();
    assert!(
        matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
        if failure.class() == crate::ExpressionAdapterFailureClass::PoolResource)
    );
    assert_eq!(observation.facade_entries, 0);
    assert_eq!(
        native.effects.borrow().warnings.as_slice(),
        &[
            (42000, "preexisting warning".to_owned()),
            (1292, "Truncated incorrect INTEGER value: '1x'".to_owned()),
        ]
    );
    drop(scope);
    execution.close();
}

#[test]
fn next_bytes_dispatch_keeps_distinct_normalization_and_native_result_kinds() {
    use EvaluatedBytesOp::*;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &execution,
    };
    for (operation, input, expected) in [
        (
            Crc32,
            Datum::new_string("mysql"),
            Datum::UInt(2_501_908_538),
        ),
        (
            Reverse,
            Datum::new_bytes(vec![0xe2, 0x82, b'a']),
            Datum::new_bytes(vec![b'a', 0x82, 0xe2]),
        ),
        (
            ReverseUtf8,
            Datum::new_string(vec![0xe2, 0x82, b'a']),
            Datum::new_string("a\u{fffd}\u{fffd}"),
        ),
        (CharLength, Datum::new_string("é"), Datum::Int(2)),
        (CharLengthUtf8, Datum::new_string("é"), Datum::Int(1)),
        // Go normalization emits one replacement per byte of this suffix;
        // QUOTE deliberately preserves Rust's single replacement grouping.
        (
            CharLengthUtf8,
            Datum::new_string(vec![0xe2, 0x82]),
            Datum::Int(2),
        ),
        (
            Quote,
            Datum::new_bytes(vec![0xe2, 0x82]),
            Datum::new_bytes("'\u{fffd}'"),
        ),
        (Quote, Datum::Null, Datum::new_string("NULL")),
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &input, &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(
            observation.after_kernel_invocations,
            observation
                .before_kernel_invocations
                .map(|before| before + 1)
        );
        assert_eq!(
            scope
                .lease
                .borrow()
                .as_ref()
                .unwrap()
                .worker
                .as_ref()
                .unwrap()
                .operation(),
            operation
        );
    }
    drop(scope);
    execution.close();
}

#[test]
fn next_bytes_dispatch_pb_char_length_preserves_go_bytes_and_demands_null() {
    use crate::ExpressionAdapterFailureClass as Class;
    use tidb_proto::tipb;
    let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
    for slots in [0, 1] {
        let owner = AsciiPoolOwner::new(test_policy(slots, slots)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let columns = AdvertisedAsciiColumns {
            scope: None,
            execution: &execution,
        };
        for signature in [
            tipb::ScalarFuncSig::CharLength,
            tipb::ScalarFuncSig::CharLengthUtf8,
        ] {
            for input in [Some(vec![0xe2, 0x82]), None] {
                let null = input.is_none();
                let child = tipb::Expr {
                    tp: Some(
                        (if null {
                            tipb::ExprType::Null
                        } else {
                            tipb::ExprType::String
                        }) as i32,
                    ),
                    val: input,
                    field_type: Some(tipb::FieldType {
                        tp: Some(253),
                        charset: Some("utf8mb4".into()),
                        collate: Some(46),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let pb = tipb::Expr {
                    tp: Some(tipb::ExprType::ScalarFunc as i32),
                    sig: Some(signature as i32),
                    children: vec![child],
                    field_type: Some(tipb::FieldType {
                        tp: Some(8),
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let expression = crate::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
                arm_eval_one_observation();
                let result = expression.eval(&columns, row.to_row());
                let observation = take_eval_one_observation();
                if slots == 0 {
                    assert!(
                        matches!(result, Err(EvalError::ExpressionAdapterFailure(failure))
                        if failure.class() == Class::PoolResource)
                    );
                    assert_eq!(observation.facade_entries, 0);
                } else {
                    assert_eq!(result, Ok(if null { Datum::Null } else { Datum::Int(2) }));
                    assert_eq!(observation.facade_entries, 1, "PB NULL may not bypass C4");
                    assert_eq!(
                        observation.after_kernel_invocations,
                        observation
                            .before_kernel_invocations
                            .map(|before| before + 1)
                    );
                }
            }
        }
        execution.close();
    }
}

#[test]
fn bytes_dispatch_switches_one_cached_worker_in_the_same_active_root() {
    use EvaluatedBytesOp::*;
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let other_owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let other_execution = other_owner.begin_execution().unwrap();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &other_execution,
    };
    for (index, (operation, input, expected)) in [
        (Ascii, Datum::Raw(vec![b'A', 0xff]), Datum::Int(65)),
        (Length, Datum::Raw(vec![0xff, 0]), Datum::Int(2)),
        (BitLength, Datum::UInt(u64::MAX), Datum::Int(160)),
        (
            LTrim,
            Datum::new_string(b" \xff\t ".to_vec()),
            Datum::new_string(b"\xff\t ".to_vec()),
        ),
        (
            RTrim,
            Datum::new_bytes(b" \xff\t ".to_vec()),
            Datum::new_bytes(b" \xff\t".to_vec()),
        ),
        (
            UnHex,
            Datum::new_string("fF00"),
            Datum::new_bytes(vec![0xff, 0]),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut address = None;
        for calls in 1..=2 {
            assert_eq!(
                dispatch_bytes_family(operation, &input, &columns),
                Ok(expected.clone())
            );
            let (actual, invocations, _, _, _) = scope_worker_observation(&scope);
            assert_eq!(*address.get_or_insert(actual), actual);
            assert_eq!(invocations, calls);
            assert_eq!(
                scope
                    .lease
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .worker
                    .as_ref()
                    .unwrap()
                    .operation(),
                operation
            );
        }
        assert_eq!(
            dispatch_bytes_family(operation, &Datum::Null, &columns),
            Ok(Datum::Null)
        );
        assert_eq!(
            scope_worker_observation(&scope).1,
            3,
            "NULL must invoke C4 too"
        );
        let snapshot = owner.snapshot().unwrap();
        assert_eq!(snapshot.factory_successes, index as u64 + 1);
        assert_eq!(snapshot.retired, index as u64);
        assert_eq!((snapshot.live, snapshot.idle), (1, 0));
    }
    // The original public ASCII-only API also replaces a cached Bytes worker.
    assert_eq!(
        scope.evaluate_value(&Datum::new_string("A")),
        Ok(Datum::Int(65))
    );
    assert_eq!(scope_worker_observation(&scope).1, 1);
    assert_eq!(other_owner.snapshot().unwrap().factory_attempts, 0);
    drop(scope);
    execution.close();
    other_execution.close();
}

#[test]
fn bytes_dispatch_matches_idle_operations_and_retires_for_a_full_slot_set() {
    use EvaluatedBytesOp::*;
    let owner = AsciiPoolOwner::new(test_policy(2, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let columns = AdvertisedAsciiColumns {
        scope: None,
        execution: &execution,
    };
    for (operation, expected, before) in [
        (Ascii, Datum::Int(65), 0),
        (Length, Datum::Int(2), 0),
        (Ascii, Datum::Int(65), 1),
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &Datum::new_string("AB"), &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(observation.before_kernel_invocations, Some(before));
        assert_eq!(observation.after_kernel_invocations, Some(before + 1));
    }
    let cached = owner.snapshot().unwrap();
    assert_eq!(
        (cached.factory_successes, cached.idle, cached.retired),
        (2, 2, 0)
    );
    assert_eq!(
        dispatch_bytes_family(BitLength, &Datum::new_string("AB"), &columns),
        Ok(Datum::Int(16))
    );
    let replaced = owner.snapshot().unwrap();
    assert_eq!(
        (replaced.factory_successes, replaced.idle, replaced.retired),
        (3, 2, 1)
    );
    assert_eq!(
        (replaced.live, replaced.creating, replaced.retiring),
        (0, 0, 0)
    );
    execution.close();
}

#[test]
fn bytes_dispatch_explicit_exhausted_roots_never_create_an_alternate_pool() {
    use crate::ExpressionAdapterFailureClass as Class;
    use crate::ExpressionAdapterFailureOrigin as Origin;
    use EvaluatedBytesOp::*;
    for slots in [0, 1] {
        let owner = AsciiPoolOwner::new(test_policy(slots, slots)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let held = execution.scope();
        if slots != 0 {
            assert_eq!(held.evaluate_value(&Datum::Null), Ok(Datum::Null));
        }
        let columns = AdvertisedAsciiColumns {
            scope: None,
            execution: &execution,
        };
        let before = owner.snapshot().unwrap();
        for operation in [Ascii, Length, BitLength, LTrim, RTrim, UnHex] {
            assert!(matches!(
                dispatch_bytes_family(operation, &Datum::Null, &columns),
                Err(EvalError::ExpressionAdapterFailure(failure))
                    if failure.class() == Class::PoolResource && failure.origin() == Origin::Pool
            ));
            assert!(matches!(
                dispatch_bytes_family(operation, &Datum::MinNotNull, &columns),
                Err(EvalError::Unsupported(_))
            ));
        }
        assert_eq!(
            owner.snapshot().unwrap(),
            before,
            "no side pool or failed coercion admission"
        );
        execution.close();
    }
}

#[test]
fn bytes_dispatch_one_shot_preserves_native_packing_and_large_owned_output() {
    use EvaluatedBytesOp::*;
    for (operation, input, expected) in [
        (
            LTrim,
            Datum::new_string(b" \xff\t ".to_vec()),
            Datum::new_string(b"\xff\t ".to_vec()),
        ),
        (
            RTrim,
            Datum::new_bytes(b" \xff\t ".to_vec()),
            Datum::new_bytes(b" \xff\t".to_vec()),
        ),
        (UnHex, Datum::new_string("f"), Datum::new_bytes(vec![0x0f])),
        (UnHex, Datum::new_bytes(vec![0xff]), Datum::Null),
    ] {
        arm_eval_one_observation();
        let result = dispatch_bytes_family(operation, &input, &crate::NoColumns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Ok(expected));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(observation.before_kernel_invocations, Some(0));
        assert_eq!(observation.after_kernel_invocations, Some(1));
    }
    // A computed Bytes output larger than the 1 MiB retained-worker cap still
    // belongs to the call/native-result domain, not the idle worker's footprint.
    let mut bytes = vec![b'A'; 2 << 20];
    bytes[1] = 0xff;
    let expected = Datum::new_bytes(bytes.clone());
    assert_eq!(
        dispatch_bytes_family(LTrim, &Datum::new_bytes(bytes), &crate::NoColumns),
        Ok(expected)
    );
    let built = crate::BuildContext::default().build_string_length(
        crate::StringLengthFunction::Length,
        FieldType::new(FieldTypeCode::VarString),
    );
    assert_eq!(built.eval(&Datum::Raw(vec![0xff, 0])), Ok(Datum::Int(2)));
}

#[test]
fn ascii_dispatch_without_capabilities_uses_real_c4_including_null() {
    for (input, expected) in [
        (Datum::Null, Datum::Null),
        (Datum::Raw(vec![0xff, 0x00]), Datum::Int(255)),
        (Datum::Int(23), Datum::Int(50)),
        (Datum::Bytes(vec![]), Datum::Int(0)),
    ] {
        arm_eval_one_observation();
        let result =
            crate::func::eval_func_values("ASCII", std::slice::from_ref(&input), &crate::NoColumns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Some(Ok(expected)));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(observation.before_kernel_invocations, Some(0));
        assert_eq!(observation.after_kernel_invocations, Some(1));
    }
    assert_eq!(
        crate::func::eval_func_values("ASCII", &[Datum::MaxValue], &crate::NoColumns),
        Some(Err(EvalError::Unsupported("range sentinel byte coercion")))
    );
}

#[test]
fn ascii_dispatch_one_shot_accepts_input_larger_than_worker_retained_cap() {
    // The one-shot worker cap is 1 MiB; this demanded 2 MiB raw value belongs
    // to the call/input domain, not a new implicit maximum string length.
    let mut bytes = vec![b'x'; 2 << 20];
    bytes[0] = b'A';
    arm_eval_one_observation();
    let result = crate::func::eval_func_values("ASCII", &[Datum::Raw(bytes)], &crate::NoColumns);
    let observation = take_eval_one_observation();
    assert_eq!(result, Some(Ok(Datum::Int(65))));
    assert_eq!(observation.facade_entries, 1);
    assert_eq!(observation.before_kernel_invocations, Some(0));
    assert_eq!(observation.after_kernel_invocations, Some(1));
    // Returning from the first dispatcher call has already dropped its scope
    // and closer. A subsequent independent call must remain usable.
    assert_eq!(
        crate::func::eval_func_values("ASCII", &[Datum::Null], &crate::NoColumns),
        Some(Ok(Datum::Null))
    );
}

#[test]
fn ascii_dispatch_execution_capability_reuses_worker_without_closing_epoch() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let columns = AdvertisedAsciiColumns {
        scope: None,
        execution: &execution,
    };
    for (index, (input, expected)) in [
        (Datum::Null, Datum::Null),
        (Datum::Raw(vec![0xff]), Datum::Int(255)),
        (Datum::Int(23), Datum::Int(50)),
    ]
    .into_iter()
    .enumerate()
    {
        arm_eval_one_observation();
        let result = crate::func::eval_func_values("ASCII", &[input], &columns);
        let observation = take_eval_one_observation();
        assert_eq!(result, Some(Ok(expected)));
        assert_eq!(observation.facade_entries, 1);
        assert_eq!(observation.before_kernel_invocations, Some(index as u64));
        assert_eq!(observation.after_kernel_invocations, Some(index as u64 + 1));
        let snapshot = owner.snapshot().unwrap();
        assert_eq!(
            (snapshot.factory_attempts, snapshot.factory_successes),
            (1, 1)
        );
        assert_eq!(snapshot.idle, 1);
    }
    assert_eq!(
        execution.scope().evaluate_value(&Datum::Null),
        Ok(Datum::Null)
    );
    execution.close();
}

#[test]
fn ascii_dispatch_active_scope_wins_and_keeps_real_worker_identity() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let other_owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let other_execution = other_owner.begin_execution().unwrap();
    let columns = AdvertisedAsciiColumns {
        scope: Some(&scope),
        execution: &other_execution,
    };
    let untouched = other_owner.snapshot().unwrap();
    let mut address = None;
    for (index, (input, expected)) in [
        (Datum::Null, Datum::Null),
        (Datum::Raw(vec![0xff]), Datum::Int(255)),
        (Datum::Int(23), Datum::Int(50)),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            crate::func::eval_func_values("ASCII", &[input], &columns),
            Some(Ok(expected))
        );
        let (actual, invocations, _, _, _) = scope_worker_observation(&scope);
        assert_eq!(*address.get_or_insert(actual), actual);
        assert_eq!(invocations, index as u64 + 1, "NULL also reaches fn_ptr");
    }
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 1);
    assert_eq!(other_owner.snapshot().unwrap(), untouched);
    drop(scope);
    execution.close();
    other_execution.close();
}

#[test]
fn ascii_dispatch_zero_slot_capabilities_never_fall_back_even_for_null() {
    use crate::ExpressionAdapterFailureClass as Class;
    use crate::ExpressionAdapterFailureOrigin as Origin;
    let owner = AsciiPoolOwner::new(test_policy(0, 0)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    for active in [None, Some(&scope)] {
        let columns = AdvertisedAsciiColumns {
            scope: active,
            execution: &execution,
        };
        assert_eq!(
            crate::func::eval_func_values("ASCII", &[], &columns),
            Some(Err(EvalError::Unsupported("bad function arity")))
        );
        assert_eq!(
            crate::func::eval_func_values("ASCII", &[Datum::MinNotNull], &columns),
            Some(Err(EvalError::Unsupported("range sentinel byte coercion")))
        );
        for input in [Datum::Null, Datum::Raw(vec![0xff]), Datum::Int(23)] {
            assert!(matches!(
                crate::func::eval_func_values("ASCII", &[input], &columns),
                Some(Err(EvalError::ExpressionAdapterFailure(failure)))
                    if failure.class() == Class::PoolResource && failure.origin() == Origin::Pool
            ));
        }
    }
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    execution.close();
}

#[test]
fn ascii_dispatch_one_shot_closer_closes_on_normal_return_and_unwind() {
    use crate::ExpressionAdapterFailureClass as Class;
    for unwind in [false, true] {
        let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
        let execution = owner.begin_execution().unwrap();
        // Exercise the production closer with a real worker, not a panic-capable
        // replacement kernel or a test-only execution factory.
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let close = OneShotAsciiExecution(execution.clone());
            let scope = close.0.scope();
            let mut guard = NativeGuard::new(&scope);
            assert_eq!(scope.evaluate_value(&Datum::Null), Ok(Datum::Null));
            if unwind {
                std::panic::panic_any("one-shot operation unwind");
            }
            guard.disarm();
        }));
        assert_eq!(outcome.is_err(), unwind);
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 1);
        assert!(matches!(
            execution.scope().evaluate_value(&Datum::Null),
            Err(EvalError::ExpressionAdapterFailure(failure))
                if failure.class() == Class::PoolClosed
        ));
    }
}

#[test]
fn public_value_producer_invokes_real_c4_for_null_bytes_and_reuses_one_worker() {
    let owner = crate::AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    let cases = [
        (Datum::Null, Datum::Null),
        (Datum::Bytes(vec![]), Datum::Int(0)),
        (Datum::Raw(vec![0xff, 0xfe]), Datum::Int(255)),
        (Datum::Bytes(vec![0, b'x']), Datum::Int(0)),
        (Datum::Int(2), Datum::Int(50)),
    ];
    let calls = cases.len() as u64;
    let mut identity = None;
    for (index, (input, expected)) in cases.into_iter().enumerate() {
        assert_eq!(scope.evaluate_value(&input), Ok(expected));
        let (address, invocations, inline, heap, total) = scope_worker_observation(&scope);
        assert_eq!(invocations, index as u64 + 1, "NULL also invokes fn_ptr");
        assert_eq!(
            *identity.get_or_insert((address, inline, heap, total)),
            (address, inline, heap, total)
        );
        let snapshot = owner.snapshot().unwrap();
        assert_eq!(
            (snapshot.factory_attempts, snapshot.factory_successes),
            (1, 1)
        );
    }
    drop(scope);
    assert_eq!(owner.snapshot().unwrap().idle, 1);
    let reused = execution.scope();
    assert_eq!(reused.evaluate_value(&Datum::Null), Ok(Datum::Null));
    let (address, invocations, inline, heap, total) = scope_worker_observation(&reused);
    assert_eq!(invocations, calls + 1);
    assert_eq!(identity, Some((address, inline, heap, total)));
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 1);
}

#[test]
fn public_dynamic_capabilities_are_sized_and_keep_effective_scope_execution_identity() {
    let owner_a = crate::AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let owner_b = crate::AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution_a = owner_a.begin_execution().unwrap();
    let execution_b = owner_b.begin_execution().unwrap();
    let active = execution_a.scope();
    let requested = execution_b.scope();
    assert_eq!(requested.evaluate_value(&Datum::Int(2)), Ok(Datum::Int(50)));
    let untouched_b = owner_b.snapshot().unwrap();
    let conflicting = AdvertisedAsciiColumns {
        scope: Some(&active),
        execution: &execution_b,
    };
    requested.with_columns(&conflicting, |bound: &crate::ScopedAsciiColumns<'_, '_>| {
        let dynamic: &dyn Columns = bound;
        assert!(std::ptr::eq(
            dynamic.evaluated_ascii_scope().unwrap(),
            &active
        ));
        assert!(std::ptr::eq(
            dynamic.evaluated_ascii_execution().unwrap(),
            &active.execution
        ));
        assert!(!Arc::ptr_eq(
            &dynamic.evaluated_ascii_execution().unwrap().core,
            &execution_b.core
        ));
        assert_eq!(
            value_through_sized_columns(bound, &Datum::Null),
            Ok(Datum::Null)
        );
        requested.with_columns(dynamic, |nested| {
            assert!(std::ptr::eq(
                nested.evaluated_ascii_scope().unwrap(),
                &active
            ));
            assert!(std::ptr::eq(
                nested.evaluated_ascii_execution().unwrap(),
                &active.execution
            ));
            assert_eq!(
                value_through_sized_columns(nested, &Datum::Raw(vec![0xff])),
                Ok(Datum::Int(255))
            );
        });
    });
    assert_eq!(scope_worker_observation(&active).1, 2);
    assert_eq!(scope_worker_observation(&requested).1, 1);
    assert_eq!(owner_b.snapshot().unwrap(), untouched_b);

    // With no active scope, the requested binding wins even if the base
    // context advertises a different execution. Do not forward that token.
    let execution_only = AdvertisedAsciiColumns {
        scope: None,
        execution: &execution_a,
    };
    requested.with_columns(&execution_only, |bound| {
        assert!(std::ptr::eq(
            bound.evaluated_ascii_scope().unwrap(),
            &requested
        ));
        assert!(std::ptr::eq(
            bound.evaluated_ascii_execution().unwrap(),
            &requested.execution
        ));
        assert_eq!(
            value_through_sized_columns(bound, &Datum::Null),
            Ok(Datum::Null)
        );
    });
    assert_eq!(scope_worker_observation(&requested).1, 2);
    assert_eq!(owner_b.snapshot().unwrap().factory_attempts, 1);
}

#[test]
fn public_foreign_nested_native_panic_quarantines_only_the_effective_scope() {
    let owner_a = crate::AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let owner_b = crate::AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution_a = owner_a.begin_execution().unwrap();
    let execution_b = owner_b.begin_execution().unwrap();
    let active = execution_a.scope();
    let requested = execution_b.scope();
    assert_eq!(active.evaluate_value(&Datum::Null), Ok(Datum::Null));
    assert_eq!(requested.evaluate_value(&Datum::Null), Ok(Datum::Null));
    let untouched_b = owner_b.snapshot().unwrap();
    let conflicting = AdvertisedAsciiColumns {
        scope: Some(&active),
        execution: &execution_b,
    };
    let panic = catch_unwind(AssertUnwindSafe(|| {
        requested.with_columns(&conflicting, |outer| {
            requested.with_columns(outer, |inner| {
                assert!(std::ptr::eq(
                    inner.evaluated_ascii_scope().unwrap(),
                    &active
                ));
                assert_eq!(
                    value_through_sized_columns(inner, &Datum::Bytes(vec![b'Q'])),
                    Ok(Datum::Int(81))
                );
                assert_eq!(scope_worker_observation(&active).1, 2);
                std::panic::panic_any("native after effective value");
            });
        });
    }))
    .expect_err("the native panic must escape both lexical guards");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"native after effective value")
    );
    assert!(active.poisoned.get());
    assert!(active.lease.borrow().is_none());
    let disposed_a = owner_a.snapshot().unwrap();
    assert_eq!(
        (disposed_a.live, disposed_a.idle, disposed_a.retired),
        (0, 0, 1)
    );
    assert_eq!(disposed_a.reserved_bytes, disposed_a.base_bytes);
    assert!(!requested.poisoned.get());
    assert_eq!(owner_b.snapshot().unwrap(), untouched_b);
    assert_eq!(requested.evaluate_value(&Datum::Null), Ok(Datum::Null));
    assert_eq!(scope_worker_observation(&requested).1, 2);
    assert_eq!(owner_b.snapshot().unwrap().factory_attempts, 1);
    assert!(matches!(
        active.evaluate_value(&Datum::Null),
        Err(EvalError::ExpressionAdapterFailure(failure))
            if failure.class() == crate::ExpressionAdapterFailureClass::ScopePoisoned
    ));
    assert_eq!(owner_a.snapshot().unwrap(), disposed_a);
}

#[test]
fn public_capability_getter_panic_quarantines_requested_ready_scope_before_recovery() {
    struct PanickingDiscovery<'a>(&'a Cell<usize>);
    impl Columns for PanickingDiscovery<'_> {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn evaluated_ascii_scope(&self) -> Option<&crate::AsciiScope> {
            self.0.set(self.0.get() + 1);
            std::panic::panic_any("scope discovery panic");
        }
    }

    let owner = crate::AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let requested = execution.scope();
    assert_eq!(requested.evaluate_value(&Datum::Null), Ok(Datum::Null));
    assert_eq!(scope_worker_observation(&requested).1, 1);
    let discoveries = Cell::new(0);
    let body_calls = Cell::new(0);
    let native = PanickingDiscovery(&discoveries);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        requested.with_columns(&native, |_| body_calls.set(body_calls.get() + 1));
    }))
    .expect_err("a capability getter is native work and can panic");
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"scope discovery panic"));
    assert_eq!(discoveries.get(), 1);
    assert_eq!(body_calls.get(), 0);
    assert!(requested.poisoned.get());
    assert!(requested.lease.borrow().is_none());
    let disposed = owner.snapshot().unwrap();
    assert_eq!((disposed.live, disposed.idle, disposed.retired), (0, 0, 1));
    assert_eq!(disposed.reserved_bytes, disposed.base_bytes);
    assert!(matches!(
        requested.evaluate_value(&Datum::Null),
        Err(EvalError::ExpressionAdapterFailure(failure))
            if failure.class() == crate::ExpressionAdapterFailureClass::ScopePoisoned
    ));
    assert_eq!(owner.snapshot().unwrap(), disposed);
    // Nothing here claims to identify/quarantine a scope the getter never
    // returned. Only the known requested scope is protected during discovery.
}

#[test]
fn public_invalid_active_scope_never_falls_back_to_a_foreign_execution() {
    use crate::ExpressionAdapterFailureClass as AdapterClass;
    for poison in [false, true] {
        let owner_a = crate::AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
        let owner_b = crate::AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
        let execution_a = owner_a.begin_execution().unwrap();
        let execution_b = owner_b.begin_execution().unwrap();
        let active = execution_a.scope();
        let requested = execution_b.scope();
        assert_eq!(active.evaluate_value(&Datum::Null), Ok(Datum::Null));
        assert_eq!(requested.evaluate_value(&Datum::Null), Ok(Datum::Null));
        let expected_class = if poison {
            let panic = catch_unwind(AssertUnwindSafe(|| {
                active.with_columns(&crate::NoColumns, |_| {
                    std::panic::panic_any("poison active capability");
                });
            }));
            assert!(panic.is_err());
            AdapterClass::ScopePoisoned
        } else {
            execution_a.close();
            AdapterClass::PoolClosed
        };
        let untouched_b = owner_b.snapshot().unwrap();
        let conflicting = AdvertisedAsciiColumns {
            scope: Some(&active),
            execution: &execution_b,
        };
        arm_eval_one_observation();
        let result = requested.with_columns(&conflicting, |outer| {
            requested.with_columns(outer, |inner| {
                assert!(std::ptr::eq(
                    inner.evaluated_ascii_scope().unwrap(),
                    &active
                ));
                assert!(std::ptr::eq(
                    inner.evaluated_ascii_execution().unwrap(),
                    &active.execution
                ));
                value_through_sized_columns(inner, &Datum::Null)
            })
        });
        let observation = take_eval_one_observation();
        assert!(matches!(
            result,
            Err(EvalError::ExpressionAdapterFailure(failure))
                if failure.class() == expected_class
        ));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(observation.before_kernel_invocations, None);
        assert_eq!(owner_b.snapshot().unwrap(), untouched_b);
        assert_eq!(scope_worker_observation(&requested).1, 1);
        assert!(!requested.poisoned.get());
        assert_eq!(owner_a.snapshot().unwrap().factory_attempts, 1);
    }
}

#[test]
fn public_value_errors_capture_actual_prepare_and_invoke_phases_without_recapture() {
    use crate::ExpressionRuntimeFailureClass as RuntimeClass;
    use crate::ExpressionRuntimeFailurePhase as Phase;
    // The first case makes the real factory refuse its worker allowance; the
    // second really prepares, then refuses work before its fn_ptr is entered.
    // Observe failures are not fabricated: this test makes no runtime claim
    // for the structurally captured retained_storage error sites.
    for (worker_cap, steps, phase) in [(1, 64, Phase::Prepare), (TEST_WORKER_CAP, 0, Phase::Invoke)]
    {
        let policy = crate::AsciiPoolPolicy::checked(
            1,
            1,
            TEST_POOL_BYTES,
            worker_cap,
            TEST_CREATION_RESERVATION,
            steps,
            8,
            TEST_CALL_BYTES,
        )
        .unwrap();
        let owner = crate::AsciiPoolOwner::new(policy).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        for (index, value) in [Datum::Null, Datum::Int(2)].into_iter().enumerate() {
            let failure = match scope.evaluate_value(&value) {
                Err(EvalError::ExpressionRuntimeFailure(failure)) => failure,
                other => panic!("expected real public C4 resource failure: {other:?}"),
            };
            assert_eq!(failure.class(), RuntimeClass::ResourceLimit);
            assert_eq!(failure.phase(), Some(phase));
            assert!(matches!(
                failure.local_error(),
                LocalError::ResourceLimit(_)
            ));
            let snapshot = owner.snapshot().unwrap();
            if phase == Phase::Prepare {
                assert_eq!(snapshot.factory_attempts, index as u64 + 1);
                assert_eq!(snapshot.factory_successes, 0);
                assert_eq!(snapshot.reserved_bytes, snapshot.base_bytes);
                assert!(scope.lease.borrow().is_none());
            } else {
                assert_eq!(
                    (snapshot.factory_attempts, snapshot.factory_successes),
                    (1, 1)
                );
                assert_eq!(scope_worker_observation(&scope).1, 0);
            }
            assert!(!scope.poisoned.get());
        }
    }
}

#[test]
fn public_frontend_errors_precede_zero_slots_closed_epochs_and_scope_poison() {
    use crate::ExpressionAdapterFailureClass as AdapterClass;
    for (state, expected_class) in [
        (0, AdapterClass::PoolResource),
        (1, AdapterClass::PoolClosed),
        (2, AdapterClass::ScopePoisoned),
    ] {
        let slots = usize::from(state != 0);
        let owner = crate::AsciiPoolOwner::new(test_policy(slots, slots)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        if state != 0 {
            assert_eq!(scope.evaluate_value(&Datum::Null), Ok(Datum::Null));
            if state == 1 {
                execution.close();
            } else {
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    scope.with_columns(&crate::NoColumns, |_| {
                        std::panic::panic_any("native operation poisoned scope");
                    });
                }));
                assert!(panic.is_err());
            }
        }
        let before = owner.snapshot().unwrap();
        arm_eval_one_observation();
        for value in [Datum::MinNotNull, Datum::MaxValue] {
            assert_eq!(
                scope.evaluate_value(&value),
                Err(EvalError::Unsupported("range sentinel byte coercion"))
            );
        }
        let observation = take_eval_one_observation();
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(
            owner.snapshot().unwrap(),
            before,
            "frontend errors precede admission"
        );
        assert!(matches!(
            scope.evaluate_value(&Datum::Null),
            Err(EvalError::ExpressionAdapterFailure(failure))
                if failure.class() == expected_class
        ));
    }
}

#[test]
fn actual_ready_values_include_null_raw_bytes_and_original_numeric_coercion() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let cases = [
        (Datum::Null, Datum::Null),
        (Datum::Bytes(vec![]), Datum::Int(0)),
        (Datum::Raw(vec![0xff, 0xfe]), Datum::Int(255)),
        (Datum::Bytes(vec![0, b'x']), Datum::Int(0)),
        (Datum::Bytes(vec![b'x', 0, b'z']), Datum::Int(120)),
        (Datum::Int(2), Datum::Int(50)),
        (Datum::Bytes(vec![b'a'; 1]), Datum::Int(97)),
        (Datum::Bytes(vec![b'b'; 64]), Datum::Int(98)),
        (Datum::Bytes(vec![b'c'; 4096]), Datum::Int(99)),
        (Datum::new_string("你好"), Datum::Int(228)),
        (Datum::Null, Datum::Null),
        (Datum::Bytes(vec![]), Datum::Int(0)),
    ];
    assert!(scope.lease.borrow().is_none());
    let mut published_storage = None;
    for (index, (input, expected)) in cases.into_iter().enumerate() {
        let ready = coerce_ready(&input).unwrap();
        let computed = eval_ready(&scope, ready).unwrap();
        assert!(
            !scope.busy.get(),
            "materialization follows the short worker borrow"
        );
        assert_eq!(computed.metadata.kind, DatumKind::Int);
        assert!(computed.metadata.string_collation.is_none());
        assert!(computed.metadata.decimal_declared_shape.is_none());
        assert_eq!(computed.into_datum().unwrap(), expected);
        let (address, invocations, inline, heap, total) = scope_worker_observation(&scope);
        assert_eq!(
            invocations,
            index as u64 + 1,
            "NULL also enters the real fn_ptr wrapper"
        );
        assert_eq!(inline + heap, total);
        assert!(total <= TEST_WORKER_CAP);
        let current = (address, inline, heap, total);
        if let Some(first) = published_storage {
            assert_eq!(
                current, first,
                "no stale operand or per-call retained buffer"
            );
        } else {
            published_storage = Some(current);
        }
    }
}

#[test]
fn nested_ready_calls_are_sequential_and_repeated_scopes_reuse_the_actual_worker() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let input = NativeInputs::new();
    let mut previous = None;
    for round in 0..8 {
        let scope = execution.scope();
        assert!(
            scope.lease.borrow().is_none(),
            "a new scope does not eagerly check out"
        );
        let native = ForwardingSentinel::new(false, None);
        let result = scope.with_columns(&native, |columns| {
            columns.append_note(44000, &input.message);
            let inner = evaluate_ascii_value(&scope, &Datum::Int(2)).unwrap();
            assert_eq!(inner, Datum::Int(50));
            assert!(!scope.busy.get());
            columns.append_warning(44001, &input.message);
            let outer = evaluate_ascii_value(&scope, &inner).unwrap();
            assert!(!scope.busy.get());
            columns.set_uservar(&input.name, outer.clone());
            outer
        });
        assert_eq!(result, Datum::Int(53));
        let observation = scope_worker_observation(&scope);
        assert_eq!(observation.1, (round + 1) * 2);
        if let Some((address, inline, heap, total)) = previous {
            assert_eq!(
                (observation.0, observation.2, observation.3, observation.4),
                (address, inline, heap, total)
            );
        }
        previous = Some((observation.0, observation.2, observation.3, observation.4));
        assert_eq!(
            native.effects.borrow().uservars.get(&input.name),
            Some(&Datum::Int(53))
        );
        drop(scope);
        let idle = owner.snapshot().unwrap();
        assert_eq!((idle.factory_attempts, idle.factory_successes), (1, 1));
        assert_eq!((idle.live, idle.idle, idle.creating), (0, 1, 0));
    }
}

#[test]
fn normal_native_error_after_real_kernel_preserves_error_and_healthy_scope() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let native = ForwardingSentinel::new(true, None);
    let first = EvalError::UnknownColumn("sentinel original error: param_value".into());
    let result = scope.with_columns(&native, |columns| -> Result<Datum, EvalError> {
        assert_eq!(
            evaluate_ascii_value(&scope, &Datum::Int(2)).unwrap(),
            Datum::Int(50)
        );
        columns.param_value(39)
    });
    assert_eq!(result, Err(first));
    assert!(!scope.poisoned.get());
    assert!(!scope.busy.get());
    assert_eq!(scope_worker_observation(&scope).1, 1);
    assert_eq!(
        evaluate_ascii_value(&scope, &Datum::Null).unwrap(),
        Datum::Null
    );
    assert_eq!(scope_worker_observation(&scope).1, 2);
}

#[test]
fn actual_worker_owner_and_scope_traits_are_checked_without_unsafe_impls() {
    fn require_send<T: Send>() {}
    fn require_send_sync<T: Send + Sync>() {}
    require_send_sync::<AsciiPoolOwner>();
    require_send_sync::<AsciiExecution>();
    require_send::<AsciiLease>();
    require_send::<AsciiScope>();
    require_send::<EvaluatedBytesWorker>();

    // Dependency-free negative assertions: if a type implements the forbidden
    // trait, the placeholder has two applicable implementations and the test
    // fails to compile. These do not supply any Send/Sync implementation.
    trait AmbiguousIfSync<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfSync<()> for T {}
    struct SyncMarker;
    impl<T: ?Sized + Sync> AmbiguousIfSync<SyncMarker> for T {}
    let _ = <AsciiLease as AmbiguousIfSync<_>>::check;
    let _ = <AsciiScope as AmbiguousIfSync<_>>::check;
    let _ = <EvaluatedBytesWorker as AmbiguousIfSync<_>>::check;

    trait AmbiguousIfClone<A> {
        fn check() {}
    }
    impl<T: ?Sized> AmbiguousIfClone<()> for T {}
    struct CloneMarker;
    impl<T: ?Sized + Clone> AmbiguousIfClone<CloneMarker> for T {}
    let _ = <AsciiLease as AmbiguousIfClone<_>>::check;
    let _ = <AsciiScope as AmbiguousIfClone<_>>::check;
    let _ = <EvaluatedBytesWorker as AmbiguousIfClone<_>>::check;
}

#[test]
fn skipped_native_work_and_original_coercion_refusal_do_not_reserve_or_prepare() {
    for (workers, creating) in [(0, 0), (1, 0)] {
        let owner = AsciiPoolOwner::new(test_policy(workers, creating)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        let before = owner.snapshot().unwrap();
        let native = ForwardingSentinel::new(true, None);
        let native_error = scope.with_columns(&native, |columns| columns.param_value(1));
        assert_eq!(
            native_error,
            Err(EvalError::UnknownColumn(
                "sentinel original error: param_value".into()
            ))
        );
        for value in [Datum::MinNotNull, Datum::MaxValue] {
            match evaluate_ascii_value(&scope, &value) {
                Err(AsciiBoundaryError::Frontend(error)) => assert_eq!(
                    error,
                    EvalError::Unsupported("range sentinel byte coercion")
                ),
                other => panic!("original coercion error must precede admission: {other:?}"),
            }
        }
        assert_eq!(owner.snapshot().unwrap(), before);
        assert!(scope.lease.borrow().is_none());
        assert!(!scope.poisoned.get());
        assert!(matches!(
            evaluate_ascii_value(&scope, &Datum::Null),
            Err(AsciiBoundaryError::Owner(AsciiOwnerError {
                kind: OwnerErrorKind::Resource,
                ..
            }))
        ));
        assert_eq!(owner.snapshot().unwrap(), before);
    }

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let occupied = execution.scope();
    assert_eq!(
        evaluate_ascii_value(&occupied, &Datum::Int(2)).unwrap(),
        Datum::Int(50)
    );
    let dormant = execution.scope();
    let before = owner.snapshot().unwrap();
    for scope in [&occupied, &dormant] {
        assert!(matches!(
            evaluate_ascii_value(scope, &Datum::MinNotNull),
            Err(AsciiBoundaryError::Frontend(EvalError::Unsupported(
                "range sentinel byte coercion"
            )))
        ));
    }
    assert_eq!(owner.snapshot().unwrap(), before);
    assert_eq!(scope_worker_observation(&occupied).1, 1);
    assert!(dormant.lease.borrow().is_none());
}

#[test]
fn all_native_callbacks_run_outside_busy_cell_borrow_and_pool_mutex() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let probes = Cell::new(0);
    let probe = || {
        assert!(!scope.busy.get());
        assert!(
            scope.lease.try_borrow_mut().is_ok(),
            "native callback cannot inherit a cell borrow"
        );
        assert!(
            owner.core.state.try_lock().is_ok(),
            "native callback cannot inherit the root mutex"
        );
        probes.set(probes.get() + 1);
    };
    let native = ForwardingSentinel::new(false, Some(&probe));
    let input = NativeInputs::new();
    scope.with_columns(&native, |columns| {
        exercise_columns(columns, &input);
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
        assert!(scope.lease.borrow().is_none());
        assert_eq!(
            evaluate_ascii_value(&scope, &Datum::Int(2)).unwrap(),
            Datum::Int(50)
        );
        assert_eq!(owner.snapshot().unwrap().factory_attempts, 1);
        exercise_columns(columns, &input);
    });
    assert_eq!(probes.get(), native.calls.borrow().len());
    assert!(probes.get() >= 2 * COLUMNS_FORWARDED_METHODS.len());
    assert_eq!(scope_worker_observation(&scope).1, 1);
}

#[test]
fn like_ready_recipes_own_signed_metadata_and_legacy_presence() {
    use tidb_query_expr::local::NativeCollation;

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    // Original like.rs source rows: wildcard matching and escape-sensitive
    // ASCII folding. Neither the arguments nor expected answers are prelowered.
    for (operation, ready, expected) in [
        (
            EvaluatedBytesOp::LikeNative,
            EvaluatedArgs::Like {
                invocation: NativeLikeInvocation::like(NativeCollation::Utf8Mb4Bin, None),
                text: Some(b"pending deposits".to_vec()),
                pattern: Some(b"%pending%deposits%".to_vec()),
                escape: Some(i64::from(b'\\')),
            },
            Some(1),
        ),
        (
            EvaluatedBytesOp::IlikeNative,
            EvaluatedArgs::Like {
                invocation: NativeLikeInvocation::ilike(NativeCollation::Utf8Mb4Bin, None),
                text: Some(b"abc".to_vec()),
                pattern: Some(b"ABC".to_vec()),
                escape: Some(i64::from(b'A')),
            },
            Some(0),
        ),
        (
            EvaluatedBytesOp::LikeLegacyNative,
            EvaluatedArgs::Like {
                invocation: NativeLikeInvocation::legacy(false),
                text: Some(b"a%b".to_vec()),
                pattern: Some(b"a\\%b".to_vec()),
                escape: Some(i64::from(b'\\')),
            },
            Some(1),
        ),
        (
            EvaluatedBytesOp::LikeNullIntNative,
            EvaluatedArgs::NullWitness(None),
            None,
        ),
        (
            EvaluatedBytesOp::LikeMissingLegacyNative,
            EvaluatedArgs::NoArgs,
            None,
        ),
    ] {
        let mut invocation = Invocation::enter(&scope).unwrap();
        let result = invocation.run_args(operation, ready);
        let computed = require_computed_int(invocation.finish(result).unwrap()).unwrap();
        assert_eq!(computed.metadata(), ComputedIntMetadata::OwnSignedInt);
        assert_eq!(computed.value(), expected);
        let native = own_computed_int(computed);
        assert_eq!(native.metadata.kind, DatumKind::Int);
        assert_eq!(native.metadata.string_collation, None);
        assert_eq!(native.metadata.decimal_declared_shape, None);
        let expected = expected.map_or(Datum::Null, Datum::Int);
        assert_eq!(native.into_datum().unwrap(), expected);
        assert_eq!(
            materialize_computed(operation, ComputedValue::Int(computed))
                .unwrap()
                .into_boolean_datum()
                .unwrap(),
            expected,
        );
        assert_eq!(scope_worker_observation(&scope).1, 1);
    }
    scope.with_columns(&crate::NoColumns, |columns| {
        for args in [LegacyLikeArgs::Missing, LegacyLikeArgs::NullWitness(None)] {
            let (result, observation) =
                observe_wide_math(|| eval_legacy_like_in(false, args, columns));
            assert_eq!(result, Ok(None));
            assert_wide_math_c4(observation);
        }
        let (result, observation) = observe_wide_math(|| {
            eval_legacy_like_in(
                false,
                LegacyLikeArgs::Values {
                    target: b"a%b".to_vec(),
                    pattern: b"a\\%b".to_vec(),
                    escape: b'\\',
                },
                columns,
            )
        });
        assert_eq!(result, Ok(Some(1_i128)));
        assert_wide_math_c4(observation);
    });
    drop(scope);
    execution.close();
}

#[test]
fn like_legacy_null_and_missing_cannot_bypass_actual_work_budget() {
    let mut policy = test_policy(1, 1);
    policy.max_steps = 0;
    let owner = AsciiPoolOwner::new(policy).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    scope.with_columns(&crate::NoColumns, |columns| {
        let before = owner.snapshot().unwrap();
        let (result, observation) = observe_wide_math(|| {
            eval_legacy_like_in(false, LegacyLikeArgs::NullWitness(Some(1)), columns)
        });
        assert!(matches!(
            result,
            Err(EvalError::ExpressionAdapterFailure(_))
        ));
        assert_eq!(observation.facade_entries, 0);
        assert_eq!(owner.snapshot().unwrap(), before);
        for args in [
            LegacyLikeArgs::Missing,
            LegacyLikeArgs::NullWitness(None),
            LegacyLikeArgs::Values {
                target: b"a%b".to_vec(),
                pattern: b"a\\%b".to_vec(),
                escape: b'\\',
            },
        ] {
            let (result, observation) =
                observe_wide_math(|| eval_legacy_like_in(false, args, columns));
            assert!(matches!(
                result,
                Err(EvalError::ExpressionRuntimeFailure(failure))
                    if matches!(failure.local_error(), LocalError::ResourceLimit(_))
                        && failure.phase() == Some(ExpressionRuntimeFailurePhase::Invoke)
            ));
            assert_eq!(observation.facade_entries, 1);
            assert_eq!(observation.before_kernel_invocations, Some(0));
            assert_eq!(observation.after_kernel_invocations, Some(0));
            assert_eq!(scope_worker_observation(&scope).1, 0);
            assert!(!scope.poisoned.get());
        }
    });
    drop(scope);
    execution.close();
}

#[test]
fn real_call_budget_errors_keep_original_kernel_error_and_do_not_reprepare() {
    // Frozen C4's two-node path enters eval_frames: push_frame checks depth(1)
    // and nonzero frame storage, then the loop charges BEFORE the first node.
    // The witness advances later, only in eval_prepared_kernel at fn_ptr.
    // ResourceLimit with clean postflight is reusable in finish_invocation.
    for (steps, depth, retained) in [
        (0, 8, TEST_CALL_BYTES),
        (64, 0, TEST_CALL_BYTES),
        (64, 8, 0),
    ] {
        let policy = AsciiPoolPolicy::checked(
            1,
            1,
            TEST_POOL_BYTES,
            TEST_WORKER_CAP,
            TEST_CREATION_RESERVATION,
            steps,
            depth,
            retained,
        )
        .unwrap();
        let owner = AsciiPoolOwner::new(policy).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        for value in [Datum::Int(2), Datum::Null, Datum::Bytes(vec![])] {
            assert!(matches!(
                evaluate_ascii_value(&scope, &value),
                Err(AsciiBoundaryError::Kernel(failure))
                    if matches!(failure.local_error(), LocalError::ResourceLimit(_))
            ));
            assert_eq!(scope_worker_observation(&scope).1, 0);
            assert!(!scope.poisoned.get());
            assert!(!scope.busy.get());
            let snapshot = owner.snapshot().unwrap();
            assert_eq!(
                (snapshot.factory_attempts, snapshot.factory_successes),
                (1, 1)
            );
            assert_eq!((snapshot.live, snapshot.idle, snapshot.creating), (1, 0, 0));
            assert_eq!(
                snapshot.reserved_bytes,
                snapshot.base_bytes + TEST_WORKER_CAP
            );
        }
        drop(scope);
        assert_eq!(owner.snapshot().unwrap().idle, 1);
    }
}

#[test]
fn ready_empty_vec_capacity_is_charged_and_not_retained_after_real_refusal() {
    let bytes = Vec::<u8>::with_capacity(4096);
    assert!(bytes.is_empty());
    let policy = AsciiPoolPolicy::checked(
        1,
        1,
        TEST_POOL_BYTES,
        TEST_WORKER_CAP,
        TEST_CREATION_RESERVATION,
        64,
        8,
        bytes.capacity() - 1,
    )
    .unwrap();
    let owner = AsciiPoolOwner::new(policy).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    assert!(matches!(
        eval_ready(&scope, ReadyAsciiBytes(Some(bytes))),
        Err(AsciiBoundaryError::Kernel(failure))
                    if matches!(failure.local_error(), LocalError::ResourceLimit(_))
    ));
    let refused = scope_worker_observation(&scope);
    assert_eq!(refused.1, 0);
    assert_eq!(
        evaluate_ascii_value(&scope, &Datum::Bytes(vec![])).unwrap(),
        Datum::Int(0)
    );
    let accepted = scope_worker_observation(&scope);
    assert_eq!(accepted.1, 1);
    assert_eq!(
        (accepted.0, accepted.2, accepted.3, accepted.4),
        (refused.0, refused.2, refused.3, refused.4)
    );
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 1);
}

#[test]
fn real_factory_low_worker_budget_releases_full_creating_reservation() {
    let policy = AsciiPoolPolicy::checked(
        1,
        1,
        TEST_POOL_BYTES,
        1,
        TEST_CREATION_RESERVATION,
        64,
        8,
        TEST_CALL_BYTES,
    )
    .unwrap();
    let owner = AsciiPoolOwner::new(policy).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    for attempts in 1..=2 {
        assert!(matches!(
            evaluate_ascii_value(&scope, &Datum::Int(2)),
            Err(AsciiBoundaryError::Kernel(failure))
                    if matches!(failure.local_error(), LocalError::ResourceLimit(_))
        ));
        let snapshot = owner.snapshot().unwrap();
        assert_eq!(snapshot.factory_attempts, attempts);
        assert_eq!(snapshot.factory_successes, 0);
        assert_eq!(
            (
                snapshot.live,
                snapshot.idle,
                snapshot.creating,
                snapshot.retiring
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(snapshot.reserved_bytes, snapshot.base_bytes);
        assert!(scope.lease.borrow().is_none());
        assert!(!scope.poisoned.get());
    }
}

#[test]
fn cold_real_factory_is_prewarmed_without_invoking_and_idle_observation_counts_box_body_once() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let creation = match execution.checkout().unwrap() {
        Checkout::Create(creation) => creation,
        Checkout::Idle(_) => panic!("fresh root has no worker"),
    };
    let reserved = owner.snapshot().unwrap();
    assert_eq!(reserved.creating, 1);
    assert_eq!(
        reserved.reserved_bytes,
        reserved.base_bytes + TEST_CREATION_RESERVATION
    );
    assert_eq!(reserved.factory_attempts, 0);
    let lease = creation.prepare().unwrap();
    let worker = lease.worker.as_ref().unwrap();
    let cold = worker.retained_storage().unwrap();
    assert_eq!(worker.kernel_invocations(), 0);
    assert_eq!(worker.retained_storage().unwrap(), cold);
    assert_eq!(
        cold.inline_bytes(),
        std::mem::size_of::<EvaluatedBytesWorker>()
    );
    assert_eq!(
        cold.total_bytes(),
        cold.inline_bytes() + cold.owned_heap_bytes()
    );
    assert!(cold.total_bytes() < TEST_WORKER_CAP);
    lease.return_to_pool();
    let idle = owner.snapshot().unwrap();
    assert_eq!((idle.live, idle.idle, idle.creating), (0, 1, 0));
    assert_eq!((idle.factory_attempts, idle.factory_successes), (1, 1));
    assert_eq!(idle.idle_observed_bytes, cold.total_bytes());
    assert_eq!(idle.reserved_bytes, idle.base_bytes + TEST_WORKER_CAP);
    // This is reservation/observation algebra only. No proxy=size_of(proxy)
    // assertion is offered as an actual Arc allocation measurement.
    assert!(idle.caller_arc_measurement_required);
    execution.close();
    let closed = owner.snapshot().unwrap();
    assert_eq!(closed.idle_observed_bytes, 0);
    assert_eq!(closed.reserved_bytes, closed.base_bytes);
    assert_eq!(closed.retired, 1);
}

#[test]
fn checked_reentry_and_cell_conflict_return_errors_without_refcell_panics_or_new_workers() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    assert_eq!(
        evaluate_ascii_value(&scope, &Datum::Int(2)).unwrap(),
        Datum::Int(50)
    );
    let mut invocation = Invocation::enter(&scope).unwrap();
    assert!(scope.busy.get());
    assert!(
        scope.lease.try_borrow_mut().is_ok(),
        "the busy invocation owns its lease outside the cell"
    );
    let reentry = catch_unwind(AssertUnwindSafe(|| {
        evaluate_ascii_value(&scope, &Datum::Null)
    }));
    assert!(matches!(
        reentry,
        Ok(Err(AsciiBoundaryError::Scope {
            kind: ScopeFailureKind::Reentry,
            reason: "reentrant ASCII runtime borrow",
        }))
    ));
    assert_eq!(
        invocation
            .lease
            .as_ref()
            .unwrap()
            .worker
            .as_ref()
            .unwrap()
            .kernel_invocations(),
        1
    );
    let result = invocation.run(coerce_ready(&Datum::Null).unwrap());
    assert_eq!(invocation.finish(result).unwrap().value(), None);
    assert!(!scope.busy.get());
    assert_eq!(scope_worker_observation(&scope).1, 2);

    let held = scope.lease.borrow_mut();
    let conflict = catch_unwind(AssertUnwindSafe(|| {
        evaluate_ascii_value(&scope, &Datum::Null)
    }));
    assert!(matches!(
        conflict,
        Ok(Err(AsciiBoundaryError::Scope {
            kind: ScopeFailureKind::Reentry,
            reason: "ASCII scope cell is already borrowed",
        }))
    ));
    drop(held);
    assert!(!scope.poisoned.get());
    assert_eq!(scope_worker_observation(&scope).1, 2);
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 1);
}

fn new_creation(execution: &AsciiExecution) -> Creation {
    match execution.checkout().unwrap() {
        Checkout::Create(creation) => creation,
        Checkout::Idle(_) => panic!("this test requires an actual creation, not idle reuse"),
    }
}

#[test]
fn committed_creation_blocks_concurrent_miss_by_creating_limit_and_full_byte_reservation() {
    let one_creation_bytes = base_charge(2).unwrap() + TEST_CREATION_RESERVATION;
    for (policy, reason) in [
        (test_policy(2, 1), "ASCII creating-worker limit exceeded"),
        (
            AsciiPoolPolicy::checked(
                2,
                2,
                one_creation_bytes,
                TEST_WORKER_CAP,
                TEST_CREATION_RESERVATION,
                64,
                8,
                TEST_CALL_BYTES,
            )
            .unwrap(),
            "ASCII owner reservation budget exceeded",
        ),
    ] {
        let owner = AsciiPoolOwner::new(policy).unwrap();
        let execution = owner.begin_execution().unwrap();
        let committed = Barrier::new(2);
        let proceed = Barrier::new(2);
        let (before, refused, after, joined) = thread::scope(|threads| {
            let worker = threads.spawn(|| {
                let reserved = execution.checkout();
                committed.wait();
                proceed.wait();
                let mut lease = reserved.unwrap().ready().unwrap();
                assert_eq!(
                    lease
                        .worker
                        .as_mut()
                        .unwrap()
                        .eval_one(Some(vec![0xff]))
                        .unwrap()
                        .value(),
                    Some(255)
                );
                assert_eq!(lease.worker.as_ref().unwrap().kernel_invocations(), 1);
                lease.return_to_pool();
            });
            committed.wait();
            // Capture first, release the other thread, THEN assert, so an
            // assertion failure cannot strand a thread at the second barrier.
            let before = owner.snapshot();
            let refused = execution.checkout();
            let after = owner.snapshot();
            proceed.wait();
            (before, refused, after, worker.join())
        });
        joined.unwrap();
        let before = before.unwrap();
        assert_eq!(
            before,
            after.unwrap(),
            "refusal must not partially mutate accounting"
        );
        assert_eq!(
            (before.live, before.creating, before.factory_attempts),
            (0, 1, 0)
        );
        assert_eq!(
            before.reserved_bytes,
            before.base_bytes + TEST_CREATION_RESERVATION
        );
        match refused {
            Err(error) => {
                assert_eq!(error.kind, OwnerErrorKind::Resource);
                assert_eq!(error.message, reason);
            }
            Ok(_) => panic!("a concurrently committed creation must consume its full limit"),
        }
        let final_state = owner.snapshot().unwrap();
        assert_eq!(
            (final_state.live, final_state.idle, final_state.creating),
            (0, 1, 0)
        );
        assert_eq!(
            (final_state.factory_attempts, final_state.factory_successes),
            (1, 1)
        );
        assert_eq!(
            final_state.reserved_bytes,
            final_state.base_bytes + TEST_WORKER_CAP
        );
    }
}

#[test]
fn live_real_worker_plus_concurrent_creating_worker_exhaust_total_slot_cap() {
    let owner = AsciiPoolOwner::new(test_policy(2, 2)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let mut live = new_creation(&execution).prepare().unwrap();
    assert_eq!(
        live.worker
            .as_mut()
            .unwrap()
            .eval_one(None)
            .unwrap()
            .value(),
        None
    );
    let committed = Barrier::new(2);
    let proceed = Barrier::new(2);
    let (before, refused, after, joined) = thread::scope(|threads| {
        let worker = threads.spawn(|| {
            let reserved = execution.checkout();
            committed.wait();
            proceed.wait();
            let mut lease = reserved.unwrap().ready().unwrap();
            assert_eq!(
                lease
                    .worker
                    .as_mut()
                    .unwrap()
                    .eval_one(Some(vec![b'A']))
                    .unwrap()
                    .value(),
                Some(65)
            );
            lease.return_to_pool();
        });
        committed.wait();
        let before = owner.snapshot();
        let refused = execution.checkout();
        let after = owner.snapshot();
        proceed.wait();
        (before, refused, after, worker.join())
    });
    joined.unwrap();
    let before = before.unwrap();
    assert_eq!(before, after.unwrap());
    assert_eq!((before.live, before.creating), (1, 1));
    assert_eq!(
        before.reserved_bytes,
        before.base_bytes + TEST_WORKER_CAP + TEST_CREATION_RESERVATION
    );
    assert!(matches!(
        refused,
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Resource,
            message: "ASCII worker-slot limit exceeded"
        })
    ));
    live.return_to_pool();
    let after = owner.snapshot().unwrap();
    assert_eq!((after.live, after.idle, after.creating), (0, 2, 0));
    assert_eq!((after.factory_attempts, after.factory_successes), (2, 2));
    assert_eq!(after.reserved_bytes, after.base_bytes + 2 * TEST_WORKER_CAP);
}

#[test]
fn creation_drop_and_caller_unwind_release_only_after_actual_prepared_worker_drops() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let unbuilt = new_creation(&execution);
    assert_eq!(owner.snapshot().unwrap().factory_attempts, 0);
    drop(unbuilt);
    let empty = owner.snapshot().unwrap();
    assert_eq!(empty.reserved_bytes, empty.base_bytes);
    assert_eq!(empty.retired, 0);

    let mut built = new_creation(&execution);
    built.build_worker().unwrap();
    assert_eq!(built.worker.as_ref().unwrap().kernel_invocations(), 0);
    assert!(built.worker.as_ref().unwrap().is_healthy());
    let creating = owner.snapshot().unwrap();
    assert_eq!((creating.creating, creating.live, creating.idle), (1, 0, 0));
    assert_eq!(
        (creating.factory_attempts, creating.factory_successes),
        (1, 1)
    );
    assert_eq!(
        creating.reserved_bytes,
        creating.base_bytes + TEST_CREATION_RESERVATION
    );
    drop(built);
    let dropped = owner.snapshot().unwrap();
    assert_eq!(dropped.reserved_bytes, dropped.base_bytes);
    assert_eq!((dropped.creating, dropped.retired), (0, 1));

    // A caller unwind while a REAL prepared worker is still Creating. This is
    // not a claim that a panic was injected into C4's factory implementation.
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let mut creating = new_creation(&execution);
        creating.build_worker().unwrap();
        assert_eq!(
            owner.snapshot().unwrap().reserved_bytes,
            empty.base_bytes + TEST_CREATION_RESERVATION
        );
        panic!("caller unwind before creation publication");
    }));
    assert!(panic.is_err());
    let after = owner.snapshot().unwrap();
    assert_eq!(after.reserved_bytes, after.base_bytes);
    assert_eq!(
        (after.creating, after.live, after.idle, after.retired),
        (0, 0, 0, 2)
    );
    assert_eq!((after.factory_attempts, after.factory_successes), (2, 2));
}

#[test]
fn close_before_factory_releases_reservation_without_a_factory_attempt() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let creating = new_creation(&execution);
    execution.close();
    let pending = owner.snapshot().unwrap();
    assert_eq!(pending.creating, 1);
    assert_eq!(
        pending.reserved_bytes,
        pending.base_bytes + TEST_CREATION_RESERVATION
    );
    assert!(matches!(
        creating.prepare(),
        Err(AsciiBoundaryError::Owner(AsciiOwnerError {
            kind: OwnerErrorKind::Closed,
            ..
        }))
    ));
    let after = owner.snapshot().unwrap();
    assert_eq!(
        (
            after.creating,
            after.factory_attempts,
            after.factory_successes
        ),
        (0, 0, 0)
    );
    assert_eq!(after.reserved_bytes, after.base_bytes);
}

#[test]
fn close_or_reset_after_real_factory_prevents_publication_and_preserves_full_f_until_drop() {
    for reset in [false, true] {
        let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
        let old = owner.begin_execution().unwrap();
        let prepared = Barrier::new(2);
        let proceed = Barrier::new(2);
        let (pending, invalidated, newer, joined) = thread::scope(|threads| {
            let worker = threads.spawn(|| {
                let mut reserved = old.checkout();
                let built = match &mut reserved {
                    Ok(Checkout::Create(creating)) => Some(creating.build_worker()),
                    _ => None,
                };
                prepared.wait();
                proceed.wait();
                // Both rendezvous complete before any Result assertion, so a
                // checkout/build refusal cannot strand the parent at a barrier.
                let creating = match reserved.unwrap() {
                    Checkout::Create(creating) => creating,
                    Checkout::Idle(_) => panic!("fresh root must reserve a creation"),
                };
                built
                    .expect("creation must attempt the actual factory")
                    .unwrap();
                assert_eq!(creating.worker.as_ref().unwrap().kernel_invocations(), 0);
                match creating.publish() {
                    Err(AsciiBoundaryError::Owner(error)) => {
                        assert_eq!(error.kind, OwnerErrorKind::Closed)
                    }
                    Err(error) => panic!("expected post-factory epoch refusal: {error:?}"),
                    Ok(_) => panic!("old creation must not publish after close/reset"),
                }
            });
            prepared.wait();
            let pending = owner.snapshot();
            let newer = if reset {
                Some(owner.begin_execution())
            } else {
                old.close();
                None
            };
            let invalidated = owner.snapshot();
            proceed.wait();
            (pending, invalidated, newer, worker.join())
        });
        joined.unwrap();
        let pending = pending.unwrap();
        assert_eq!(pending, invalidated.unwrap());
        assert_eq!(
            (
                pending.creating,
                pending.factory_attempts,
                pending.factory_successes
            ),
            (1, 1, 1)
        );
        assert_eq!(
            pending.reserved_bytes,
            pending.base_bytes + TEST_CREATION_RESERVATION
        );
        let disposed = owner.snapshot().unwrap();
        assert_eq!(
            (
                disposed.live,
                disposed.idle,
                disposed.creating,
                disposed.retired
            ),
            (0, 0, 0, 1)
        );
        assert_eq!(disposed.reserved_bytes, disposed.base_bytes);
        if let Some(newer) = newer {
            let newer = newer.unwrap();
            old.close();
            let scope = newer.scope();
            assert_eq!(
                evaluate_ascii_value(&scope, &Datum::Int(2)).unwrap(),
                Datum::Int(50)
            );
            assert_eq!(owner.snapshot().unwrap().factory_successes, 2);
        }
    }
}

#[test]
fn retirement_barrier_holds_slot_and_byte_debt_across_epoch_rotation_until_actual_drop() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let old = owner.begin_execution().unwrap();
    let mut lease = new_creation(&old).prepare().unwrap();
    assert_eq!(
        lease
            .worker
            .as_mut()
            .unwrap()
            .eval_one(Some(vec![b'R']))
            .unwrap()
            .value(),
        Some(82)
    );
    let retired = Barrier::new(2);
    let dispose = Barrier::new(2);
    let (pending, newer, rotated, refused, joined) = thread::scope(|threads| {
        let worker = threads.spawn(|| {
            let debt = lease.into_retirement();
            retired.wait();
            dispose.wait();
            assert_eq!(debt.worker.as_ref().unwrap().kernel_invocations(), 1);
            assert!(owner.core.state.try_lock().is_ok());
            drop(debt);
        });
        retired.wait();
        let pending = owner.snapshot();
        let newer = owner.begin_execution();
        old.close();
        let rotated = owner.snapshot();
        let refused = match newer.as_ref() {
            Ok(execution) => execution.checkout(),
            Err(error) => Err(error.clone()),
        };
        dispose.wait();
        (pending, newer, rotated, refused, worker.join())
    });
    joined.unwrap();
    let pending = pending.unwrap();
    assert_eq!(pending, rotated.unwrap());
    assert_eq!(
        (
            pending.live,
            pending.idle,
            pending.creating,
            pending.retiring
        ),
        (0, 0, 0, 1)
    );
    assert_eq!(pending.reserved_bytes, pending.base_bytes + TEST_WORKER_CAP);
    assert_eq!(pending.retired, 0);
    assert!(matches!(
        refused,
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Resource,
            ..
        })
    ));
    let newer = newer.unwrap();
    let disposed = owner.snapshot().unwrap();
    assert_eq!((disposed.retiring, disposed.retired), (0, 1));
    assert_eq!(disposed.reserved_bytes, disposed.base_bytes);
    let scope = newer.scope();
    assert_eq!(
        evaluate_ascii_value(&scope, &Datum::Null).unwrap(),
        Datum::Null
    );
    assert_eq!(owner.snapshot().unwrap().factory_successes, 2);
}

#[test]
fn closing_live_epoch_keeps_old_worker_charged_until_disposal_and_cannot_fill_new_epoch() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let old = owner.begin_execution().unwrap();
    let old_scope = old.scope();
    assert_eq!(
        evaluate_ascii_value(&old_scope, &Datum::Int(2)).unwrap(),
        Datum::Int(50)
    );
    old.close();
    let closed = owner.snapshot().unwrap();
    assert_eq!((closed.live, closed.idle, closed.retired), (1, 0, 0));
    assert_eq!(closed.reserved_bytes, closed.base_bytes + TEST_WORKER_CAP);
    let newer = owner.begin_execution().unwrap();
    let newer_scope = newer.scope();
    assert!(matches!(
        evaluate_ascii_value(&newer_scope, &Datum::Null),
        Err(AsciiBoundaryError::Owner(AsciiOwnerError {
            kind: OwnerErrorKind::Resource,
            ..
        }))
    ));
    assert_eq!(owner.snapshot().unwrap().factory_successes, 1);
    drop(old_scope);
    let disposed = owner.snapshot().unwrap();
    assert_eq!((disposed.live, disposed.idle, disposed.retired), (0, 0, 1));
    assert_eq!(disposed.reserved_bytes, disposed.base_bytes);
    assert_eq!(
        evaluate_ascii_value(&newer_scope, &Datum::Null).unwrap(),
        Datum::Null
    );
    assert_eq!(scope_worker_observation(&newer_scope).1, 1);
    assert_eq!(owner.snapshot().unwrap().factory_successes, 2);
    old.close();
    assert_eq!(
        evaluate_ascii_value(&newer_scope, &Datum::Int(2)).unwrap(),
        Datum::Int(50)
    );
    let stale = old.scope();
    assert!(matches!(
        evaluate_ascii_value(&stale, &Datum::Null),
        Err(AsciiBoundaryError::Owner(AsciiOwnerError {
            kind: OwnerErrorKind::Closed,
            ..
        }))
    ));
}

#[test]
fn reset_retires_idle_workers_and_stale_close_or_handle_drop_cannot_close_new_epoch() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let old = owner.begin_execution().unwrap();
    let old_clone = old.clone();
    drop(old.clone());
    {
        let scope = old.scope();
        assert_eq!(
            evaluate_ascii_value(&scope, &Datum::Null).unwrap(),
            Datum::Null
        );
    }
    assert_eq!(owner.snapshot().unwrap().idle, 1);
    let newer = owner.begin_execution().unwrap();
    let reset = owner.snapshot().unwrap();
    assert_eq!((reset.idle, reset.live, reset.retired), (0, 0, 1));
    assert_eq!(reset.reserved_bytes, reset.base_bytes);
    {
        let scope = newer.scope();
        assert_eq!(
            evaluate_ascii_value(&scope, &Datum::Int(2)).unwrap(),
            Datum::Int(50)
        );
    }
    let idle = owner.snapshot().unwrap();
    assert_eq!(idle.idle, 1);
    old.close();
    old_clone.close();
    drop(newer.clone());
    assert_eq!(owner.snapshot().unwrap(), idle);
    {
        let scope = newer.scope();
        assert_eq!(
            evaluate_ascii_value(&scope, &Datum::Null).unwrap(),
            Datum::Null
        );
        assert_eq!(scope_worker_observation(&scope).1, 2);
    }
    assert_eq!(owner.snapshot().unwrap().factory_successes, 2);
    newer.close();
    newer.close();
    let closed = owner.snapshot().unwrap();
    assert_eq!(
        (closed.idle, closed.live, closed.retiring, closed.retired),
        (0, 0, 0, 2)
    );
    assert_eq!(closed.reserved_bytes, closed.base_bytes);
}

#[test]
fn post_kernel_epoch_check_is_the_success_publication_boundary() {
    for close_before_finish in [true, false] {
        let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        let mut invocation = Invocation::enter(&scope).unwrap();
        let result = invocation.run(ReadyAsciiBytes(Some(vec![b'A'])));
        assert_eq!(result.as_ref().unwrap().value(), Some(65));
        assert_eq!(
            invocation
                .lease
                .as_ref()
                .unwrap()
                .worker
                .as_ref()
                .unwrap()
                .kernel_invocations(),
            1
        );
        if close_before_finish {
            execution.close();
        }
        let publication = invocation.finish(result);
        if close_before_finish {
            assert!(matches!(
                publication,
                Err(AsciiBoundaryError::Owner(AsciiOwnerError {
                    kind: OwnerErrorKind::Closed,
                    ..
                }))
            ));
            assert!(scope.poisoned.get());
            assert!(scope.lease.borrow().is_none());
        } else {
            let published = publication.unwrap();
            execution.close();
            assert_eq!(
                published.value(),
                Some(65),
                "close cannot retract an already published scalar"
            );
            assert_eq!(owner.snapshot().unwrap().live, 1);
        }
        drop(scope);
        let after = owner.snapshot().unwrap();
        assert_eq!(
            (after.live, after.idle, after.retiring, after.retired),
            (0, 0, 0, 1)
        );
        assert_eq!(after.reserved_bytes, after.base_bytes);
    }
}

#[test]
fn original_owned_kernel_error_survives_a_simultaneous_close_cleanup_error() {
    let policy = AsciiPoolPolicy::checked(
        1,
        1,
        TEST_POOL_BYTES,
        TEST_WORKER_CAP,
        TEST_CREATION_RESERVATION,
        0,
        8,
        TEST_CALL_BYTES,
    )
    .unwrap();
    let owner = AsciiPoolOwner::new(policy).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    let mut invocation = Invocation::enter(&scope).unwrap();
    let primary = invocation.run(coerce_ready(&Datum::Null).unwrap());
    let (original_message_allocation, original_failure) = match &primary {
        Err(AsciiBoundaryError::Kernel(failure)) => {
            let LocalError::ResourceLimit(message) = failure.local_error() else {
                panic!("expected the original C4 ResourceLimit cause");
            };
            assert_eq!(failure.phase(), Some(ExpressionRuntimeFailurePhase::Invoke));
            (message.as_ptr(), failure.clone())
        }
        other => panic!("expected a real C4 work-budget error, got {other:?}"),
    };
    assert_eq!(
        invocation
            .lease
            .as_ref()
            .unwrap()
            .worker
            .as_ref()
            .unwrap()
            .kernel_invocations(),
        0
    );
    execution.close();
    match invocation.finish(primary) {
        Err(AsciiBoundaryError::Kernel(failure)) => {
            let LocalError::ResourceLimit(message) = failure.local_error() else {
                panic!("cleanup must preserve the original C4 ResourceLimit cause");
            };
            // Even the original owned error payload survives; no string mapping
            // to a new error and no replacement with the cleanup Closed error.
            assert_eq!(message.as_ptr(), original_message_allocation);
            assert_eq!(failure, original_failure, "same opaque Arc, no recapture");
            assert_eq!(failure.phase(), Some(ExpressionRuntimeFailurePhase::Invoke));
            let native = AsciiBoundaryError::Kernel(failure).into_eval_error();
            assert_eq!(
                native,
                EvalError::ExpressionRuntimeFailure(original_failure),
                "the native mapper must also move the same captured failure"
            );
        }
        other => panic!("cleanup must preserve the primary engine error: {other:?}"),
    }
    assert!(scope.poisoned.get());
    assert!(scope.lease.borrow().is_none());
    let after = owner.snapshot().unwrap();
    assert_eq!(
        (
            after.factory_attempts,
            after.factory_successes,
            after.retired
        ),
        (1, 1, 1)
    );
    assert_eq!(after.reserved_bytes, after.base_bytes);
}

#[test]
fn caught_native_panics_before_next_kernel_after_kernel_and_during_return_poison_surviving_scope() {
    for phase in 0_usize..3 {
        let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
        let execution = owner.begin_execution().unwrap();
        let scope = execution.scope();
        assert_eq!(
            evaluate_ascii_value(&scope, &Datum::Int(2)).unwrap(),
            Datum::Int(50)
        );
        assert_eq!(scope_worker_observation(&scope).1, 1);
        let native = ForwardingSentinel::new(false, None);
        // The catcher is OUTSIDE the guard while the scope survives outside
        // both. Thread exit or late Scope::drop alone cannot pass this test.
        let panic = catch_unwind(AssertUnwindSafe(|| {
            scope.with_columns(&native, |columns| -> Datum {
                columns.append_warning(45000, "native child effect before possible panic");
                if phase == 0 {
                    std::panic::panic_any(phase);
                }
                let value = evaluate_ascii_value(&scope, &Datum::Bytes(vec![b'Q'])).unwrap();
                assert_eq!(value, Datum::Int(81));
                assert_eq!(scope_worker_observation(&scope).1, 2);
                if phase == 1 {
                    std::panic::panic_any(phase);
                }
                // Native return-processing stand-in, not an assertion about
                // integrated ScalarFunction::coerce_to_ret_type dispatch.
                let native_return = || -> Datum {
                    assert!(!scope.busy.get());
                    columns
                        .handle_truncate("native return-processing stand-in")
                        .unwrap();
                    std::panic::panic_any(phase);
                };
                native_return()
            })
        }))
        .err()
        .expect("the original native panic must escape the lexical guard");
        assert_eq!(panic.downcast_ref::<usize>(), Some(&phase));
        assert!(scope.poisoned.get());
        assert!(!scope.busy.get());
        assert!(scope.lease.borrow().is_none());
        assert!(
            native.effects.borrow().warnings.len() >= 2,
            "native diagnostics are not reset"
        );
        let disposed = owner.snapshot().unwrap();
        assert_eq!(
            (
                disposed.live,
                disposed.idle,
                disposed.retiring,
                disposed.retired
            ),
            (0, 0, 0, 1)
        );
        assert_eq!(disposed.reserved_bytes, disposed.base_bytes);
        assert!(matches!(
            evaluate_ascii_value(&scope, &Datum::Null),
            Err(AsciiBoundaryError::Scope {
                kind: ScopeFailureKind::Poisoned,
                reason: "ASCII scope is poisoned",
            })
        ));
        assert_eq!(
            owner.snapshot().unwrap(),
            disposed,
            "sticky poison cannot silently prepare a replacement"
        );
    }
}

#[test]
fn caught_invocation_guard_unwind_retires_a_real_used_worker_before_recovery() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    assert_eq!(
        evaluate_ascii_value(&scope, &Datum::Int(2)).unwrap(),
        Datum::Int(50)
    );
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let mut invocation = Invocation::enter(&scope).unwrap();
        assert_eq!(
            invocation
                .run(coerce_ready(&Datum::Null).unwrap())
                .unwrap()
                .value(),
            None
        );
        assert_eq!(
            invocation
                .lease
                .as_ref()
                .unwrap()
                .worker
                .as_ref()
                .unwrap()
                .kernel_invocations(),
            2
        );
        // The TiDB guard is interrupted after an actual C4 call, before finish.
        // This does not pretend to inject an unwind inside the sealed KV driver.
        panic!("interrupted TiDB invocation finish");
    }));
    assert!(panic.is_err());
    assert!(scope.poisoned.get());
    assert!(!scope.busy.get());
    assert!(scope.lease.borrow().is_none());
    let after = owner.snapshot().unwrap();
    assert_eq!((after.live, after.idle, after.retired), (0, 0, 1));
    assert_eq!(after.reserved_bytes, after.base_bytes);
}

#[test]
fn structural_validate_rejects_epoch_and_uncertainty_from_different_instants() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let mut lease = new_creation(&execution).prepare().unwrap();
    assert_eq!(
        lease
            .worker
            .as_mut()
            .unwrap()
            .eval_one(None)
            .unwrap()
            .value(),
        None
    );
    assert!(lease.validate().is_ok());
    assert_eq!(lease.worker.as_ref().unwrap().kernel_invocations(), 1);

    // STRUCTURAL concurrent-ledger regression, not a natural dirty-C4-context
    // observation. The worker and validate call are real; only retirement-debt
    // state is synthesized. Before the target validate starts, (epoch E, debt 1)
    // is already installed under the bookkeeping lock.
    {
        let _state = owner.core.state.lock().unwrap();
        assert_eq!(owner.core.epoch.load(Ordering::SeqCst), execution.epoch);
        owner.core.uncertain.store(1, Ordering::SeqCst);
    }
    let (epoch_read_tx, epoch_read_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (joined, transition) = thread::scope(|threads| {
        let validator = threads.spawn(move || {
            // Install only after the real lease was prepared and used. Pause
            // the ACTUAL check_epoch path after its epoch read, before debt.
            set_after_epoch_read_hook(move || {
                epoch_read_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
            });
            let accepted = lease.validate().is_ok();
            (lease, accepted)
        });
        epoch_read_rx
            .recv()
            .expect("validate must reach the epoch-read rendezvous");
        let transition = match owner.core.state.try_lock() {
            Ok(_state) => {
                // Model close, then acknowledgement of outstanding retirement:
                // (E,1) -> (0,1) -> (0,0). There is NO (E,0) instant in this
                // validation interval, even though two independent loads can
                // incorrectly assemble it. Do not call a locking close here.
                owner.core.epoch.store(0, Ordering::SeqCst);
                owner.core.uncertain.store(0, Ordering::SeqCst);
                Ok(true)
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                // Also safe if a later fix locks validate: let it observe the
                // original debt=1 and reject before normal cleanup. An atomic
                // double-epoch-read fix still follows the mutation arm above.
                Ok(false)
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err("unexpected poisoned accounting mutex at rendezvous")
            }
        };
        // Always release the paused validator before inspecting a transition
        // error. A regression must fail an assertion, not strand a test thread.
        resume_tx.send(()).unwrap();
        (validator.join(), transition)
    });
    let (lease, accepted) = joined.unwrap();

    // Clear only the synthesized debt, after validation has returned and any
    // future validation lock is gone. Use the normal close/lease-drop cleanup
    // before the regression assertion, including on the intentionally RED path.
    {
        let _state = owner.core.state.lock().unwrap();
        owner.core.uncertain.store(0, Ordering::SeqCst);
    }
    execution.close();
    drop(lease);
    let after = owner.snapshot().unwrap();
    assert_eq!(
        (after.live, after.idle, after.retiring, after.uncertain),
        (0, 0, 0, 0)
    );
    assert_eq!(after.reserved_bytes, after.base_bytes);
    assert_eq!(after.retired, 1);
    let mutated_while_paused = transition.expect("rendezvous must not poison the owner");
    assert!(
        !accepted,
        "validate accepted an epoch/debt pair with no common valid instant; \
         mutated_while_paused={mutated_while_paused}"
    );
}

#[test]
fn structurally_marked_uncertain_retirement_freezes_new_epochs_until_actual_worker_disposal() {
    let owner = AsciiPoolOwner::new(test_policy(2, 2)).unwrap();
    let old = owner.begin_execution().unwrap();
    let mut lease = new_creation(&old).prepare().unwrap();
    assert_eq!(
        lease
            .worker
            .as_mut()
            .unwrap()
            .eval_one(None)
            .unwrap()
            .value(),
        None
    );
    assert!(lease.worker.as_ref().unwrap().is_healthy());
    // STRUCTURAL caller-ledger test: classify a REAL healthy worker's
    // retirement as uncertain. No fake C4 observer/dirty-warning behavior is
    // inferred; those sealed-context failure cases belong to C4's own tests.
    let worker = lease.worker.take();
    let token = lease.token;
    assert!(owner.core.start_retirement(token, true));
    let debt = Retirement {
        core: Arc::clone(&owner.core),
        token,
        worker,
        recorded: true,
    };
    drop(lease);
    let frozen = owner.snapshot().unwrap();
    assert_eq!((frozen.retiring, frozen.uncertain), (1, 1));
    assert_eq!(frozen.reserved_bytes, frozen.base_bytes + TEST_WORKER_CAP);
    let newer = owner.begin_execution().unwrap();
    old.close();
    assert!(matches!(
        newer.checkout(),
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Resource,
            message: "ASCII uncertain retirement debt remains"
        })
    ));
    assert_eq!(owner.snapshot().unwrap(), frozen);
    drop(debt);
    let disposed = owner.snapshot().unwrap();
    assert_eq!(
        (disposed.retiring, disposed.uncertain, disposed.retired),
        (0, 0, 1)
    );
    assert_eq!(disposed.reserved_bytes, disposed.base_bytes);
    let scope = newer.scope();
    assert_eq!(
        evaluate_ascii_value(&scope, &Datum::Null).unwrap(),
        Datum::Null
    );
}

#[test]
fn checked_policy_and_reservation_overflow_refuse_before_allocating_or_mutating_slots() {
    assert!(matches!(
        AsciiPoolPolicy::checked(
            1,
            2,
            TEST_POOL_BYTES,
            TEST_WORKER_CAP,
            TEST_CREATION_RESERVATION,
            64,
            8,
            TEST_CALL_BYTES
        ),
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Policy,
            ..
        })
    ));
    assert!(matches!(
        AsciiPoolPolicy::checked(1, 1, TEST_POOL_BYTES, 2, 1, 64, 8, TEST_CALL_BYTES),
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Policy,
            ..
        })
    ));
    assert!(base_charge(usize::MAX).is_err());
    assert!(
        AsciiPoolPolicy::checked(usize::MAX, 0, usize::MAX, 0, 0, 64, 8, TEST_CALL_BYTES).is_err()
    );
    assert!(AsciiPoolPolicy::checked(0, 0, 0, 0, 0, 64, 8, TEST_CALL_BYTES).is_err());
    let policy = AsciiPoolPolicy::checked(
        1,
        1,
        usize::MAX,
        TEST_WORKER_CAP,
        usize::MAX,
        64,
        8,
        TEST_CALL_BYTES,
    )
    .unwrap();
    let owner = AsciiPoolOwner::new(policy).unwrap();
    let execution = owner.begin_execution().unwrap();
    let before = owner.snapshot().unwrap();
    assert!(matches!(
        execution.checkout(),
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Resource,
            ..
        })
    ));
    assert_eq!(owner.snapshot().unwrap(), before);
}

#[test]
fn structural_mutex_poison_refuses_cached_ready_call_without_snapshot_or_kernel_entry() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    assert_eq!(
        evaluate_ascii_value(&scope, &Datum::Null).unwrap(),
        Datum::Null
    );
    let before = scope
        .lease
        .borrow()
        .as_ref()
        .unwrap()
        .worker
        .as_ref()
        .unwrap()
        .kernel_invocations();
    assert_eq!(
        before, 1,
        "warm the actual nullable C4 wrapper and cache its lease"
    );

    // STRUCTURAL bookkeeping panic, not a naturally failing/dirty C4 worker.
    // Do not surround this with with_columns: its guard would correctly poison
    // the scope already, masking the separate unobserved std::Mutex poison bug.
    arm_eval_one_observation();
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _state = owner.core.state.lock().unwrap();
        panic!("structural accounting-mutex panic with a cached healthy C4 lease");
    }));
    // Deliberately NO snapshot/checkout/close/drop/PoolCore::lock between the
    // caught mutex panic and this hot cached call. A prior lock would copy the
    // std poison marker into core.poisoned and hide the defect.
    let result = evaluate_ascii_value(&scope, &Datum::Null);
    let observation = take_eval_one_observation();

    // Capture observation before any disposal. A correct refusal may already
    // have destroyed the worker, so do not invent a post-drop getter value.
    // Normal cleanup happens before the deliberately RED assertion as well.
    drop(scope);
    assert!(panic.is_err());
    assert!(
        matches!(
            &result,
            Err(AsciiBoundaryError::Owner(AsciiOwnerError {
                kind: OwnerErrorKind::Poisoned,
                ..
            }))
        ) && observation.facade_entries == 0,
        "cached call must reject std::Mutex poison before eval_one; \
         result={result:?}, observation={observation:?}, warm_actual_counter={before}"
    );
    assert!(observation.before_kernel_invocations.is_none());
    assert!(observation.after_kernel_invocations.is_none());
}

#[test]
fn structural_epoch_serial_overflow_and_mutex_poison_fail_closed_without_reuse() {
    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    owner.core.state.lock().unwrap().next_serial = u64::MAX;
    let before = owner.snapshot().unwrap();
    assert!(matches!(
        execution.checkout(),
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Resource,
            ..
        })
    ));
    assert_eq!(owner.snapshot().unwrap(), before);
    owner.core.state.lock().unwrap().next_epoch = u64::MAX;
    assert!(matches!(
        owner.begin_execution(),
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Contract,
            ..
        })
    ));
    assert!(matches!(
        execution.checkout(),
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Poisoned,
            ..
        })
    ));

    let owner = AsciiPoolOwner::new(test_policy(1, 1)).unwrap();
    let execution = owner.begin_execution().unwrap();
    let scope = execution.scope();
    assert_eq!(
        evaluate_ascii_value(&scope, &Datum::Null).unwrap(),
        Datum::Null
    );
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _locked = owner.core.state.lock().unwrap();
        panic!("structural mutex poison after actual worker use");
    }));
    assert!(panic.is_err());
    assert!(matches!(
        owner.snapshot(),
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Poisoned,
            ..
        })
    ));
    drop(scope);
    assert!(matches!(
        owner.begin_execution(),
        Err(AsciiOwnerError {
            kind: OwnerErrorKind::Poisoned,
            ..
        })
    ));
    // Disposal-only recovery may inspect/drain poisoned bookkeeping; it must
    // never clear the poison and admit another worker.
    let state = match owner.core.state.lock() {
        Err(error) => error.into_inner(),
        Ok(_) => panic!("the poisoned root must not silently recover"),
    };
    assert!(state.slots.iter().all(|slot| matches!(slot, Slot::Empty)));
    assert_eq!(state.reserved_bytes, state.base_bytes);
    assert_eq!(state.retired, 1);
}

#[test]
#[ignore = "parent external allocation observation only; not a measurement"]
fn parent_external_observer_actual_pool_owner_arc_new_fixture() {
    // Safe APIs exercise the inherited Rust allocator, not an observer's own
    // C allocation calls. The external observer validates the actual routes;
    // these Rust capacities/layouts alone are not allocation evidence.
    fn positive_controls() {
        let mut bytes = Vec::<u8>::with_capacity(std::hint::black_box(73));
        bytes.resize(73, 7);
        std::hint::black_box(bytes.as_ptr());
        bytes.reserve_exact(149 - bytes.len());
        std::hint::black_box(bytes.as_ptr());
        drop(bytes);
        let zeros = vec![0u8; std::hint::black_box(91)];
        std::hint::black_box(zeros.as_slice());
        drop(zeros);
        #[repr(align(64))]
        struct Aligned([u8; 257]);
        // Padding makes the requested layout 320 bytes, not 257.
        let aligned = Box::new(std::hint::black_box(Aligned([7; 257])));
        std::hint::black_box(&aligned.0);
        drop(aligned);
    }

    // The parent supplies an independent process-level allocator observer. No
    // allocator replacement, new_in path, portable Arc ABI, or measured-byte
    // assertion is introduced here. All diagnostic initialization, policy
    // construction and candidate-layout printing precede the marked phases.
    // Zero slots means no Vec allocation and no C4 worker; the snapshot's
    // caller_arc_measurement_required remains true regardless of this fixture.
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER DIAGNOSTIC_WARMUP NOT A MEASUREMENT");
    let policy = test_policy(0, 0);
    eprintln!(
        "ASCII_POOL_EXTERNAL_OBSERVER candidate PoolArcAllocation size={} align={}; actual PoolCore size={} align={}; NOT A MEASUREMENT",
        std::mem::size_of::<PoolArcAllocation>(),
        std::mem::align_of::<PoolArcAllocation>(),
        std::mem::size_of::<PoolCore>(),
        std::mem::align_of::<PoolCore>(),
    );
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER CONTROL_PRE_BEGIN");
    positive_controls();
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER CONTROL_PRE_END");
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER EMPTY_PRE_BEGIN");
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER EMPTY_PRE_END");
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER OWNER_NEW_BEGIN");
    let owner = AsciiPoolOwner::new(policy).unwrap();
    std::hint::black_box(&owner);
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER OWNER_NEW_END");
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER STACK_CLONES_BEGIN");
    let clones: [AsciiPoolOwner; 64] = std::array::from_fn(|_| std::hint::black_box(owner.clone()));
    std::hint::black_box(&clones);
    drop(clones);
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER STACK_CLONES_END");
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER OWNER_DROP_BEGIN");
    std::hint::black_box(&owner);
    drop(owner);
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER OWNER_DROP_END");
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER EMPTY_POST_BEGIN");
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER EMPTY_POST_END");
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER CONTROL_POST_BEGIN");
    positive_controls();
    eprintln!("ASCII_POOL_EXTERNAL_OBSERVER CONTROL_POST_END");
}
