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
            | ComputedValue::Ieee754Bits(_)
            | ComputedValue::Decimal(_)
            | ComputedValue::Int128(_) => {
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

fn observe_wide_math(
    evaluate: impl FnOnce() -> Result<Datum, EvalError>,
) -> (Result<Datum, EvalError>, EvalOneObservation) {
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
            ("1 NOT IN (1, 2)", Datum::Int(0), 1, Some(1)),
            // Instrumentation changes from one facade to two: AND then NOT.
            // Their different workers' getter snapshots are not a total delta.
            ("1 NOT BETWEEN 0 AND 2", Datum::Int(0), 2, None),
            ("NULL NOT LIKE '%'", Datum::Null, 1, Some(1)),
            ("'a' NOT REGEXP 'b'", Datum::Int(1), 1, Some(1)),
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
                // Independent snapshots from the AND and NOT workers.
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
    for function in [&isnull, &not] {
        let mut untouched = vec![42];
        arm_eval_one_observation();
        assert_eq!(
            function.vec_eval_bool(&chunk, &[0, 1], &mut untouched),
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
