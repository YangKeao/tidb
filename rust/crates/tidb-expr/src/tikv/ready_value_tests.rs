// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Executor-lane cache tests. The removed statement pool had separate policy,
//! slot, lease and lifecycle tests; those contracts no longer exist.

use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tidb_datatype::Datum;
use tidb_query_expr::local::{
    prepare_evaluated_bytes, EvaluatedArgs, EvaluatedBytesOp, ExecutionLimits, LocalCompileContext,
};

use super::*;

use crate::constant::Constant;
use crate::context::{BlockEncryptionMode, ErrorLevel, EvalError};
use crate::Columns;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tidb_datatype::{
    ConversionFlags, CoreTime, DateModes, FieldType, FieldTypeCode, SessionTimeZone, Time, TimeType,
};

std::thread_local! {
    static EVAL_ONE_OBSERVATION: RefCell<Option<(usize, Option<u64>, Option<u64>)>> =
        const { RefCell::new(None) };
}

pub(super) fn before_eval_one_for_test(actual_kernel_invocations: u64) {
    EVAL_ONE_OBSERVATION.with(|slot| {
        if let Some((entries, before, _)) = slot.borrow_mut().as_mut() {
            *entries += 1;
            before.get_or_insert(actual_kernel_invocations);
        }
    });
}

pub(super) fn after_eval_one_for_test(actual_kernel_invocations: u64) {
    EVAL_ONE_OBSERVATION.with(|slot| {
        if let Some((_, _, after)) = slot.borrow_mut().as_mut() {
            *after = Some(actual_kernel_invocations);
        }
    });
}

#[test]
fn lane_cache_is_lazy_and_reuses_one_worker_per_operation() {
    let cache = ReadyValueCache::new();
    assert_eq!(cache.prepared_worker_count(), 0);
    cache.with_columns(&crate::NoColumns, |ctx| {
        assert_eq!(
            evaluate_ascii_in(&Datum::Bytes(b"A".to_vec()), ctx),
            Ok(Datum::Int(65))
        );
        assert_eq!(cache.prepared_worker_count(), 1);
        assert_eq!(
            evaluate_ascii_in(&Datum::Bytes(b"B".to_vec()), ctx),
            Ok(Datum::Int(66))
        );
        assert_eq!(cache.prepared_worker_count(), 1);
        assert_eq!(
            evaluate_args_in(
                EvaluatedBytesOp::Length,
                ctx,
                || Ok(EvaluatedArgs::Bytes(Some(b"hello".to_vec()))),
                EvaluatedBytesResult::into_int_datum,
            ),
            Ok(Datum::Int(5))
        );
        assert_eq!(cache.prepared_worker_count(), 2);
    });
}

#[test]
fn audited_sql_operations_prepare_in_optimized_builds() {
    for operation in [
        EvaluatedBytesOp::IsNull,
        EvaluatedBytesOp::Length,
        EvaluatedBytesOp::CharLength,
        EvaluatedBytesOp::CharLengthUtf8,
        EvaluatedBytesOp::BitLength,
        EvaluatedBytesOp::Replace,
        EvaluatedBytesOp::HexInt,
        EvaluatedBytesOp::HexStr,
        EvaluatedBytesOp::UnHex,
        EvaluatedBytesOp::BitAnd,
        EvaluatedBytesOp::BitOr,
        EvaluatedBytesOp::BitXor,
        EvaluatedBytesOp::LeftShift,
        EvaluatedBytesOp::RightShift,
        EvaluatedBytesOp::InetAton,
        EvaluatedBytesOp::Inet6Aton,
        EvaluatedBytesOp::Md5,
        EvaluatedBytesOp::Sha1,
    ] {
        prepare_evaluated_bytes(
            operation,
            LocalCompileContext::default(),
            ExecutionLimits::default(),
            usize::MAX,
        )
        .unwrap_or_else(|error| panic!("{operation:?} did not prepare: {error:?}"));
    }
}

#[test]
fn unbound_context_uses_one_shot_cache_without_persisting_capability() {
    assert_eq!(
        evaluate_ascii_in(&Datum::Bytes(b"A".to_vec()), &crate::NoColumns),
        Ok(Datum::Int(65))
    );
    assert!(crate::NoColumns.ready_value_cache().is_none());
}

#[test]
fn nested_binding_keeps_the_existing_lane_cache() {
    let outer = ReadyValueCache::new();
    let unused = ReadyValueCache::new();
    outer.with_columns(&crate::NoColumns, |outer_columns| {
        unused.with_columns(outer_columns, |nested| {
            assert!(std::ptr::eq(
                nested.ready_value_cache().expect("bound cache"),
                &outer
            ));
            assert_eq!(
                evaluate_ascii_in(&Datum::Bytes(b"C".to_vec()), nested),
                Ok(Datum::Int(67))
            );
        });
    });
    assert_eq!(outer.prepared_worker_count(), 1);
    assert_eq!(unused.prepared_worker_count(), 0);
}

#[test]
fn unwind_poison_is_lane_local_and_never_replays_natively() {
    let poisoned = ReadyValueCache::new();
    let healthy = ReadyValueCache::new();
    let unwind = catch_unwind(AssertUnwindSafe(|| {
        poisoned.with_columns(&crate::NoColumns, |_| panic!("native callback panic"));
    }));
    assert!(unwind.is_err());
    poisoned.with_columns(&crate::NoColumns, |ctx| {
        assert!(matches!(
            evaluate_ascii_in(&Datum::Bytes(b"A".to_vec()), ctx),
            Err(crate::EvalError::ExpressionAdapterFailure(_))
        ));
    });
    healthy.with_columns(&crate::NoColumns, |ctx| {
        assert_eq!(
            evaluate_ascii_in(&Datum::Bytes(b"A".to_vec()), ctx),
            Ok(Datum::Int(65))
        );
    });
}

#[test]
fn actual_kernel_invocation_is_observed_once_per_call() {
    EVAL_ONE_OBSERVATION.with(|slot| *slot.borrow_mut() = Some((0, None, None)));
    let cache = ReadyValueCache::new();
    cache.with_columns(&crate::NoColumns, |ctx| {
        assert_eq!(
            evaluate_ascii_in(&Datum::Bytes(b"A".to_vec()), ctx),
            Ok(Datum::Int(65))
        );
    });
    let observation = EVAL_ONE_OBSERVATION.with(|slot| slot.borrow_mut().take().unwrap());
    assert_eq!(observation.0, 1);
    assert_eq!(observation.2, observation.1.map(|before| before + 1));
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
const COLUMNS_CAPABILITY_METHODS: [&str; 1] = ["ready_value_cache"];

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
    assert_eq!(capabilities.len(), 1);
    assert!(forwarded.is_disjoint(&capabilities));
    let expected: BTreeSet<_> = forwarded.union(&capabilities).copied().collect();
    assert_eq!(
        expected.len(),
        64,
        "63 forwarded plus one lane-cache capability"
    );
    for (source, marker) in [
        (include_str!("../context.rs"), "pub trait Columns"),
        (
            include_str!("ready_value.rs"),
            "Columns for ScopedReadyValueColumns",
        ),
    ] {
        let methods = declared_methods(source, marker);
        assert_eq!(methods.len(), 64, "method declaration count: {marker}");
        assert_eq!(
            methods.into_iter().collect::<BTreeSet<_>>(),
            expected,
            "review forwarding and behavioral coverage when Columns changes: {marker}"
        );
    }
    // The ordinary-method sentinel deliberately inherits default-None
    // capabilities. Separate runtime tests below observe both overrides.
    let sentinel = declared_methods(
        include_str!("ready_value_tests.rs"),
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
    let outer = ReadyValueCache::new();
    let inner = ReadyValueCache::new();
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
    assert!(!outer.busy.get());
    assert!(!inner.busy.get());
}
