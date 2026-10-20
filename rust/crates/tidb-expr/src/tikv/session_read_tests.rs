// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Original session-read observations before shared-value extraction.
use crate::{scalar_function::ScalarFunction, Columns, Datum, EvalError};
use std::cell::{Cell, RefCell};
use tidb_ast::CiString;
use tidb_chunk::row::Row;
pub(crate) const NAMES: [&str; 14] = [
    "database",
    "schema",
    "version",
    "tidb_version",
    "current_user",
    "current_role",
    "current_resource_group",
    "user",
    "session_user",
    "system_user",
    "connection_id",
    "found_rows",
    "row_count",
    "last_insert_id",
];
pub(crate) struct Context {
    pub(crate) profile: usize,
    pub(crate) value: Datum,
    pub(crate) enabled: Cell<bool>,
    pub(crate) events: RefCell<Vec<String>>,
    reads: Cell<usize>,
}
impl Context {
    pub(crate) fn new(profile: usize, value: Datum) -> Self {
        Self {
            profile,
            value,
            enabled: Cell::new(false),
            events: RefCell::new(vec![]),
            reads: Cell::new(0),
        }
    }
    fn read(&self, name: &str) {
        let n = self.reads.get();
        self.reads.set(n + 1);
        self.events.borrow_mut().push(format!("read:{name}:{n}"));
    }
    fn text(&self, name: &str) -> Option<String> {
        self.read(name);
        match self.profile {
            0 => None,
            1 => Some(String::new()),
            2 => Some(format!("{name}:héllo\0用户")),
            3 => Some("2024-03-04 05:06:07.123456".into()),
            _ => Some(format!("{name}:12tail")),
        }
    }
    fn number(&self, name: &str) -> Option<u64> {
        self.read(name);
        match self.profile {
            0 => None,
            1 => Some(0),
            2 => Some(u64::MAX),
            3 => Some((1 << 63) + 17),
            _ => Some(u64::MAX - 1),
        }
    }
}
impl Columns for Context {
    fn get(&self, _: &[String]) -> Option<Datum> {
        panic!("no invented session-read column")
    }
    fn enable_vectorized_expression(&self) -> bool {
        self.enabled.get()
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((1700000000, 123456000, 0))
    }
    fn current_database(&self) -> Option<String> {
        self.text("database")
    }
    fn current_user(&self) -> Option<String> {
        self.text("grant_user")
    }
    fn login_user(&self) -> Option<String> {
        self.text("login_user")
    }
    fn current_role(&self) -> Option<String> {
        self.text("roles")
    }
    fn current_resource_group(&self) -> Option<String> {
        self.text("resource_group")
    }
    fn connection_id(&self) -> Option<u64> {
        self.number("connection_id")
    }
    fn found_rows(&self) -> Option<u64> {
        self.number("found_rows")
    }
    fn row_count(&self) -> Option<i64> {
        self.number("row_count").map(|v| v as i64)
    }
    fn last_insert_id(&self) -> Option<u64> {
        self.number("last_insert_id")
    }
    fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        assert!(scope.is_none());
        assert_eq!(name, "version");
        self.read("version");
        (self.profile != 0).then(|| self.value.clone())
    }
    fn tidb_info(&self) -> String {
        self.text("process_info")
            .unwrap_or_else(|| "no session process".into())
    }
    fn truncate_level(&self) -> crate::context::ErrorLevel {
        match self.profile % 3 {
            0 => crate::context::ErrorLevel::Ignore,
            1 => crate::context::ErrorLevel::Warn,
            _ => crate::context::ErrorLevel::Error,
        }
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warning:{code}:{message}"));
    }
}
pub(crate) fn values() -> Vec<Datum> {
    use tidb_datatype::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, MySqlDuration, MysqlEnum,
        MysqlSet, Time, TimeType, VectorFloat32,
    };
    vec![
        Datum::Null,
        Datum::MinNotNull,
        Datum::MaxValue,
        Datum::Int(i64::MIN),
        Datum::UInt(u64::MAX),
        Datum::Float32(f64::NAN),
        Datum::Real(f64::INFINITY),
        Datum::Decimal(Decimal::from_literal("12.50")),
        Datum::new_string("2024-03-04 05:06:07.123456"),
        Datum::Bytes(vec![255, 0, 195]),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![255, 1])),
        Datum::Bit(BinaryLiteral::from(vec![255, 1])),
        Datum::Enum(MysqlEnum::new("é", 7), Collation::Utf8Mb4Bin),
        Datum::Set(MysqlSet::new("a,b", 3), Collation::Binary),
        Datum::Time(Time::from_raw_parts(
            CoreTime::from_raw(0),
            TimeType::DateTime,
            6,
        )),
        Datum::Duration(MySqlDuration::from_raw_parts(123456789, 6)),
        Datum::Json(BinaryJSON::parse("{\"a\":[1,true]}").unwrap()),
        Datum::VectorFloat32(VectorFloat32::init(2)),
        Datum::Raw(vec![255, 0, 195]),
    ]
}
pub(crate) fn observe(body: impl FnOnce() -> Result<Datum, EvalError>, ctx: &Context) -> Vec<u8> {
    let mut out = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        Ok(Ok(value)) => match crate::tikv::identity_value::encode(&value) {
            Ok(Some(frame)) => {
                let mut out = vec![0];
                out.extend_from_slice(&(frame.len() as u64).to_le_bytes());
                out.extend(frame);
                out
            }
            other => format!("ENCODE:{other:?}").into_bytes(),
        },
        Ok(Err(e)) => format!("ERROR:{e:?}").into_bytes(),
        Err(e) => format!(
            "PANIC:{:?}",
            e.downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| e.downcast_ref::<&str>().copied())
        )
        .into_bytes(),
    };
    out.extend_from_slice(format!("|{:?}", ctx.events.borrow()).as_bytes());
    out
}
#[test]
fn session_read_original_observations() {
    const KEY: &str = "TIDB_SESSION_READ_ORIGINAL";
    if std::env::var(KEY).as_deref() != Ok("1") {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tikv::session_read_tests::session_read_original_observations",
                "--nocapture",
            ])
            .env(KEY, "1")
            .env("TZ", "UTC")
            .output()
            .unwrap();
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        eprintln!("{}", String::from_utf8_lossy(&output.stdout));
        assert!(output.status.success());
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for name in NAMES {
            for profile in 0..5 {
                for ret in crate::tikv::eager_scalar_value_tests::types() {
                    let values = if name == "version" {
                        values()
                    } else {
                        vec![Datum::Null]
                    };
                    for (index, value) in values.into_iter().enumerate() {
                        let f = ScalarFunction::new(CiString::new(name), ret.clone(), vec![]);
                        let ctx = Context::new(profile, value);
                        let mut record = format!("{name}|{profile}|{ret:?}|{index}|").into_bytes();
                        record.extend(observe(|| f.eval(&ctx, Row::empty()), &ctx));
                        for b in record {
                            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                        }
                        count += 1;
                    }
                }
            }
        }
        assert_eq!((count, hash), (1760, 0x33F3607B995E80CC));
        eprintln!("SESSION READ ORIGINAL {count} {hash:016X}");
    });
}

#[test]
fn session_read_shared_value_preserves_original_observations() {
    const KEY: &str = "TIDB_SESSION_READ_SHARED_REGRESSION";
    if std::env::var(KEY).as_deref() != Ok("1") {
        let output=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","tikv::session_read_tests::session_read_shared_value_preserves_original_observations","--nocapture"]).env(KEY,"1").env("TZ","UTC").output().unwrap();
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        eprintln!("{}", String::from_utf8_lossy(&output.stdout));
        assert!(output.status.success());
        return;
    }
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-20T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let (mut count, mut hash) = (0usize, 0xcbf29ce484222325u64);
        for name in NAMES {
            for profile in 0..5 {
                for ret in crate::tikv::eager_scalar_value_tests::types() {
                    let values = if name == "version" {
                        values()
                    } else {
                        vec![Datum::Null]
                    };
                    for (index, value) in values.into_iter().enumerate() {
                        let new = Context::new(profile, value);
                        let actual = observe(
                            || {
                                crate::scalar_function::session_read::SessionRead::from_name(name)
                                    .unwrap()
                                    .eval(&new)
                                    .and_then(|v| {
                                        crate::scalar_function::coerce_function_result(
                                            name,
                                            Some(&ret),
                                            v,
                                        )
                                    })
                            },
                            &new,
                        );
                        let mut record = format!("{name}|{profile}|{ret:?}|{index}|").into_bytes();
                        record.extend(actual);
                        for b in record {
                            hash = (hash ^ u64::from(b)).wrapping_mul(0x100000001b3);
                        }
                        count += 1;
                    }
                }
            }
        }
        assert_eq!((count, hash), (1760, 0x33F3607B995E80CC));
        eprintln!("SESSION READ SHARED ORIGINAL WITNESS {count} {hash:016X}");
    });
}
