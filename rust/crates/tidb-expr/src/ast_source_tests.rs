// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Independent genuine parsed-AST values, live effects and occurrence owners.
use super::*;
use crate::tikv::dynamic_leaf_tests as leaf;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use tidb_ast::{Expr, QueryStmt, SelectField, Stmt, Visitable, Visitor};

thread_local! { static USE_SHARED_AST: Cell<bool> = const { Cell::new(false) }; }
fn invoke(root: &Expr, context: &dyn Columns) -> Result<Datum, EvalError> {
    if USE_SHARED_AST.with(Cell::get) {
        crate::evaluator::batch::ast_source::BorrowedAst::compile(root)?.eval(context)
    } else {
        crate::eval_in(root, context)
    }
}
fn with_shared_ast<T>(body: impl FnOnce() -> T) -> T {
    struct Guard(bool);
    impl Drop for Guard {
        fn drop(&mut self) {
            USE_SHARED_AST.with(|v| v.set(self.0));
        }
    }
    let _guard = Guard(USE_SHARED_AST.with(|v| v.replace(true)));
    body()
}

struct Context {
    inner: leaf::Context,
    owners: BTreeMap<usize, usize>,
    random: Cell<usize>,
    aes_mode: crate::BlockEncryptionMode,
    seeded: RefCell<BTreeMap<usize, usize>>,
    sequences: RefCell<BTreeMap<Vec<String>, i64>>,
}
impl Context {
    fn new(profile: usize, value: Datum, owners: &BTreeMap<usize, usize>) -> Self {
        let inner = leaf::Context::new(profile, value);
        inner.vars.borrow_mut().insert("x".into(), Datum::Int(1));
        Self {
            inner,
            owners: owners.clone(),
            random: Cell::new(0),
            aes_mode: crate::BlockEncryptionMode::Aes128Ecb,
            seeded: RefCell::new(BTreeMap::new()),
            sequences: RefCell::new(BTreeMap::new()),
        }
    }
    fn event(&self, event: impl Into<String>) {
        self.inner.events.borrow_mut().push(event.into());
    }
    fn snapshot(&self) -> String {
        format!(
            "{}|random:{}|sequences:{:?}",
            self.inner.snapshot(),
            self.random.get(),
            *self.sequences.borrow()
        )
    }
}
impl Columns for Context {
    fn get(&self, path: &[String]) -> Option<Datum> {
        let n = self.inner.calls.get();
        self.inner.calls.set(n + 1);
        self.event(format!("named:{path:?}:{n}"));
        if self.inner.profile == 1 {
            return None;
        }
        match path.last().map(String::as_str) {
            Some("c") => Some(self.inner.value.clone()),
            Some("d") => Some(Datum::Int(2)),
            Some("n") => Some(Datum::Null),
            _ => None,
        }
    }
    fn get_uservar(&self, name: &str) -> Option<Datum> {
        self.inner.get_uservar(name)
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.inner.set_uservar(name, value);
    }
    fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
        self.event(format!("sysvar:{scope:?}:{name}"));
        (name.eq_ignore_ascii_case("audit") && self.inner.profile != 1).then(|| Datum::Int(17))
    }
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.inner.time_zone()
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        self.inner.type_flags()
    }
    fn div_precision_increment(&self) -> u32 {
        self.event("div-precision");
        if self.inner.profile == 3 {
            7
        } else {
            4
        }
    }
    fn no_unsigned_subtraction(&self) -> bool {
        self.event("no-unsigned-subtraction");
        self.inner.profile == 3
    }
    fn context_id(&self) -> u64 {
        self.event("context-id");
        self.inner.profile as u64 + 1
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        self.event("now");
        Some((1_700_000_000, 123_000_000, 0))
    }
    fn sysdate_is_now(&self) -> bool {
        self.event("sysdate-is-now");
        true
    }
    fn current_database(&self) -> Option<String> {
        self.event("database");
        Some("ast-db".into())
    }
    fn block_encryption_mode(&self) -> crate::BlockEncryptionMode {
        self.event("encryption-mode");
        self.aes_mode
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.inner.append_warning(code, message);
    }
    fn append_note(&self, code: u16, message: &str) {
        self.event(format!("note:{code}:{message}"));
    }
    fn sleep_for(&self, duration: std::time::Duration) -> bool {
        self.event(format!("sleep:{}", duration.as_nanos()));
        false
    }
    fn rand_next(&self) -> Option<f64> {
        let n = self.random.get();
        self.random.set(n + 1);
        self.event(format!("random:{n}"));
        (self.inner.profile != 1).then_some((n as f64 + 1.0) / 128.0)
    }
    fn rand_seeded_next(&self, key: usize, seed: i64) -> Option<f64> {
        let owner = *self
            .owners
            .get(&key)
            .expect("RAND must use its actual original AST occurrence");
        let n = {
            let mut seeded = self.seeded.borrow_mut();
            let n = seeded.entry(key).or_default();
            let old = *n;
            *n += 1;
            old
        };
        self.event(format!("seeded:{owner}:{seed}:{n}"));
        (self.inner.profile != 1).then_some(
            ((seed as u64)
                .wrapping_mul(1103515245)
                .wrapping_add(n as u64 * 12345)
                % 1_000_000) as f64
                / 1_000_000.0,
        )
    }
    fn sequence_nextval(&self, path: &[String]) -> Result<Datum, EvalError> {
        self.event(format!("next:{path:?}"));
        let mut values = self.sequences.borrow_mut();
        let n = values.entry(path.to_vec()).or_insert(10);
        *n += 1;
        Ok(Datum::Int(*n))
    }
    fn sequence_lastval(&self, path: &[String]) -> Result<Datum, EvalError> {
        self.event(format!("last:{path:?}"));
        Ok(self
            .sequences
            .borrow()
            .get(path)
            .map_or(Datum::Null, |n| Datum::Int(*n)))
    }
    fn sequence_setval(&self, path: &[String], value: i64) -> Result<Datum, EvalError> {
        self.event(format!("set-sequence:{path:?}:{value}"));
        let mut values = self.sequences.borrow_mut();
        let old = values.entry(path.to_vec()).or_insert(10);
        if value <= *old {
            Ok(Datum::Null)
        } else {
            *old = value;
            Ok(Datum::Int(value))
        }
    }
}
pub(crate) fn parsed(sql: &str) -> Expr {
    let Stmt::Query(query) = tidb_parser::parse(&format!("select {sql}"))
        .unwrap_or_else(|e| panic!("AST parse {sql}: {e:?}"))
    else {
        panic!("query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("select")
    };
    let SelectField::Expr { expr, .. } = select.fields.into_iter().next().unwrap() else {
        panic!("expression")
    };
    expr
}
fn roots() -> Vec<(String, Expr)> {
    let queries = [
        "c",
        "t.c",
        "n",
        "missing",
        "@x",
        "@missing",
        "@x:=c",
        "@x:=NULL",
        "@x:=@y:=c",
        "@@audit",
        "@@session.audit",
        "@@global.audit",
        "@@unknown",
        "1",
        "18446744073709551615",
        "1.25",
        "1.25e0",
        "NULL",
        "TRUE",
        "FALSE",
        "'text'",
        "_latin1'abc'",
        "_binary'a'",
        "0xFF",
        "b'10000000'",
        "_utf8 0x61",
        "(c)",
        "+c",
        "-c",
        "~c",
        "NOT c",
        "c+2",
        "c-2",
        "c*2",
        "c/2",
        "c DIV 2",
        "c%2",
        "c=2",
        "c<=>2",
        "c<2",
        "c AND (@x:=2)",
        "c OR (@x:=2)",
        "c XOR (@x:=2)",
        "IF(c,@x:=11,@x:=22)",
        "IFNULL(c,@x:=22)",
        "COALESCE(c,NULL,@x:=33)",
        "IF(1,7,unknown_ast(@x:=99))",
        "IF(0,ABS(@x:=99,2),8)",
        "IFNULL(7,1/0)",
        "CASE WHEN c THEN @x:=11 WHEN @x:=2 THEN @x:=22 ELSE @x:=33 END",
        "CASE c WHEN @x:=1 THEN @x:=11 WHEN 2 THEN @x:=22 ELSE @x:=33 END",
        "c BETWEEN (@x:=1) AND (@x:=9)",
        "c NOT BETWEEN (@x:=1) AND (@x:=9)",
        "c IN (@x:=1,@x:=2,NULL)",
        "c NOT IN (NULL,@x:=2)",
        "ROW(@x:=1,@x:=2)=ROW(@x,@x:=3)",
        "ROW(c,@x:=1)<ROW(2,@x:=2)",
        "ROW(c,@x:=1) IN (ROW(1,@x:=2),ROW(2,@x:=3))",
        "ROW(1,2)",
        "c IS NULL",
        "c IS NOT NULL",
        "c IS TRUE",
        "c IS NOT FALSE",
        "c IS UNKNOWN",
        "c IS NOT UNKNOWN",
        "c LIKE (@x:='a%')",
        "c NOT LIKE 'a%' ESCAPE '!'",
        "c ILIKE 'A%'",
        "c REGEXP (@x:='a')",
        "c NOT REGEXP 'a'",
        "REGEXP_LIKE(c,@x:='a','i')",
        "POSITION(c IN (@x:='abc'))",
        "TRIM(BOTH (@x:='x') FROM c)",
        "WEIGHT_STRING(c)",
        "WEIGHT_STRING(c AS BINARY(4))",
        "CAST(c AS SIGNED)",
        "CAST(c AS UNSIGNED)",
        "CAST(c AS CHAR(3))",
        "CAST(c AS DECIMAL(8,2))",
        "CAST(c AS DATE)",
        "CAST(c AS TIME(3))",
        "CAST(c AS JSON)",
        "CONVERT(c USING latin1)",
        "_utf8 0x61 COLLATE utf8_bin",
        "1 COLLATE utf8mb4_bin",
        "DATE '2007-10-00'",
        "EXTRACT(DAY FROM c)",
        "TIMESTAMPADD(DAY,c,'2026-01-01')",
        "GET_FORMAT(DATE,c)",
        "DATE_ADD(c,INTERVAL (@x:=1) DAY)",
        "ADDDATE(c,1)",
        "DATE_SUB(c,INTERVAL 1 HOUR)",
        "NEXTVAL(seq)",
        "LASTVAL(db.seq)",
        "SETVAL(seq,c)",
        "SETVAL(seq,NULL)",
        "RAND()",
        "RAND(7)",
        "RAND(c)",
        "RAND(1+2)",
        "RAND(@x)",
        "ABS(c)",
        "SIGN(c)",
        "SIN(c)",
        "COS(c)",
        "TAN(c)",
        "EXP(c)",
        "COT(c)",
        "POW(c,2)",
        "ROUND(c,1)",
        "LENGTH(c)",
        "CHAR_LENGTH(c)",
        "LENGTH(_latin1'abc')",
        "CHAR(65 USING latin1)",
        "CONCAT(c,@x:='!')",
        "CONCAT_WS(',',c,NULL,@x:='!')",
        "LOWER(c)",
        "HEX(c)",
        "JSON_ARRAY(c)",
        "JSON_OBJECT('x',c)",
        "AES_ENCRYPT(c,'key')",
        "AES_DECRYPT(c,'key')",
        "MD5(c)",
        "SHA2(c,256)",
        "COMPRESS(c)",
        "BENCHMARK(2,c)",
        "BENCHMARK(0,LENGTH('a','b'))",
        "BENCHMARK(c%3,@x:=2)",
        "CHARSET(_latin1'a')",
        "COLLATION('a')",
        "COERCIBILITY(NULL)",
        "DATABASE()",
        "NOW(3)",
        "SYSDATE(3)",
        "SLEEP(0)",
        "SLEEP(NULL)",
        "unknown_ast(@x:=99)",
        "ABS(@x:=1,2)",
    ];
    let mut roots = queries
        .into_iter()
        .map(|q| (q.to_owned(), parsed(q)))
        .collect::<Vec<_>>();
    roots.push((
        "invalid-integer".into(),
        Expr::Int("18446744073709551616".into()),
    ));
    roots.push(("raw-string-root".into(), Expr::RawString("gbk".into())));
    roots.push((
        "unknown-introducer".into(),
        Expr::CharsetString {
            charset: "UNKNOWN_AST_CHARSET".into(),
            value: "a".into(),
        },
    ));
    roots.push((
        "float-nan".into(),
        Expr::Float(f64::from_bits(0x7ff8_0000_0000_0042)),
    ));
    roots.push(("float-infinity".into(), Expr::Float(f64::INFINITY)));
    roots
}
fn owner_map(roots: &mut [(String, Expr)]) -> BTreeMap<usize, usize> {
    #[derive(Default)]
    struct Owners(BTreeMap<usize, usize>);
    impl Visitor for Owners {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expr) = node.downcast_ref::<Expr>() {
                if matches!(expr,Expr::Func{name,..}if name.eq_ignore_ascii_case("rand")) {
                    let ordinal = self.0.len();
                    assert!(self
                        .0
                        .insert(std::ptr::from_ref(expr) as usize, ordinal)
                        .is_none());
                }
            }
            false
        }
        fn leave(&mut self, _: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut owners = Owners::default();
    for (_, root) in roots {
        assert!(root.accept(&mut owners));
    }
    owners.0
}
fn digest(records: &[String]) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for record in records {
        for byte in record.len().to_le_bytes().into_iter().chain(record.bytes()) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    hash
}
fn records(public_entry: bool) -> Vec<String> {
    // Explicit development-only original input, shared by all candidate runs.
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-27T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || records_at_clock(public_entry))
}
fn records_at_clock(public_entry: bool) -> Vec<String> {
    let mut roots = roots();
    let owners = owner_map(&mut roots);
    let mut records = vec![];
    for (index, (label, root)) in roots.iter().enumerate() {
        for profile in [0, 1, 3] {
            for (vi, value) in crate::tikv::byte_unary_value_tests::values()
                .into_iter()
                .enumerate()
            {
                let mut context = Context::new(profile, value.clone(), &owners);
                for phase in 0..3 {
                    if phase == 1 {
                        context.inner.value = Datum::Null;
                    } else if phase == 2 {
                        context.inner.value = value.clone();
                    }
                    let invoke = || invoke(root, &context);
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if public_entry {
                            crate::eval_in(root, &context)
                        } else {
                            invoke()
                        }
                    }));
                    let result = match result {
                        Ok(Ok(value)) => format!("VALUE:{}", leaf::frame(&value)),
                        Ok(Err(error)) => format!("ERROR:{error:?}"),
                        Err(p) => format!(
                            "PANIC:{}",
                            p.downcast_ref::<String>()
                                .map(String::as_str)
                                .or_else(|| p.downcast_ref::<&str>().copied())
                                .unwrap_or("opaque")
                        ),
                    };
                    records.push(format!(
                        "{index}/{label}/{profile}/{vi}/{phase}|{result}|{}",
                        context.snapshot()
                    ));
                }
            }
        }
    }
    records
}
#[test]
fn genuine_ast_public_values_effects_owners_and_live_series() {
    let original = records(true);
    eprintln!(
        "N59 AST ORIGINAL {} {:016X}",
        original.len(),
        digest(&original)
    );
    assert_eq!(
        (original.len(), digest(&original)),
        (191763, 0x789B6C5CB3543C08)
    );
}

#[test]
fn genuine_ast_huge_benchmark_stops_at_actual_third_lookup_failure() {
    struct StopAfterTwo(Cell<usize>);
    impl Columns for StopAfterTwo {
        fn get(&self, path: &[String]) -> Option<Datum> {
            assert_eq!(path, &["c"]);
            let n = self.0.get();
            self.0.set(n + 1);
            (n < 2).then_some(Datum::Int(7))
        }
    }
    let root = parsed("BENCHMARK(9223372036854775807,c)");
    let context = StopAfterTwo(Cell::new(0));
    let result = crate::eval_in(&root, &context);
    assert_eq!(
        result.unwrap_err(),
        EvalError::Unsupported("unknown column")
    );
    assert_eq!(context.0.get(), 3);
    let context = StopAfterTwo(Cell::new(0));

    let actual = crate::evaluator::batch::ast_source::BorrowedAst::compile(&root)
        .unwrap()
        .eval(&context);
    assert_eq!(
        actual.unwrap_err(),
        EvalError::Unsupported("unknown column")
    );
    assert_eq!(context.0.get(), 3);
}

fn registry_roots() -> Vec<(String, Expr)> {
    let mut roots = vec![];
    for name in crate::scalar_function::eager_policy_tests::names() {
        // Entropy from these four bodies is checked separately as a property,
        // not replaced by a deterministic output in the original digest.
        if matches!(
            name.as_str(),
            "uuid" | "uuid_v4" | "uuid_v7" | "random_bytes"
        ) {
            continue;
        }
        for arity in crate::scalar_function::eager_policy_tests::arities(&name) {
            for uppercase in [false, true] {
                let mut args = (0..arity)
                    .map(|i| {
                        if i == 0 {
                            Expr::Column(vec!["c".into()])
                        } else {
                            Expr::Assign {
                                name: format!("arg{i}"),
                                value: Box::new(Expr::Column(vec!["c".into()])),
                            }
                        }
                    })
                    .collect::<Vec<_>>();
                if name == "benchmark" && arity == 2 {
                    args[0] = Expr::Binary(
                        tidb_ast::BinaryOp::Mod,
                        Box::new(Expr::Column(vec!["c".into()])),
                        Box::new(Expr::Int("3".into())),
                    );
                }
                if name == "char_func" && arity != 0 {
                    args[arity - 1] = Expr::RawString("latin1".into());
                }
                let label = format!("registry/{name}/{arity}/{uppercase}");
                roots.push((
                    label,
                    Expr::Func {
                        name: if uppercase {
                            name.to_ascii_uppercase()
                        } else {
                            name.clone()
                        },
                        args,
                        origin_position: 17,
                    },
                ));
            }
        }
    }
    roots
}
fn registry_records(public_entry: bool) -> Vec<String> {
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-27T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    tidb_datatype::with_test_conversion_clock(now, || {
        let mut roots = registry_roots();
        let owners = owner_map(&mut roots);
        let mut records = vec![];
        for (label, root) in &roots {
            for profile in [0, 1, 3] {
                for (vi, value) in [Datum::Null, Datum::Int(7), Datum::new_string("abc")]
                    .into_iter()
                    .enumerate()
                {
                    let context = Context::new(profile, value, &owners);
                    for phase in 0..2 {
                        let invoke = || invoke(root, &context);
                        let outcome =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                if public_entry {
                                    crate::eval_in(root, &context)
                                } else {
                                    invoke()
                                }
                            }));
                        let result = match outcome {
                            Ok(Ok(value)) => format!("VALUE:{}", leaf::frame(&value)),
                            Ok(Err(error)) => format!("ERROR:{error:?}"),
                            Err(p) => format!(
                                "PANIC:{}",
                                p.downcast_ref::<String>()
                                    .map(String::as_str)
                                    .or_else(|| p.downcast_ref::<&str>().copied())
                                    .unwrap_or("opaque")
                            ),
                        };
                        records.push(format!(
                            "{label}/{profile}/{vi}/{phase}|{result}|{}",
                            context.snapshot()
                        ));
                    }
                }
            }
        }
        records
    })
}
#[test]
fn genuine_ast_original_registry_headers_and_value_families() {
    let original = registry_records(true);
    eprintln!(
        "N59 AST REGISTRY ORIGINAL {} {:016X}",
        original.len(),
        digest(&original)
    );
    assert_eq!(
        (original.len(), digest(&original)),
        (82080, 0x3E5E6C815EB16E9C)
    );
}

fn aes_records(public_entry: bool) -> Vec<String> {
    use crate::BlockEncryptionMode::*;
    let mut roots = [
        "AES_ENCRYPT(c,@x:='key')",
        "AES_ENCRYPT(c,@x:='key',@iv:='1234567890123456')",
        "AES_DECRYPT(c,@x:='key',@iv:=NULL)",
        "AES_ENCRYPT(c,@x:=NULL,@iv:='bad')",
        "AES_ENCRYPT(c,@x:='key',@iv:='bad')",
        "AES_ENCRYPT(@data:=NULL,@key:=1,@iv:=1)",
        "IF(c,AES_ENCRYPT('a','key',@iv:='1234567890123456'),@x:=1)",
        "AES_ENCRYPT(@data:='a')",
        "AES_DECRYPT(@data:='a','key','1234567890123456',@extra:=1)",
        "AES_ENCRYPT(c,'key',@iv:=REPEAT('x',16))",
    ]
    .into_iter()
    .map(|q| (q.to_owned(), parsed(q)))
    .collect::<Vec<_>>();
    let owners = owner_map(&mut roots);
    let mut records = vec![];
    for mode in [
        Aes128Ecb, Aes192Ecb, Aes256Ecb, Aes128Cbc, Aes192Cbc, Aes256Cbc, Aes128Ofb, Aes192Ofb,
        Aes256Ofb, Aes128Cfb, Aes192Cfb, Aes256Cfb,
    ] {
        for (label, root) in &roots {
            for profile in [0, 1, 3] {
                for (vi, value) in [
                    Datum::Null,
                    Datum::new_string("a"),
                    Datum::Bytes(vec![0; 16]),
                ]
                .into_iter()
                .enumerate()
                {
                    let mut context = Context::new(profile, value, &owners);
                    context.aes_mode = mode;
                    for phase in 0..2 {
                        let invoke = || invoke(root, &context);
                        let result = if public_entry {
                            crate::eval_in(root, &context)
                        } else {
                            invoke()
                        };
                        let result = match result {
                            Ok(value) => format!("VALUE:{}", leaf::frame(&value)),
                            Err(error) => format!("ERROR:{error:?}"),
                        };
                        records.push(format!(
                            "{mode:?}/{label}/{profile}/{vi}/{phase}|{result}|{}",
                            context.snapshot()
                        ));
                    }
                }
            }
        }
    }
    records
}
#[test]
fn genuine_ast_original_aes_modes_ignored_arguments_and_late_callbacks() {
    let original = aes_records(true);
    eprintln!(
        "N59 AST AES ORIGINAL {} {:016X}",
        original.len(),
        digest(&original)
    );
    assert_eq!(original.len(), 2160);
    assert_eq!(digest(&original), 0x6E2E_CFB3_5F59_1274);
}

fn assert_same_records(old: &[String], actual: &[String], domain: &str) {
    assert_eq!(old.len(), actual.len());
    let mut groups = BTreeMap::<String, (usize, usize)>::new();
    let mut mismatches = 0;
    for (i, (old, new)) in old.iter().zip(actual).enumerate() {
        if old != new {
            mismatches += 1;
            let key = old
                .split('|')
                .next()
                .unwrap()
                .split('/')
                .take(3)
                .collect::<Vec<_>>()
                .join("/");
            let entry = groups.entry(key).or_insert((0, i));
            entry.0 += 1;
        }
    }
    eprintln!(
        "N59 AST {domain}: {} records / {mismatches} differences / {} groups",
        old.len(),
        groups.len()
    );
    for (key, (count, i)) in groups.iter().take(32) {
        eprintln!(
            "N59 AST difference {domain} {key} count={count} index={i}\nOLD {}\nNEW {}",
            old[*i], actual[*i]
        );
    }
    assert_eq!(mismatches, 0, "AST {domain} changed original observations");
}

#[test]
fn genuine_ast_prepared_root_keeps_live_context_and_recovers_after_errors() {
    let mut roots = roots();
    let owners = owner_map(&mut roots);
    for (label, root) in &roots {
        let mut program = crate::evaluator::batch::ast_source::BorrowedAst::compile(root).unwrap();
        for profile in [0, 1, 3] {
            let mut old = Context::new(profile, Datum::Null, &owners);
            let mut actual = Context::new(profile, Datum::Null, &owners);
            for value in [
                Datum::Int(7),
                Datum::Null,
                Datum::Bytes(vec![0xff]),
                Datum::Int(-1),
                Datum::Null,
                Datum::Int(14),
            ] {
                old.inner.value = value.clone();
                actual.inner.value = value;
                let frame = |result: std::thread::Result<Result<Datum, EvalError>>| match result {
                    Ok(Ok(value)) => format!("VALUE:{}", leaf::frame(&value)),
                    Ok(Err(error)) => format!("ERROR:{error:?}"),
                    Err(p) => format!(
                        "PANIC:{}",
                        p.downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| p.downcast_ref::<&str>().copied())
                            .unwrap_or("opaque")
                    ),
                };
                let before = frame(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || crate::eval_in(root, &old),
                )));

                let after = frame(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || program.eval(&actual),
                )));
                assert_eq!(
                    (before, old.snapshot()),
                    (after, actual.snapshot()),
                    "prepared actual root {label} profile {profile}"
                );
            }
        }
    }
}

struct CacheContext {
    a: ReadyValueCache,
    b: ReadyValueCache,
    selected_b: Cell<bool>,
    panic_next: Cell<bool>,
    null_next: Cell<bool>,
    mode: BlockEncryptionMode,
    events: RefCell<Vec<String>>,
}
impl CacheContext {
    fn new(mode: BlockEncryptionMode) -> Self {
        Self {
            a: ReadyValueCache::new(),
            b: ReadyValueCache::new(),
            selected_b: Cell::new(false),
            panic_next: Cell::new(false),
            null_next: Cell::new(false),
            mode,
            events: RefCell::new(Vec::new()),
        }
    }
    fn snapshot(&self) -> String {
        format!(
            "{:?}|A:{}/{:?}|B:{}/{:?}",
            *self.events.borrow(),
            self.a.prepared_worker_count(),
            self.a.original_scope_status(),
            self.b.prepared_worker_count(),
            self.b.original_scope_status()
        )
    }
}
impl Columns for CacheContext {
    fn get(&self, path: &[String]) -> Option<Datum> {
        self.events.borrow_mut().push(format!("get:{path:?}"));
        if path.last().is_some_and(|s| s == "c") {
            self.selected_b.set(true);
            if self.panic_next.replace(false) {
                panic!("original AST live source panic");
            }
            return Some(if self.null_next.get() {
                Datum::Null
            } else {
                Datum::new_string("text".to_owned())
            });
        }
        match path.last().map(String::as_str) {
            Some("d") => Some(Datum::new_string("key".to_owned())),
            Some("p") => Some(Datum::new_string(".".to_owned())),
            Some("iv") => Some(Datum::Bytes(vec![0; 16])),
            _ => None,
        }
    }
    fn ready_value_cache(&self) -> Option<&ReadyValueCache> {
        self.events
            .borrow_mut()
            .push(format!("cache:{}", self.selected_b.get()));
        Some(if self.selected_b.get() {
            &self.b
        } else {
            &self.a
        })
    }
    fn block_encryption_mode(&self) -> BlockEncryptionMode {
        self.events
            .borrow_mut()
            .push(format!("mode:{:?}", self.mode));
        self.mode
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.events
            .borrow_mut()
            .push(format!("warning:{code}:{message}"));
    }
    fn set_uservar(&self, name: &str, value: Datum) {
        self.events
            .borrow_mut()
            .push(format!("write:{name}:{}", leaf::frame(&value)));
    }
}
fn cache_records(shared: bool) -> Vec<String> {
    let mut records = Vec::new();
    for sql in [
        "AES_ENCRYPT(c,d,@x:=iv)",
        "AES_DECRYPT(c,d,@x:=iv)",
        "AES_ENCRYPT(c)",
        "AES_ENCRYPT()",
        "REGEXP_LIKE(c,p)",
        "c REGEXP p",
        "IFNULL(AES_ENCRYPT(NULL,d),'fallback')",
    ] {
        let root = parsed(sql);
        let mut program = shared
            .then(|| crate::evaluator::batch::ast_source::BorrowedAst::compile(&root).unwrap());
        for mode in [
            BlockEncryptionMode::Aes128Ecb,
            BlockEncryptionMode::Aes128Cbc,
        ] {
            let ctx = CacheContext::new(mode);
            for phase in 0..5 {
                if phase == 1 {
                    ctx.selected_b.set(false);
                    ctx.panic_next.set(true);
                }
                if phase == 3 {
                    ctx.null_next.set(true);
                }
                if phase == 4 {
                    ctx.null_next.set(false);
                }
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if shared {
                        program
                            .as_mut()
                            .expect("only the shared observer compiles a program")
                            .eval(&ctx)
                    } else {
                        crate::eval_in(&root, &ctx)
                    }
                }));
                let result = match result {
                    Ok(Ok(v)) => format!("VALUE:{}", leaf::frame(&v)),
                    Ok(Err(e)) => format!("ERROR:{e:?}"),
                    Err(p) => format!(
                        "PANIC:{}",
                        p.downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| p.downcast_ref::<&str>().copied())
                            .unwrap_or("opaque")
                    ),
                };
                records.push(format!(
                    "{sql}/{mode:?}/{phase}|{result}|{}",
                    ctx.snapshot()
                ));
            }
        }
    }
    records
}
#[test]
fn genuine_ast_cache_selection_panic_poison_and_later_owner_recovery() {
    let old = cache_records(false);
    assert_eq!((old.len(), digest(&old)), (70, 0x53FD_82D7_D84E_CAF7));
    eprintln!("N59 AST CACHE ORIGINAL {} {:016X}", old.len(), digest(&old));

    let actual = cache_records(true);
    assert_same_records(&old, &actual, "cache-lifecycle");
}

#[test]
fn genuine_ast_entropy_sources_keep_actual_value_properties_and_null_errors() {
    for (sql, version) in [("UUID()", b'1'), ("UUID_V4()", b'4'), ("UUID_V7()", b'7')] {
        let root = parsed(sql);
        let mut plan = crate::evaluator::batch::ast_source::BorrowedAst::compile(&root).unwrap();
        for _ in 0..8 {
            let old = crate::eval_in(&root, &NoColumns).unwrap();

            let actual = plan.eval(&NoColumns).unwrap();
            assert_eq!(
                std::mem::discriminant(&old),
                std::mem::discriminant(&actual)
            );
            assert_eq!(old.collation(), actual.collation());
            for value in [old, actual] {
                let text = value.sql_string().unwrap();
                let bytes = text.as_bytes();
                assert_eq!(bytes.len(), 36);
                assert_eq!(bytes[14], version);
                for (i, &b) in bytes.iter().enumerate() {
                    if [8, 13, 18, 23].contains(&i) {
                        assert_eq!(b, b'-');
                    } else {
                        assert!(b.is_ascii_hexdigit());
                    }
                }
                assert!(b"89ab".contains(&bytes[19]));
            }
        }
    }
    for sql in [
        "RANDOM_BYTES(NULL)",
        "RANDOM_BYTES(0)",
        "RANDOM_BYTES(1)",
        "RANDOM_BYTES(16)",
        "RANDOM_BYTES(1024)",
        "RANDOM_BYTES(1025)",
    ] {
        let root = parsed(sql);
        let mut plan = crate::evaluator::batch::ast_source::BorrowedAst::compile(&root).unwrap();
        for _ in 0..4 {
            let old = crate::eval_in(&root, &NoColumns);

            let actual = plan.eval(&NoColumns);
            match (old, actual) {
                (Ok(Datum::Null), Ok(Datum::Null)) => {}
                (Ok(old), Ok(actual)) => {
                    assert_eq!(
                        std::mem::discriminant(&old),
                        std::mem::discriminant(&actual)
                    );
                    assert_eq!(old.collation(), actual.collation());
                    assert_eq!(old.go_bytes().len(), actual.go_bytes().len());
                    assert!((1..=1024).contains(&actual.go_bytes().len()));
                }
                (Err(old), Err(actual)) => assert_eq!(old, actual),
                (old, actual) => panic!("actual entropy source {sql}: {old:?} / {actual:?}"),
            }
        }
    }
}

#[test]
fn genuine_ast_prepared_primary_matches_frozen_results() {
    let actual = with_shared_ast(|| records(false));
    assert_eq!(
        (actual.len(), digest(&actual)),
        (191763, 0x789B6C5CB3543C08)
    );
}
#[test]
fn genuine_ast_prepared_registry_matches_frozen_results() {
    let actual = with_shared_ast(|| registry_records(false));
    assert_eq!((actual.len(), digest(&actual)), (82080, 0x3E5E6C815EB16E9C));
}
#[test]
fn genuine_ast_prepared_aes_matches_frozen_results() {
    let actual = with_shared_ast(|| aes_records(false));
    assert_eq!(
        (actual.len(), digest(&actual)),
        (2160, 0x6E2E_CFB3_5F59_1274)
    );
}
