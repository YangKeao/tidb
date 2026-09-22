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

use tidb_datatype::Datum;
use tidb_session::{Session, StmtResult};

fn fixture() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE t (id BIGINT, a BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1, 41), (2, NULL), (3, -2)")
        .unwrap();
    session
}

fn expected() -> StmtResult {
    StmtResult::Rows(vec![
        vec![Datum::Int(42)],
        vec![Datum::Null],
        vec![Datum::Int(-1)],
    ])
}

#[cfg(feature = "tikv-expr")]
#[test]
fn default_session_executes_expressions_in_tikv() {
    use tidb_session::TikvExpressionBackend;

    let mut session = fixture();
    let before = session.tikv_expression_rows();
    assert_eq!(
        session.tikv_expression_backend(),
        Some(TikvExpressionBackend::Copying)
    );
    assert_eq!(
        session.run("SELECT a+1 FROM t ORDER BY id").unwrap(),
        expected()
    );
    assert!(
        session.tikv_expression_rows() >= before + 3,
        "default engine-only session silently bypassed TiKV"
    );
    assert_eq!(session.tikv_borrowed_expression_rows(), 0);
    assert!(session.warnings().is_empty());
}

#[cfg(feature = "tikv-expr")]
#[test]
fn backend_selection_never_enables_native_fallback() {
    use tidb_session::TikvExpressionBackend;

    let sql = "SELECT a+1 FROM t ORDER BY id";
    for backend in [
        TikvExpressionBackend::Copying,
        TikvExpressionBackend::Borrowed,
    ] {
        let mut session = fixture().with_tikv_expression_backend(backend);
        let before = session.tikv_expression_rows();
        assert_eq!(session.run(sql).unwrap(), expected());
        assert!(session.tikv_expression_rows() >= before + 3);
        if backend == TikvExpressionBackend::Borrowed {
            assert!(session.tikv_borrowed_expression_rows() >= 3);
        } else {
            assert_eq!(session.tikv_borrowed_expression_rows(), 0);
        }

        let before_disable = session.tikv_expression_rows();
        session.set_tikv_expression_backend(None);
        assert_eq!(
            session.run("SELECT id FROM t ORDER BY id").unwrap(),
            StmtResult::Rows(vec![
                vec![Datum::Int(1)],
                vec![Datum::Int(2)],
                vec![Datum::Int(3)],
            ]),
            "direct-column movement requires no expression engine"
        );
        assert_eq!(session.tikv_expression_rows(), before_disable);
        let error = session
            .run(sql)
            .expect_err("engine-only execution must not fall back when disabled");
        assert!(
            error
                .to_string()
                .contains("requires a TiKV expression context"),
            "{error}"
        );
        assert_eq!(session.tikv_expression_rows(), before_disable);
    }
}
