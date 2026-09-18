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

#[test]
fn default_session_expression_semantics_remain_native() {
    let mut session = fixture();
    assert_eq!(
        session.run("SELECT a+1 FROM t ORDER BY id").unwrap(),
        StmtResult::Rows(vec![
            vec![Datum::Int(42)],
            vec![Datum::Null],
            vec![Datum::Int(-1)]
        ])
    );
    assert!(session.warnings().is_empty());
    #[cfg(feature = "tikv-expr")]
    {
        assert_eq!(session.tikv_expression_backend(), None);
        assert_eq!(session.tikv_expression_rows(), 0);
        assert_eq!(session.tikv_borrowed_expression_rows(), 0);
    }
}

#[cfg(feature = "tikv-expr")]
#[test]
fn session_opt_in_is_explicit_counted_and_can_be_disabled() {
    use tidb_session::TikvExpressionBackend;
    let sql = "SELECT a+1 FROM t ORDER BY id";
    let mut native = fixture();
    let expected = native.run_with_columns(sql).unwrap();
    for backend in [
        TikvExpressionBackend::Copying,
        TikvExpressionBackend::Borrowed,
    ] {
        let mut session = fixture().with_tikv_expression_backend(backend);
        assert_eq!(session.tikv_expression_backend(), Some(backend));
        assert_eq!(session.run_with_columns(sql).unwrap(), expected);
        let first = session.tikv_expression_rows();
        assert!(first >= 3, "{backend:?} silently stayed native");
        if backend == TikvExpressionBackend::Borrowed {
            assert!(session.tikv_borrowed_expression_rows() >= 3);
        } else {
            assert_eq!(session.tikv_borrowed_expression_rows(), 0);
        }
        assert_eq!(session.run_with_columns(sql).unwrap(), expected);
        assert!(session.tikv_expression_rows() >= first + 3);
        let before_disable = session.tikv_expression_rows();
        session.set_tikv_expression_backend(None);
        assert_eq!(session.run_with_columns(sql).unwrap(), expected);
        assert_eq!(session.tikv_expression_rows(), before_disable);
        assert_eq!(
            native.tikv_expression_rows(),
            0,
            "opt-in leaked to a peer session"
        );
    }
}
