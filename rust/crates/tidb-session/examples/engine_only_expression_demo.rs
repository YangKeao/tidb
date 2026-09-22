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

#[cfg(feature = "tikv-expr")]
fn main() {
    use tidb_session::Session;

    let mut session = Session::new();
    session
        .run("CREATE TABLE demo (id BIGINT PRIMARY KEY, a BIGINT)")
        .expect("create demo table");
    session
        .run("INSERT INTO demo VALUES (1, 41), (2, NULL), (3, -2)")
        .expect("insert demo rows");

    let before = session.tikv_expression_rows();
    let result = session
        .run("SELECT id, a + 1 FROM demo WHERE a IS NULL OR a > 0 ORDER BY id")
        .expect("TiKV evaluates the retained expression shapes");
    let engine_rows = session.tikv_expression_rows() - before;
    assert!(engine_rows > 0, "the SQL demo must execute in TiKV");

    println!("result={result:?}");
    println!("tikv_engine_rows={engine_rows}");
    println!(
        "tikv_borrowed_rows={}",
        session.tikv_borrowed_expression_rows()
    );

    session.set_tikv_expression_backend(None);
    let error = session
        .run("SELECT a + 1 FROM demo")
        .expect_err("engine-only mode must not replay through native evaluation");
    assert!(
        error
            .to_string()
            .contains("requires a TiKV expression context"),
        "unexpected refusal: {error}"
    );
    println!("native_fallback=refused: {error}");
}

#[cfg(not(feature = "tikv-expr"))]
fn main() {
    eprintln!("re-run with --features tikv-expr");
    std::process::exit(2);
}
