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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Narrow adapters for the five explicitly deferred host-owned builtins.
//!
//! This is not a second expression evaluator: callers evaluate ordinary
//! arguments, while these adapters perform only compatibility-heavy TiDB work.
//! JSON schema validation has one explicit expression-level adapter because its
//! cache and no-I/O-on-NULL contract require lazy demand. Unknown names never
//! fall back.

use crate::{expression::Expression, Columns, Datum, EvalError};
use tidb_chunk::row::Row;

pub(crate) fn eval_json_schema(
    cache: &crate::builtin_ext::JsonSchemaCache,
    args: &[Expression],
    ctx: &dyn Columns,
    row: Row<'_>,
) -> Result<Datum, EvalError> {
    cache.eval(args, ctx, row)
}

pub(crate) fn eval(
    name: &str,
    values: &[Datum],
    ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    Some(match (name, values) {
        ("JSON_SCHEMA_VALID", [schema, document]) => {
            crate::builtin_ext::json::json_schema_valid(schema, document)
        }
        ("TIDB_DECODE_PLAN", [value]) => crate::builtin_ext::info::decode_plan(value),
        ("TIDB_DECODE_BINARY_PLAN", [value]) => {
            crate::builtin_ext::info::decode_binary_plan(value, ctx)
        }
        ("TIDB_ENCODE_SQL_DIGEST", [value]) => crate::builtin_ext::info::encode_sql_digest(value),
        ("VALIDATE_PASSWORD_STRENGTH", [value]) => {
            crate::builtin_ext::crypto::validate_password_strength(value, ctx)
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_host_builtins_are_absent_from_generic_family_dispatch() {
        let ctx = crate::NoColumns;
        for (name, values) in [
            ("JSON_SCHEMA_VALID", vec![Datum::Null, Datum::Null]),
            ("TIDB_DECODE_PLAN", vec![Datum::Null]),
            ("TIDB_DECODE_BINARY_PLAN", vec![Datum::Null]),
            ("TIDB_ENCODE_SQL_DIGEST", vec![Datum::Null]),
            ("VALIDATE_PASSWORD_STRENGTH", vec![Datum::Null]),
        ] {
            assert!(eval(name, &values, &ctx).is_some(), "{name}");
        }
        assert!(crate::builtin_ext::json::dispatch_in(
            "JSON_SCHEMA_VALID",
            &[Datum::Null, Datum::Null],
            &ctx,
        )
        .is_none());
        assert!(
            crate::builtin_ext::info::dispatch("TIDB_DECODE_PLAN", &[Datum::Null], &ctx,).is_none()
        );
        assert!(crate::builtin_ext::crypto::dispatch(
            "VALIDATE_PASSWORD_STRENGTH",
            &[Datum::Null],
            &ctx,
        )
        .is_none());
    }
}
