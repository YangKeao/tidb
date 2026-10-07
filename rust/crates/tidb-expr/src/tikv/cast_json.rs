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

use crate::Datum;
use tidb_datatype::BinaryJSON;

/// Projects one concrete legacy CAST source into the shared JSON representation.
/// Source selection and NULL demand remain with the direct legacy evaluator.
pub fn eval_legacy_cast_json_datum(value: &Datum) -> Option<BinaryJSON> {
    tidb_query_datatype::codec::native_mysql_json::native_legacy_cast_json(
        value.as_shared_json_input(),
    )
    .map(|(type_code, bytes)| BinaryJSON::from_encoded_parts(type_code, bytes))
}

#[cfg(test)]
#[test]
fn legacy_json_cast_bridge_projects_shared_encoded_values_and_folded_errors() {
    assert_eq!(
        eval_legacy_cast_json_datum(&Datum::Int(-7))
            .unwrap()
            .as_i64(),
        Some(-7)
    );
    assert_eq!(
        eval_legacy_cast_json_datum(&Datum::new_bytes(br#"{"a":1}"#.to_vec()))
            .unwrap()
            .element_count(),
        Ok(1)
    );
    assert!(eval_legacy_cast_json_datum(&Datum::Real(f64::INFINITY)).is_none());
}
