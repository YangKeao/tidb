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

use super::FieldTypeCode;
use tidb_query_datatype::codec::native_type_name::{self, NativeTypeNameCode};

/// Returns the source type label for one code.
pub fn type_str(code: FieldTypeCode) -> &'static str {
    tidb_query_datatype::codec::native_type_name::native_type_str(code.as_shared_type_name_code())
}

/// Returns the source type label, applying binary text/blob aliases.
pub fn type_to_str(code: FieldTypeCode, charset: &str) -> &'static str {
    tidb_query_datatype::codec::native_type_name::native_type_to_str(
        code.as_shared_type_name_code(),
        charset,
    )
}

/// Converts a source type label to its code, including blob/binary aliases.
pub fn str_to_type(label: &str) -> FieldTypeCode {
    match native_type_name::native_str_to_type(label) {
        NativeTypeNameCode::Known(raw) | NativeTypeNameCode::Unknown(raw) => {
            FieldTypeCode::from_mysql_type(raw)
        }
    }
}

#[cfg(test)]
#[test]
fn shared_field_name_policy_keeps_first_alias_replacement_and_fallback() {
    for (label, expected) in [
        ("blob", FieldTypeCode::Blob),
        ("longblob", FieldTypeCode::LongBlob),
        ("binary", FieldTypeCode::String),
        ("varbinary", FieldTypeCode::Varchar),
        ("blobbinary", FieldTypeCode::Unspecified),
        ("unknown", FieldTypeCode::Unspecified),
    ] {
        assert_eq!(str_to_type(label), expected, "{label}");
    }
}
