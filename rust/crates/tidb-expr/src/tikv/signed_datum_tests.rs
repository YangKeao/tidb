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

#[test]
fn signed_datum_consumers_keep_ordinals_raw_float_and_temporal_carry() {
    use tidb_datatype::{
        BinaryLiteral, Collation, CoreTime, Datum, MySqlDuration, MysqlEnum, MysqlSet,
        SessionTimeZone, Time, TimeType,
    };
    let utc = SessionTimeZone::utc();
    let cases = [
        (
            Datum::Enum(MysqlEnum::new("not-a-number", 7), Collation::DEFAULT),
            7,
        ),
        (
            Datum::Set(MysqlSet::new("also-not-a-number", 5), Collation::DEFAULT),
            5,
        ),
        (Datum::Float32(16777217.0), 16777217),
        (
            Datum::BinaryLiteral(BinaryLiteral::from_uint(u64::MAX, None)),
            i64::MAX,
        ),
        (Datum::Bit(BinaryLiteral::from_uint(u64::MAX, None)), -1),
        (
            Datum::Duration(MySqlDuration::from_raw_parts(43_199_999_999_000, 6)),
            120000,
        ),
    ];
    for (value, expected) in cases {
        assert_eq!(value.to_i64_in(&utc).unwrap().value, expected);
        assert_eq!(super::eval_cast_signed_value_in(&value, &utc), expected);
    }
    let time = Datum::Time(
        Time::new(
            CoreTime::from_date(2011, 3, 13, 1, 59, 59, 999999),
            TimeType::DateTime,
            6,
        )
        .unwrap(),
    );
    let la = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
    assert_eq!(time.to_i64_in(&la).unwrap().value, 20110313030000);
    assert_eq!(super::eval_cast_signed_value_in(&time, &la), 20110313030000);
    // Explicit CAST reinterprets UInt; Datum conversion saturates instead.
    let uint = Datum::UInt(u64::MAX);
    assert_eq!(uint.to_i64_in(&utc).unwrap().value, i64::MAX);
    assert_eq!(super::eval_cast_signed_value_in(&uint, &utc), -1);
}
