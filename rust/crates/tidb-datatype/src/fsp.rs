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

/// Shared native error identity; variants, byte subjects and Display are
/// unchanged, while the nominal error type now belongs to the shared datatype.
pub use tidb_query_datatype::codec::mysql::time::NativeFspError as FspError;
use tidb_query_datatype::codec::mysql::Time as SharedTime;

/// The unspecified fractional-seconds precision accepted by TiDB.
pub const UNSPECIFIED_FSP: i64 = -1;
/// The maximum fractional-seconds precision accepted by MySQL and TiDB.
pub const MAX_FSP: i64 = 6;
/// The minimum fractional-seconds precision accepted by MySQL and TiDB.
pub const MIN_FSP: i64 = 0;
/// MySQL's default fractional-seconds precision.
pub const DEFAULT_FSP: i64 = 0;

/// Applies TiDB's `CheckFsp` normalization.
///
/// An unspecified precision becomes the MySQL default, values above six are
/// clamped, and any other negative value is rejected.
pub const fn check_fsp(fsp: i64) -> Result<i64, FspError> {
    match SharedTime::native_normalize_fsp(fsp) {
        Some(value) => Ok(value),
        None => Err(FspError::InvalidFsp(fsp)),
    }
}

/// Parses and rounds a fractional-second byte string to microseconds.
///
/// The byte slice is deliberate: Go strings can contain arbitrary bytes and
/// `ParseFrac` performs byte-indexed slicing. Using `&str` here would narrow
/// that source contract and could introduce UTF-8 boundary panics.
pub fn parse_frac(input: &[u8], fsp: i64) -> Result<(i64, bool), FspError> {
    SharedTime::native_parse_fraction(input, fsp)
}

/// Pads a fractional-second byte string to the requested digit width.
///
/// A leading minus sign does not count toward the width, matching TiDB's
/// internal `alignFrac` helper.
pub fn align_frac(input: &[u8], fsp: usize) -> Vec<u8> {
    let digits = input
        .len()
        .saturating_sub(usize::from(input.first() == Some(&b'-')));
    if digits >= fsp {
        return input.to_vec();
    }

    let aligned_len = input.len() + fsp - digits;
    let mut aligned = Vec::with_capacity(aligned_len);
    aligned.extend_from_slice(input);
    aligned.resize(aligned_len, b'0');
    aligned
}
