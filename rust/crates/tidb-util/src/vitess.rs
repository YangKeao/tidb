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

/// Implements Vitess' method of calculating a hash used for determining a shard
/// key range: a DES encryption with a 64-bit null key over a 64-bit block.
pub fn hash_uint64(shard_key: u64) -> u64 {
    tidb_query_crypto::hash_uint64(shard_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn to_hex(value: u64) -> String {
        format!("{value:016X}")
    }

    #[test]
    fn test_vitess_hash() {
        assert_eq!(to_hex(hash_uint64(30375298039)), "031265661E5F1133");
        assert_eq!(to_hex(hash_uint64(1123)), "031B565D41BDF8CA");
        assert_eq!(to_hex(hash_uint64(30573721600)), "1EFD6439F2050FFD");
        assert_eq!(to_hex(hash_uint64(116)), "1E1788FF0FDE093C");
        assert_eq!(to_hex(hash_uint64(u64::MAX)), "355550B2150E2451");
    }
}
