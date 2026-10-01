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

//! Original Go zlib byte fixtures against the unique shared TiKV encoder.
//! Production COMPRESS reaches that encoder only through the closed worker.

#[cfg(test)]
use crate::tikv::go_zlib_deflate;

#[cfg(test)]
mod tests {
    use super::go_zlib_deflate;

    /// Captured from `go run` against compress/zlib (NewWriter + Write +
    /// Close), which is TiDB's `deflate()` helper byte for byte.
    #[test]
    fn compresses_like_go_zlib() {
        assert_eq!(
            go_zlib_deflate(b"aaaaaaaaaa"),
            [
                0x78, 0x9c, 0x4a, 0x84, 0x03, 0x40, 0x00, 0x00, 0x00, 0xff, 0xff, 0x14, 0xe1, 0x03,
                0xcb,
            ]
        );
        assert_eq!(
            go_zlib_deflate(b"hello world"),
            [
                0x78, 0x9c, 0xca, 0x48, 0xcd, 0xc9, 0xc9, 0x57, 0x28, 0xcf, 0x2f, 0xca, 0x49, 0x01,
                0x04, 0x00, 0x00, 0xff, 0xff, 0x1a, 0x0b, 0x04, 0x5d,
            ]
        );
    }
}
