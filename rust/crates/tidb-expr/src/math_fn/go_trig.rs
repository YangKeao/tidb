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

//! Original Go-bit fixtures, now exercising the unique shared TiKV core.
//! Production evaluation reaches that core only through the closed worker.

#[cfg(test)]
use crate::tikv::{go_cos, go_sin, go_tan, trig_reduce};

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden vectors computed by running Go 1.25's own `math.Sin/Cos/Tan`
    /// (see scripts note in the crate docs): the whole point of this module
    /// is that these calls agree bit-for-bit with Go on every input.
    #[test]
    fn matches_go_bit_for_bit_on_golden_vectors() {
        // (input, sin, cos, tan) bits, produced by `go run` over
        // {-100.5, -3.75, -1.0, -1e-9, 0.0, 1e-9, 0.5, 1.0, 2.0, 3.14159,
        //  10.25, 100.5, 1e8, 1e9, 5.3e8, 1e15}.
        let goldens: &[(u64, u64, u64, u64)] = &[
            (
                0xc059200000000000,
                0x3f9fb3f833470ff1,
                0x3feffc12adaecec1,
                0x3f9fb7dcab49130d,
            ), // -100.5
            (
                0xc00e000000000000,
                0x3fe24a3af6750622,
                0xbfea4205b28667f6,
                0xbfe64a2502b0ca3b,
            ), // -3.75
            (
                0xbff0000000000000,
                0xbfeaed548f090cee,
                0x3fe14a280fb5068c,
                0xbff8eb245cbee3a5,
            ), // -1
            (
                0xbe112e0be826d695,
                0xbe112e0be826d695,
                0x3ff0000000000000,
                0xbe112e0be826d695,
            ), // -1e-09
            (0x0, 0x0, 0x3ff0000000000000, 0x0), // 0
            (
                0x3e112e0be826d695,
                0x3e112e0be826d695,
                0x3ff0000000000000,
                0x3e112e0be826d695,
            ), // 1e-09
            (
                0x3fe0000000000000,
                0x3fdeaee8744b05f0,
                0x3fec1528065b7d50,
                0x3fe17b4f5bf3474a,
            ), // 0.5
            (
                0x3ff0000000000000,
                0x3feaed548f090cee,
                0x3fe14a280fb5068c,
                0x3ff8eb245cbee3a5,
            ), // 1
            (
                0x4000000000000000,
                0x3fed18f6ead1b445,
                0xbfdaa22657537205,
                0xc0017af62e0950f8,
            ), // 2
            (
                0x400921f9f01b866e,
                0x3ec6428a6aa44cd1,
                0xbfefffffffff8420,
                0xbec6428a6aa4a2fd,
            ), // 3.14159
            (
                0x4024800000000000,
                0xbfe782a648605b2a,
                0xbfe5b5670532f73c,
                0x3ff153f48c125ae1,
            ), // 10.25
            (
                0x4059200000000000,
                0xbf9fb3f833470ff1,
                0x3feffc12adaecec1,
                0xbf9fb7dcab49130d,
            ), // 100.5
            (
                0x4197d78400000000,
                0x3fedcffca623a20b,
                0xbfd741b388a8c029,
                0xc004829e83f49589,
            ), // 1e+08
            (
                0x41cdcd6500000000,
                0x3fe1778cae83c69a,
                0x3feacff8c7364234,
                0x3fe4d8b249e3dba5,
            ), // 1e+09
            (
                0x41bf972880000000,
                0xbfeb283be499a2bd,
                0x3fe0ed0c5923fb27,
                0xbff9abe5d8168959,
            ), // 5.3e+08
            (
                0x430c6bf526340000,
                0x3feb76f88136ceba,
                0xbfe06c154609d33e,
                0xbffac23600a95be5,
            ), // 1e+15
        ];
        for &(in_bits, sin_bits, cos_bits, tan_bits) in goldens {
            let x = f64::from_bits(in_bits);
            assert_eq!(go_sin(x).to_bits(), sin_bits, "sin({x})");
            assert_eq!(go_cos(x).to_bits(), cos_bits, "cos({x})");
            assert_eq!(go_tan(x).to_bits(), tan_bits, "tan({x})");
        }
        // Special cases follow Go too.
        assert!(go_sin(f64::INFINITY).is_nan());
        assert!(go_cos(f64::NEG_INFINITY).is_nan());
        assert_eq!(go_tan(-0.0f64).to_bits(), (-0.0f64).to_bits());
    }

    /// The regression that motivated this module: `cot(1)` must produce
    /// Go's `0x3fe48e...` answer, not libm's one-ulp-different neighbor.
    #[test]
    fn cot_one_matches_go_engine_value() {
        assert_eq!(1.0 / go_tan(1.0), 0.6420926159343308f64);
    }

    /// Large arguments exercise the Payne-Hanek path (`trig_reduce`).
    #[test]
    fn large_arguments_reduce_like_go() {
        for x in [5.3e8f64, 1e9, 1.0000001e9, 1e15] {
            let (j, z) = trig_reduce(x);
            assert!(j < 8, "octant out of range for {x}");
            assert!(z.abs() <= core::f64::consts::FRAC_PI_4 * (1.0 + 1e-12));
        }
    }
}
