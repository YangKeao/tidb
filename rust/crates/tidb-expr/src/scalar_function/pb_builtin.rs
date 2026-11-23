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

//! Go `getSignatureByPB`: select an implementation from the wire enum.
//! SQL function names are diagnostic metadata, never an overload selector here.

use tidb_proto::tipb::ScalarFuncSig;

/// Keeps the validated wire signature through rewrites and cloning.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PbBuiltin(ScalarFuncSig);

impl PbBuiltin {
    pub(crate) fn signature(self) -> ScalarFuncSig {
        self.0
    }

    pub(crate) fn new(signature: ScalarFuncSig) -> Option<Self> {
        use ScalarFuncSig::*;
        let supported = matches!(
            signature,
            PlusInt
                | PlusDecimal
                | MinusDecimal
                | MultiplyDecimal
                | DivideReal
                | DivideDecimal
                | ModReal
                | ModDecimal
                | ModIntSignedSigned
                | ModIntSignedUnsigned
                | ModIntUnsignedSigned
                | ModIntUnsignedUnsigned
                | EqInt
                | GtInt
                | LogicalAnd
                | LogicalOr
                | IntIsNull
                | RealIsNull
                | DecimalIsNull
                | StringIsNull
                | TimeIsNull
                | DurationIsNull
                | VectorFloat32IsNull
                | UnaryNotInt
                | IntIsTrueWithNull
                | CaseWhenInt
                | CaseWhenReal
                | CaseWhenDecimal
                | CaseWhenTime
                | CaseWhenDuration
                | CaseWhenJson
                | IfInt
                | IfReal
                | IfDecimal
                | IfTime
                | IfDuration
                | IfJson
                | IfNullInt
                | IfNullReal
                | IfNullDecimal
                | IfNullString
                | IfNullTime
                | IfNullDuration
                | IfNullJson
                | CharLength
                | CharLengthUtf8
                | Upper
                | UpperUtf8
                | Lower
                | LowerUtf8
                | Substring2Args
                | Substring3Args
                | Substring2ArgsUtf8
                | Substring3ArgsUtf8
                | Acos
                | Asin
                | Atan1Arg
                | Atan2Args
                | Cos
                | Cot
                | Sin
                | Pow
                | Pi
                | Conv
                | RoundInt
                | RoundReal
                | RoundDec
                | Date
                | DateDiff
                | DateFormatSig
                | Hour
                | Minute
                | Second
                | MicroSecond
                | Month
                | WeekWithoutMode
                | TimestampDiff
                | UnixTimestampInt
                | UnixTimestampDec
                | FromUnixTime1Arg
                | FromUnixTime2Arg
                | RegexpLikeSig
                | JsonMemberOfSig
                | JsonReplaceSig
                | JsonArrayAppendSig
                | JsonMergePatchSig
        ) || is_supported_cast(signature);
        supported.then_some(Self(signature))
    }
}

fn is_supported_cast(sig: ScalarFuncSig) -> bool {
    use ScalarFuncSig::*;
    matches!(sig, |CastDecimalAsDuration| CastDecimalAsInt
        | CastDecimalAsJson
        | CastDecimalAsReal
        | CastDecimalAsString
        | CastDecimalAsTime
        | CastDurationAsDecimal
        | CastDurationAsInt
        | CastDurationAsJson
        | CastDurationAsReal
        | CastDurationAsString
        | CastDurationAsTime
        | CastIntAsDecimal
        | CastIntAsDuration
        | CastIntAsJson
        | CastIntAsReal
        | CastIntAsString
        | CastIntAsTime
        | CastJsonAsDecimal
        | CastJsonAsDuration
        | CastJsonAsInt
        | CastJsonAsReal
        | CastJsonAsString
        | CastJsonAsTime
        | CastRealAsDecimal
        | CastRealAsDuration
        | CastRealAsInt
        | CastRealAsJson
        | CastRealAsString
        | CastRealAsTime
        | CastStringAsDecimal
        | CastStringAsDuration
        | CastStringAsInt
        | CastStringAsJson
        | CastStringAsReal
        | CastStringAsTime
        | CastTimeAsDecimal
        | CastTimeAsDuration
        | CastTimeAsInt
        | CastTimeAsJson
        | CastTimeAsReal
        | CastTimeAsString
        | CastTimeAsTime)
}
