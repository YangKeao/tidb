// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

// Operand declarations captured from the previously reviewed binding registry.
// Execution uses the original TiKV protobuf builder and validators.
use super::RpnArgTypes;
use tidb_query_datatype::EvalType;
use tipb::ScalarFuncSig;

pub(super) fn shape(sig: ScalarFuncSig) -> Option<(EvalType, RpnArgTypes)> {
    use EvalType::*;
    use RpnArgTypes::*;
    use ScalarFuncSig::*;
    Some(match sig {
        AbsInt
        | AbsUInt
        | CeilIntToInt
        | FloorIntToInt
        | RoundInt
        | UnaryNotInt
        | UnaryMinusInt
        | IntIsNull
        | BitNegSig
        | IntIsTrue
        | IntIsFalse
        | IntIsTrueWithNull
        | IntIsFalseWithNull
        | BitCount => (Int, Fixed(&[Int])),
        CoalesceInt | GreatestInt | LeastInt | IntervalInt | InInt | IntAnyValue | FieldInt => {
            (Int, Uniform(Int))
        }
        CoalesceReal | GreatestReal | LeastReal | RealAnyValue => (Real, Uniform(Real)),
        CoalesceDecimal | GreatestDecimal | LeastDecimal | DecimalAnyValue => {
            (Decimal, Uniform(Decimal))
        }
        CoalesceString
        | GreatestString
        | LeastString
        | StringAnyValue
        | Concat
        | ConcatWs
        | GreatestCmpStringAsTime
        | LeastCmpStringAsTime
        | GreatestCmpStringAsDate
        | LeastCmpStringAsDate => (Bytes, Uniform(Bytes)),
        CoalesceTime | GreatestTime | LeastTime | TimeAnyValue | GreatestDate | LeastDate => {
            (DateTime, Uniform(DateTime))
        }
        CoalesceDuration | DurationAnyValue | GreatestDuration | LeastDuration => {
            (Duration, Uniform(Duration))
        }
        CoalesceJson | JsonAnyValue | JsonMergeSig => (Json, Uniform(Json)),
        LtInt
        | LeInt
        | GtInt
        | GeInt
        | EqInt
        | NeInt
        | NullEqInt
        | PlusInt
        | MinusInt
        | MultiplyInt
        | IntDivideInt
        | ModInt
        | MultiplyIntUnsigned
        | PlusIntUnsignedUnsigned
        | PlusIntUnsignedSigned
        | PlusIntSignedUnsigned
        | PlusIntSignedSigned
        | ModIntUnsignedUnsigned
        | ModIntUnsignedSigned
        | ModIntSignedUnsigned
        | ModIntSignedSigned
        | RoundWithFracInt
        | TruncateInt
        | TruncateUint
        | LogicalAnd
        | LogicalOr
        | LogicalXor
        | BitAndSig
        | BitOrSig
        | BitXorSig
        | LeftShift
        | RightShift
        | IfNullInt
        | PeriodAdd
        | PeriodDiff => (Int, Fixed(&[Int, Int])),
        LtReal | LeReal | GtReal | GeReal | EqReal | NeReal | NullEqReal => {
            (Int, Fixed(&[Real, Real]))
        }
        LtDecimal | LeDecimal | GtDecimal | GeDecimal | EqDecimal | NeDecimal | NullEqDecimal
        | IntDivideDecimal => (Int, Fixed(&[Decimal, Decimal])),
        LtString | LeString | GtString | GeString | EqString | NeString | NullEqString
        | FindInSet | InstrUtf8 | Instr | Locate2ArgsUtf8 | Locate2Args | Strcmp => {
            (Int, Fixed(&[Bytes, Bytes]))
        }
        LtTime | LeTime | GtTime | GeTime | EqTime | NeTime | NullEqTime | DateDiff => {
            (Int, Fixed(&[DateTime, DateTime]))
        }
        LtDuration | LeDuration | GtDuration | GeDuration | EqDuration | NeDuration
        | NullEqDuration => (Int, Fixed(&[Duration, Duration])),
        LtJson | LeJson | GtJson | GeJson | EqJson | NeJson | NullEqJson => {
            (Int, Fixed(&[Json, Json]))
        }
        IntervalReal | InReal | FieldReal => (Int, Uniform(Real)),
        PlusReal | MinusReal | MultiplyReal | DivideReal | ModReal | Log2Args | Pow | Atan2Args
        | IfNullReal => (Real, Fixed(&[Real, Real])),
        PlusDecimal | MinusDecimal | MultiplyDecimal | DivideDecimal | ModDecimal
        | IfNullDecimal => (Decimal, Fixed(&[Decimal, Decimal])),
        AbsReal | CeilReal | FloorReal | RoundReal | Log1Arg | Log2 | Log10 | Sqrt | Acos
        | Asin | Atan1Arg | Cos | Cot | Degrees | Exp | Radians | Sin | Tan | UnaryMinusReal => {
            (Real, Fixed(&[Real]))
        }
        AbsDecimal | CeilDecToDec | FloorDecToDec | RoundDec | UnaryMinusDecimal => {
            (Decimal, Fixed(&[Decimal]))
        }
        CeilIntToDec | FloorIntToDec => (Decimal, Fixed(&[Int])),
        CeilDecToInt
        | FloorDecToInt
        | UnaryNotDecimal
        | DecimalIsNull
        | DecimalIsTrue
        | DecimalIsFalse
        | DecimalIsTrueWithNull
        | DecimalIsFalseWithNull => (Int, Fixed(&[Decimal])),
        RoundWithFracReal | TruncateReal => (Real, Fixed(&[Real, Int])),
        RoundWithFracDec | TruncateDecimal => (Decimal, Fixed(&[Decimal, Int])),
        Rand | Pi => (Real, Fixed(&[])),
        RandWithSeedFirstGen => (Real, Fixed(&[Int])),
        Conv | Substring3ArgsUtf8 | Substring3Args => (Bytes, Fixed(&[Bytes, Int, Int])),
        Crc32 | StringIsNull | UncompressedLength | InetAton | IsIPv4 | IsIPv4Compat
        | IsIPv4Mapped | IsIPv6 | UuidVersion | BitLength | Ascii | CharLengthUtf8 | Length
        | Ord | CharLength => (Int, Fixed(&[Bytes])),
        Sign | UnaryNotReal | RealIsNull | RealIsTrue | RealIsFalse | RealIsTrueWithNull
        | RealIsFalseWithNull => (Int, Fixed(&[Real])),
        UnaryNotJson | JsonIsNull | JsonDepthSig => (Int, Fixed(&[Json])),
        DurationIsNull | Hour | Minute | Second | MicroSecond | TimeToSec => {
            (Int, Fixed(&[Duration]))
        }
        TimeIsNull | Month | DayOfMonth | DayOfWeek | DayOfYear | WeekWithoutMode | WeekDay
        | WeekOfYear | Year | YearWeekWithoutMode | UnixTimestampInt | Quarter | ToDays
        | ToSeconds => (Int, Fixed(&[DateTime])),
        InDecimal => (Int, Uniform(Decimal)),
        InString | FieldString => (Int, Uniform(Bytes)),
        InTime => (Int, Uniform(DateTime)),
        InDuration => (Int, Uniform(Duration)),
        InJson => (Int, Uniform(Json)),
        IfNullString | Trim2Args => (Bytes, Fixed(&[Bytes, Bytes])),
        IfNullTime => (DateTime, Fixed(&[DateTime, DateTime])),
        IfNullDuration
        | DurationDurationTimeDiff
        | AddDurationAndDuration
        | SubDurationAndDuration => (Duration, Fixed(&[Duration, Duration])),
        IfInt => (Int, Fixed(&[Int, Int, Int])),
        IfReal => (Real, Fixed(&[Int, Real, Real])),
        IfDecimal => (Decimal, Fixed(&[Int, Decimal, Decimal])),
        IfString | AddDateIntString | SubDateIntString => (Bytes, Fixed(&[Int, Bytes, Bytes])),
        IfTime => (DateTime, Fixed(&[Int, DateTime, DateTime])),
        IfDuration => (Duration, Fixed(&[Int, Duration, Duration])),
        IfNullJson => (Json, Fixed(&[Json, Json])),
        IfJson => (Json, Fixed(&[Int, Json, Json])),
        CaseWhenInt | JsonValidOthersSig => (Int, Custom),
        CaseWhenReal => (Real, Custom),
        CaseWhenDecimal => (Decimal, Custom),
        CaseWhenString => (Bytes, Custom),
        CaseWhenTime => (DateTime, Custom),
        CaseWhenDuration => (Duration, Custom),
        CaseWhenJson => (Json, Custom),
        Compress | Md5 | Password | Sha1 | Uncompress | Inet6Aton | Inet6Ntoa | JsonUnquoteSig
        | JsonQuoteSig | FromBase64 | HexStrArg | LTrim | Lower | OctString | Quote | RTrim
        | ReverseUtf8 | Reverse | ToBase64 | Trim1Arg | UnHex | UpperUtf8 | Upper | LowerUtf8
        | ToBinary | FromBinary => (Bytes, Fixed(&[Bytes])),
        RandomBytes | InetNtoa | Bin | HexIntArg | OctInt | Space => (Bytes, Fixed(&[Int])),
        Sha2
        | LeftUtf8
        | Left
        | tipb::ScalarFuncSig::Repeat
        | RightUtf8
        | Right
        | Substring2ArgsUtf8
        | Substring2Args => (Bytes, Fixed(&[Bytes, Int])),
        Uuid => (Bytes, Fixed(&[])),
        UuidTimestamp => (Decimal, Fixed(&[Bytes])),
        LikeSig | Locate3ArgsUtf8 | Locate3Args => (Int, Fixed(&[Bytes, Bytes, Int])),
        RegexpSig | RegexpUtf8Sig | RegexpLikeSig => (Int, OptionalTail(&[Bytes, Bytes, Bytes])),
        RegexpInStrSig => (Int, OptionalTail(&[Bytes, Bytes, Int, Int, Int, Bytes])),
        RegexpReplaceSig => (Bytes, OptionalTail(&[Bytes, Bytes, Bytes, Int, Int, Bytes])),
        RegexpSubstrSig => (Bytes, OptionalTail(&[Bytes, Bytes, Int, Int, Bytes])),
        JsonExtractSig | JsonRemoveSig | JsonKeysSig | JsonKeys2ArgsSig => (
            Json,
            Repeated {
                prefix: &[Json],
                group: &[Bytes],
                prefix_json_values: false,
                json_values: false,
            },
        ),
        JsonTypeSig => (Bytes, Fixed(&[Json])),
        JsonSetSig | JsonInsertSig | JsonReplaceSig | JsonArrayAppendSig => (
            Json,
            Repeated {
                prefix: &[Json],
                group: &[Bytes, Json],
                prefix_json_values: false,
                json_values: true,
            },
        ),
        JsonObjectSig => (
            Json,
            Repeated {
                prefix: &[],
                group: &[Bytes, Json],
                prefix_json_values: false,
                json_values: true,
            },
        ),
        JsonArraySig => (
            Json,
            Repeated {
                prefix: &[],
                group: &[Json],
                prefix_json_values: false,
                json_values: true,
            },
        ),
        JsonContainsSig => (
            Int,
            Repeated {
                prefix: &[Json, Json],
                group: &[Bytes],
                prefix_json_values: false,
                json_values: false,
            },
        ),
        JsonMergePatchSig => (
            Json,
            Repeated {
                prefix: &[],
                group: &[Json],
                prefix_json_values: false,
                json_values: false,
            },
        ),
        JsonLengthSig => (
            Int,
            Repeated {
                prefix: &[Json],
                group: &[Bytes],
                prefix_json_values: false,
                json_values: false,
            },
        ),
        JsonMemberOfSig => (
            Int,
            Repeated {
                prefix: &[Json],
                group: &[Json],
                prefix_json_values: true,
                json_values: false,
            },
        ),
        VecAsTextSig => (Bytes, Fixed(&[VectorFloat32])),
        VecDimsSig | VectorFloat32IsNull => (Int, Fixed(&[VectorFloat32])),
        VecL1DistanceSig | VecL2DistanceSig | VecNegativeInnerProductSig | VecCosineDistanceSig => {
            (Real, Fixed(&[VectorFloat32, VectorFloat32]))
        }
        VecL2NormSig => (Real, Fixed(&[VectorFloat32])),
        VectorFloat32AnyValue => (VectorFloat32, Uniform(VectorFloat32)),
        LtVectorFloat32 | LeVectorFloat32 | GtVectorFloat32 | GeVectorFloat32 | EqVectorFloat32
        | NeVectorFloat32 | NullEqVectorFloat32 => (Int, Fixed(&[VectorFloat32, VectorFloat32])),
        DateFormatSig => (Bytes, Fixed(&[DateTime, Bytes])),
        NullTimeDiff => (Duration, Fixed(&[])),
        DurationStringTimeDiff | AddDurationAndString | SubDurationAndString => {
            (Duration, Fixed(&[Duration, Bytes]))
        }
        StringDurationTimeDiff => (Duration, Fixed(&[Bytes, Duration])),
        StringStringTimeDiff | StrToDateDuration => (Duration, Fixed(&[Bytes, Bytes])),
        Date | LastDay => (DateTime, Fixed(&[DateTime])),
        MonthName | DayName => (Bytes, Fixed(&[DateTime])),
        WeekWithMode | YearWeekWithMode => (Int, Fixed(&[DateTime, Int])),
        SysDateWithFsp | FromDays => (DateTime, Fixed(&[Int])),
        SysDateWithoutFsp => (DateTime, Fixed(&[])),
        AddDatetimeAndDuration | SubDatetimeAndDuration => (DateTime, Fixed(&[DateTime, Duration])),
        AddDatetimeAndString | AddDateAndString | SubDatetimeAndString => {
            (DateTime, Fixed(&[DateTime, Bytes]))
        }
        AddTimeDateTimeNull => (DateTime, Unused),
        AddStringAndDuration | SubStringAndDuration => (Bytes, Fixed(&[Bytes, Duration])),
        AddTimeStringNull => (Bytes, Unused),
        AddTimeDurationNull => (Duration, Unused),
        UnixTimestampDec => (Decimal, Fixed(&[DateTime])),
        MakeDate => (DateTime, Fixed(&[Int, Int])),
        MakeTime => (Duration, Fixed(&[Int, Int, Real])),
        StrToDateDate | StrToDateDatetime => (DateTime, Fixed(&[Bytes, Bytes])),
        FromUnixTime1Arg => (DateTime, Fixed(&[Decimal])),
        FromUnixTime2Arg => (Bytes, Fixed(&[Decimal, Bytes])),
        AddDateStringString | SubDateStringString | Replace => {
            (Bytes, Fixed(&[Bytes, Bytes, Bytes]))
        }
        AddDateStringInt | SubDateStringInt | LpadUtf8 | Lpad | RpadUtf8 | Rpad => {
            (Bytes, Fixed(&[Bytes, Int, Bytes]))
        }
        AddDateStringDecimal | SubDateStringDecimal => (Bytes, Fixed(&[Bytes, Decimal, Bytes])),
        AddDateIntInt | SubDateIntInt => (Bytes, Fixed(&[Int, Int, Bytes])),
        AddDateDatetimeString | SubDateDatetimeString => {
            (DateTime, Fixed(&[DateTime, Bytes, Bytes]))
        }
        AddDateDatetimeInt | SubDateDatetimeInt => (DateTime, Fixed(&[DateTime, Int, Bytes])),
        TimestampDiff => (Int, Fixed(&[Bytes, DateTime, DateTime])),
        SubDateStringReal | AddDateStringReal => (Bytes, Fixed(&[Bytes, Real, Bytes])),
        SubDateIntReal | AddDateIntReal => (Bytes, Fixed(&[Int, Real, Bytes])),
        SubDateIntDecimal | AddDateIntDecimal => (Bytes, Fixed(&[Int, Decimal, Bytes])),
        SubDateDatetimeReal | AddDateDatetimeReal => (DateTime, Fixed(&[DateTime, Real, Bytes])),
        SubDateDatetimeDecimal | AddDateDatetimeDecimal => {
            (DateTime, Fixed(&[DateTime, Decimal, Bytes]))
        }
        SubDateDurationString | AddDateDurationString => {
            (Duration, Fixed(&[Duration, Bytes, Bytes]))
        }
        SubDateDurationInt | AddDateDurationInt => (Duration, Fixed(&[Duration, Int, Bytes])),
        SubDateDurationReal | AddDateDurationReal => (Duration, Fixed(&[Duration, Real, Bytes])),
        SubDateDurationDecimal | AddDateDurationDecimal => {
            (Duration, Fixed(&[Duration, Decimal, Bytes]))
        }
        AddDateRealString | SubDateRealString => (Bytes, Fixed(&[Real, Bytes, Bytes])),
        AddDateRealInt | SubDateRealInt => (Bytes, Fixed(&[Real, Int, Bytes])),
        AddDateRealReal | SubDateRealReal => (Bytes, Fixed(&[Real, Real, Bytes])),
        AddDateRealDecimal | SubDateRealDecimal => (Bytes, Fixed(&[Real, Decimal, Bytes])),
        AddDateDecimalString | SubDateDecimalString => (Bytes, Fixed(&[Decimal, Bytes, Bytes])),
        AddDateDecimalInt | SubDateDecimalInt => (Bytes, Fixed(&[Decimal, Int, Bytes])),
        AddDateDecimalReal | SubDateDecimalReal => (Bytes, Fixed(&[Decimal, Real, Bytes])),
        AddDateDecimalDecimal | SubDateDecimalDecimal => (Bytes, Fixed(&[Decimal, Decimal, Bytes])),
        AddDateDurationStringDatetime | SubDateDurationStringDatetime => {
            (DateTime, Fixed(&[Duration, Bytes, Bytes]))
        }
        AddDateDurationIntDatetime | SubDateDurationIntDatetime => {
            (DateTime, Fixed(&[Duration, Int, Bytes]))
        }
        AddDateDurationRealDatetime | SubDateDurationRealDatetime => {
            (DateTime, Fixed(&[Duration, Real, Bytes]))
        }
        AddDateDurationDecimalDatetime | SubDateDurationDecimalDatetime => {
            (DateTime, Fixed(&[Duration, Decimal, Bytes]))
        }
        Elt | MakeSet => (
            Bytes,
            Repeated {
                prefix: &[Int],
                group: &[Bytes],
                prefix_json_values: false,
                json_values: false,
            },
        ),
        InsertUtf8 | Insert => (Bytes, Fixed(&[Bytes, Int, Int, Bytes])),
        SubstringIndex | Trim3Args => (Bytes, Fixed(&[Bytes, Bytes, Int])),
        _ => return None,
    })
}
