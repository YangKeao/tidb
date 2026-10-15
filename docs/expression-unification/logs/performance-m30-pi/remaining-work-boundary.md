# Next-work boundary after M30

This is a source-based planning audit, not proof that every remaining function
is slow or impossible to optimize. The static inventory contains131signature
names,68admitted and63not admitted. It does not enumerate all SQL functions,
overloads, dynamic extension calls, input types or collation/context domains.
Previously rejected M18/M22/M28/M29 experiments remain fully reverted.

The inexpensive, proven registry/storage opportunities in this pass have been
implemented and measured. The next clear shared-storage target is avoiding
owning input/intermediate vectors and destination copies. That requires a
borrowed RpnStackNode/VectorValue lifetime and budget/ownership contract, rather
than another loop hint. M29 tried the smaller fixed-input changes and measured
4–12% Decimal regressions; it supplies no basis to retain those alternatives.
Decimal PLUS still costs substantially more than original native; this is an
explicit remaining gap, not claimed solved.

## All63remaining static signature names

| domain | count | signature names |
|---|---:|---|
| Native Go versus wire libm trigonometry | 5 | Atan1Arg, Atan2Args, Cos, Cot, Sin |
| Lazy control and NULL branching | 21 | CaseWhenDecimal, CaseWhenDuration, CaseWhenInt, CaseWhenJson, CaseWhenReal, CaseWhenTime, IfDecimal, IfDuration, IfInt, IfJson, IfNullDecimal, IfNullDuration, IfNullInt, IfNullJson, IfNullReal, IfNullString, IfNullTime, IfReal, IfTime, LogicalAnd, LogicalOr |
| Casts | 5 | CastDecimalAsReal, CastIntAsDecimal, CastIntAsReal, CastStringAsDecimal, CastStringAsReal |
| Base conversion | 1 | Conv |
| Temporal coercion and representation | 14 | Date, DateDiff, DateFormatSig, FromUnixTime1Arg, FromUnixTime2Arg, Hour, MicroSecond, Minute, Month, Second, TimestampDiff, UnixTimestampDec, UnixTimestampInt, WeekWithoutMode |
| Division disposition | 2 | DivideDecimal, DivideReal |
| Internal truth predicate | 1 | IntIsTrueWithNull |
| JSON mutation/membership | 4 | JsonArrayAppendSig, JsonMemberOfSig, JsonMergePatchSig, JsonReplaceSig |
| Remainder domains | 6 | ModDecimal, ModIntSignedSigned, ModIntSignedUnsigned, ModIntUnsignedSigned, ModIntUnsignedUnsigned, ModReal |
| Power result/error policy | 1 | Pow |
| Regular expressions | 1 | RegexpLikeSig |
| Nonidentity rounding | 2 | RoundDec, RoundReal |

## Why another registry-only batch is not established

- Lazy21: IF/IFNULL/CASE/AND/OR need demanded-row masks, left-to-right effects and
  NULL semantics. Registering eager FnCall would evaluate skipped children.
  Source: TiKV local/profile.rs registry and types/expr_eval.rs control frames;
  TiDB evaluator/batch.rs admits the closed eager source tree.
- Temporal14: existing native workers include text/coercion and raw Go time
  contracts while wire signatures use typed Duration/packed DateTime and SQL
  context. M29 demonstrates FSP normalization is observable even on identity
  transport; constructor substitution is not semantic proof. Sources:
  TiKV impl_time.rs native helpers; TiDB time_fn and catalog selectors.
- Numeric16 (five trig, two ROUND, POW, six MOD, two DIV): native Go trig differs
  from libm; nonidentity ROUND has distinct native/wire policy. POW shares pow_f64
  but native finite-result validation and wire NaN-to-NULL differ. Real MOD
  native rejects nonfinite results, and integer MOD overloads require exact
  per-argument signedness; Decimal MOD/DIV use explicit budget/scale/disposition.
  Sources: TiKV impl_math.rs trig_raw_*/pow_native/pow, impl_arithmetic.rs
  real_modulo/mod_int_*_native/native decimal division; TiDB ops.rs and catalog.
- Cast5 plus Conv/Regexp/JSON6: source type, coercion/precision, collation and
  native error/warning rules need producer-specific review. A wire signature
  name alone does not establish native SQL admission. Existing compiler/profile
  keeps these outside the closed eager capability contract.
- Internal1: IntIsTrueWithNull has a native BooleanFunction implementation; the
  GetBuiltinList NOT_IMPLEMENTED exclusion is inherited reporting behavior and
  is NOT proof that evaluation is absent. Its internal producer/coercion and
  independently exposed workload need review. No performance gain is claimed
  merely by increasing its metadata count.

The remaining work should therefore begin with a deliberate contract/ownership
milestone or a separately proven semantic domain. This pass stops at that cost
boundary after its final measured checkpoint; it does not claim full expression
coverage, universal speedup or that no smaller future opportunity can exist.
