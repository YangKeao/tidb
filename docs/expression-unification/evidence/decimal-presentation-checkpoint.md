# Decimal presentation prerequisite

Checkpoint **decimal-presentation-80**, following **unix-timestamp-79**. Functional coverage remains **220/245**, strict0: no evaluator family is added. FROM_UNIXTIME remains native and uncredited; its ordinary/PB/legacy stage contract is recorded in the Plan for the next migration.

## Shared implementation

TiKV datatype `Decimal::native_format_visible` owns native value-layer Display. Direct raw formatting keeps original sign, leading zeros, empty coefficient, valid non-digit UTF8, and substring/panic behavior. It does not introduce ASCII or arithmetic admission checks. Hidden storage first uses the existing shared native HalfUp rounding and logical coefficient projection; retained storage is not replaced by visible digits in the original value. Native Display is a thin adapter. It now materializes an intermediate String; allocation, peak memory and performance equivalence are not claimed.

`Decimal::native_format_go_shortest_float` moves the original Ryu-backed Go-g spelling without retaining a native copy. `Decimal::native_from_f64` performs finite checking, that spelling and the existing MySQL nine-word parser. Overflow/truncation still return actual value payloads; only nonfinite input returns None. Native from_f64 imports the actual shared words, not reparsed text.

A narrow compatibility finish is necessary: the MySQL parser's finite-underflow zero can have no active words, while the original native value is coefficient0/scale0/storage0. Only that empty-header zero is replaced by `Decimal::zero()` in the new float constructor. The general parser, shift policies and raw-math importer are not changed or loosened. General native parsing differs from the wire MySQL parser on nested-round overflow; finite Ryu mantissas have at most17 significant digits, so this difference is outside this constructor's input domain. General native parse_mysql remains unchanged and is not claimed migrated.

`codec::convert::native_warning_subject_byte_cap` owns the original128-byte cut rounded down to a UTF8 boundary. The native function is an alias. Trimming and NUL handling stay with each original caller.

Ryu's existing **1.0.23** package identity is unchanged. Its direct dependency moves from native datatype to TiKV datatype; the unused native workspace declaration is removed. Both locks are generated with offline Cargo metadata, not hand-edited. Package identities/versions/checksums are unchanged; only the three dependency-edge lines differ across both locks.

## Scope and verification

Two exclusive writers (SDK/native Decimal), one independent read-only parser/domain reviewer and parent warning/dependency/integration work. Four Rust files and five new tests. CPP129/native31 original test bodies were byte-compared, unchanged. General parser, wire constructors/formatting and caller algorithms remain outside this edit.

Tests cover raw display, hidden rounding/carry, negative/raw zero, preserved invalid-representation panics, Go-g exponent thresholds, finite underflow/subnormals, nine-word overflow, Float32's unnarrowed f64 source and warning UTF8 boundaries. Native float projection compares sign/coefficient/visible/storage against source-pinned Go-g text parsed by the unchanged old native parser; expected values are not provider recordings. Seven locked nonzero test launches completed: CPPdecimal111/warning1, native decimal101/warning5 and existing FROM_UNIXTIME3 pass. All five new tests passed first matching gate, with no retries or expectation correction. Full expression1578/4old/94ignored and unistore212/1old/13ignored keep exactly normalized failure sections and remain RED. Six original wire and five original native production bodies were byte-compared. [Exact commands and receipts](../logs/decimal-presentation-summary.txt) record all attempts and remaining failures.

No new SQL probes, FROM_UNIXTIME evaluator closure, M6/default-NoColumns completion, whole-package transcreation or PR readiness is claimed. Existing INTDIV raw-empty, mode forwarding, CAST diagnostic, JSON/metadata and other compatibility gaps remain. Whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, allocation/physical heap/peak/OOM/performance/zero-copy and dual-timezone footprint are deferred.

## Next evaluator contract (not implemented)

Ordinary FROM_UNIXTIME requires source-kind-specific numeric versus coerced-text parsing; text truncation policy is replayed before the timezone getter, and format coercion only follows a valid computed local result. Preserve -0.x's old positive fraction behavior, first-nine-character validation, pre-round maximum check, carry above the maximum and raw Fixed offset. Keep the native PB one-argument post-cast and outer declared-family conversion.

Legacy instead consumes typed Decimal and the borrowed request zone, with its existing f64 range and u32 nanos-times1000 behavior. Two arguments retain missing-first panic and delayed lossy layout evaluation. A scoped callback can reuse DateFormatCoreNative; a short-lived legacy evaluator may forward raw_columns, but SimpleExpr::Shared retains its own context, so broader M6 ownership is still not implied.
