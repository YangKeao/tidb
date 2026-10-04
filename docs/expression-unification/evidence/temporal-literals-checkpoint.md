# Temporal literal evaluator checkpoint

Checkpoint **temporal-literals-76** follows **temporal-parser-75**. Closed functional families: `date_literal` (S0095/S0096) and `timestamp_literal` (S0412/S0413), not ordinary `timestamp`. Core gates pass, yielding217/245 with28eligible remaining and4needed for221. All215 prior family objects remain unchanged; strict final-audited count stays0 and M6 remains open.

## Single implementation and exact caller scope

TiKV `native_time_literal.rs` owns the original ASCII regex gates, shared temporal parsing, DATE zero policies, unpadded parse-error diagnostic and hard-error code/message. DATE regex rejection is1292/date, DATE parse rejection is1292/datetime with unpadding, then NO_ZERO_DATE precedes NO_ZERO_IN_DATE. TIMESTAMP literal regex rejection is1525/datetime and parse rejection1292/datetime; only ALLOW_INVALID_DATES affects its calendar policy. The original regex, source text, FSP, and ignored parser diagnostic bits are preserved. No native fallback remains.

Both ordinary literal syntax and ODBC `{d ...}`/`{ts ...}` reach the same rewrite-time CastStyle. The original sequence is literal_text (rewrite/fold and require Constant) → resolver.time_zone → date_modes → literal helper → Constant(Datum::Time, own FieldType). Native wrappers retain their signatures and explicit NoColumns one-shot ownership. New `_in` variants use Columns only for scope authority, not new getters. Actual SQL tests exercise folding and constant broadcasting, **not nonfolded literal workers or resolver-owned M6 propagation**.

Direct AST literal execution remains Unsupported, and ODBC nonconstant input remains rejected. There are no existing PB/unistore literal signatures to connect. The registry does contain two internal mangled arity records, but they have no corresponding runtime name dispatch; frozen inventory's empty registry-name field is not evidence that those records do not exist. No new admission is created. Ordinary TIMESTAMP() remains separate: left parse failure must precede right coercion; both1/2-argument signatures remain unclaimed.

Native code only projects computed raw/kind/FSP using Time::from_raw_parts, preserves DATE FieldType(10,0) or DATETIME FieldType(19+FSP+dot,FSP), and maps the computed hard error unchanged. TIMESTAMP literal produces **DateTime**, not Timestamp. Native regex/parse/zero-policy/error-format implementations were deleted.

## Closed carrier, binding and ownership

Two fixed profiles, DateLiteralNative/TimestampLiteralNative, use a distinct TemporalText role with actual raw UTF8 bytes, actual three mode bits (allow-invalid1/no-zero-date2/no-zero-in-date4), and owned shared NativeSessionTimeZone. The physical scalar shape is BytesInt,2columns+1call, but plain BytesInt cannot enter these profiles. Transport admission checks presence/UTF8/bits0..7 only; invalid dates are worker business input. No parsed core/FSP, ready host answer, operation selector, fake NULL or source-origin tag is transported.

Final L78.1 metadata is zone-only, with new()/zone() and no temporal-kind enum. Fixed function metadata identities distinguish the profiles. The zone is moved into an invocation binding, borrowed without per-row clone, and removed by a pre-bind RAII guard on success/error/unwind before lease finish. Worker storage must be unbound at entry/exit. Named0.10.4 identity, Local behavior and Fixed name/raw offset remain intact; no EvalConfig change, wire-Tz conversion, name reparse or offset snapshot.

Fixed name **capacity** is counted with text capacity before binding and throughout output/copy overlap. The reply bound is max(11,text length+64). This is logical resource accounting, not full physical heap/allocator/OOM verification. Tests cover oversized Fixed name refusal before a kernel and subsequent recovery, zero-step refusal, same-worker zone rebinding and error cleanup. Native zero-slot tests independently cover both valid and invalid literal input.

Success reports reuse exact identity Time bytes(tag15,LE raw8,kind,FSP;11bytes). Hard errors use tag0+LE u16 code+UTF8 message. Decoder rejects other identity variants, malformed sizes/UTF8/unknown codes; values retain raw representation without new validation. Profiles forbid SQL NULL reports. Native checks the expected literal kind before projection.

## Validation

Seven exclusive writers plus bounded caller review;13 Rust files(9CPP/4native), one new module and seven new tests(4CPP/2native-expression/1SQL). No manifest, lock, Go, Bazel or generated/fixture changes. Pinned formatting and diff checks pass; CPP198/native358 original test bodies are byte-identical.

Initial CPP core2 passed. First local gate326/1new/1ignored failed because a **new test** incorrectly expected padded DATE diagnostics for DATE calendar parse failure. Frozen prior native source and the byte-identical moved unpadded_datetime_message prove the expected diagnostic is datetime with unpadded fields. Only that newly authored oracle was corrected from source; no provider output was adopted as oracle, and production/old tests were not changed. Local retry passed. All raw failed and retried commands remain in [receipts](../logs/temporal-literals-summary.txt).

Native root5, gateway1, corrected SQL1 and original timezone-literal SQL1 pass. Full expression1570/4old/94ignored and unistore211/1old/13ignored retain entire normalized failure sections against R77. Ten exact locked test commands:6green,2new-test assumption failures with2separate retries,2oldfull failures. No compile failure/interruption/zero-match; all7newtests pass finally,5on first matching gate. No production change after the first gate.

The initial new SQL test incorrectly extended direct custom-DateModes expectations to table projection. Existing PlanScopeResolver forwards the zone but omits date_modes(), inheriting TIDB_DEFAULT_SQL_MODE(true,true,false). Five relevant production sources are byte-identical to e5a05f2a; this is **source attribution, not a baseline execution replay**. Three permissive-mode cases now explicitly check those old refusal boundaries, not Go mode parity. Mode forwarding remains unfixed; cases after the initial failed assertion are not claimed executed in that run. Final SQL matrix has5positive,9hard-error and1nonconstant refusal under both vector flags:30SELECT probes,20successful cells. Six of the18hard-error probes document the old planner limitation. Direct native/CPP tests separately cover the actual custom mode bits. The previous CAST zero-date warning mismatch was not rerun/repaired.

One preliminary source-audit script used the wrong stmt_context.rs crate path; glob identified executor, and the corrected five-file audit passed. This was not a production or Cargo failure. No fixture recording or actual-output oracle was used.

## Remaining limits

Strict count0, ordinary TIMESTAMP and other eligible families, resolver M6/default-NoColumns ownership, broader temporal/YEAR/INTERVAL work, prior raw-empty INTDIV exception, zero-date CAST warning and old expression/unistore failures remain explicit. No whole workspace/lint/dev/bazel_prepare/release, exhaustive differential/TiFlash/FIPS, performance/zero-copy/physical heap/peak/OOM or dual-timezone footprint claim. Publish paired Plan snapshots and exact receipts only after the bounded gates close.
