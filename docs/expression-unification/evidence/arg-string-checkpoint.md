# Shared string-argument value and metadata

**arg-string-123 / R128**, after [datetime control](datetime-control-checkpoint.md). Functional238/245, strict0 and remaining7 stay unchanged; no whole CAST/M2 or Go-package credit.

SDK `native_coerce_string.rs::native_coerce_bytes` closes generic byte coercion: raw byte-bearing kinds are not UTF-8-decoded, non-byte values reuse existing expression string rendering, and range sentinels keep their distinct byte-coercion error. Rust float display is not replaced with SQL float formatting.

SDK `native_cast_arg_string.rs` owns argument value selection, target metadata and string-cast width arithmetic. Its explicit Original result preserves the actual native Datum or FieldType rather than reconstructing it from bytes or inferred metadata. Thus String collation, Enum/Set names and BinaryLiteral identity survive; BIT alone becomes binary bytes. Other values become connection-string storage through the shared byte coercer.

Metadata uses actual array-aware EvalType, physical code, width, decimal and charset/collation. String-typed sources return unchanged even when explicit/connection metadata differs. Otherwise explicit collation wins before BIT's binary default, then connection metadata applies. The same width helper serves the rewriter: original 20/87/370 widths, BIT byte sizing, decimal +3, temporal fractions and unspecified/max-long-blob behavior are retained, including unchecked arithmetic and the existing unmodeled Decimal precision path.

Native cast/coerce/rewriter functions are thin data/storage adapters. No parser/formatter callbacks, fabricated descriptors, hidden origin tags or new RPN profiles. Integer-argument control is deferred until its unsigned conversion dependencies can be closed; other typed/write selectors remain.

## Validation

Seven final matched gates GREEN: SDK new2/coercion2, native new2/arguments8/coercion1/metadata33, SQL1. Eight launches include an initial compile failure: parent-authorized removal of the production-unused MAX_LONG_BLOB_WIDTH missed references in the separate old metadata test module. Restored the same literal under cfg(test), without changing old tests/oracles or production policy; RED retained. [Exact commands/counts/hashes](../logs/arg-string-summary.txt).

Five new tests;2 SDK/222 native old touched-file test bodies unchanged. New SQL runs two SELECTs/12 cells across both vector modes, checking BIT raw bytes, Enum names, large Rust-display floats, decimal scale, invalid-UTF8 binary data, NULL, DOUBLE/DECIMAL metadata widths and no warnings. Old tests/oracles remain immutable, new expected values come from source. Full suites/lint/release/exhaustive/performance/physical memory/OOM/allocator/TiFlash/FIPS/dual-tzdata/whole-Go-package/PR readiness remain unverified. Historical R100 expression4/unistore1 failures remain unrepaired.
