# Remaining CAST matrix after R187

This freezes the remaining native compatibility surface; it does **not** claim CAST family credit. Functional coverage remains 239/245, strict 0, remaining 6.

## Still local

| Value channel | Signatures | Remaining behavior | Blocker |
|---|---|---|---|
| Time | `CastInt/Real/Decimal/String/TimeAsTime` | Numeric temporal parsing; lossy text; DATE-vs-DATETIME inference; FSP 6; folded errors | Legacy wire loses target/source `FieldType`; generic SDK cast requires target kind/FSP, date modes and warning policy. Zone exists; `now` is not needed for these sources. |
| Time | `CastJsonAsTime` | Opaque time first, otherwise lossy JSON string and DATE-vs-DATETIME inference | Must preserve opaque-first ordering and folded error/warning behavior before using generic JSON coercion. |
| Duration | `CastTimeAsDuration` | `Time::to_duration()` with folded failure | Generic duration CAST has broader FSP, metadata and warning behavior. |
| Integer identity | `CastIntAsInt` | Full-width `Option<i128>` passthrough | Typed SDK integer converters narrow to 64-bit and require signedness/flags absent from this legacy wire. |

Condition-only cast cases are not separate conversion implementations: they call the value channels and project nonzero/non-NaN or presence. They remain native until their value channel moves.

## Explicit compatibility exception

Duration-to-time conversion remains refused because upstream anchors elapsed time on the current date. It would demand statement `now` and date modes that this legacy predicate seam intentionally does not expose. The eight baseline-unimplemented `Duration*Datetime` arithmetic signatures remain separate from CAST.

## Next safe work

1. Add a closed legacy time request carrying explicit target kind/FSP and opaque-first JSON route, with zone but no warning or `now` demand for admitted sources.
2. Differentially pin DATE-vs-DATETIME numeric classification and JSON opaque/string ordering.
3. Move `CastTimeAsDuration` separately with an exact folded-outcome API.
4. Keep `CastIntAsInt` as an i128 compatibility adapter unless the wire gains signedness metadata.
