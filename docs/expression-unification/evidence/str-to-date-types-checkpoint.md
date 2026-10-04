# Public STR_TO_DATE datatype parser and metadata foundation

**str-to-date-types-94**, after **convert-charset-93**. Functional229/245 and strict0 unchanged; all229 family objects remain byte-identical. Three type deduplications only, no new evaluator profile/admission or family credit.

## Ownership and policy

SDK `codec/mysql/time/native_str_to_date.rs` owns the public datatype parser, format classifier and Go punctuation predicate. Native `str_to_date.rs` keeps public names and the original parse→Time::new→validate(flags,timezone) sequence. The latter constructor/validator already use SDK services.

The shared parser returns raw-packed NativeTemporalValue(DateTime,FSP0) and the original trailing-input bool. No premature validation: raw field masks and hidden microseconds remain; `%T`23 followed by PM still produces35 then masks to hour3, rather than silently repairing that old behavior. Existing year and packing helpers are reused.

Classifier preserves `#[must_use]`, both-kind early stop versus a dangling `%`, and `(has_time,has_date)` ordering. Punctuation uses exactly unicode-general-category1.1.0 (Unicode16 data) minus the existing13 Go15 exclusions, not ASCII punctuation. The native direct/workspace dependency moves to SDK; offline metadata updates only the dependency edges and the one newly needed TiKV lock package.

There are three distinct grammars: this public datatype API, ordinary `calendar.rs` string/warning parsing and TiKV wire parsing. The broader public grammar is not substituted for either other policy. Ordinary runtime remains native; only its punctuation helper and the SQL return-type classifier are now shared through existing consumers.

## Verification

[Commands and receipts](../logs/str-to-date-types-summary.txt), [manifest](../checkpoint.json).

Four locked single-threaded test launches:

| Gate | Passed | Failed | Filtered |
|---|---:|---:|---:|
| SDK parser | 1 | 0 | 460 |
| Native datatype | 8 | 0 | 452 |
| Existing expression subset | 5 | 1 old | 1696 |
| Existing SQL consumers | 2 | 0 | 2237 |

Both new tests pass on first execution. No compile failure, new execution failure, oracle correction, zero-match, interruption or fixture recording.

The old `str_to_date_partial_formats_follow_no_zero_date` mismatch at source888 expects relaxed `%d` input01 to yield0000-00-01, while the existing month-zero sentinel produces NULL/1411. It is already present in R96's full expression log. Four diagnostic lines match after numeric thread-ID normalization (`be2845834c8200d7061858af936c9defe375be04d077794e11365fdd1775520c`). The subset additionally prints the standard first-panic backtrace hint: an initial whole-block comparison caught that difference, and whole-block identity is not claimed. No production/test behavior was changed to hide this RED.

The two original SQL tests cover5SELECTs: constant DATE/Duration/DATETIME metadata, dynamic-format DATETIME policy, and CREATE/ADD/MODIFY/SET DEFAULT paths. These are existing consumer checks, not new SQL probes or ordinary evaluator takeover evidence. The six expression tests contain50 source-table calls, but the failing test stops early; all50 are not claimed executed. Full expression and unistore are not rerun; R96's4+1 full-suite failures remain historical receipts only.

Two CPP/one native Rust files, one new module, one new test each;52CPP/7native original test bodies byte-identical. Pinned formatting/diff and narrow lock ownership checks pass. H's independent current-contract review found no blocker; historical exact algorithm-body comparison is not claimed. Guides updated descriptively; Cargo manifests/locks changed explicitly, no Go/Bazel edits. Unrelated BUILD excluded.

## Next runtime boundary

Plan records ordinary two-stage parsing: only successful scanned date candidates request `date_modes`, including eventual month-zero1411; time-only/early-failure paths do not. Parsed `%j` day width must remain u32 before validation, not a pre-truncated packed date. Warning selection/rendering belongs in the SDK, with original append_warning behavior.

Typed DATETIME's time-only zero-date-prefix decision needs a separate late mode read. Existing generic finishing keeps its own getter order; Duration(NULL) still reads timezone while Date/Datetime(NULL) does not. Three STR_TO_DATE PB signatures and legacy are not currently admitted, and must not be added merely to simplify migration.

[Remaining acceptance](remaining-acceptance.md) stays active:16families, request-root/default-NoColumns/liveDAG/final acceptance, known mismatches and deferred workspace/lint/dev/release/exhaustive/performance/physicalheap/OOM/TiFlash/FIPS/allocator/dual-tzdata/Go-package/PR-readiness gates.
