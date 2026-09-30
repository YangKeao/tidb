# Two hash families — hash-two-12

Functional delegation/native-deletion progress: 29/245; final acceptance: 0/245. MD5 and SHA1 are two complete functional families; SHA is an alias of the frozen SHA1 family, not a third family. This checkpoint does not complete the overall unification target.

## Changes and domain coverage

The cut covers three TiDB caller files plus one SQL-test file and five TiKV files. MD5/SHA(SHA1) delegate to the official TiKV kernels. The crypto caller's generic `hash_unary` computation using `D::digest` and its Md5/Sha1 imports are removed. `hash_input` and `hex_lower` remain for other consumers. SHA2 keeps its original algorithm; its Digest import now uses the existing reexport instead.

Original input conversion and result channels are preserved. Tests cover raw 0xFF, GBK, Decimal, empty input, NULL and the String result tag, with original sentinel errors preceding runtime admission. Independent Python `hashlib` generated the golden values; they were not recorded from the current engine. Official OpenSSL errors propagate as real runtime failures without native retry, but runtime error injection was not tested.

The two new dispatcher tests and two new SQL tests cover values and same-root refusal. SQL exercises MD5/SHA/SHA1 on binary and text columns. Three spellings × two column domains × NULL/non-NULL produce 12 direct zero-slot refusals, without an outer migrated function hiding bypass.

## Actual validation

Parent ran the pinned January TiKV and August TiDB wrappers. [Commands and exact result lines](../logs/hash-two-summary.txt) are retained separately.

- TiKV `local::`: 193 passed/1 ignored/468 filtered.
- TiKV same-carrier operation/kernel-drift guard: 1 passed/661 filtered.
- TiDB `hash_dispatch_`: 2 passed/1483 filtered.
- Session lifecycle/SQL filter: 27 passed/2078 filtered.
- Full expression suite: 1387 passed/4 existing failures/94 ignored; 1485 discovered, exit101. Parent compared all four complete failure blocks against boolean-five-11: identical after only thread-ID normalization. The full suite remains non-green.

No old expected values were changed and no new failing test names appear. Targeted results do not establish untested failure injection or final acceptance.

## Outside this cut and not verified

PASSWORD's existing parser/authentication double-SHA1 belongs to an unmigrated family; this is not a claim that SHA1 code was deleted across the repository. SHA2's warning 1583 behavior, UNCOMPRESS warnings 1258/1259 and bounded-behavior differences, COMPRESS's Go-flate bytes and ORD's NULL/charset behavior remain unmigrated and earn no credit here.

Parent reviewed the `crypto.rs` diff. Architecture-index changes only map entrypoints/counts; parent checked paths and scoped commands against `agents-review-guide`. No new policy, PR metadata, Bazel or Go changes were introduced. `make lint` remains unrun; this is not PR-ready.

Release performance, complete business-wrapper operation scopes/guards, allocator remeasurement, physical peak/OOM guarantees, full-workspace validation and final acceptance remain open. Earlier allocation receipts and these targeted tests do not close those gates.
