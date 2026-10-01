# AES shared-worker checkpoint

`aes-two-51` follows `div-one-50`: **166/245 functional families; strict final acceptance0**. AES_ENCRYPT/AES_DECRYPT add2 families, not25. Target221 needs55;79 eligible families remain. Incomplete, not PR-ready.

## Review map and ownership

- TiKV `components/tidb_query_crypto/src/aes.rs` now owns the original public AES compatibility closure: ECB/CBC/CTR/OFB/CFB, MySQL XOR key folding, PKCS#7 and EncryptError. Existing RustCrypto AES **0.9.1** is retained, not replaced by OpenSSL or a new mode implementation. `lib.rs` exposes the module.
- Native `rust/crates/tidb-util/src/encrypt/aes.rs` reexports all13 original public functions and error. Its unchanged original tests use test-only raw-block adapters; no production mode/key/padding implementation remains there. A narrow opaque AesCipher exposes new/encrypt_block/decrypt_block and block size, without cipher variants or RustCrypto internals, for the unchanged native random-access CTR I/O layer. Unrelated `master_key/gcm.rs` still uses AES directly.
- TiKV `impl_encryption.rs` and its root exports provide24 value recipes, a NULL witness and exact typed short-IV causes. `local/{batch,registry}.rs` and `types/{function,expr_eval}.rs` close admission and authenticate receipts. Generic compilation, local exports and old helper tests need no change.
- Native `builtin_ext/crypto.rs` retains the existing lazy interface; `tikv/{evaluated_ascii,evaluated_ascii_tests}.rs` adapt actual Bytes and typed failures. SQL tests live in `tidb-session/src/tests_core/lifecycle.rs`. Existing AST, typed, values and row-batch callers and result metadata remain unchanged.

13 Rust sources (TiKV8/native5, one new module),9 new test functions. Six parallel exclusive owners; parent alone changed manifests/locks, formatted, ran serialized gates, updated guides/Plan and published. Final manifest changes are the shared AES dependency and native workspace ownership comment; native-util manifest is unchanged. Cargo-generated locks add only shared crypto→AES edges, plus AES0.9.1/cpubits0.1.1 in TiKV; native adds no package/version, and no other locked package changes.

## Closed protocol and semantics

| Profiles | Existing actual input carrier | Result |
|---|---|---|
| Encrypt/Decrypt ×128/192/256 ×ECB (6) | Bytes2(input, full password) | OwnBytes |
| Encrypt/Decrypt ×128/192/256 ×CBC/OFB/CFB (18) | Bytes3(input, full password, full IV) | OwnBytes |
| AesNullNative (1) | NullWitness(None), one actual Int NULL | OwnBytes(None) |

All25 identities/runtime metadata are unit. Only24 new value operations require nonnull inputs at both facade and official readiness; old nullable Bytes recipes are unchanged. Wrong roles/arity/terminal Some are refused. There is no fake IV, hidden mode operand, new carrier/result kind/binding/driver or PB/legacy admission.

Recognized function preparation runs inside the existing scope guard: read mode once → original arity check → input/coercion → password/coercion → necessary IV/coercion. Each actual NULL immediately admits the dedicated witness and suppresses the remaining demand. ECB never evaluates its third child; after its first two nonnull inputs, the original1618 warning precedes admission. Unknown function still returns None outside the boundary, without touching context. Cipher output retains Datum::new_string and original binary result metadata.

Only the actual worker validates IV length and takes its first16 bytes, then derives the key and calls the shared cipher. An intrinsically short-IV-only NativeAesError has private operation/profile fields and no public constructor or secret bytes. Only an actual caused error with the exact direction/key-width/mode tuple and this-call wrapper witness authenticates its receipt, and only for18 IV recipes. The adapter maps that receipt to the original function-specific IncorrectArguments message. ECB, NULL, cross-profile, resource and contract failures cannot fabricate it. The shared cipher's four source-compatible EncryptError variants retain original silent NULL behavior; no blanket evaluator error is swallowed.

No native divisor-style shortcut, cipher execution, key folding, IV slicing or short-IV decision remains in the SQL frontend. CTR is kept in the shared public utility closure but is not added as a SQL mode. No KDF/HKDF/PBKDF2 or ordinary wire signature is invented.

## Validation and incidents

[Exact commands and12 Cargo log hashes](../logs/aes-two-summary.txt); current structured ledger in `../checkpoint.json`.
Six final focused green gates: shared crypto6, native encrypt21, TiKV AES4, local293 (1 ignored), native AES5, SQL3. Nine new tests use immutable Go/NIST constants; decryption consumes fixed ciphertext, never encryption output as its oracle. Two-block NIST vectors distinguish OFB/CFB. SQL includes168 direct-column zero-slot cases across all12 modes/both directions/seven value-presence states, with preserved pre-admission1618, plus original warning/coercion order, long IV, exact short-IV diagnostics, malformed cipher NULL and empty payload/key.

Nine test attempts =8 actual runs +1 compile failure. Parent initially removed the native AES dependency after checking only manifests, missing `master_key/gcm.rs`; the native-util gate produced two E0433 and one E0432, zero tests. Restoring the independent dependency and running the third offline lock command fixed it; no GCM code was migrated. Separately, one formatter invocation used root-prefixed paths from rust/, failed before Cargo and changed nothing; `git diff --relative` corrected the paths. Both incidents are recorded, not test REDs or hidden retries. No fixture recording or original expected-value changes.

Full expression: **1494 passed,4 old failures,94 ignored; exit101**. Full unistore: **201 passed,1 old failure,13 ignored; exit101**. Complete failure sections match div-one-50 after numeric panic-thread IDs only, SHA `27654f2c0ad2971242c00f50182467598cdfd9c2c65e5bf5a32e683708448637` / `b64dcced405c699c11b72ee31888c58e491b4e5b4e74fa2d445454e1d1e146e9`; no address mapping.
Original native AES and TiKV encryption complete test suffixes, native AES layer, GCM and crypto source-test files are byte-identical to the starting checkpoint; exact hashes are in receipts. All13 final pinned formatter checks and both diff checks pass. Existing164 family objects are unchanged.

## Deferred

No whole Go-package transcreation, whole workspace, make lint/dev/bazel_prepare, release/performance/zero-copy, allocator headers/physical heap/peak/OOM/M6,150-row differential, TiFlash or FIPS acceptance is claimed. Existing compatibility exceptions remain. IntDIV would need ordered quotient/warning/conversion and precision-demand handling. A possible six-comparison batch first needs the actual native raw JSON comparison/decoder closure; numeric-only migration cannot earn six-family credit. Both are read-only candidates, not implemented work. Paired commit and byte-identical frozen Plan hashes are recorded in checkpoint.json.
