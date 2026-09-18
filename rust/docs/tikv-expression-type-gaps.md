# TiKV datatype-layer gaps for removing the native expression evaluator

Read-only inventory. **Nothing was compiled, built, or tested for this document**;
all evidence is from source reading (grep/read) in the two checkouts. No file other
than this one was written.

Scope: the native Rust expression evaluator in
`rust/crates/tidb-expr` + `rust/crates/tidb-datatype` + `rust/crates/tidb-chunk` +
`rust/crates/tidb-codec`, and the TiKV engine it is to be replaced by
(`components/tidb_query_datatype`, `components/tidb_query_expr`,
`components/tidb_query_aggr`). The parity target is the **current native surface**,
not all of MySQL: a type only matters if the native evaluator can produce or consume
it today.

Provenance caveat: at the time of writing, `/home/agent/tidb/expression-reuse/tidb`
is at `881c71b1d81e12fef74e61c5e18464e121c45933` (2 doc-only commits ahead of the
stated `abffd0ab9f355b1090205f92fc36f8b022bb6460`) and the shared working tree is
being edited concurrently by the parent agent (new
`rust/crates/tidb-expr/src/tikv/admission.rs`; changes under `tidb-executor`). The
files this document leans on most — `tidb-expr/src/tikv/bridge.rs`,
`tidb-expr/src/tikv/lowering.rs`, `tidb-datatype/*`, `tidb-chunk/*`, and the TiKV
components — were unchanged at read time. Line numbers below are from that read;
symbol names are the stable anchors. `git diff abffd0ab..HEAD` shows only
`rust/docs/tikv-expression-known-divergences.md` and
`rust/docs/tikv-expression-removal-execplan.md`.

## 1. Headline

**Set is the only value type that must be added.** Everything else the native
evaluator can produce or consume is already representable by TiKV's engine, or is
not an evaluator value at all.

- **Add: `SET`.** It is a first-class input value of the native evaluator (read from
  SET columns and consumed by string/integer coercions), and TiKV's engine explicitly
  refuses it today (`bridge.rs` `family()`; `EvalType::try_from(Set)` returns `Err`).
  TiKV already owns most of the Set scaffolding (see §4), so this is mostly wiring up
  dead code.
- **Do not add: GEOMETRY.** TiDB has no geometry `Datum` kind, no geometry `EvalType`,
  and no geometry builtin. It is a wire `FieldTypeTp` name only.
- **Do not add: ARRAY.** There is no array `Datum`; the only array path is an explicit
  `CAST(... AS ... ARRAY)` which the native evaluator already rejects.
- **Already representable, no work: UInt, Float32, Bit, BinaryLiteral, Enum, Raw,
  MinNotNull/MaxValue, Time/Timestamp, JSON, VectorFloat32, Decimal, Duration,
  String/Bytes.** UInt and Float32 are carried inside `EvalType::Int`/`EvalType::Real`
  plus `FieldType` metadata; Bit inside `EvalType::Int`; Enum/Bit/BinaryLiteral are
  either already bridged or lowered to Bytes/Int on the wire.

## 2. The native value domain

`tidb_datatype::Datum` (`rust/crates/tidb-datatype/src/datum/mod.rs:220-260`), with
the eval family the native evaluator assigns
(`rust/crates/tidb-expr/src/scalar_function.rs:107-121`, `datum_eval_type`) and the
TiKV carrier that already exists.

| `Datum` variant | Native eval family | First-class result? | TiKV representation | Needed? |
| --- | --- | --- | --- | --- |
| `Null` | `None` | n/a | per-kind `None`/null bitmap | present |
| `MinNotNull`, `MaxValue` | `None` | no — range sentinels | none | **no** |
| `Int(i64)` | `Int` | yes | `EvalType::Int` / `Column::Int` | present |
| `UInt(u64)` | `Int` | yes (survives; see §5) | `EvalType::Int` + `FieldTypeFlag::UNSIGNED` | present |
| `Decimal` | `Decimal` | yes | `EvalType::Decimal` | present |
| `Real(f64)` | `Real` | yes | `EvalType::Real` | present |
| `Float32(f64)` | `Real` (code `Float`) | yes — `CAST(x AS FLOAT)` | `EvalType::Real` + `FieldTypeTp::Float` 4-byte cell | present |
| `String(StringDatum)` | `String` | yes | `EvalType::Bytes` | present |
| `Bytes(Vec<u8>)` | `String` | yes | `EvalType::Bytes` | present |
| `BinaryLiteral` | `String` | no — literal/column input only | lowered to `BytesLiteral`/`BitLiteral` on the wire | present |
| `Duration` | `Duration` | yes | `EvalType::Duration` | present |
| `Enum(MysqlEnum)` | `String` (code `Enum`) | no — column input only | `EvalType::Enum` / `Column::Enum` | present |
| `Bit(BinaryLiteral)` | `String` (code `Bit`) | yes — cast-to-BIT | `EvalType::Int` + `FieldTypeTp::Bit` | present |
| `Set(MysqlSet)` | `String` (code `Set`) | no — column input only | **none** | **YES (§4)** |
| `Time(Time)` | `Datetime`/`Timestamp` | yes | `EvalType::DateTime` (both) | present |
| `Json(BinaryJSON)` | `Json` | yes | `EvalType::Json` | present |
| `Raw(Vec<u8>)` | `None` | no — internal codec kind | none | **no** |
| `VectorFloat32` | `VectorFloat32` | yes | `EvalType::VectorFloat32` | present |

## 3. The parity boundary already in the native code

The native→engine adapter states the boundary itself. `family()` in
`rust/crates/tidb-expr/src/tikv/bridge.rs:46-69` maps a `FieldType` to an engine
carrier and returns `None` for everything else, with the comment at `:65-67`:

> `SET, legacy NEWDATE, GEOMETRY and unspecified/unknown codes have no implemented
> native engine EvalType/owned carrier.`

and the covering test asserts exactly that set
(`bridge.rs:548-571`, `Set, NewDate, Geometry, Unspecified, Unknown(42)` and every
`with_array(true)` code). The same test also pins two defensive metadata exclusions
that are not value types: `Duration` with `decimal() == 7` and `Bit` with
`flen > 64` (`bridge.rs:564-566`). `admitted()` in `lowering.rs:58-87` gates on
`bridge::supported_type`, and `pushdown_catalog::leaf_column_family`
(`tidb-expr/src/pushdown_catalog.rs:2589-2619`) omits `Set` and `Geometry`, so a Set
leaf also fails the local schema encoder (`field_type_to_pb`, `:2131-2150`).

So the current fallback policy is: **any expression touching a Set (or Geometry/array)
column runs natively.** When the native evaluator is deleted there is no fallback, so
each such type is either carried by the engine or becomes an explicit error.

## 4. SET — needed; concrete additions

### 4(a) Does the native evaluator use it? Yes, as an input.

- **Not a function result.** No production code in `tidb-expr` constructs a
  `Datum::Set`: the only occurrences outside `#[cfg(test)]` are match/consume arms.
  The producers are storage decode
  (`rust/crates/tidb-codec/src/rowcodec.rs:782-790`) and the write path
  `convert_to_set` (`tidb-datatype/src/datum_convert.rs:783-837`). `Column::eval`
  reads whatever the chunk row holds (`tidb-expr/src/column.rs:222-237`). There is no
  `SetLiteral` in the pushdown catalog (only `EnumLiteral`,
  `pushdown_catalog.rs:1789`), so a Set never arrives as a wire literal.
- **Consumed generically**, because Set is a string-kind hybrid. There is no builtin
  with a dedicated `Datum::Set` arm; every use flows through shared, type-directed
  coercion helpers, so *every implemented function with an `ETString` or `ETInt`
  argument accepts a Set*:
  - `arg_eval_type::eval_string` reads the SET **name** —
    `rust/crates/tidb-expr/src/arg_eval_type.rs:422-436` (Set arm `:429`).
  - `coerce::coerce_str` / `coerce_str_bytes` → name bytes —
    `rust/crates/tidb-expr/src/coerce.rs:155-159` and `:193`.
  - `coerce::integer_of` → the unsigned **bitmask** —
    `rust/crates/tidb-expr/src/coerce.rs:33-57` (Set arm `:41`).
  - `cast_arg_as_int` (the wrapper for any `types.ETInt` position) —
    `rust/crates/tidb-expr/src/cast.rs:1390-1404`; `MAKE_SET`'s selector is declared
    `ETInt` at `arg_eval_type.rs:219`.
  - `cast_arg_as_string` passes a SET through unchanged —
    `rust/crates/tidb-expr/src/cast.rs:1454-1475` (Set arm `:1469`).
  - comparison operators compare SET by **name** —
    `rust/crates/tidb-expr/src/ops.rs:1218-1236` (Set arm `:1233`).
- **Concrete implemented builtins reachable with a SET argument** (all read the NAME,
  except the `ETInt` slots which read the bitmask):
  `FIND_IN_SET` (`builtin_ext/string2.rs:34,253`), `EXPORT_SET`
  (`builtin_ext/string2.rs:35,302`; string args and ETInt arg0, `arg_eval_type.rs:226`),
  `MAKE_SET` (`func.rs:801`; ETInt selector `arg_eval_type.rs:219`),
  `CONCAT` (`func.rs:746`), `HEX` (`func.rs:774`), `OCT` (`func.rs:777`),
  `FIELD` (`func.rs:779`), `ELT` (`func.rs:780`; string tail `arg_eval_type.rs:354`).

### 4(b) Encoding — Go and native Rust agree; TiKV is the missing side

- **Row / raw datum / storage wire = the unsigned integer bitmask**, with the name
  reconstructed from `FieldType.elems`:
  Go `pkg/util/rowcodec/encoder.go:199-202` (`KindMysqlSet` → `encodeUint(Value)`),
  `pkg/util/rowcodec/decoder.go:532-533` (`TypeSet` → `UintFlag`) and
  `:155-167` (`ParseSetValue(elems, decodeUint)`); name join at
  `pkg/types/set.go:112-132`. TiKV already decodes this: `codec/row/v2/compat_v1.rs:86-90`
  writes Enum/Bit/Set alike as a `u64` datum.
- **Chunk cell = `[8-byte LE bitmask][comma-joined name bytes]`** (var-length):
  Go `pkg/util/chunk/column.go:46-53` (`appendNameValue`), `AppendSet` `:68-70`,
  `GetSet`/`getNameValue` `:747-749,762-770`; SET is a `VarElemLen` column
  (`pkg/util/chunk/codec.go:163-179`). Native Rust is byte-identical:
  `rust/crates/tidb-chunk/src/column.rs:743-796` (`append_name_value`, `append_set`,
  `get_name_value`, `get_set`), `get_fixed_len` → VAR, chunk dispatch
  `tidb-chunk/src/chunk.rs:771-775,816`, row decode
  `tidb-chunk/src/row.rs:90-95` (validated at `:162-173`); the in-row datum codec
  stores the bitmask (`tidb-codec/src/datum.rs:169-176,279-286`).
- **TiKV's Enum path is the exact template**, and it is complete:
  `EvalType::Enum` (`def/eval_type.rs:81`) → var-length column
  (`codec/chunk/column.rs:68`) → `append_enum_datum` (`:965-998`) →
  `mysql/enums.rs:205-284` (`write_enum_to_chunk` = u64 LE + name,
  `read_enum_from_chunk`, bitmask→name via `get_value_name`) → `get_enum`
  (`column.rs:1000-1007`) → `RawDatumDecoder<Enum>` (`codec/datum_codec.rs:580-602,
  660-664`). **SET requires the comma-join of the selected `elems`**, not
  `elems[value-1]`, so the name reconstruction is a new helper, not a reuse.

### 4(c) Concrete `tidb_query_datatype` + standalone additions

TiKV already has the value carriers for Set; most arms are `unimplemented!()`
placeholders. Required work:

1. `def/eval_type.rs` — map `FieldTypeTp::Set => EvalType::Set` in
   `TryFrom<FieldTypeTp>`, replacing the catch-all TODO at `:82-88`; update the tests
   that currently assert `(Set, None)` (`:122`).
2. `codec/mysql/set.rs` — add the encoder/decoder traits that `enums.rs` has and Set
   lacks: `write_set_to_chunk(value, name)` (`u64` LE + name),
   `write_set_to_chunk_by_datum_payload_{compact_bytes,uint,var_uint}` (bitmask →
   comma-join name over `field_type.elems`), `read_set_from_chunk`.
3. `codec/datum_codec.rs` — implement `RawDatumDecoder<Set>`, currently
   `unimplemented!()` (`:666-669`); mirror `decode_enum_datum` (`:580-602`) with the
   comma-join name.
4. `codec/chunk/column.rs` — add the four arms and helpers:
   `from_raw_datums` `EvalType::Set => append_set_datum` (replace `:147`),
   `from_vector_value` `VectorValue::Set` (replace `:306`),
   `get_datum` `FieldTypeTp::Set => Datum::Set(get_set(idx)?)` (replace the error at
   `:340-345`), `append_datum` Set arm (`:359-381`), plus `append_set`/`append_set_datum`/
   `get_set`. Note the existing `get_enum` (`:1000-1007`) indexes `idx * fixed_len`,
   which is wrong for a var-length column; the Set reader must use `var_offsets`, and
   Enum should be fixed with it.
5. `codec/data_type/mod.rs` — extend the two hybrid-aware carriers, exactly as Enum
   is already handled:
   - `Int::borrow_scalar_value`/`borrow_scalar_value_ref`/`borrow_vector_value`
     (`:307-352`) gain `ScalarValue::Set`/`ScalarValueRef::Set` → `SetRef::value_ref()`
     and `VectorValue::Set` → `ChunkedVecSet::as_vec_int()`.
   - `BytesRef::borrow_*` (`:450-503`) gain `ScalarValue::Set` → `SetRef::name()` and
     `VectorValue::Set` → `as_vec_bytes()`.
   (`EvaluableRef for SetRef` and `impl_evaluable_ret! { Set, ChunkedVecSet }` already
   exist at `:687-732` and `:381`.)
6. `codec/data_type/chunked_vec_set.rs` — the missing "set set column data":
   `with_capacity` starts with an empty `Arc<BufferVec>` (`:47-53`), `push_data`
   never touches `data` (`:55-59`), `append` skips it (`:82-85`); the test works
   around it by assigning `x.data` directly (`:155-163`). Add an element-name install
   API (e.g. from `FieldType.elems`) and keep value+bitmap+data coherent; to provide
   the `as_vec_int`/`as_vec_bytes` views, restructure like `ChunkedVecEnum`
   (`chunked_vec_enum.rs:26-30,68-77`), which stores both `ChunkedVecSized<Int>` and
   `ChunkedVecBytes`.
7. `codec/data_type/scalar.rs` — `ScalarValueRef::Set` encode arm (`:338-340`) and
   `cmp_sort_key` template list (`:380`). `codec/data_type/vector.rs` — the
   `VectorValue::Set` `unimplemented!()` arms for `maximum_encoded_size` (`:282-284`),
   `maximum_encoded_size_chunk` (`:356-357`) and `encode` (`:473-474`).
8. `codec/datum.rs` — `Datum::Set` (and `Datum::Enum`) write/size, currently
   `unimplemented!()` (`:1027-1029,1074-1076`).
9. `tidb_query_expr/src/standalone.rs` — add `Column::Set` (`:62-72`), `eval_type`
   (`:94-106`), `empty` (replace `:119 unreachable!("Set has no engine codec")`),
   `copy_rows` (`:123-194`), `push_result` (`:196-212`); add the `ExprType::MysqlSet`
   constant arm in `validate_expr` (`:481-599`, next to `MysqlEnum` at `:555`); add a
   `standalone/values.rs` Set helper if the bridge transports it directly; flip the
   rejection test `standalone/tests.rs:451`.
10. `types/function.rs` — extend `validate_expr_return_type` (`:273-284`) with
    `(Int, Set)` and `(Bytes, Set)` next to the existing `(Int, Enum)`/`(Bytes, Enum)`.
11. `tidb_query_expr/src/impl_cast.rs` — register the Set cast arms. `cast_set_as_int`
    **already exists with tests** (`:418-422`, tests `:2270-2283`) but is absent from
    the `(from_eval_type, to_eval_type)` dispatch (`get_cast_fn_rpn_meta` match at
    `:41`, where only `Enum` arms appear; `map_cast_func` resolves casts from the
    child/return `FieldType`s, so the tipb signature name does not matter). Add
    `(Set, Int) => cast_set_as_int_fn_meta()` and the Real/Bytes/Decimal
    arms the native lowering emits (mirror `cast_enum_as_*`).
12. No aggregation work needed: `EvalType::Set` aggregators already exist
    (`tidb_query_aggr/src/impl_sum.rs:59,228`, `impl_max_min.rs:105`,
    `impl_avg.rs`, `impl_variance.rs`, `impl_first.rs`).
13. No row-decoding work needed: `codec/row/v2/compat_v1.rs:86-90` already maps Set to
    a `u64` datum. (General `codec/table.rs::unflatten` still rejects Enum/Set/Bit at
    `:281-284`; verify whether the removal path uses it.)

### 4(d) How many kernel signatures need it

**No new ordinary kernel signatures.** Set is consumed through the existing `Int`
and `Bytes` argument types once those two `Evaluable`/`EvaluableRef` carriers gain
their Set arms, mirroring how Enum is already accepted
(`data_type/mod.rs:314-316,329-331,344` for Int; `:459,472,485` for Bytes). Without
those arms the engine's `ArgConstructor` panics on a type mismatch
(`types/function.rs:243-269`), so the alternative — a Set variant per string/int
kernel — would touch hundreds of signatures. With the two-carrier approach the
signature-level additions are only the cast family: 1 kernel already written
(`cast_set_as_int`) plus up to 6 more cast arms if the lowering emits them
(`cast_enum_as_*` has 7 arms: Int, Real, Bytes, Decimal, Duration, DateTime, Json).

### 4(e) Native-side adapter changes (co-requisites, not TiKV datatype work)

- `bridge.rs` `family()` (`:46-69`) and `supported_type` must admit Set; add a
  `Family::Set` and `copy_column`/`into_datums` arms (or handle Set across
  Int/Bytes).
- `pushdown_catalog.rs` `leaf_column_family` (`:2589-2619`) / `field_type_to_pb`
  (`:2131-2150`) must encode a Set schema `FieldType` for the local adapter.
  Go's distributed `columnToPBExpr` deliberately refuses `TypeSet`/`TypeGeometry`
  (`pkg/expression/expr_to_pb.go:236`), so this is a local-adapter-only change and
  does not by itself enable distributed Set pushdown.
- `lowering.rs` `same_family`/`coerce` (`:203-257`) must special-case Set like Enum
  (`:208,215`) so the correct `CastSetAs*` signature is inserted rather than a
  name-parsing string cast.

## 5. Types that are NOT needed, with reasons

| Type | Needed? | Reason / evidence |
| --- | --- | --- |
| **Geometry** | **No** | No `KindMysqlGeometry` in Go (`pkg/types/datum.go:45-62`) and no `Geometry` variant in Rust `Datum` (`datum/mod.rs:220-260`). No geometry builtin: the 309-entry `FUNCTION_CLASSES` (`builtin_registry.rs:66`) and `func.rs` dispatch contain no `ST_`/`geom`/WKB name. `FieldTypeCode::Geometry` exists only as a wire name (`field_type/mod.rs:201`); conversion target name only (`datum_convert.rs:1500`). TiKV likewise: `FieldTypeTp::Geometry = 0xff` (`def/field_type.rs:49`) with no `EvalType`, carrier or chunk arm. Both sides refuse it. |
| **Array** | **No** | No array `Datum` variant; `FieldType::is_array`/`with_array` exist (`field_type/mod.rs:914-920`) but the only array-producing path is an explicit `CAST(... AS ... ARRAY)`, which the native evaluator rejects (`tidb-expr/src/lib.rs:1007`). The bridge refuses arrays (`bridge.rs:48-50`, test `:556-559`). TiKV has no top-level array kind; JSON arrays are `EvalType::Json`. |
| **Raw** | **No** | Internal codec kind (`tidb-codec/src/package.rs:168-176`), never a scalar-function value; `datum_eval_type` returns `None` (`scalar_function.rs:123`) and the evaluator only rejects it (`coerce.rs:119-124`, `ops.rs:424-428`). TiKV has no `Raw`. |
| **MinNotNull / MaxValue** | **No** | Planner/ranger range bounds (`tidb-planner/src/ranger/points.rs:121-150`), not evaluator values; every evaluator path rejects them (`cast.rs:61`, `coerce.rs:53-55,167-169,199-201`). |
| **NewDate / Unspecified / Unknown** | **No** | Legacy/unused wire `FieldTypeTp` codes; no `Datum` value and no column is created with them. `bridge.rs` excludes NewDate defensively (`:561-563`); the native evaluator treats NewDate as Date only in metadata paths. |
| **UInt** | **No** | First-class (`Datum::UInt`, `datum/mod.rs:231`) but represented as `EvalType::Int` + `FieldTypeFlag::UNSIGNED`, which TiKV preserves end-to-end (`def/field_type.rs:329`, `codec/chunk/column.rs:91-99,316-328`). The bridge returns `Datum::UInt` from the Int carrier by reading the schema flag (`bridge.rs:574-595` test). |
| **Float32 (4-byte FLOAT)** | **No** | Represented as `EvalType::Real` + `FieldTypeTp::Float`; TiKV's chunk codec keeps the 4-byte cell (`codec/chunk/column.rs:62-63,101-110`), the raw decoder narrows to f32 (`codec/datum_codec.rs:434-439`), and the native bridge widens to f64 and re-narrows by result code (`bridge.rs:187-197,330-346`). |
| **Bit / BinaryLiteral** | **No** | `Bit` maps to `EvalType::Int` with special chunk handling (`def/eval_type.rs:63`, `codec/chunk/column.rs:83-90,162-178,339`); `b'...'`/`x'...'` lower to tipb `BitLiteral`/`BytesLiteral` (`pushdown_catalog.rs:1775-1782` and the `Datum`→`PbScalar` mapping at `:2078-2086`), never as a distinct wire value. |
| **Enum** | **No** | Already fully carried: `EvalType::Enum` (`def/eval_type.rs:81`), chunk codec (`codec/chunk/column.rs:142-146,296-305,338,957-1007`), raw decoder (`codec/datum_codec.rs:580-602,660-664`), standalone `Column::Enum` (`standalone.rs:70`), cast arms (`impl_cast.rs:80,91,117,148,158,172,191`), `validate_expr_return_type` allowance (`types/function.rs:280`). |
| **Time / Timestamp / Date** | **No** | Collapse onto `EvalType::DateTime` (`def/eval_type.rs:67-69`); the bridge validates wall fields and FSP (`bridge.rs:207-210`). |
| **JSON, VectorFloat32, Decimal, Duration, String/Bytes** | **No** | All present in the standalone carriers (`standalone.rs:62-72`) with codecs and validators. |

## 6. Recommended order

1. **Set — the only must-add.** Land it as the Enum analogue: `EvalType::try_from`,
   chunk/raw codecs with the comma-join name helper, `ChunkedVecSet` element-name
   setter, `Int`/`Bytes` hybrid borrow arms, standalone `Column::Set`, the
   `(Set, Int)` cast registration (kernel already exists), then the `bridge.rs`/
   `lowering.rs`/`pushdown_catalog.rs` adapter co-requisites.
2. **Harden Enum's `get_enum`** (`codec/chunk/column.rs:1000-1007` indexes
   `idx * fixed_len` on a var-length column) while adding `get_set`; otherwise the Set
   reader will copy a latent bug. Covered by the same round-trip fixture.
3. **Fix `ChunkedVecSet`/`ChunkedVecEnum` structural TODOs**
   (`chunked_vec_set.rs:22-23`, `chunked_vec_enum.rs:24`) so the shared element-name
   table is populated by `push_data`, not just in tests.
4. **Geometry / arrays: nothing to add.** Record them as explicit "unrepresentable,
   refused with an error" cases, which the removal plan already requires
   (`tikv-expression-removal-execplan.md`, Milestone E: "Record the remaining
   exclusions as explicit errors, not as silent native execution").

## 7. Open verification items (not done here; no build was run)

- The Set round-trip fixture must be captured from the **native Rust** encoding
  (bitmask in row/raw, `[u64 LE][comma-join name]` in chunk) before the TiKV decoder
  is written, per Milestone D of the removal plan.
- `Column::get_enum`'s `idx * fixed_len` may actually be reached through
  `Chunk::get_datum` on Enum columns in some executor; confirm the fix does not
  change existing Enum behavior.
- Confirm whether the removal path ever routes through
  `codec/table.rs::unflatten` (which rejects Enum/Set/Bit at `:281-284`).
- Confirm the exact set of `CastSetAs*` target types the native lowering emits for
  Set arguments, to bound the cast-arm additions in §4(d).
