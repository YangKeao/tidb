# Temporal raw storage

`temporal-raw-storage-199` makes TiKV `NativeTemporalValue` the sole storage for unchecked calendar bits plus independent `TimeType` and FSP metadata. Its raw constructor deliberately performs no normalization; checked constructors remain separate.

TiDB `Time` deletes its `{core, kind, fsp}` fields and becomes a thin shared-value wrapper while preserving `CoreTime`, public methods, setters, Go raw encoding, timezone behavior, and independence between low raw bits and metadata. TiKV's wire `Time(u64)` is intentionally not used because it overlays metadata into the raw word.

Three focused filters are GREEN. [Receipts](../logs/temporal-raw-storage-summary.txt).
