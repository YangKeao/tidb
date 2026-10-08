# Duration raw storage

`duration-raw-parts-198` makes TiKV `NativeDurationParts` the sole `{nanoseconds, fsp}` raw-storage representation, including unvalidated chunk metadata. It now supplies the raw constructor/accessors and required value derives.

TiDB `MySqlDuration` deletes its duplicate fields and retains its public/inherent API as a transparent shared-parts wrapper. Conversion calls pass the stored shared value directly, while public TIME limit constants reference the SDK duration constants.

Four focused filters are GREEN. A misspelled display filter selected zero tests and is explicitly excluded; a real public-method test replaced it. [Receipts](../logs/duration-raw-parts-summary.txt).
