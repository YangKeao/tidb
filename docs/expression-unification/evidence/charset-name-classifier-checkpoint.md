# Charset name classifier

`charset-name-classifier-197` makes TiKV FieldType `Charset` the single owner of seven canonical names, allocation-free ASCII-case-insensitive classification, and the legacy `utf8mb3` alias. Its existing wire-facing `from_name` adds an exact-canonical filter, preserving rejection of uppercase and aliases.

TiDB `Charset::from_name` deletes its seven-name branch table and only projects the shared enum into the native public enum. Three focused filters are GREEN. [Receipts](../logs/charset-name-classifier-summary.txt).

The audited raw Duration wrapper was not aliased because TiDB owns substantial inherent API around it; the audited packed-Time candidate pointed in the wrong ownership direction. Neither was changed.
