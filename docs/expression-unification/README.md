# Expression unification experiment

Current paired checkpoint **decimal-binary-decode-195**. Functional Demo: **240/245 (97.96%)**, strict **0**.

TiKV now uniquely owns Decimal fixed binary size, writer, and decoder algorithms. TiDB retains only concrete Decimal/failure projection; its duplicate word reader, word-count clamp, and decoder body are deleted. Four focused filters are GREEN. [Evidence](evidence/decimal-binary-decode-checkpoint.md) · [receipts](logs/decimal-binary-decode-summary.txt).

Broader M2 and M6 final acceptance remain active. PR readiness is not claimed.
