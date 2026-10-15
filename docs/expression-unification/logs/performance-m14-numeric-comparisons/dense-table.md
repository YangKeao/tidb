| comparison | current fallback ablation | batch | ablation speedup | frozen native | native / batch |
|---|---:|---:|---:|---:|---:|
| `decimal_compare_eq` | 1634.582 | 147.140 | 11.14x | 175.654 | 1.19x |
| `decimal_compare_ge` | 1633.398 | 147.200 | 11.12x | 176.259 | 1.20x |
| `decimal_compare_gt` | 1643.559 | 147.545 | 11.13x | 178.813 | 1.21x |
| `decimal_compare_le` | 1655.036 | 147.408 | 11.24x | 177.132 | 1.20x |
| `decimal_compare_lt` | 1633.466 | 147.324 | 11.07x | 175.773 | 1.19x |
| `decimal_compare_ne` | 1644.747 | 147.446 | 11.13x | 175.229 | 1.19x |
| `decimal_compare_nulleq` | 1603.675 | 147.328 | 10.89x | 166.678 | 1.13x |
| `real_compare_eq` | 1217.180 | 11.719 | 103.77x | 80.790 | 6.89x |
| `real_compare_ge` | 1222.839 | 11.809 | 102.96x | 80.429 | 6.81x |
| `real_compare_gt` | 1225.585 | 11.760 | 103.98x | 80.689 | 6.86x |
| `real_compare_le` | 1219.966 | 11.638 | 104.39x | 80.967 | 6.96x |
| `real_compare_lt` | 1223.032 | 11.618 | 105.14x | 80.729 | 6.95x |
| `real_compare_ne` | 1209.555 | 11.724 | 103.26x | 80.609 | 6.88x |
| `real_compare_nulleq` | 1217.628 | 11.846 | 102.81x | 80.363 | 6.78x |
