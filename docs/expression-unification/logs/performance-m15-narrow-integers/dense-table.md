| input type | workload | current fallback ablation | batch | ablation speedup | frozen native | native / batch |
|---|---|---:|---:|---:|---:|---:|
| int24 | integer_add | 973.244 | 12.840 | 75.80x | 21.822 | 1.70x |
| int24 | integer_compare_lt | 1097.262 | 11.303 | 97.05x | 72.284 | 6.40x |
| long | integer_add | 976.054 | 12.840 | 76.13x | 21.862 | 1.70x |
| long | integer_compare_lt | 1085.028 | 11.301 | 96.24x | 72.246 | 6.39x |
| short | integer_add | 977.411 | 12.879 | 75.88x | 21.772 | 1.69x |
| short | integer_compare_lt | 1085.797 | 11.368 | 95.19x | 72.254 | 6.36x |
| tiny | integer_add | 978.241 | 12.873 | 75.84x | 21.782 | 1.69x |
| tiny | integer_compare_lt | 1097.769 | 11.293 | 97.33x | 72.018 | 6.38x |
