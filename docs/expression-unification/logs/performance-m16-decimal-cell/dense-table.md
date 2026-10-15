| workload | M15 batch | M16 batch | paired speedup | frozen native | native / M16 |
|---|---:|---:|---:|---:|---:|
| any_value_datetime | 10.156 | 10.564 | 0.96x | 122.356 | 11.58x |
| any_value_duration | 14.418 | 14.750 | 0.98x | 122.708 | 8.32x |
| any_value_json | 133.266 | 133.404 | 1.00x | 141.862 | 1.06x |
| any_value_timestamp | 10.163 | 10.570 | 0.96x | 122.464 | 11.59x |
| any_value_vector | 71.898 | 70.557 | 1.02x | 154.897 | 2.20x |
| decimal_add | 228.438 | 182.680 | 1.25x | 70.089 | 0.38x |
| decimal_compare_lt | 148.866 | 102.485 | 1.45x | 167.289 | 1.63x |
| decimal_compare_nulleq | 148.356 | 102.584 | 1.45x | 164.923 | 1.61x |
| integer_add | 12.878 | 13.569 | 0.95x | 21.791 | 1.61x |
| lower | 32.986 | 33.186 | 0.99x | 152.290 | 4.59x |
| real_add | 13.724 | 14.039 | 0.98x | 106.288 | 7.57x |
