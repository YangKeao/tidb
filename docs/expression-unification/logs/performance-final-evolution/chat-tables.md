For 1 row（AST，复用 lane；ns/eval）：

| workload | frozen native | unified | unified / frozen |
|---|---:|---:|---:|
| integer_add | 30.997 | 1,020.009 | 32.907x |
| lazy_if | 92.582 | 1,997.194 | 21.413x |
| strcmp | 168.482 | 1,446.590 | 8.586x |
| decimal_add | 339.710 | 2,235.248 | 6.571x |
| json_type | 227.548 | 1,145.166 | 5.033x |
| regexp_like | 7,700.182 | 9,123.701 | 1.188x |

For batched multiple rows（unified / frozen native）：

| workload | 1 | 8 | 64 | 256 | 1024 |
|---|---:|---:|---:|---:|---:|
| integer_add | 26.976x | 9.387x | 1.924x | 0.886x | 0.597x |
| integer_add_literal | 21.481x | 8.839x | 1.746x | 0.743x | 0.455x |
| abs | 17.208x | 2.347x | 0.348x | 0.136x | 0.078x |
| lazy_if | 29.012x | 30.752x | 31.069x | 30.950x | 31.168x |
| coalesce_nullif | 17.895x | 19.172x | 19.154x | 19.729x | 19.262x |
| strcmp | 15.036x | 14.538x | 14.917x | 14.800x | 15.023x |
| lower | 11.893x | 1.836x | 0.422x | 0.260x | 0.216x |
| upper | 11.679x | 1.826x | 0.411x | 0.251x | 0.205x |
| concat | 13.272x | 13.608x | 13.517x | 13.608x | 13.639x |
| substring | 7.414x | 1.114x | 0.254x | 0.162x | 0.134x |
| decimal_add | 16.648x | 4.617x | 2.149x | 1.845x | 1.800x |
| json_type | 7.622x | 7.529x | 7.436x | 7.501x | 7.547x |
| regexp_like | 6.172x | 6.081x | 6.082x | 5.948x | 5.775x |
