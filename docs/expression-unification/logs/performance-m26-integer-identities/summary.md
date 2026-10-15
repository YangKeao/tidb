# M26: integer identities and result flags on the shared batch driver (2026-10-16)

One-argument RoundInt and IntAnyValue preserve all Int/UInt bits and NULL.
The private shared result policy now declares Signed/Unsigned/FirstArgument,
independently of operand unsigned permission. Integer identities require the
result flag to match the first argument. Bitwise unsigned and comparison signed
results keep their old contracts. No new driver or mathematical algorithm.

Native one-argument RoundIntNative calls the same round_int function as wire
RoundInt; it is an identity even beyond f64's53-bit exact range. Two-argument
ROUND is a different signature and remains native fallback, with an explicit
MAX-1→MAX golden test. This is not admission of scaled/noninteger ROUND.

For round_int_unsigned/any_value_int_unsigned only, both current/native probes
set test-schema EXPR_BATCH_PERF_INT_UNSIGNED=1. Original raw i/j cells are the
same64bits and now read with matching unsigned field flags. No new column or
payload pattern is introduced. The signed new query and all old controls retain
their original signed LongLong schema. Raw INPUT_SIGN headers record the flag.
This is test data metadata, not a production execution mode or value gate.

The three new workloads compare actual current batch to a clearly labeled
same-current-binary fallback ablation, and separately to true frozen original
native production. That fallback is NOT original native or historical TiKV.
Eleven existing control workloads instead compare distinct actual M25 and M26
batch binaries. No forced singleton route is used as a historical baseline.

Three alternating pairs;7samples75ms,1024selected rows and5selection/NULL shapes.
Latency is median of3process medians; ratios are medians of within-pair ratios.
Actual native dense runs after each pair on identical defaultLongLong inputs.
All2940current timing records/210workload-shape-pair checksum groups match,
including42fresh actual-native/current dense groups. All optimized/control
batch routes are prepared-only with0fallback/refusal. The fallback ablation is
explicitly selected, not an automatic refusal. Full rows remain in summary.json.

## Newly admitted operations

Dense ns/selected-row:

| workload | current fallback ablation | batch | paired ablation/batch | true native | native/batch |
|---|---:|---:|---:|---:|---:|
| any_value_int_unsigned | 1093.195 | 10.726 | 101.85x | 122.044 | 11.38x |
| round_int_signed | 946.231 | 8.369 | 113.31x | 100.852 | 12.05x |
| round_int_unsigned | 949.726 | 8.410 | 112.93x | 101.790 | 12.10x |

## Existing workload controls

These baseline cells are actual M25 batch code, unlike the new-family ablation
above. Flat and slower results remain visible.

| workload | M25 | M26 | paired M25/M26 | true native |
|---|---:|---:|---:|---:|
| any_value_datetime | 10.265 | 10.863 | 0.945x | 121.807 |
| any_value_duration | 13.495 | 15.048 | 0.897x | 121.206 |
| any_value_json | 122.028 | 130.890 | 0.937x | 144.192 |
| any_value_timestamp | 10.194 | 10.942 | 0.932x | 121.632 |
| any_value_vector | 69.911 | 71.791 | 0.972x | 161.506 |
| decimal_add | 130.581 | 131.733 | 0.991x | 69.519 |
| decimal_compare_lt | 74.109 | 75.852 | 0.978x | 169.726 |
| decimal_compare_nulleq | 74.086 | 75.977 | 0.975x | 164.625 |
| integer_add | 12.898 | 13.421 | 0.961x | 20.962 |
| lower | 32.836 | 34.490 | 0.952x | 154.917 |
| real_add | 13.089 | 13.872 | 0.945x | 107.310 |

## Independent M15 controls

| workload | shape | M15 | M26 | paired M15/M26 |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.131 | 10.910 | 0.932x |
| any_value_datetime | dense_null10 | 10.327 | 11.128 | 0.930x |
| any_value_datetime | duplicate | 10.320 | 11.212 | 0.923x |
| any_value_datetime | reverse | 10.462 | 11.203 | 0.934x |
| any_value_datetime | sparse_quarter | 16.176 | 17.089 | 0.947x |
| any_value_duration | dense | 14.421 | 15.191 | 0.948x |
| any_value_duration | dense_null10 | 14.027 | 14.155 | 0.991x |
| any_value_duration | duplicate | 14.583 | 15.088 | 0.962x |
| any_value_duration | reverse | 14.614 | 15.141 | 0.964x |
| any_value_duration | sparse_quarter | 20.351 | 21.064 | 0.966x |
| integer_add | dense | 12.852 | 13.406 | 0.960x |
| integer_add | dense_null10 | 12.615 | 13.182 | 0.957x |
| integer_add | duplicate | 13.108 | 13.817 | 0.945x |
| integer_add | reverse | 13.118 | 13.662 | 0.965x |
| integer_add | sparse_quarter | 19.763 | 20.546 | 0.963x |
| lower | dense | 33.061 | 34.531 | 0.957x |
| lower | dense_null10 | 31.184 | 32.465 | 0.959x |
| lower | duplicate | 33.452 | 34.761 | 0.962x |
| lower | reverse | 33.320 | 34.850 | 0.956x |
| lower | sparse_quarter | 42.913 | 44.678 | 0.956x |

All840records/60checksum groups agree. This older baseline preserves earlier regressions rather than resetting them away.

## Process repeatability

| workload | baseline range | M26 range |
|---|---:|---:|
| any_value_datetime | 10.206–10.309 | 10.843–10.985 |
| any_value_duration | 13.495–13.516 | 14.992–15.071 |
| any_value_int_unsigned | 1092.348–1093.724 | 10.725–10.776 |
| any_value_json | 120.977–122.721 | 130.291–132.056 |
| any_value_timestamp | 10.181–10.203 | 10.902–10.994 |
| any_value_vector | 69.809–69.966 | 71.648–74.228 |
| decimal_add | 129.769–130.716 | 131.565–131.790 |
| decimal_compare_lt | 74.026–74.282 | 75.794–76.019 |
| decimal_compare_nulleq | 74.017–74.172 | 75.765–75.998 |
| integer_add | 12.863–12.898 | 13.376–13.434 |
| lower | 32.809–32.849 | 34.361–34.552 |
| real_add | 13.066–13.113 | 13.808–13.876 |
| round_int_signed | 939.994–955.351 | 8.351–8.657 |
| round_int_unsigned | 948.916–951.126 | 8.346–8.429 |

## Longer confirmation of old regressions

Three additional reverse-initial-order pairs, seven150ms samples, actual M25/M26 code and three controls. All630records/45checksum groups agree. These are additional measurements, not replacements for the main matrix.

| workload | shape | M25 | M26 | paired M25/M26 |
|---|---|---:|---:|---:|
| any_value_datetime | dense | 10.186 | 10.854 | 0.939x |
| any_value_datetime | dense_null10 | 10.359 | 11.110 | 0.932x |
| any_value_datetime | duplicate | 10.329 | 11.064 | 0.933x |
| any_value_datetime | reverse | 10.371 | 11.015 | 0.941x |
| any_value_datetime | sparse_quarter | 16.101 | 16.825 | 0.957x |
| any_value_duration | dense | 13.533 | 14.997 | 0.900x |
| any_value_duration | dense_null10 | 13.343 | 14.171 | 0.942x |
| any_value_duration | duplicate | 13.520 | 15.014 | 0.901x |
| any_value_duration | reverse | 13.739 | 15.212 | 0.903x |
| any_value_duration | sparse_quarter | 19.478 | 21.102 | 0.921x |
| any_value_json | dense | 122.082 | 130.664 | 0.934x |
| any_value_json | dense_null10 | 114.908 | 123.115 | 0.933x |
| any_value_json | duplicate | 122.621 | 130.347 | 0.940x |
| any_value_json | reverse | 123.144 | 132.316 | 0.931x |
| any_value_json | sparse_quarter | 142.705 | 148.496 | 0.961x |

Separate Duration profiles,2472before/2557after samples with zero lost, identify constructor/import and fixed-output append as major costs. They do not establish the cause of the regression; the signature registry is not a leading self-time entry. Raw perf.data stays outside git; report text/run commands are retained. Profile runs do not overlap formal timing and are not used as benchmark latency samples.

Ranges are3process medians, not confidence intervals. CPU2 pinning does not isolate SMT/boost/thermal/other host effects. No compilation overlaps timing. No application QPS or all-function performance claim is made.

## Decision: retain the identity contract with confirmed tradeoffs

Signed ROUND8.369ns/row versus true native100.852 (12.05x), unsigned ROUND8.410
versus101.790 (12.10x), unsigned ANY_VALUE10.726 versus122.044 (11.38x).
Same-current fallback ablations946–1093ns remain separately labeled; their
roughly102–113x ratios are not historical-native gains. Retain the complete
integer identity contract and its exact result flags, with scaled/noninteger
ROUND excluded. No f64 conversion or value-range gate is added.

Old controls regress: Duration13.495→15.048ns (~12%), Datetime10.265→10.863 and
Timestamp10.194→10.942 (~6–7%), JSON122.028→130.890 (~7%), Int12.898→13.421,
Real13.089→13.872, LOWER32.836→34.490 (~4–6%), Vector69.911→71.791 (~3%).
DecimalPLUS130.581→131.733 and comparisons74.1→75.9 are also slower. Longer
reverse-order confirmation finds Datetime10.186→10.854, Duration13.533→14.997,
JSON122.082→130.664; all630checksums agree. These are repeatable tradeoffs,
not dismissed as noise. The cause is not isolated. New functionality and its
11–12x native gains justify retention, not a universal improvement claim.

Independent M15 controls also retain old losses: Int12.852→13.406, Datetime
10.131→10.910, Duration14.421→15.191 and LOWER33.061→34.531. Every shape and
process range remains visible. Duration profiles point at constructor/import
and fixed-output append as major costs, not proof of a regression cause. Next
measure a shared output-buffer experiment; do not remove value normalization
checks to chase a profile percentage. No application QPS claim is made.

Retained TiKVd5817d0bfa15ccfad1fe93bea82e779dadf5e4c9 / this TiDB report commit.
Private result facts replace the old Boolean; no duplicate contract/driver or
migration fallback remains. Existing signed/unsigned fixed-result families
keep their rules. Benchmark schema selection is test-only; all original control
schemas/data are unchanged.

## Validation/reproduction

Batch33/fullTiDB1385same4 and freshTiKV1031same6 names verified. New flag-admission and exact identity goldens coverUIntMAX/i64MIN/MAX/NULL,values beyond53bits,literals,nested calls and2050reverse/duplicate rows. ScaledROUND remains native fallback with a nonidentity golden. Actual SQL smoke3prepared, source flag headers verified. Scoped fmt, make lint and diff checks pass. Datatype516/565 are reused unchanged-core results. No fresh server/PD-TiKV/FIPS/workspace clippy pass. Inventory67/131static signatures is metadata, not SQL coverage percent.

validation.md pins exact source/binary/probe hashes, schema flag choice and commands. Build current as after-bin, original plus the archived probe as frozen-bin and actual M25 as before-bin. collect.py/reference-control.py generate logs; summarize.py/summarize-reference.py assert counts/checksums and produce JSON; make-report.py regenerates this receipt using decision.md. Binaries stay outside git.
