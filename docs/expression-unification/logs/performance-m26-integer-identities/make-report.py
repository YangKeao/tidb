import json
from pathlib import Path
r=Path(__file__).resolve().parent
rows=json.loads((r/'summary.json').read_text())
dense=[x for x in rows if x['scenario']=='dense']
new=[x for x in dense if x['workload'] in ['round_int_signed','round_int_unsigned','any_value_int_unsigned']]
controls=[x for x in dense if not x['workload'] in ['round_int_signed','round_int_unsigned','any_value_int_unsigned']]
s='''# M26: integer identities and result flags on the shared batch driver (2026-10-16)

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
'''
for x in new:s+=f"| {x['workload']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.2f}x | {x['native_median']:.3f} | {x['native_median']/x['after_median']:.2f}x |\n"
s+='''
## Existing workload controls

These baseline cells are actual M25 batch code, unlike the new-family ablation
above. Flat and slower results remain visible.

| workload | M25 | M26 | paired M25/M26 | true native |
|---|---:|---:|---:|---:|
'''
for x in controls:s+=f"| {x['workload']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.3f}x | {x['native_median']:.3f} |\n"
s+='\n## Independent M15 controls\n\n'
s+='| workload | shape | M15 | M26 | paired M15/M26 |\n|---|---|---:|---:|---:|\n'
for x in json.loads((r/'reference-control-summary.json').read_text()):s+=f"| {x['workload']} | {x['scenario']} | {x['m15_median']:.3f} | {x['current_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\nAll840records/60checksum groups agree. This older baseline preserves earlier regressions rather than resetting them away.\n\n## Process repeatability\n\n'
s+='| workload | baseline range | M26 range |\n|---|---:|---:|\n'
for x in dense:s+=f"| {x['workload']} | {min(x['before']):.3f}–{max(x['before']):.3f} | {min(x['after']):.3f}–{max(x['after']):.3f} |\n"
s+='\n## Longer confirmation of old regressions\n\nThree additional reverse-initial-order pairs, seven150ms samples, actual M25/M26 code and three controls. All630records/45checksum groups agree. These are additional measurements, not replacements for the main matrix.\n\n| workload | shape | M25 | M26 | paired M25/M26 |\n|---|---|---:|---:|---:|\n'
for x in json.loads((r/'regression-summary.json').read_text()):s+=f"| {x['workload']} | {x['scenario']} | {x['before_median']:.3f} | {x['after_median']:.3f} | {x['paired_speedup']:.3f}x |\n"
s+='\nSeparate Duration profiles,2472before/2557after samples with zero lost, identify constructor/import and fixed-output append as major costs. They do not establish the cause of the regression; the signature registry is not a leading self-time entry. Raw perf.data stays outside git; report text/run commands are retained. Profile runs do not overlap formal timing and are not used as benchmark latency samples.\n'
s+='\nRanges are3process medians, not confidence intervals. CPU2 pinning does not isolate SMT/boost/thermal/other host effects. No compilation overlaps timing. No application QPS or all-function performance claim is made.\n\n'
s+=(r/'decision.md').read_text()
s+='\n## Validation/reproduction\n\nBatch33/fullTiDB1385same4 and freshTiKV1031same6 names verified. New flag-admission and exact identity goldens coverUIntMAX/i64MIN/MAX/NULL,values beyond53bits,literals,nested calls and2050reverse/duplicate rows. ScaledROUND remains native fallback with a nonidentity golden. Actual SQL smoke3prepared, source flag headers verified. Scoped fmt, make lint and diff checks pass. Datatype516/565 are reused unchanged-core results. No fresh server/PD-TiKV/FIPS/workspace clippy pass. Inventory67/131static signatures is metadata, not SQL coverage percent.\n\nvalidation.md pins exact source/binary/probe hashes, schema flag choice and commands. Build current as after-bin, original plus the archived probe as frozen-bin and actual M25 as before-bin. collect.py/reference-control.py generate logs; summarize.py/summarize-reference.py assert counts/checksums and produce JSON; make-report.py regenerates this receipt using decision.md. Binaries stay outside git.\n'
(r/'summary.md').write_text(s)
