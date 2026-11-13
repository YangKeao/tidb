#!/usr/bin/env python3
"""Cost witnesses for the fixed checkpoint7 pair; estimates are not coverage credit."""
import argparse
import collections
import csv
import hashlib
import json
from pathlib import Path
import re
import subprocess

TIDB = '4958bb6959104db9b17258212c887befaa84d1d7'
TIKV = '3ad4dc75eec6af91b77f2c36f3d10ee61073b942'
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--tidb', type=Path, required=True)
p.add_argument('--tikv', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=True)

def git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), *args], text=True)

# Families reference these shared work packages, rather than charging each helper
# once per family. Units are planning ownership, not proof of disjoint future line edits.
# add/delete/test-add/test-edit are remaining changed-line ranges after cp7.
# test-edit counts BOTH sides of modified existing tests; tests are not discarded.
GROUPS = {
 'operator_numeric': ([900,1800],[900,1700],[300,700],[100,250],
  'builtin_arithmetic.rs ops.rs ops/operand.rs ops/integer_coerce.rs ops/real_coerce.rs',
  'Signed/unsigned arithmetic, mixed Decimal/Real, native overflow text, division policy and bit width; shared expression algorithms only, retain storage codecs.',
  'Unsigned edges; mixed types; signed zero/NaN; 81-digit bounds; NULL demand; exact warning code/count.'),
 'math': ([650,1300],[800,1400],[250,650],[100,250],
  'math_fn/mod.rs math_fn/go_exp_log.rs math_fn/go_trig.rs',
  'Retain Go-compatible transcendental behavior and rounding; share numeric coercion with cast package. RAND state identity is charged to lifecycle, not every math function.',
  'ULP/large-angle vectors; signed-zero/NaN; decimal rounding; numeric prefixes; seed/reset lifetime.'),
 'string': ([1200,2600],[1800,3300],[600,1400],[200,500],
  'string_fn.rs string_packet.rs string_signature.rs builtin_ext/string2.rs',
  'Unicode case and byte/character indexing, result carrier/collation, packet limits and type-dependent demand. Charset conversion charged once to convert_charset.',
  'Invalid UTF8, GBK/binary, multibyte positions, packet warnings, NULL/effect ordering, output collation.'),
 'pattern': ([350,900],[350,600],[250,600],[100,250],
  'builtin_ext/regexp.rs like.rs',
  'Shared regex/LIKE algorithm with native diagnostics, sequential argument checks, constant caches and replacement instruction semantics; cache lifetime charged separately.',
  'Invalid pattern/UTF8/flags; NULL skips later writes; Unicode positions; empty matches; replacement escapes; rebinding.'),
 'temporal': ([2200,4800],[2800,5200],[900,2000],[350,800],
  'time_fn/mod.rs time_fn/add_sub.rs time_fn/calendar.rs time_fn/convert_tz.rs time_fn/duration_parse.rs time_fn/extract.rs time_fn/session_tz.rs time_literal.rs',
  'Migrate only needed native calendar/format/parse policies into shared execution; SDK time representation is not native coercion equivalence. Retain timezone/storage APIs and statement clocks.',
  'Zero/invalid dates; DST/named zones; SQL modes; fsp; calendar carries; duration versus datetime signatures; stable statement time.'),
 'json': ([1600,3600],[1700,3000],[700,1600],[300,650],
  'builtin_ext/json/mod.rs builtin_ext/json/construct.rs builtin_ext/json/merge.rs builtin_ext/json/modify.rs builtin_ext/json/path.rs builtin_ext/json/predicate.rs builtin_ext/json/report.rs builtin_ext/json/search.rs builtin_ext/json/text.rs builtin_ext/json/value.rs builtin_ext/json2.rs',
  'Native typed JSON versus text, argument ParseToJSONFlag, path grammar/equality/rendering and CRC32. Reuse SDK binary JSON where compatible; preserve differing native policies. Schema service integration charged to exceptions.',
  'Typed versus text JSON; invalid/path wildcard; numeric precision; key ordering; duplicate keys; mutation path cache; schema errors; CRC32.'),
 'crypto': ([250,700],[350,550],[180,450],[80,200],
  'builtin_ext/crypto.rs',
  'Reuse digest/compression/AES libraries; adapt AES lazy argument requirements, mode/IV/session policy, encoding and framing diagnostics. Password service capability charged to exceptions.',
  'AES mode/IV/key; skipped argument effects; corrupt compression; raw bytes; digest variants; password session policy.'),
 'network': ([100,300],[120,250],[80,220],[40,100],
  'builtin_ext/compare2.rs',
  'Network parsing helpers are inside compare2; budget only inet/is_ipv functions here, comparison helpers below. Preserve invalid-input NULL and byte-width behavior.',
  'IPv4 shorthand/overflow; IPv6 mapped/compat; binary length; invalid UTF8 and NULL.'),
 'vector': ([80,250],[70,130],[100,250],[30,80],
  'builtin_ext/vec.rs',
  'Reuse vector kernels after native vector carrier and dimension/NaN behavior match. Carrier construction charged to cast, not eight times.',
  'Dimension mismatch; empty vectors; zero norm; NaN/Inf; parse/format; signed-zero accumulation.'),
 'misc': ([250,700],[250,450],[150,400],[60,180],
  'builtin_ext/misc.rs builtin_ext/info.rs',
  'UUID byte transforms, grouping metadata, formatting and hash functions; service plan functions excluded from this package. Identity-return functions need complete Datum carriers.',
  'UUID layout/version/timestamp; invalid binary; grouping bits; identity carrier preservation; hash signedness.'),
 'comparison': ([500,1200],[500,850],[300,750],[150,350],
  'builtin_compare.rs builtin_ext/compare2.rs',
  'Typed signed/unsigned/Decimal/time/JSON comparison, collation, IN preparation, extrema and INTERVAL demand. Network helpers in same file charged only to network.',
  'Mixed domains; collation weights/padding; NULL IN; prepared IN sets; extrema/time errors; lazy interval effects.'),
 'control': ([300,750],[0,0],[300,650],[120,300],
  'builtin_op.rs scalar_function.rs',
  'Generalize shared demand/coercion for CASE/COALESCE/logical/NULLIF; PB eager versus native scalar/vector stays explicit. Whole old dispatcher deletion charged to integration.',
  'Selected/unselected errors and writes; three-valued logic; mixed branch types; PB eager IF; zero/duplicate rows.'),
 'cast': ([1800,4000],[700,1800],[700,1600],[300,700],
  'cast.rs coerce.rs builtin_cast_semantics.rs arg_eval_type.rs',
  'Single shared coercion layer for full native Decimal/Real/unsigned/enum/set/bit/time/duration/JSON/vector; preserve declared result shape. Do not relocate entire datatype storage package.',
  'All admitted source/result domains; strict/warning modes; string prefix; rounding; timezone; hidden precision; metadata.'),
 'convert_charset': ([400,1000],[100,200],[200,500],[80,200],
  'convert_charset.rs',
  'Share BinAware/to_binary/from_binary/CONVERT USING policies. Reuse existing charset/collation tables without copying generated tables into expression kernel.',
  'GBK/UTF8/binary; invalid sequence; source-versus-result collation; warning/error and packet boundary.'),
 'exceptions': ([300,1000],[0,200],[200,600],[80,200],
  'builtin_ext/info.rs builtin_ext/crypto.rs builtin_ext/json/report.rs',
  'Five existing exceptions stay in the245 denominator. Budget only concrete service capabilities/wrappers; underlying plan/schema/password services remain owned outside shared loop. Full service relocation is unbounded, not hidden in this interval.',
  'Unavailable service; session identity/policy; exact errors; parameter changes; invalid schemas/plans; sensitive output behavior.'),
 'input_lifecycle': ([1200,2600],[100,350],[600,1300],[200,450],
  'column.rs constant.rs context.rs',
  'Parameter rebinding, correlated/ordinary columns, complete immutable carriers, program validity, statement/evaluator reset, RAND identity after destruction, caches and scratch cleanup.',
  'Rebind values/types/context; column remap; empty/duplicate selections; clone/drop/recreate; cache invalidation; concurrent sessions; failures then reuse.'),
 'integration': ([900,2200],[2800,6000],[450,1100],[400,1000],
  'lib.rs func.rs expression.rs scalar_function.rs evaluator.rs simple_expr.rs pb_predicate.rs distsql_builtin.rs',
  'Build native AST/PB into shared program; integrate projection/filter/sort/aggregate and Unistore. Retire only runtime evaluation branches; retain construction/type/planner APIs and compatibility entry adapters.',
  'Real SQL entry routes; AST/PB differences; filter/sort/aggregate effects; Unistore response errors; plan cache; release performance.'),
 'engine': ([650,1500],[80,300],[350,750],[120,300],
  'shared_kernel.rs',
  'Generalized native signature mapping and shared SDK control/type validation; remove remaining nested execution only when PB callers migrate. No second native evaluator or host-resume protocol.',
  'Malformed programs; deep controls; batching and side effects; output traversal; diagnostics policy; cancellation.'),
}
# These split existing GROUPS.add ranges; they are NOT additional charges.
LARGEST_SPLIT = {
 'temporal': {'semantic_add':[1400,3200], 'boundary_add':[800,1600]},
 'integration': {'semantic_add':[200,600], 'boundary_add':[700,1600]},
 'json': {'semantic_add':[1000,2600], 'boundary_add':[600,1000]},
 'cast': {'semantic_add':[1200,2800], 'boundary_add':[600,1200]},
 'string': {'semantic_add':[800,1800], 'boundary_add':[400,800]},
}
for group,parts in LARGEST_SPLIT.items():
    assert [sum(v[i] for v in parts.values()) for i in (0,1)] == GROUPS[group][0]

# Exact per-family audit differences beyond the common work-package contract.
SPECIAL = {
 'md5':'cp7 proves byte/NULL output on both entry types; arbitrary native argument coercion still belongs to cast.',
 'mul':'cp7 fixes hidden precision via native checked-i128 branch; remaining unsigned/mixed signatures are not covered.',
 'plus':'Real scalar plus eagerly demands RHS even on NULL; other numeric signatures differ; avoid blanket guard.',
 'div':'cp7 wide Decimal policy proven only admitted matching domains; remaining mixed/unsigned/shape rules need mapping.',
 'rand':'Integer seed live-instance/cross-batch probes passed; destroyed-address reuse/reset and broader seed conversion missing.',
 'if':'cp7 numeric branches proven; string/decorated/mixed branch coercions remain rejected; PB IF remains eager.',
 'regexp_like':'cp7 argument NULL/UTF8 demand and1105 diagnostics verified; full signature/collation scope not proved.',
 'json_length':'cp7 parses/checks native JSON through shared nodes; typed JSON/path variants remain a separate expansion.',
 'json_sum_crc32':'No original SDK function-name match establishes equivalence; inspect native recursive CRC semantics and binary rendering before sharing.',
 'json_schema_valid':'PreparedJsonSchema caches draft201909 validation and lazy external refs; NULL document must skip retrieval; retain diagnostics, reset/cache and file/HTTP capability boundaries.',
 'validate_password_strength':'Uses concrete password-validation/session identity policy; retain service implementation outside expression scheduling.',
 'tidb_decode_plan':'Calls tidb_util::plancodec::decode_plan; malformed input returns original bytes; retain codec for other callers.',
 'tidb_decode_binary_plan':'Calls plancodec::decode_binary_plan; malformed input yields empty string plus1105 warning, unlike textual decode; retain codec owner.',
 'tidb_encode_sql_digest':'Calls tidb_parser::normalize_digest, not raw SHA over input; retain parser normalizer for non-expression callers.',
 'date_literal':'Parse/build-time cast semantics in time_literal and rewriter, not only a runtime time function.',
 'timestamp_literal':'Build-time timezone/SQL-mode/fsp semantics plus runtime carrier; test cached/rebound plans.',
 'between':'AST lowers with repeated operand/demand semantics; a generic comparison kernel does not eliminate builder work.',
 'weight_string':'Collation weight bytes and declared width/BINARY padding; collation tables stay owned by datatype.',
 'any_value':'Identity result must retain complete Datum; byte-only or fixed Decimal conversion loses supported metadata.',
 'name_const':'First argument labels output, second retains complete carrier; do not evaluate it as generic string formatting.',
 'grouping':'Requires prepared grouping metadata, not a pure byte/int leaf.',
}
ALIASES = {'ceil':'ceil_floor','floor':'ceil_floor','round':'round_or_truncate','truncate':'round_or_truncate',
 'left':'str_take','right':'str_take','lower':'case_convert','upper':'case_convert','instr':'locate',
 'lpad':'pad','rpad':'pad','insert_func':'str_insert','curdate':'current_date','curtime':'current_time',
 'dayofmonth':'day_of_month','dayofweek':'day_of_week','dayofyear':'day_of_year','weekofyear':'week_of_year_builtin',
 'datediff':'date_diff','timediff':'time_diff','timestampdiff':'timestamp_diff','date_sub':'date_add',
 'json_insert':'json_modify','json_set':'json_modify','json_replace':'json_modify','json_array_append':'json_array_append',
 'addtime':'add_sub_time','subtime':'add_sub_time','sha1':'hash_unary','sha2':'sha2_hash','password':'password_hash',
 'sm3':'sm3_hash','decode':'sql_decode','encode':'sql_encode','tidb_decode_plan':'decode_plan',
 'tidb_decode_binary_plan':'decode_binary_plan','tidb_encode_sql_digest':'encode_sql_digest',
 'is_ipv4':'is_ipv4','is_ipv6':'is_ipv6','format':'format_num','convert_charset':'convert_using'}

# A lexical mask removes comments/literals and cfg(test) items, preserving newlines.
# shortcut: lexical scanner, not Rust name resolution; require semantic analysis before deletion.
# Escaped newlines are legal in Rust strings and must not desynchronize the mask.
TOKEN = re.compile(r'//[^\n]*|/\*[\s\S]*?\*/|(?:br|r)(\#*)"[\s\S]*?"\1|b?"(?:\\[\s\S]|[^"\\])*"|b?\'(?:\\.|[^\'\\\n])\'')
def mask(text):
    return TOKEN.sub(lambda m: ''.join('\n' if c=='\n' else ' ' for c in m[0]), text)
def production(text):
    clean = mask(text)
    spans = []
    for m in re.finditer(r'#\[cfg\(test\)\]', clean):
        start = m.start(); pos = m.end()
        opening = clean.find('{',pos); semi = clean.find(';',pos)
        if semi != -1 and (opening == -1 or semi < opening): end=semi+1
        elif opening != -1:
            depth=1;end=opening+1
            while depth and end<len(clean):
                depth += (clean[end]=='{')-(clean[end]=='}');end+=1
        else: raise ValueError('unclosed cfg(test) item')
        spans.append((start,end))
    chars=list(text)
    for start,end in spans:
        chars[start:end]=['\n' if c=='\n' else ' ' for c in chars[start:end]]
    return ''.join(chars)

# Keep continued strings from hiding test markers; retain production after tests.
probe='fn before() { let text = "continued\\\nstring"; }\n#[cfg(test)]\nfn test_one() { let x = "}"; }\nfn after() {}\n#[cfg(test)]\nmod tests { fn nested() {} }\n'
filtered=production(probe)
assert 'fn before' in filtered and 'fn after' in filtered
assert 'fn test_one' not in filtered and 'fn nested' not in filtered
for values in GROUPS.values():
    assert all(0 <= low <= high for low,high in values[:4])

sources={}
for repo,root,rev,prefix in [('tidb',a.tidb,TIDB,'rust/crates/tidb-expr/src'),('tikv',a.tikv,TIKV,'components/tidb_query_expr/src')]:
    for path in git(root,'ls-tree','-r','--name-only',rev,'--',prefix).splitlines():
        if not path.endswith('.rs') or '/tests/' in path or path.endswith(('_tests.rs','/tests.rs')) or Path(path).name.startswith('tests_'):continue
        text=git(root,'show',rev+':'+path)
        sources[repo+':'+path]=(text,production(text))
# Reviewer regression: line-continued strings must not hide later test modules.
rewriter = sources['tidb:rust/crates/tidb-expr/src/rewriter.rs'][1].splitlines()
assert not rewriter[3716].strip() and not rewriter[3727].strip()

ledger=json.loads(git(a.tidb,'show',TIDB+':rust/docs/shared-kernel/coverage-candidates.json'))
assert ledger['denominator']==245 and len(set(ledger['exceptions']))==5
rows=[]
for r in ledger['rows']:
    family=r['family'];group='exceptions' if family in ledger['exceptions'] else r['category']
    files=GROUPS[group][4].split()
    # Family-level production witnesses prefer definitions, then actual dispatch;
    # dispatcher syntax witnesses are explicitly not algorithm ownership proof.
    aliases={family,ALIASES.get(family,family)}
    defs=[];dispatch=[]
    for key,(_,text) in sources.items():
        if not key.startswith('tidb:'):continue
        path=key.split(':',1)[1]; short=path.removeprefix('rust/crates/tidb-expr/src/')
        if short not in files and short not in ['func.rs','scalar_function.rs','lib.rs','builtin_op.rs','builtin_arithmetic.rs','builtin_compare.rs']:continue
        for n,line in enumerate(text.splitlines(),1):
            if re.match(r'\s*(?:pub(?:\([^)]*\))?\s+)?(?:const\s+)?fn\s+('+ '|'.join(map(re.escape,aliases))+r')\b',line):defs.append(f'{path}:{n}')
            if line.lstrip().startswith(('//','*')):continue
            if any('"'+name+'"' in line for name in set(r['spellings']+[family]+[s.upper() for s in r['spellings']+[family]])):
                dispatch.append(f'{path}:{n}')
    # Operator/language syntax uses enum branches, so provide explicit owning API.
    if not defs and not dispatch:
        fallback={'cast':('cast.rs','cast'),'operator_numeric':('builtin_arithmetic.rs','eval'),
         'comparison':('builtin_compare.rs','eval'),'control':('builtin_op.rs','eval')}
        path,needle=fallback.get(group,('func.rs','eval_func_values'))
        key='tidb:rust/crates/tidb-expr/src/'+path
        for n,line in enumerate(sources[key][1].splitlines(),1):
            if re.search(r'\bfn\s+\w*'+needle,line):defs.append(key.split(':',1)[1]+':'+str(n));break
    if family=='between':
        dispatch=[f'rust/crates/tidb-expr/src/lib.rs:{n}' for n,line in enumerate(sources['tidb:rust/crates/tidb-expr/src/lib.rs'][1].splitlines(),1) if 'Expr::Between {' in line]
    assert defs or dispatch, family
    sdk=[]
    for anchor in r['sdk_candidates']:
        file,n=anchor.rsplit(':',1);key='tikv:'+file
        # Old candidate line offsets differ after edits; locate declaration name
        # from original text, then verify its production declaration in cp7.
        old=git(a.tikv,'show',ledger['tikv_base']+':'+file).splitlines()[int(n)-1]
        m=re.search(r'\bfn\s+(\w+)',old)
        if not m or m[1].startswith('test'):continue
        for i,line in enumerate(sources[key][1].splitlines(),1):
            if re.match(r'\s*(?:pub(?:\([^)]*\))?\s+)?fn\s+'+m[1]+r'\b',line):sdk.append(file+':'+str(i))
    rows.append({'family':family,'group':group,'exception':family in ledger['exceptions'],
      'native_definitions':';'.join(sorted(set(defs))), 'native_dispatch':';'.join(sorted(set(dispatch))),
      'sdk_declarations':';'.join(sorted(set(sdk))),
      'algorithm_scope':GROUPS[group][5], 'specific_difference':SPECIAL.get(family,'Apply group contract to all overloads; declaration match is not equivalence proof.'),
      'extra_tests':GROUPS[group][6],
      'cost_refs':';'.join(dict.fromkeys([group,'cast','input_lifecycle','engine','integration'])),
      'retirement':'Conditional group algorithm/runtime branches only; retain until all callers use shared program. See caller-witnesses.tsv; storage/planner/service APIs retained.',
      'unresolved':'Per-overload equivalence and exact deletion slice are not yet proved; no verified-coverage credit.'})
assert len(rows)==len({r['family'] for r in rows})==245
with (a.output/'family-costs.tsv').open('w') as f:
    writer=csv.DictWriter(f,fieldnames=list(rows[0]),delimiter='\t',lineterminator='\n');writer.writeheader();writer.writerows(rows)

units={}
for group,values in GROUPS.items():
    for short in values[4].split():
        key='tidb:rust/crates/tidb-expr/src/'+short
        assert key in sources, key
        raw,prod=sources[key]
        units.setdefault(key,{'groups':[],'raw_lines':len(raw.splitlines()),
           'non_test_nonblank_lines':sum(bool(s.strip()) for s in prod.splitlines()),
           'sha256':hashlib.sha256(raw.encode()).hexdigest()})['groups'].append(group)
CALLERS = {
 'native_entry': r'tidb_expr::(eval_expression_once|eval_in|apply_binary|apply_unary|avg_of|date_add_interval)',
 'decimal_algorithm': r'\.(mul_mysql|div_mysql|add_mysql|sub_mysql)\(',
 'planner_lifecycle': r'(replace_cached_value|invalidate_cached_arguments|param_marker|correlated_column)',
 'service_dependencies': r'(plancodec::|normalize_digest\(|password_validation::|prepare_json_schema|LocalSchemaRetriever)',
 'algorithm_dispatch': r'\b(math_fn::|time_fn::|string_fn::|builtin_ext::|ops::eval_binary|cast::cast)',
}
pattern='|'.join(CALLERS.values())
proc=subprocess.run(['git','-C',str(a.tidb),'grep','-n','-E',pattern,TIDB,'--','rust/crates'],capture_output=True,text=True)
assert proc.returncode in (0,1),proc.stderr
witnesses=[]; cache={}
for hit in proc.stdout.splitlines():
    _,path,n,line=hit.split(':',3);n=int(n)
    if not path.endswith('.rs') or '/tests/' in path or path.endswith(('_tests.rs','/tests.rs')) or Path(path).name.startswith('tests_'):continue
    if path not in cache:cache[path]=production(git(a.tidb,'show',TIDB+':'+path)).splitlines()
    line=cache[path][n-1].strip()
    if not line or line.startswith(('//','*')):continue
    for kind,pattern in CALLERS.items():
        if re.search(pattern,line):witnesses.append([kind,path,n,line])
with (a.output/'caller-witnesses.tsv').open('w') as f:
    writer=csv.writer(f,delimiter='\t',lineterminator='\n');writer.writerow(['kind','path','line','source']);writer.writerows(witnesses)
summary={'tidb':TIDB,'tikv':TIKV,'denominator':245,'exceptions_included':ledger['exceptions'],
 'caller_witness_count':len(witnesses), 'largest_add_split':LARGEST_SPLIT,
 'family_counts':dict(collections.Counter(r['group'] for r in rows)),
 'groups':{k:dict(zip(['add','delete','test_add','test_edit_both_sides','native_files','scope','tests'],v)) for k,v in GROUPS.items()},
 'deduplicated_source_units':units,
 'remaining_estimate':{k:[sum(v[i][j] for v in GROUPS.values()) for j in (0,1)] for i,k in enumerate(['add','delete','test_add','test_edit_both_sides'])},
 'note':'Planning interval only, conditional on reusing underlying storage/charset/service libraries. Non-test lines include comments/declarations; shared files are counted once in source exposure, and each group budget is added once, not once per family. Future changed-line overlap is not proved. Unbounded items are in cost-report.zh.md.'}
(a.output/'cost-summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({'families':len(rows),'exceptions':len(ledger['exceptions']),'groups':len(GROUPS),'deduplicated_files':len(units),'remaining':summary['remaining_estimate']}))
