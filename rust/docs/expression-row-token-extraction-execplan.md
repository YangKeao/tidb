# Share row capability extraction while preserving existing routing

This living plan follows repository-root PLANS.md; keep all required sections current.

## Purpose / Big Picture


Continue YangKeao6129cd47 shared-path simplification without performance loss, under task17. The prior combined enum-match and token extraction candidate was rejected and restored after276cells; task16 report and immutable archive remain unchanged, doc-only Main50932a1e51cb. This independently tests only common token extraction. A capability is borrowed Bytes(Some(token)); SQL NULL inside its authenticated native value differs from absent wire bytes. Keep the existing if-chain order, both task lookups, raw integer CAST, window-only rejections, optional CONCAT/REGEXP bytes and all per-family authentication/callbacks. No SDK, task fields, kernels, dependencies or public protocols change.

## Progress


- [x] Claim task17 and announce scope2f3cee5a after report-first completion of task16.
- [x] Preserve retained inputs;50extraction blocks reversed to exact productionbytes, allbranchconditions/tasklookups/returncalls/fallback andvalidate_row_reply unchanged. Production54add150del net96fewer; corrected131-line protocol/recovery test reused verbatim, total source35more.
- [x] New actualunit13/01b5, original2440+1inventory,974actual973pass/1knownfailure+exactbody/footerproof; actualservice13/6e19c629 build0/558.25sec, rootlint/dualformat/diff/fixedinput1PASS. Unit independentreview4dc554cfPASS, servicequalityreviewpending.
- [x] Freshactualquiet, strict69 and276balancedcells/190643queries over23scenarios, actualcontroller2exit0; initialmissingderiveactual1/strict20estRows-onlyfailurepreserved andexactretryPASS; ownservices/data cleaned.
- [x] Focusedactualcontroller2 exited0/1250.81sec,24strict/144cells/601495queries/sixrounds/min8sec/highresCPU/twin e17 control, all audits/sourcepins/owncleanupPASS. Original23/276 remains separate.
- [x] Analyze all primary23 and focused8 metrics/controls; conservatively reject and restore the single source file including131testlines; all9521input/HEAD/status/protectedfile restored.
- [x] Independentfocused144/config/clock/roles/ratios/cleanup/restoration reviewa8eb7f6dPASS; allprimary23 andfocused8 report/statistics generated separately.
- [x] Deliver full report/statistics via941a8b88 before localDCO; independentfocusedreviewa8eb7f6dPASS. Rawarchive delivery and the already-authorized local document DCO are final mechanical steps recorded in separate durable receipts; no push.

## Surprises & Discoveries


The previous match candidate changed both branch structure and token extraction. It saved100productionlines but131newtestlines and did not establish no regression. This trial preserves routing and observes extraction separately; do not inherit its actual behavior or performance as validation of new source. The corrected test returns BindingContract for missing wire token and HostContract for wrong ordinal. Adding this exact new test makes the test runner filtered footer2439 to2440; only that inventory-proven change plus PID/time may differ in the inherited failure output.

## Decision Log


Decision: use one local borrowed native_token closure, called only inside existing native branches. Preserve the first AST task lookup and later task lookup verbatim. Rationale: isolate common extraction from the previously rejected match dispatch, without eager extraction that would reject optional wire NULL. Author/date: ExpressionReuseEngineer, 2026-11-10.

## Outcomes & Retrospective


Both actual windows are complete and this isolated closure-extraction candidate is rejected/restored. Source saved96 productionlines but added131testlines, combined35more; none retained. Primary23/276/190643queries hadINandCONCATbenefits butdeepCPU/widthlatencyadverse. Focused8/144/601495queries with highresCPU andsame-e17A/Bcontrol retainedmixedbenefits; deepCPUvsA+0.85%all6 andvsB+0.73%median(rangecrosseszero), singletextvsB CPU/p50+0.44%/+0.36%all6. Sourcecausalitynotprovenforallchanges; no no-regressionproof. All9521inputs/head/status/protectedrestored, ownservices/data cleaned. Newretainedgain/production/testreduction0/0/0; cumulativeproduction5066/test192unchanged. Broadergoalremainsunmet; do notrerunthisrejectedclosurecandidate.

## Context and Orientation


Main /home/agent/tidb/expression-unification/tidb HEAD50932a1e51cb320b032fc3e9d74eb4b41dd64fd2; SDK sibling tikv f7672bf980143f2938fd0f900650a4c29091e70f clean. Only rust/crates/tidb-expr/src/evaluator/batch_hosts.rs::NativeBatchHosts::resume_sql is changed, plus one test in that file. validate_row_reply authenticates task and argument shape/ordinal before dispatch and remains byte-identical. Local closure reads only the one reply ScalarValueRef; each family still authenticates the native value and origin/generation at its existing phase.

## Plan of Work


Preserve full retained bytes/input manifest with this document path present. Replace each identical let-else Bytes(Some(token)) extraction with a call passing its exact old error string. Keep every if/if-let condition, return callee/args, handling and fallback. Prove reverse substitution restores the old production bytes. Reuse only the prior corrected protocol/recovery test, not its source implementation. Then compile and actually run new artifacts. SDK unchanged validation is inherited only.

## Concrete Steps


From Main, empty RUSTFLAGS and CARGO_BUILD_JOBS=1:

    ../tools/cargo-tidb test --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-expr --lib --no-run --message-format=json
    ../tools/cargo-tidb build --manifest-path rust/Cargo.toml --release --locked --offline -j1 -p tidb-server --bin tidb-server --no-default-features
    make lint

Use evidence/path-unification/row-token-extraction-isolated/functional.job.json through the durable agent job_runner.py; require actual child exits. Save actual unitfront-unit-round13 and actual servicetidb-server-after-round13. Never use mutable target or task16 candidate as accepted baseline. Only retained actualround6 e17, candidate13 and original99a with backend355b are measured.

## Validation and Acceptance


Original2440test names plus exact row_reply_dispatch_preserves_family_errors_and_reuse;974 selected consumers expected973pass/1sameknowncommon. Strict full failure text after PID/time and exact inventory-proven footer increment, no golden rebase. Required lint, Jan30/Aug22format checks on changed file, diff checks and9521 fixed inputs. UnchangedSDK1159/1/1 and171clippy messages are inherited, not newly run; no full2441/allocationfailure claim without actual execution.

Performance has23cases including15controls,4CAST/IN/INTERVAL/GREATEST and4LOWER/UPPER/CONCAT/STRCMP/REGEXP queries;69 strict results/metadata/warnings/fullEXPLAIN observations then4rounds/276cells, each>=2secAND30fullfetches/no cap. Initial known estRows-only20case differences must be archived with all69observations/column proof before any timing; only owned storage restart and exact retry, no plan normalization or weakening. Fresh actual quiet after all builds, not midnight alone; no builds/tests/perf during timing. Include all adverse/favorable data and scan/core controls. Paired observed ranges are not confidence intervals; SQL process CPU/fullfetch latency are not kernel ns/eval. Without evidence for no regression, restore exact owned source and verify all inputs.

## Idempotence and Recovery


New directory only, never overwrite previous source trials/archives or completed preflight/primary. Preserve failed controllers and use new receipts for repairs. Source stays fixed while build/test/timing runs. Only stop own PID/birth/command records and delete their exact recorded data-dir. Unrelated untracked rust/third_party/tikv-client-rs/tests/client_go_differential/BUILD.bazel SHA256cc9fb078591d7c94366d1e6f504ff2bade23ccb57a9313f9c7235807ff658d52 is protected. Stage only owned code/test/plan after reporting. Existing authorization is local DCO YangKeao/yangkeao@chunibyo.icu; no push/PR/deploy. Future genuine time/load hold needs fresh authoritative state plus Raft reminder, not stopping midjob.

## Artifacts and Notes


Retained service e17bfb3553535fc478b7cd9576f3658498611463fcfcf7561e159aba278124ee; original99a4c3e566774ba9733bc9d54366cf1e9b85446f63fa9a7418bfef5d3daeddd2; backend355be0acec593a24bb43ac0f5e6b8e30aae354b7e63fdd3530f3f2895c41e029. Prior task16 ZIPtask16-row-reply-dispatch-final.zip SHA9c2c40f7c76a2d2d1f72242f06e4237011694e19a6548ad703f3f7ad392c0696 immutable. Actual mutable target holds rejectedround12; never baseline.

## Interfaces and Dependencies


No public interface or dependency change. Existing resume_sql signature/HostArgReply/HostTaskId remain. The local closure returns the borrowed token or the same binding error; no owning copy, allocation, value-store lookup or callback is introduced.

Independent static review255da185 verified50reverse substitutions restore exact retained production, including if-order/two-task-lookups/conditions/calls/exceptions/NULLfallback and error classes/text; all9521inputs matchcandidate-input1. The131testlines are verbatim correctedpriorversion, no inheritedexecutionclaim. Savedindependent-static-review.json; actualnewunit/runtime/service/quality/performance stillpending.

2026-11-10 02:13local: actualfront-build1 exited0/279sec, newunit13SHA01b5b939f1aa0f6be3a848d05be44f5aac64e9bf80d68bb7d215fe7447478049. Exactoriginal2440names+one specifiedtest verified; protocol/recoverytestactual0, runtime187/974allpass sofar. Runtime/service/quality/performance stillpending, no inheritedactualunitfromtask16.

2026-11-10 02:20local: front-runtime1 actual0/416.58sec, all974terminal973pass/1knowncommon, newprotocoltest0. Strictchecker4 directlypassed exactassertionbody/PID-time/inventoryprovenfiltered+1; no goldenrebaseorinterruptedattempt. Samecontrollerproceeded actualserver-build1child151771birth537215; newserviceround13/quality/fixedafter1stillpending. Unit/runtimeindependentreviewrequested6734bc9b; no performanceyet.

Independentunitreview4dc554cf readall974rawtest/suiteterminals/actualexits, exact2440+1inventoryandknownbody/footerdifference, actualunit01b5and9521inputsPASS. No newbuild/test/SDKexecutionbyreviewer; service/quality/performance stillrequired. Savedindependent-unit-review.json.

2026-11-10 02:29local: actualfunctionalcontroller completed0/1259.57sec. Newservice13SHA6e19c629bcd2027f048a6987b26b1534f86cb1896e4f88e04921cc88d6674615, actualserverbuild0/558.25sec, rootlint0/2.22sec, Jan30/Aug22format0/diff0 andall9521input1=before1=after1. Freshlatestthreadreadthrough9e9ec505 andnew16secquietprobeinprogress, no performanceyet.

2026-11-10 02:32local setuprepair: freshquietpassed but firstnightcontrolleractual1/6.08sec because copiedharnessomittedderive-fixture.py, afterbase8192bootstrapandBEFOREderive/strict/timing. Failurepreserved; allregisteredownservicesstoppedandportsfree. Copiedexactmissingderivescript, checkedALLliteralP/BASE/Fscriptdependenciesexist, syntaxparsed. Preparedseparatenight_experiment2/controller2reusingonlyexistingbase, derive/postDDLownrestartthenEXACT23/69/276workflow, outerfinallyowncleanuponeveryexit. Newfreshquietinprogress; no recompileor974rerun/no timingyet. Peer94f5servicechecksreceiptsONLY, completeELF/sourcepeercheckaftertimingpending.

2026-11-10 02:46local primary result: actualcontroller2 exited0/681.96sec,276cells/190643queries/auditPASS/owncleanup and no knownwatchevents. INCPU/p50−2.53%/−3.71%,CONCATCPU−4.01%all4; deep16CPU+1.89%(+1.16to+3.05) andwidth8p50+1.71%(+.03to+3.28)all4, singleJSONp50+.77%all4. ScanCPU+2.38%range−3.45to+5.53. No retention decision/source restoration yet; cumulativeunchanged.

The primary CPU counter is /proc stat in10,000,000ns ticks on this host, and2-second cells do not establish whether small mixed changes are repeatable. An independent focused window in agent evidence/path-unification/row-token-focused-calibration is prepared before timing. It keeps EXACT same production/source/service13 and inherits the already completed behavior only after proving all9521input bytes unchanged. Mode native=retainedA e17,current=candidate6e19,frozen=retainedB SAME exact e17 ELF asA (legacyinternalprocess-slotname, NOT original99a). Eight cases: scan,deep16,width8,singleJSON,singletext,IN,CONCAT,vecOffJSON;24strict observations then144cells of six complete permutations ofthreeversionorders,>=8secondsAND30queries. No new source/build/SDK/test-suite execution.

CPU measurement now uses local POSIX clock_getcpuclockid(pid) and time.clock_gettime_ns for the whole frontend process, not kernel ns/eval. /usr/include/time.h:347 provides the actual prototype; selfcheck agrees with ownprocess_time within1ms,40mssleep uses29380nsCPU and60msbusy uses60002148nsCPU, reportedresolution1ns(not a physicalaccuracy claim). Actualsession init/max/vectorization are asserted and captured in strictpreflight. Candidate/A,candidate/B and baselineB/A selfcontrol ratios are reported without subtracting control to manufacture improvement. Preserve both independent windows, do not cross-pair, replace adverse data or claim no regression if adverse evidence remains unresolved. Six observed ratio ranges are not CIs. Fresh directive/actualquiet before starting; no running calibration yet. All helper literal dependencies present, scripts parsed, matrix and allsixorders/selfcheck validated. Initial expected6multirowestRows-onlydifferences must archive all24observations+columnproofbefore exactownedrestartretry; no relaxation. Setup/strict/timing all wrapped in ownPIDbirthcleanup finally.

Focused scope updated BEFORE any execution: primaryvecOffJSONQPS−1.135% withall4negative(−4.740to−.185), so addedthiseighthadversescenario. Final8cases/24strict/144cells/sixpermutations/min8sec, expected6multirowestRows-onlydifferences. The initial7caseproposal was never executed and is preservedasmeasurement-design-v1-7cases.preexecution.json, not mixed into actualdata. No source change or new functional execution.

2026-11-10 03:08local configuration correction: focused1 actualcontroller1/33.84sec BEFORE preflight observations or any timing because new explicitGETassertion expected init1024 but retainedAreturned32/max1024. Originalwholehelper/controller/receipts preservedattempt1-init-assumption; ownservicesstopped/portsfree. Go varsutil.go173 andRustsysvarcatalog/distsql_storage.rs316-324 setlegalinitupperbound32 (peer4228678dconfirmed). Actual18configurationreplays onretainede17/candidate6e19/original99a validatelegacyinit1024warn1292/clamp32; legalinit32givesSAMEeffective[32,1024,1]/[32,32,1]/[32,1024,0]withoutthatSETwarning. TheseareLATERreplay, notprimary-timeGETlogs; oldprimaryignoredreturnedGETvalues. Preserveallprimaryrawdata, describebatch1024asINITIAL32/MAX1024, batch32INITIAL32/MAX32, notinitial/constant1024. No production source correction needed.

Correctedfocusedcommon requestslegalinit=min(max,32) andkeepsEXACTeffectiveGETassertion/capture. Newfocused2 guardsoriginalactual1/replayPASS/allstopped/derived8192sourcepin, reusesexistingtypedfixturewithoutDDL/bootstrap/build/testredo, thenUNCHANGED8/24/144sixround/highresCPU/selfcontrolprotocol. AllstagesownPIDbirthcleanupfinally. Configurationprobeoriginal99aisextraconfig_originalroleonly, no focusedtimedcell; focusedfrozenBremainssamee17asA. Freshquietafterprobeinprogress, sourceunit/serviceunchanged. Earlierquietfailure5.38%andpasseskept, no thresholdrelaxation. Finalreportwillincludefocusedpreparationfailure/configlimitsandalloriginalPplusfocusedSdata separately.

2026-11-10 03:12local actualfocused2started afterfreshquietPASS/latestpeer9a240configurationreplayreviewPASS. Existingderived8192reused; ALL24strictobservations8pairsPASS FIRSTtrywithcaptured/asserteffectiveinitial32/max1024/vec1or0, no estimate-onlyfailureinthisfocusedwindow. LivefrontendPOSIXCPUclock/units/8sec30queryguardvalidated;144sixroundcellsnowrunning, first5done, no source/build/SDK/behaviorrerun. Currentactualreceiptfocused2, NOTfailedfocused1. Fullprimary23/276retainedseparate; no finalretentiondecision/96notcounted.

2026-11-10 03:33local final decision: focused2 actualexit0/1250.806sec,144cells/601495queries/24strict allPASS, no knownwatch events, actualCPU-ns/8secminimum/rolee17A=B andsourcepins audited. Allownedprocesses/data cleaned. CONCATCPUmedian−2.94%vsA/−1.41%vsB(all6negativevsB); deep16CPU+.85%vsA(+.14to+1.72all6),+.73%vsB(−.07to+1.17). SingletextvsB CPU+.44%,p50+.36%,QPS−.41%all6; vsA rangescrosszero. Widthlatency/vecOffQPSinitialregressionnotrepeatconfirmed, so do not claimallinitialeffectsconfirmed orallchangescausedbysource. SelfcontrolB/Ahasvariation, never subtracted. Combined evidence doesnotprove the requested no-loss condition: REJECT.

Onlyownedbatch_hosts.rs was restored after verifyingexactcandidatebytes/after1sourcepins andbothactualterminal0/cleanup receipts. Actualsnapshotmatchesinitial-input.json for all9521source/configandallHEAD/status/protected; no newtestexecutionafterrestorationclaimed. Complete report generatedwithallprimary23CPU/latency/QPSandallfocused8CPU/p50/p95/QPS/B-Acontroltables, bothactualfailedpreparationsand18laterconfigurationreplays preserved. Metadata labels initial32/max1024 or32 explicitly, laterreplaynotprimary-timeGET. Finalfocusedreadonlyreview and report-firstlocaldocumentationDCO pending; no sourceoptimizationkept, no push. Actualmutabletargetstillrejectedround13, acceptedruntimebaselinee17unchanged.

Independentreviewa8eb7f6d read/recomputedall144cells/601495queries/24strict/configuration/CPU-ns/6orders/twin-e17ratios, bothoriginalfailures/final0 andowncleanupPASS; complete9521files/list/HEAD/status/protectedrestoredverified. No newfunction/performanceexecutionsbyreviewer. ActualconfigurationGETassertionruneachcell, buttuplessavedONLY24preflightobservations/notembeddedtimingrows; reportstateslimitation. Worktotals420cells/792138queriesareonlyarithmeticworkcounts, notcombinedcohort/crosspairs. Bothwindows/currentrejectedcandidate/restorationfullyreported. Pendingreport+archive-firstauthorizedlocaldocDCOonly.

Final report ecc64acb and statistics b93bbafe delivered in941a8b88 BEFORE localdocumentDCO. Allprimary23 andfocused8 tables, effectiveinitial32/max1024or32 limits,18LATERreplays(notprimary-timeGET), timingcellGETassertionswithoutembeddedtuples, originalsetupfailures and restoration evidence preserved. Immutable combined rawarchive task17-row-token-extraction-final.zip packagesbothdistinctwindows; archive receipt and postcommitreceipt areseparate toavoidself-referentialupdates. LocalDCO recordsONLY this resultdocument, no implementation/test/SDK/protected changes or push. Actualnewretainedgain/code0/0, cumulativeproduction5066/test192, originalgoalnotcomplete.
