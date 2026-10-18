# STR_TO_DATE original observations and explicit calendar correction

`str_to_date_original_n7b.json.gz` was captured BEFORE semantic edits at
TiDB64bce47f/TiKV8e3bf36. Its844344 original observations exactly produced the
immutable N7b digest d7dd9f9ee5b55431. All three cache profiles were independently
observed and asserted identical, allowing one281448-record profile to be stored
as indices into534 observation strings. The fixture also stores3906 actual
calendar head states, extracted only in a temporary test generator AFTER the
SDK's official whole-report decoder validated the original output. Wide fields
are preserved, not packed/truncated. The generator is retained outside the tree
as replayable n7d-original-capture.patch; it is not production machinery.

`str_to_date_go_partial_calendar_n7d.json` comes from an independently compiled
original-Go program importing pkg/types, not Rust output. It checks every168
representable zero-month calendar states with types.Time.Check under SELECT
IgnoreZeroInDate(true), both invalid-date settings, verifies packed fields are
lossless, and renders the accepted calendar. It also calls original StrToDate
on six recorded partial-date examples, four full-zero examples and diagnostic/
wide-day audit examples. This proves calendar VALIDATION, not complete parser
parity. Native %j scanner policy remains separately pending; do not reinterpret
its original fields through the Go parser in this fixture.

Current tests require826200 observations to remain exactly original, and18144
relaxed-mode records to match explicit Go-validated accepted text, identity and
mode-only events. They replay the old hash from the captured historical stream,
with that distinction stated explicitly; the original digest is never regenerated
from intentionally changed production output. Both fixture byte streams have
fixed test fingerprints. Original gzip SHA256: fd9ed6080649e8987dd86b64bd761998c7db2bf66cd8aff52fdbd4a9a9464571.
Go oracle JSON SHA256: b39e3c61207f877917bea98ff68b87f2c01901b04070f644e2a70eb40ef84a8f. No ignored tests or production compatibility
branch are introduced by the fixtures.

## Reproduce the independent Go oracle

The standalone source below imports the repository's original production Go
code. Save the Go block as a temporary `.go` file, then run from TiDB root:

    go run -p=8 /absolute/path/to/probe.go rust/crates/tidb-expr/src/tikv/testdata/str_to_date_original_n7b.json.gz > /tmp/str_to_date_go_calendar_actual.json
    cmp /tmp/str_to_date_go_calendar_actual.json rust/crates/tidb-expr/src/tikv/testdata/str_to_date_go_partial_calendar_n7d.json

The recorded compiler was go1.26.5-X:nodwarf5 linux/amd64; the imported Go
production sources were unchanged by this Rust checkpoint. The output checks
calendar validation and includes direct parser audits; `%j` divergence remains
visible, rather than being recast as complete parser parity.

```go
// Independent original-Go calendar validation and direct parser evidence.
// This probe imports the repository's production types; it is not a Go package
// transcreation or a replacement parser.
package main

import (
 "compress/gzip"
 "encoding/json"
 "fmt"
 "os"
 "time"

 "github.com/pingcap/tidb/pkg/parser/mysql"
 "github.com/pingcap/tidb/pkg/types"
 contextutil "github.com/pingcap/tidb/pkg/util/context"
)
type Calendar struct { Fields []int `json:"fields"`; Flags byte `json:"flags"` }
type Original struct { HistoricalHash string `json:"historical_hash"`; Calendars []*Calendar `json:"calendars"` }
type CalendarAnswer struct { Pair int `json:"pair"`; Fields []int `json:"fields"`; Flags byte `json:"flags"`; Text string `json:"text"` }
type ParserAnswer struct { Input string `json:"input"`; Format string `json:"format"`; Success bool `json:"success"`; Text string `json:"text"` }
func main() {
 answers:=[]CalendarAnswer{}
 if len(os.Args)>1 {
  file,err:=os.Open(os.Args[1]);if err!=nil {panic(err)}; defer file.Close()
  gz,err:=gzip.NewReader(file);if err!=nil {panic(err)};defer gz.Close()
  var original Original;if err:=json.NewDecoder(gz).Decode(&original);err!=nil {panic(err)}
  if original.HistoricalHash!="d7dd9f9ee5b55431" {panic("wrong historical source")}
  for pair,c:=range original.Calendars {
   if c==nil || c.Fields[1]!=0 || c.Fields[2]>31 {continue}
   if len(c.Fields)!=7 {panic("wrong actual calendar field count")}
   f:=c.Fields
   kind:=byte(mysql.TypeDate);fsp:=0
   if c.Flags&1!=0 {kind=mysql.TypeDatetime};if c.Flags&2!=0 {fsp=6}
   value:=types.NewTime(types.FromDate(f[0],f[1],f[2],f[3],f[4],f[5],f[6]),kind,fsp)
   if value.Year()!=f[0] || value.Month()!=f[1] || value.Day()!=f[2] || value.Hour()!=f[3] || value.Minute()!=f[4] || value.Second()!=f[5] || value.Microsecond()!=f[6] {panic("packed Go core lost an actual field")}
   for _,allowInvalid:=range []bool{false,true} {
    ctx:=types.NewContext(types.StrictFlags.WithIgnoreZeroInDate(true).WithIgnoreInvalidDateErr(allowInvalid),time.UTC,contextutil.IgnoreWarn)
    if err:=value.Check(ctx);err!=nil {panic(fmt.Sprintf("Go rejects actual partial calendar %v: %v",f,err))}
   }
   answers=append(answers,CalendarAnswer{pair,f,c.Flags,value.String()})
  }
 }
 parser:=[]ParserAnswer{}
 for _,pair:=range [][2]string{{"1","%m"},{"01","%d"},{"2019","%Y"},{"5,2019","%m,%Y"},{"01,2019","%d,%Y"},{"01,5","%d,%m"},{"0","%m"},{"0","%d"},{"0000","%Y"},{"0000-00-00","%Y-%m-%d"},{"2021-02-30","%Y-%m-%d"},{"2020 12 999","%Y %m %j"},{"2020 999","%Y %j"}} {
  ctx:=types.NewContext(types.StrictFlags.WithIgnoreZeroInDate(true),time.UTC,contextutil.IgnoreWarn)
  var value types.Time;ok:=value.StrToDate(ctx,pair[0],pair[1])
  // Preserve failure's actual parsed Time for diagnostic evidence.
  if ok {duration,date:=types.GetFormatType(pair[1]);if date&&!duration {value.SetType(mysql.TypeDate)};value.SetFsp(0)}
  parser=append(parser,ParserAnswer{pair[0],pair[1],ok,value.String()})
 }
 output:=map[string]any{"authority":"original pkg/types.Time.Check and StrToDate at TiDB64bce47f; SELECT IgnoreZeroInDate(true)","historical_hash":"d7dd9f9ee5b55431","calendars":answers,"parser":parser}
 encoder:=json.NewEncoder(os.Stdout);encoder.SetIndent("","  ");if err:=encoder.Encode(output);err!=nil {panic(err)}
}

```
