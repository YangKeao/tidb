// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"honnef.co/go/tools/quickfix/qf1002"
	"honnef.co/go/tools/quickfix/qf1004"
	"honnef.co/go/tools/quickfix/qf1012"
	"honnef.co/go/tools/simple/s1000"
	"honnef.co/go/tools/simple/s1001"
	"honnef.co/go/tools/simple/s1002"
	"honnef.co/go/tools/simple/s1003"
	"honnef.co/go/tools/simple/s1004"
	"honnef.co/go/tools/simple/s1005"
	"honnef.co/go/tools/simple/s1006"
	"honnef.co/go/tools/simple/s1007"
	"honnef.co/go/tools/simple/s1008"
	"honnef.co/go/tools/simple/s1009"
	"honnef.co/go/tools/simple/s1010"
	"honnef.co/go/tools/simple/s1011"
	"honnef.co/go/tools/simple/s1012"
	"honnef.co/go/tools/simple/s1016"
	"honnef.co/go/tools/simple/s1017"
	"honnef.co/go/tools/simple/s1018"
	"honnef.co/go/tools/simple/s1019"
	"honnef.co/go/tools/simple/s1020"
	"honnef.co/go/tools/simple/s1021"
	"honnef.co/go/tools/simple/s1023"
	"honnef.co/go/tools/simple/s1024"
	"honnef.co/go/tools/simple/s1025"
	"honnef.co/go/tools/simple/s1028"
	"honnef.co/go/tools/simple/s1029"
	"honnef.co/go/tools/simple/s1030"
	"honnef.co/go/tools/simple/s1031"
	"honnef.co/go/tools/simple/s1032"
	"honnef.co/go/tools/simple/s1033"
	"honnef.co/go/tools/simple/s1034"
	"honnef.co/go/tools/simple/s1035"
	"honnef.co/go/tools/simple/s1036"
	"honnef.co/go/tools/simple/s1037"
	"honnef.co/go/tools/simple/s1038"
	"honnef.co/go/tools/simple/s1039"
	"honnef.co/go/tools/simple/s1040"
	"honnef.co/go/tools/staticcheck/sa1019"
	"honnef.co/go/tools/staticcheck/sa1029"
	"honnef.co/go/tools/staticcheck/sa2000"
	"honnef.co/go/tools/staticcheck/sa2001"
	"honnef.co/go/tools/staticcheck/sa2003"
	"honnef.co/go/tools/staticcheck/sa3000"
	"honnef.co/go/tools/staticcheck/sa3001"
	"honnef.co/go/tools/staticcheck/sa4004"
	"honnef.co/go/tools/staticcheck/sa4009"
	"honnef.co/go/tools/staticcheck/sa4018"
	"honnef.co/go/tools/staticcheck/sa5000"
	"honnef.co/go/tools/staticcheck/sa5001"
	"honnef.co/go/tools/staticcheck/sa5002"
	"honnef.co/go/tools/staticcheck/sa5003"
	"honnef.co/go/tools/staticcheck/sa5004"
	"honnef.co/go/tools/staticcheck/sa5005"
	"honnef.co/go/tools/staticcheck/sa5007"
	"honnef.co/go/tools/staticcheck/sa5008"
	"honnef.co/go/tools/staticcheck/sa5009"
	"honnef.co/go/tools/staticcheck/sa5010"
	"honnef.co/go/tools/staticcheck/sa5012"
	"honnef.co/go/tools/staticcheck/sa6000"
	"honnef.co/go/tools/staticcheck/sa6002"
	"honnef.co/go/tools/staticcheck/sa6005"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/multichecker"

	"github.com/pingcap/tidb/build/linter/allrevive"
	"github.com/pingcap/tidb/build/linter/asciicheck"
	"github.com/pingcap/tidb/build/linter/bodyclose"
	"github.com/pingcap/tidb/build/linter/bootstrap"
	"github.com/pingcap/tidb/build/linter/constructor"
	"github.com/pingcap/tidb/build/linter/copyloopvar"
	"github.com/pingcap/tidb/build/linter/deferrecover"
	"github.com/pingcap/tidb/build/linter/durationcheck"
	"github.com/pingcap/tidb/build/linter/etcdconfig"
	"github.com/pingcap/tidb/build/linter/forcetypeassert"
	"github.com/pingcap/tidb/build/linter/gci"
	"github.com/pingcap/tidb/build/linter/gofmt"
	"github.com/pingcap/tidb/build/linter/gosec"
	"github.com/pingcap/tidb/build/linter/ineffassign"
	"github.com/pingcap/tidb/build/linter/intrange"
	"github.com/pingcap/tidb/build/linter/lll"
	"github.com/pingcap/tidb/build/linter/makezero"
	"github.com/pingcap/tidb/build/linter/mirror"
	"github.com/pingcap/tidb/build/linter/misspell"
	"github.com/pingcap/tidb/build/linter/prealloc"
	"github.com/pingcap/tidb/build/linter/predeclared"
	"github.com/pingcap/tidb/build/linter/printexpression"
	"github.com/pingcap/tidb/build/linter/revive"
	"github.com/pingcap/tidb/build/linter/rowserrcheck"
	"github.com/pingcap/tidb/build/linter/toomanytests"
	"github.com/pingcap/tidb/build/linter/unconvert"
	"github.com/pingcap/tidb/build/linter/util"
)

func main() {
	// SkipAnalyzerByConfig will modify the original analyzer to skip it if it is disabled in the configuration.
	staticCheckAnalyzerList := []*analysis.Analyzer{
		s1000.Analyzer,
		s1001.Analyzer,
		s1002.Analyzer,
		s1003.Analyzer,
		s1004.Analyzer,
		s1005.Analyzer,
		s1006.Analyzer,
		s1007.Analyzer,
		s1008.Analyzer,
		s1009.Analyzer,
		s1010.Analyzer,
		s1011.Analyzer,
		s1012.Analyzer,
		s1016.Analyzer,
		s1017.Analyzer,
		s1018.Analyzer,
		s1019.Analyzer,
		s1020.Analyzer,
		s1021.Analyzer,
		s1023.Analyzer,
		s1024.Analyzer,
		s1025.Analyzer,
		s1028.Analyzer,
		s1029.Analyzer,
		s1030.Analyzer,
		s1031.Analyzer,
		s1032.Analyzer,
		s1033.Analyzer,
		s1034.Analyzer,
		s1035.Analyzer,
		s1036.Analyzer,
		s1037.Analyzer,
		s1038.Analyzer,
		s1039.Analyzer,
		s1040.Analyzer,
		sa1019.Analyzer,
		sa1029.Analyzer,
		sa2000.Analyzer,
		sa2001.Analyzer,
		sa2003.Analyzer,
		sa3000.Analyzer,
		sa3001.Analyzer,
		sa4004.Analyzer,
		sa4009.Analyzer,
		sa4018.Analyzer,
		sa5000.Analyzer,
		sa5001.Analyzer,
		sa5002.Analyzer,
		sa5003.Analyzer,
		sa5004.Analyzer,
		sa5005.Analyzer,
		sa5007.Analyzer,
		sa5008.Analyzer,
		sa5009.Analyzer,
		sa5010.Analyzer,
		sa5012.Analyzer,
		sa6000.Analyzer,
		sa6002.Analyzer,
		sa6005.Analyzer,
		qf1002.Analyzer,
		qf1004.Analyzer,
		qf1012.Analyzer,
		// TODO: add u1000.Analyzer. I don't find it.
	}

	for _, analyzer := range staticCheckAnalyzerList {
		// SkipAnalyzerByConfig will modify the original analyzer to skip it if it is disabled in the configuration.
		// All other analyzers in TiDB repo has been skipped in their own init functions.
		util.SkipAnalyzerByConfig(analyzer)
	}

	allAnalyzers := append(staticCheckAnalyzerList,
		asciicheck.Analyzer,
		bodyclose.Analyzer,
		bootstrap.Analyzer,
		constructor.Analyzer,
		copyloopvar.Analyzer,
		deferrecover.Analyzer,
		durationcheck.Analyzer,
		etcdconfig.Analyzer,
		forcetypeassert.Analyzer,
		gofmt.Analyzer,
		gci.Analyzer,
		gosec.Analyzer,
		ineffassign.Analyzer,
		intrange.Analyzer,
		makezero.Analyzer,
		mirror.Analyzer,
		misspell.Analyzer,
		prealloc.Analyzer,
		predeclared.Analyzer,
		printexpression.Analyzer,
		unconvert.Analyzer,
		rowserrcheck.Analyzer,
		toomanytests.Analyzer,
		allrevive.Analyzer,
		// TODO: add errcheck analyzer. Currently it's configuration is wrong and will report "no such flag -excludes" because of its init function.
		lll.Analyzer,
		revive.Analyzer)

	multichecker.Main(
		allAnalyzers...,
	)
}
