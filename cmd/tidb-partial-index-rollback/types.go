// Copyright 2026 PingCAP, Inc.
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
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/pingcap/tidb/pkg/kv"
)

const (
	modeCheck  = "check"
	modeApply  = "apply"
	modeVerify = "verify"

	sourceBootstrapVersion int64 = 251
	targetBootstrapVersion int64 = 220
	targetTiDBVersion            = "8.5.2"
	targetTiDBGitHash            = "f43a13324440f92209e2a9f04c0bbe9cf763978d"
)

type runner struct {
	db    *sql.DB
	store kv.Storage
	out   io.Writer
}

func newRunner(db *sql.DB, store kv.Storage, out io.Writer) *runner {
	return &runner{db: db, store: store, out: out}
}

type report struct {
	sqlBootstrap int64
	kvBootstrap  int64
	blockers     []string
	actions      []string
}

func (r *report) block(format string, args ...any) {
	r.blockers = append(r.blockers, fmt.Sprintf(format, args...))
}

func (r *report) action(format string, args ...any) {
	r.actions = append(r.actions, fmt.Sprintf(format, args...))
}

func (r *report) print(out io.Writer) {
	fmt.Fprintf(out, "SQL_BOOTSTRAP_VERSION: %d\n", r.sqlBootstrap)
	fmt.Fprintf(out, "KV_BOOTSTRAP_VERSION: %d\n", r.kvBootstrap)
	for _, item := range r.actions {
		fmt.Fprintf(out, "ACTION: %s\n", item)
	}
	r.printBlockers(out)
	if len(r.blockers) == 0 {
		fmt.Fprintln(out, "RESULT: READY")
		return
	}
	fmt.Fprintf(out, "RESULT: BLOCKED (%d blocker(s))\n", len(r.blockers))
}

func (r *report) printBlockers(out io.Writer) {
	for _, item := range r.blockers {
		fmt.Fprintf(out, "BLOCKER: %s\n", item)
	}
}

type rollbackOperation struct {
	version int
	name    string
	apply   func(context.Context) error
}

func (r *report) error() error {
	if len(r.blockers) == 0 {
		return nil
	}
	return fmt.Errorf("found %d blocker(s); resolve them and rerun check", len(r.blockers))
}
