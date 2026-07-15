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
	"errors"
	"fmt"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
)

func (r *runner) apply(ctx context.Context) error {
	initial, err := r.preflight(ctx)
	if err != nil {
		return err
	}
	initial.print(r.out)
	if err := initial.error(); err != nil {
		return err
	}
	for _, operation := range r.rollbackOperations() {
		fmt.Fprintf(r.out, "APPLY: v%d %s\n", operation.version, operation.name)
		if err := operation.apply(ctx); err != nil {
			return fmt.Errorf("execute rollback step v%d %s: %w; rerun --mode check, then rerun the same apply command", operation.version, operation.name, err)
		}
	}

	afterSchema := &report{}
	r.checkSystemSchema(ctx, afterSchema)
	if len(afterSchema.blockers) > 0 {
		afterSchema.printBlockers(r.out)
		return fmt.Errorf("post-system-schema check failed with %d blocker(s)", len(afterSchema.blockers))
	}
	if err := r.normalizeBootstrap(ctx); err != nil {
		return err
	}
	return r.verifyBootstrap(ctx)
}

func (r *runner) normalizeBootstrap(ctx context.Context) error {
	sqlVersion, err := queryBootstrapVersion(ctx, r.db)
	if err != nil {
		return err
	}
	kvVersion, err := r.readKVBootstrap()
	if err != nil {
		return err
	}
	if !supportedBootstrapPair(sqlVersion, kvVersion) {
		return fmt.Errorf("unsupported SQL/KV bootstrap versions %d/%d", sqlVersion, kvVersion)
	}

	if sqlVersion == sourceBootstrapVersion {
		fmt.Fprintln(r.out, "APPLY: set mysql.tidb tidb_server_version 251 -> 220")
		if err := r.compareAndSetSQLBootstrap(ctx, sourceBootstrapVersion, targetBootstrapVersion); err != nil {
			return err
		}
	}
	if kvVersion == sourceBootstrapVersion {
		fmt.Fprintln(r.out, "APPLY: set TiKV bootstrap metadata 251 -> 220")
		casErr := r.compareAndSetKVBootstrap(ctx, sourceBootstrapVersion, targetBootstrapVersion)
		observed, readErr := r.readKVBootstrap()
		if readErr != nil {
			return fmt.Errorf("KV bootstrap commit result is uncertain (%v) and readback failed: %w", casErr, readErr)
		}
		switch observed {
		case targetBootstrapVersion:
			// A commit error can be returned after the transaction committed. Readback is authoritative.
		case sourceBootstrapVersion:
			if rollbackErr := r.compareAndSetSQLBootstrap(ctx, targetBootstrapVersion, sourceBootstrapVersion); rollbackErr != nil {
				return fmt.Errorf("KV bootstrap update failed (%v) and SQL rollback failed: %w", casErr, rollbackErr)
			}
			if casErr == nil {
				casErr = errors.New("KV bootstrap version remained 251 after a successful commit response")
			}
			return fmt.Errorf("KV bootstrap update did not commit; SQL version was restored to 251: %w", casErr)
		default:
			return fmt.Errorf("KV bootstrap readback returned unexpected version %d; do not restart TiDB", observed)
		}
	}

	current, err := queryBootstrapVersion(ctx, r.db)
	if err != nil {
		return err
	}
	if current == sourceBootstrapVersion {
		return r.compareAndSetSQLBootstrap(ctx, sourceBootstrapVersion, targetBootstrapVersion)
	}
	if current != targetBootstrapVersion {
		return fmt.Errorf("unexpected SQL bootstrap version %d after KV update", current)
	}
	return nil
}

func (r *runner) compareAndSetSQLBootstrap(ctx context.Context, expected, target int64) error {
	result, err := r.db.ExecContext(ctx, `
UPDATE mysql.tidb
SET VARIABLE_VALUE=?
WHERE VARIABLE_NAME='tidb_server_version' AND VARIABLE_VALUE=?`, target, expected)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("SQL bootstrap CAS %d -> %d affected %d rows", expected, target, affected)
	}
	return nil
}

func (r *runner) compareAndSetKVBootstrap(ctx context.Context, expected, target int64) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	return kv.RunInNewTxn(ctx, r.store, true, func(_ context.Context, txn kv.Transaction) error {
		mutator := meta.NewMutator(txn)
		current, err := mutator.GetBootstrapVersion()
		if err != nil {
			return err
		}
		if current != expected {
			return fmt.Errorf("KV bootstrap CAS expected %d, found %d", expected, current)
		}
		return mutator.FinishBootstrap(target)
	})
}

func (r *runner) readKVBootstrap() (int64, error) {
	txn, err := r.store.Begin()
	if err != nil {
		return 0, err
	}
	defer txn.Rollback()
	return meta.NewReader(txn).GetBootstrapVersion()
}
