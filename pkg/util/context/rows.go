// Copyright 2024 PingCAP, Inc.
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

package context

import "sync"

type RowsTracker struct {
	mu sync.Mutex

	affectedRows uint64
	foundRows    uint64

	/*
		following variables are ported from 'COPY_INFO' struct of MySQL server source,
		they are used to count rows for INSERT/REPLACE/UPDATE queries:
		  If a row is inserted then the copied variable is incremented.
		  If a row is updated by the INSERT ... ON DUPLICATE KEY UPDATE and the
		     new data differs from the old one then the copied and the updated
		     variables are incremented.
		  The touched variable is incremented if a row was touched by the update part
		     of the INSERT ... ON DUPLICATE KEY UPDATE no matter whether the row
		     was actually changed or not.

		see https://github.com/mysql/mysql-server/blob/d2029238d6d9f648077664e4cdd611e231a6dc14/sql/sql_data_change.h#L60 for more details
	*/
	records uint64
	deleted uint64
	updated uint64
	copied  uint64
	touched uint64
}

// AddAffectedRows adds affected rows.
func (sc *RowsTracker) AddAffectedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.affectedRows += rows
}

// SetAffectedRows sets affected rows.
func (sc *RowsTracker) SetAffectedRows(rows uint64) {
	sc.mu.Lock()
	sc.affectedRows = rows
	sc.mu.Unlock()
}

// AffectedRows gets affected rows.
func (sc *RowsTracker) AffectedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.affectedRows
}

// FoundRows gets found rows.
func (sc *RowsTracker) FoundRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.foundRows
}

// AddFoundRows adds found rows.
func (sc *RowsTracker) AddFoundRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.foundRows += rows
}

// RecordRows is used to generate info message
func (sc *RowsTracker) RecordRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.records
}

// AddRecordRows adds record rows.
func (sc *RowsTracker) AddRecordRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.records += rows
}

// DeletedRows is used to generate info message
func (sc *RowsTracker) DeletedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.deleted
}

// AddDeletedRows adds record rows.
func (sc *RowsTracker) AddDeletedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.deleted += rows
}

// UpdatedRows is used to generate info message
func (sc *RowsTracker) UpdatedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.updated
}

// AddUpdatedRows adds updated rows.
func (sc *RowsTracker) AddUpdatedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.updated += rows
}

// CopiedRows is used to generate info message
func (sc *RowsTracker) CopiedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.copied
}

// AddCopiedRows adds copied rows.
func (sc *RowsTracker) AddCopiedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.copied += rows
}

// TouchedRows is used to generate info message
func (sc *RowsTracker) TouchedRows() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.touched
}

// AddTouchedRows adds touched rows.
func (sc *RowsTracker) AddTouchedRows(rows uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.touched += rows
}

// Reset resets the RowsTracker.
func (sc *RowsTracker) Reset() {
	*sc = RowsTracker{}
}

// NewRowsTracker creates a new RowsTracker.
func NewRowsTracker() *RowsTracker {
	return &RowsTracker{}
}
