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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowsTracker(t *testing.T) {
	tracker := NewRowsTracker()
	tracker.AddAffectedRows(1)
	require.Equal(t, uint64(1), tracker.AffectedRows())
	tracker.SetAffectedRows(2)
	require.Equal(t, uint64(2), tracker.AffectedRows())

	tracker.AddFoundRows(1)
	require.Equal(t, uint64(1), tracker.FoundRows())
	tracker.AddRecordRows(1)
	require.Equal(t, uint64(1), tracker.RecordRows())
	tracker.AddDeletedRows(1)
	require.Equal(t, uint64(1), tracker.DeletedRows())
	tracker.AddUpdatedRows(1)
	require.Equal(t, uint64(1), tracker.UpdatedRows())
}

func TestRowsTrackerReset(t *testing.T) {
	tracker := NewRowsTracker()
	tracker.AddAffectedRows(1)
	tracker.AddFoundRows(1)
	tracker.AddRecordRows(1)
	tracker.AddDeletedRows(1)
	tracker.AddUpdatedRows(1)
	tracker.Reset()
	require.Equal(t, uint64(0), tracker.AffectedRows())
	require.Equal(t, uint64(0), tracker.FoundRows())
	require.Equal(t, uint64(0), tracker.RecordRows())
	require.Equal(t, uint64(0), tracker.DeletedRows())
	require.Equal(t, uint64(0), tracker.UpdatedRows())
}
