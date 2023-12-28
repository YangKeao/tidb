// Copyright 2023 PingCAP, Inc.
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

package indexusage

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func assertPercentageReturned(t *testing.T, expect []uint64, actual *[6]atomic.Uint64) {
	require.Equal(t, len(expect), len(actual))
	for i := range expect {
		require.Equal(t, expect[i], actual[i].Load())
	}
}

func assertPercentageAccess(t *testing.T, expect []uint64, actual *[7]atomic.Uint64) {
	require.Equal(t, len(expect), len(actual))
	for i := range expect {
		require.Equal(t, expect[i], actual[i].Load())
	}
}

func TestGetBucket(t *testing.T) {
	testCases := []struct {
		value   float64
		hasZero bool
		expect  int
	}{
		{0.0, true, 0},
		{0.0, false, 0},
		{0.005, true, 1},
		{0.005, false, 0},
		{0.01, true, 2},
		{0.01, false, 1},
		{0.05, true, 2},
		{0.05, false, 1},
		{0.1, true, 3},
		{0.1, false, 2},
		{0.15, true, 3},
		{0.15, false, 2},
		{0.2, true, 4},
		{0.2, false, 3},
		{0.4, true, 4},
		{0.4, false, 3},
		{0.5, true, 5},
		{0.5, false, 4},
		{0.7, true, 5},
		{0.7, false, 4},
		{1.0, true, 6},
		{1.0, false, 5},
	}
	collector := New().(*collectorImpl)
	for _, c := range testCases {
		require.Equal(t,
			c.expect, collector.getBucket(c.value, c.hasZero))
	}
}

func TestCollectorImpl(t *testing.T) {
	collector := New()
	idx := IndexKey{
		TableID: 0,
		IndexID: 0,
	}

	// report a normal full scan
	collector.ReportIndex(idx, 1, 1, 1, 1)
	usage := collector.GetUsage(idx)
	require.Equal(t, uint64(1), usage.KvReqTotal.Load())
	require.Equal(t, uint64(1), usage.RowAccessTotal.Load())
	require.Equal(t, uint64(1), usage.RowReturnedTotal.Load())
	assertPercentageAccess(t, []uint64{0, 0, 0, 0, 0, 0, 1}, &usage.PercentageAccess)
	assertPercentageReturned(t, []uint64{0, 0, 0, 0, 0, 1}, &usage.PercentageReturned)

	// report a partial scan
	collector.ReportIndex(idx, 10, 10, 5, 50)
	usage = collector.GetUsage(idx)
	require.Equal(t, uint64(11), usage.KvReqTotal.Load())
	require.Equal(t, uint64(11), usage.RowAccessTotal.Load())
	require.Equal(t, uint64(6), usage.RowReturnedTotal.Load())
	assertPercentageAccess(t, []uint64{0, 0, 0, 0, 1, 0, 1}, &usage.PercentageAccess)
	assertPercentageReturned(t, []uint64{0, 0, 1, 0, 0, 1}, &usage.PercentageReturned)

	// report a 0 total row
	collector.ReportIndex(idx, 10, 10, 5, 0)
	usage = collector.GetUsage(idx)
	require.Equal(t, uint64(21), usage.KvReqTotal.Load())
	require.Equal(t, uint64(21), usage.RowAccessTotal.Load())
	require.Equal(t, uint64(11), usage.RowReturnedTotal.Load())
	assertPercentageAccess(t, []uint64{0, 0, 0, 0, 1, 0, 1}, &usage.PercentageAccess)
	assertPercentageReturned(t, []uint64{0, 0, 1, 0, 0, 1}, &usage.PercentageReturned)
}

type testOp struct {
	idx            IndexKey
	kvReqTotal     uint64
	rowAccess      uint64
	rowReturned    uint64
	tableTotalRows uint64
}

type testOpGenerator struct {
	tableCount         int64
	indexPerTableCount int64
	maxKvReqTotal      uint64
	maxTableTotalRows  uint64
}

func (g *testOpGenerator) generateTestOp() testOp {
	idx := IndexKey{
		rand.Int63() % g.tableCount,
		rand.Int63() % g.indexPerTableCount,
	}
	kvReqTotal := rand.Uint64() % g.maxKvReqTotal
	totalRows := rand.Uint64() % g.maxTableTotalRows
	rowAccess := uint64(0)
	if totalRows > 0 {
		rowAccess = rand.Uint64() % totalRows
	}
	rowReturned := uint64(0)
	if rowAccess > 0 {
		rowReturned = rand.Uint64() % rowAccess
	}

	return testOp{
		idx,
		kvReqTotal,
		rowAccess,
		rowReturned,
		totalRows,
	}
}

func assertCollector(t testing.TB, ops []testOp, actual *collectorImpl) {
	expect := New().(*collectorImpl)
	for _, op := range ops {
		expect.ReportIndex(
			op.idx, op.kvReqTotal, op.rowAccess, op.rowReturned, op.tableTotalRows)
	}

	require.Equal(t, len(expect.usages), len(actual.usages))
	for k, v := range expect.usages {
		p := actual.usages[k]
		require.Equal(t, v.QueryTotal.Load(), p.QueryTotal.Load())
		require.Equal(t, v.KvReqTotal.Load(), p.KvReqTotal.Load())
		require.Equal(t, v.RowAccessTotal.Load(), p.RowAccessTotal.Load())
		require.Equal(t, v.RowReturnedTotal.Load(), p.RowReturnedTotal.Load())
		for i := 0; i < len(v.PercentageAccess); i++ {
			require.Equal(t, v.PercentageAccess[i].Load(), p.PercentageAccess[i].Load())
		}
		for i := 0; i < len(v.PercentageReturned); i++ {
			require.Equal(t, v.PercentageReturned[i].Load(), p.PercentageReturned[i].Load())
		}
	}
}

func TestParallelReport(t *testing.T) {
	// generate operations
	const threads = 64
	const opPerThread = 100000
	const opCount = opPerThread * threads

	opGenerator := &testOpGenerator{
		10, 10, 10000, 10000,
	}
	ops := make([]testOp, 0, opCount)
	for i := 0; i < opCount; i++ {
		ops = append(ops, opGenerator.generateTestOp())
	}

	parallelCollector := New().(*collectorImpl)
	wg := &sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		localOps := ops[i*opPerThread : (i+1)*opPerThread]
		wg.Add(1)
		go func() {
			for _, op := range localOps {
				parallelCollector.ReportIndex(
					op.idx, op.kvReqTotal, op.rowAccess, op.rowReturned, op.tableTotalRows)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	assertCollector(t, ops, parallelCollector)
}

func BenchmarkCollector(b *testing.B) {
	b.Run("create new key", func(b *testing.B) {
		b.StopTimer()
		collector := New().(*collectorImpl)
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			collector.getUsage(IndexKey{
				TableID: int64(i),
				IndexID: int64(i),
			}, true)
		}
	})

	b.Run("update", func(b *testing.B) {
		b.StopTimer()
		opGenerator := &testOpGenerator{
			10, 10, 10000, 10000,
		}
		ops := make([]testOp, 0, b.N)
		for i := 0; i < b.N; i++ {
			ops = append(ops, opGenerator.generateTestOp())
		}
		collector := New()
		b.StartTimer()

		for _, op := range ops {
			collector.ReportIndex(
				op.idx, op.kvReqTotal, op.rowAccess, op.rowReturned, op.tableTotalRows)
		}
		b.StopTimer()
	})

	b.Run("update parallel", func(b *testing.B) {
		b.StopTimer()
		opGenerator := &testOpGenerator{
			10, 10, 10000, 10000,
		}
		ops := make([]testOp, 0, b.N)
		for i := 0; i < b.N; i++ {
			ops = append(ops, opGenerator.generateTestOp())
		}
		collector := New()
		i := &atomic.Int64{}
		b.StartTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				opIdx := i.Add(1) - 1
				op := ops[opIdx]
				collector.ReportIndex(
					op.idx, op.kvReqTotal, op.rowAccess, op.rowReturned, op.tableTotalRows)
			}
		})

		b.StopTimer()
		assertCollector(b, ops, collector.(*collectorImpl))
	})
}
