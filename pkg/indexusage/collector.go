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
	"context"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"sync"
	"sync/atomic"
	"time"
)

// IndexKey indicates the key of an index in the `Collector`
type IndexKey struct {
	// TableID represents the id of the table
	TableID int64
	// IndexID represents the id of the index
	IndexID int64
}

// IndexUsage represents the statistic data of an index
type IndexUsage struct {
	// QueryTotal is the count of queries which used the index
	QueryTotal atomic.Uint64
	// KvReqTotal is the count of KV requests related with the index
	KvReqTotal atomic.Uint64

	// RowReturnedTotal is the count of accessed rows by scanning the index
	RowAccessTotal atomic.Uint64
	// PercentageAccess represents the histogram of the ratio of the accessed row to total rows in
	// 0, 0-1, 1-10, 10-20, 20-50, 50-100, 100
	PercentageAccess [7]atomic.Uint64

	// RowReturnedTotal is the count of returned rows by using the index
	RowReturnedTotal atomic.Uint64
	// PercentageReturned represents the histogram of the ratio of the returned row to total rows in
	// 0-1, 1-10, 10-20, 20-50, 50-100, 100
	PercentageReturned [6]atomic.Uint64
}

// RowCollector collects the index usage related with rows and kv requests
type RowCollector interface {
	// ReportIndex reports one usage of the index
	ReportIndex(key IndexKey, kvReqTotal uint64, rowAccess uint64, rowReturned uint64, tableTotalRows uint64)
}

// sessionPool represents a pool to get session
// TODO: the same declaration is used everywhere: TTL, statistics, etc. Consider whether it's suitable to use a single
// interface declaration for them all.
type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

// Collector is an interface to upload and read statistic data
type Collector interface {
	RowCollector
	ReportQuery(key IndexKey, queryTotal uint64)
	GetUsage(key IndexKey) *IndexUsage

	StartGCWorker(sessionPool) gcWorker
}

// gcWorker represents the worker to run GC periodically
type gcWorker interface {
	Stop()
}

type collectorImpl struct {
	mu     sync.RWMutex
	usages map[IndexKey]*IndexUsage
}

func (g *collectorImpl) getUsage(key IndexKey, createIfNotExist bool) *IndexUsage {
	g.mu.RLock()
	usage := g.usages[key]
	g.mu.RUnlock()

	if usage != nil {
		return usage
	}
	if !createIfNotExist {
		return nil
	}

	// the following is slow path. When the index has never appeared in the `usages`,
	// it will be created and stored in the map.
	g.mu.Lock()
	defer g.mu.Unlock()
	// create the index entry
	usage = g.usages[key]
	if usage != nil {
		return usage
	}
	usage = &IndexUsage{}
	g.usages[key] = usage
	return usage
}

var bucketBound = [6]float64{0, 0.01, 0.1, 0.2, 0.5, 1.0}

func (g *collectorImpl) getBucket(percentage float64, hasZero bool) int {
	if percentage == 0 {
		return 0
	}

	bucket := 0
	for i := 1; i < len(bucketBound); i++ {
		if percentage >= bucketBound[i-1] && percentage < bucketBound[i] {
			bucket = i
			break
		}
	}
	if percentage == 1.0 {
		bucket = len(bucketBound)
	}

	if !hasZero && bucket > 0 {
		bucket -= 1
	}
	return bucket
}

// ReportQuery implements Collector
func (g *collectorImpl) ReportQuery(key IndexKey, queryTotal uint64) {
	usage := g.getUsage(key, true)
	usage.QueryTotal.Add(queryTotal)
}

// ReportIndex implements Collector
func (g *collectorImpl) ReportIndex(key IndexKey, kvReqTotal uint64, rowAccess uint64, rowReturned uint64, tableTotalRows uint64) {
	usage := g.getUsage(key, true)
	usage.KvReqTotal.Add(kvReqTotal)
	usage.RowAccessTotal.Add(rowAccess)
	usage.RowReturnedTotal.Add(rowReturned)

	if tableTotalRows == 0 {
		// skip counting percentage histogram when the `tableTotalRows` is 0
		return
	}

	rowAccessPercentage := float64(rowAccess) / float64(tableTotalRows)
	rowReturnedPercentage := float64(rowReturned) / float64(tableTotalRows)
	usage.PercentageAccess[g.getBucket(rowAccessPercentage, true)].Add(1)
	usage.PercentageReturned[g.getBucket(rowReturnedPercentage, false)].Add(1)
}

// GetUsage implements Collector
func (g *collectorImpl) GetUsage(key IndexKey) *IndexUsage {
	return g.getUsage(key, false)
}

type gcWorkerImpl struct {
	wg     *sync.WaitGroup
	cancel func()
}

// StartGCWorker starts a GC worker to remove the deleted table/index periodically
// If a GC worker has already been started, this function will stop them at first.
func (g *collectorImpl) StartGCWorker(pool sessionPool) gcWorker {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	ticker := time.NewTicker(time.Minute * 30)

	wg.Add(1)
	go func() {
	loop:
		for {
			select {
			case <-ticker.C:
				resource, err := pool.Get()
				if err != nil {
					logutil.BgLogger().Warn("fail to get session from pool, skip index usage GC")
					continue
				}
				se, ok := resource.(sessionctx.Context)
				if !ok {
					logutil.BgLogger().Warn("session pool doesn't give a session, skip index usage GC")
					continue
				}

				g.runGC(se)
			case <-ctx.Done():
				break loop
			}
		}
		ticker.Stop()
		wg.Done()
	}()

	return &gcWorkerImpl{
		wg:     wg,
		cancel: cancel,
	}
}

func (g *collectorImpl) runGC(se sessionctx.Context) {
	schema := se.GetDomainInfoSchema().(infoschema.InfoSchema)
	removedKey := make([]IndexKey, 0, len(g.usages))
	g.mu.RLock()
	for k := range g.usages {
		tbl, ok := schema.TableByID(k.TableID)
		if !ok {
			removedKey = append(removedKey, k)
			continue
		}
		foundIdx := false
		for _, idx := range tbl.Indices() {
			if idx.Meta().ID == k.IndexID {
				foundIdx = true
				break
			}
		}
		if !foundIdx {
			removedKey = append(removedKey, k)
		}
	}
	g.mu.RUnlock()
}

// Stop stops the running GC worker.
func (g *gcWorkerImpl) Stop() {
	g.cancel()
	g.wg.Wait()
}

func New() Collector {
	return &collectorImpl{
		usages: make(map[IndexKey]*IndexUsage),
	}
}
