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

package ttlworker

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestTiDBServerVersionsConsistent(t *testing.T) {
	serverInfo := func(id, version string) *serverinfo.ServerInfo {
		return &serverinfo.ServerInfo{
			StaticInfo: serverinfo.StaticInfo{
				ID:          id,
				VersionInfo: serverinfo.VersionInfo{Version: version},
			},
		}
	}
	serverInfos := func(versions ...string) map[string]*serverinfo.ServerInfo {
		infos := make(map[string]*serverinfo.ServerInfo, len(versions))
		for i, version := range versions {
			id := strconv.Itoa(i)
			infos[id] = serverInfo(id, version)
		}
		return infos
	}
	serverInfoGettersContext := func(
		getServerInfo func() (*serverinfo.ServerInfo, error),
		getAllServerInfo func(context.Context) (map[string]*serverinfo.ServerInfo, error),
	) context.Context {
		ctx := context.WithValue(context.Background(), getServerInfoForTestContextKey{}, getServerInfo)
		return context.WithValue(ctx, getAllServerInfoForTestContextKey{}, getAllServerInfo)
	}

	tests := []struct {
		name           string
		currentVersion string
		serverVersions []string
		consistent     bool
		err            bool
	}{
		{
			"same release with different prerelease",
			"8.0.11-TiDB-v9.0.0-alpha-123-g1111111",
			[]string{
				"8.0.11-TiDB-v9.0.0-alpha-456-g2222222-dirty",
				"8.0.11-TiDB-v9.0.0-beta",
			}, true, false,
		},
		{"different release", "8.0.11-TiDB-v9.0.0", []string{"8.0.11-TiDB-v8.5.0"}, false, false},
		{"current server omitted from server info", "8.0.11-TiDB-v9.0.0", []string{"8.0.11-TiDB-v8.5.0", "8.0.11-TiDB-v8.5.0"}, false, false},
		{"empty server info", "8.0.11-TiDB-v9.0.0", nil, false, true},
		{"invalid current version", "invalid", []string{"8.0.11-TiDB-v9.0.0"}, false, true},
		{"invalid remote version", "8.0.11-TiDB-v9.0.0", []string{"invalid"}, false, true},
		{"different release with invalid remote version", "8.0.11-TiDB-v9.0.0", []string{"8.0.11-TiDB-v8.5.0", "invalid"}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consistent, err := tiDBServerVersionsConsistent(tt.currentVersion, serverInfos(tt.serverVersions...))
			if tt.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.consistent, consistent)
		})
	}

	t.Run("cache version check", func(t *testing.T) {
		localVersion := "8.0.11" + mysql.VersionSeparator + "v9.0.0"
		differentVersion := "8.0.11" + mysql.VersionSeparator + "v10.0.0"

		calls := 0
		version := localVersion
		checker := &ttlJobVersionChecker{}
		ctx := serverInfoGettersContext(
			func() (*serverinfo.ServerInfo, error) {
				return serverInfo("local", localVersion), nil
			},
			func(context.Context) (map[string]*serverinfo.ServerInfo, error) {
				calls++
				return serverInfos(version), nil
			},
		)

		require.Equal(t, ttlJobVersionAllowIndexScan, checker.check(ctx))
		require.Equal(t, ttlJobVersionAllowIndexScan, checker.check(ctx))
		require.Equal(t, 1, calls)

		checker.lastCheckTime = time.Now().Add(-serverVersionAllowCacheInterval)
		require.Equal(t, ttlJobVersionAllowIndexScan, checker.check(ctx))
		require.Equal(t, 2, calls)

		version = differentVersion
		checker.lastCheckTime = time.Now().Add(-serverVersionAllowCacheInterval)
		require.Equal(t, ttlJobVersionBlockJob, checker.check(ctx))
		require.Equal(t, ttlJobVersionBlockJob, checker.check(ctx))
		require.Equal(t, 3, calls)

		checker.lastCheckTime = time.Now().Add(-serverVersionMismatchCacheInterval)
		require.Equal(t, ttlJobVersionBlockJob, checker.check(ctx))
		require.Equal(t, 4, calls)
	})

	t.Run("fallback to PK when current server info lookup fails", func(t *testing.T) {
		calls := 0
		checker := &ttlJobVersionChecker{}
		ctx := serverInfoGettersContext(
			func() (*serverinfo.ServerInfo, error) {
				return nil, errors.New("mock current server info error")
			},
			func(context.Context) (map[string]*serverinfo.ServerInfo, error) {
				calls++
				return serverInfos("8.0.11" + mysql.VersionSeparator + "v10.0.0"), nil
			},
		)
		require.Equal(t, ttlJobVersionFallbackToPK, checker.check(ctx))
		require.Equal(t, ttlJobVersionFallbackToPK, checker.check(ctx))
		require.Equal(t, 0, calls)
	})

	t.Run("fallback to PK when server info lookup fails", func(t *testing.T) {
		calls := 0
		checker := &ttlJobVersionChecker{}
		ctx := serverInfoGettersContext(
			func() (*serverinfo.ServerInfo, error) {
				return serverInfo("local", "8.0.11"+mysql.VersionSeparator+"v9.0.0"), nil
			},
			func(context.Context) (map[string]*serverinfo.ServerInfo, error) {
				calls++
				return nil, errors.New("mock server info error")
			},
		)
		require.Equal(t, ttlJobVersionFallbackToPK, checker.check(ctx))
		require.Equal(t, ttlJobVersionFallbackToPK, checker.check(ctx))
		require.Equal(t, 1, calls)
	})

	t.Run("fallback to PK when versions cannot be compared", func(t *testing.T) {
		validVersion := "8.0.11" + mysql.VersionSeparator + "v9.0.0"
		for _, tt := range []struct {
			name           string
			currentVersion string
			serverInfos    map[string]*serverinfo.ServerInfo
		}{
			{name: "server info list is empty", currentVersion: validVersion, serverInfos: map[string]*serverinfo.ServerInfo{}},
			{name: "current version is invalid", currentVersion: "invalid", serverInfos: serverInfos(validVersion)},
			{name: "remote version is invalid", currentVersion: validVersion, serverInfos: serverInfos("invalid")},
			{name: "remote server info is nil", currentVersion: validVersion, serverInfos: map[string]*serverinfo.ServerInfo{"remote": nil}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				checker := &ttlJobVersionChecker{}
				ctx := serverInfoGettersContext(
					func() (*serverinfo.ServerInfo, error) {
						return serverInfo("local", tt.currentVersion), nil
					},
					func(context.Context) (map[string]*serverinfo.ServerInfo, error) {
						return tt.serverInfos, nil
					},
				)
				require.Equal(t, ttlJobVersionFallbackToPK, checker.check(ctx))
			})
		}
	})
}
