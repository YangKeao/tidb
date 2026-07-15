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
	"crypto/tls"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/kv"
	storeDriver "github.com/pingcap/tidb/pkg/store/driver"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tikvconfig "github.com/tikv/client-go/v2/config"
)

const passwordEnv = "TIDB_ROLLBACK_PASSWORD"

type options struct {
	mode             string
	path             string
	host             string
	port             string
	user             string
	operationTimeout time.Duration

	clusterCA   string
	clusterCert string
	clusterKey  string
	sqlCA       string
	sqlCert     string
	sqlKey      string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	opts, err := parseOptions(args)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.operationTimeout)
	defer cancel()
	logConfig := &logutil.LogConfig{}
	logConfig.Level = "error"
	if err := logutil.InitLogger(logConfig); err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}
	db, err := openSQL(opts)
	if err != nil {
		return fmt.Errorf("connect to TiDB SQL endpoint: %w", err)
	}
	defer db.Close()

	store, err := openStore(opts)
	if err != nil {
		return fmt.Errorf("connect to TiKV through PD: %w", err)
	}
	defer store.Close()

	runner := newRunner(db, store, os.Stdout)
	switch opts.mode {
	case modeCheck:
		return runner.check(ctx)
	case modeApply:
		return runner.apply(ctx)
	case modeVerify:
		return runner.verify(ctx)
	default:
		return fmt.Errorf("unsupported mode %q", opts.mode)
	}
}

func parseOptions(args []string) (options, error) {
	var opts options
	fset := flag.NewFlagSet("tidb-partial-index-rollback", flag.ContinueOnError)
	fset.StringVar(&opts.mode, "mode", modeCheck, "operation mode: check, apply, or verify")
	fset.StringVar(&opts.path, "path", "", "PD addresses, in the same form as tidb-server --path")
	fset.StringVar(&opts.host, "host", "127.0.0.1", "TiDB SQL host")
	fset.StringVar(&opts.port, "port", "4000", "TiDB SQL port")
	fset.StringVar(&opts.port, "P", "4000", "TiDB SQL port (tidb-server compatible alias)")
	fset.StringVar(&opts.user, "user", "root", "TiDB SQL user")
	fset.DurationVar(&opts.operationTimeout, "operation-timeout", 30*time.Minute, "whole operation timeout")
	fset.StringVar(&opts.clusterCA, "cluster-ca", "", "CA file for PD/TiKV TLS")
	fset.StringVar(&opts.clusterCert, "cluster-cert", "", "certificate file for PD/TiKV TLS")
	fset.StringVar(&opts.clusterKey, "cluster-key", "", "private key file for PD/TiKV TLS")
	fset.StringVar(&opts.sqlCA, "sql-ca", "", "CA file for TiDB SQL TLS")
	fset.StringVar(&opts.sqlCert, "sql-cert", "", "certificate file for TiDB SQL TLS")
	fset.StringVar(&opts.sqlKey, "sql-key", "", "private key file for TiDB SQL TLS")
	if err := fset.Parse(args); err != nil {
		return options{}, err
	}
	if fset.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fset.Args(), " "))
	}
	opts.mode = strings.ToLower(opts.mode)
	if opts.mode != modeCheck && opts.mode != modeApply && opts.mode != modeVerify {
		return options{}, errors.New("--mode must be check, apply, or verify")
	}
	if strings.TrimSpace(opts.path) == "" {
		return options{}, fmt.Errorf("--path with PD addresses is required")
	}
	if strings.ContainsAny(opts.path, "?/") {
		return options{}, fmt.Errorf("--path must contain Dedicated/API-v1 PD addresses only, without a scheme, path, query, or keyspace")
	}
	if err := validateTLSGroup("cluster", opts.clusterCA, opts.clusterCert, opts.clusterKey); err != nil {
		return options{}, err
	}
	if err := validateTLSGroup("sql", opts.sqlCA, opts.sqlCert, opts.sqlKey); err != nil {
		return options{}, err
	}
	return opts, nil
}

func validateTLSGroup(name, ca, cert, key string) error {
	if cert != "" || key != "" {
		if cert == "" || key == "" {
			return fmt.Errorf("--%s-cert and --%s-key must be specified together", name, name)
		}
		if ca == "" {
			return fmt.Errorf("--%s-ca, --%s-cert, and --%s-key must be specified together", name, name, name)
		}
	}
	return nil
}

func openSQL(opts options) (*sql.DB, error) {
	cfg := mysqlDriver.NewConfig()
	cfg.User = opts.user
	cfg.Passwd = os.Getenv(passwordEnv)
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(opts.host, opts.port)
	cfg.Collation = "utf8mb4_bin"
	cfg.Timeout = 10 * time.Second
	cfg.ReadTimeout = opts.operationTimeout
	cfg.WriteTimeout = opts.operationTimeout
	if opts.sqlCA != "" {
		tlsConfig, err := loadTLSConfig(opts.sqlCA, opts.sqlCert, opts.sqlKey, opts.host)
		if err != nil {
			return nil, err
		}
		cfg.TLS = tlsConfig
	}
	connector, err := mysqlDriver.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func openStore(opts options) (kv.Storage, error) {
	security := tikvconfig.Security{
		ClusterSSLCA:   opts.clusterCA,
		ClusterSSLCert: opts.clusterCert,
		ClusterSSLKey:  opts.clusterKey,
	}
	return (&storeDriver.TiKVDriver{}).OpenWithOptions(
		"tikv://"+opts.path+"?disableGC=true",
		storeDriver.WithSecurity(security),
	)
}

func loadTLSConfig(caPath, certPath, keyPath, serverName string) (*tls.Config, error) {
	config, err := tidbutil.NewTLSConfig(tidbutil.WithCAPath(caPath), tidbutil.WithCertAndKeyPath(certPath, keyPath))
	if err != nil {
		return nil, err
	}
	config.InsecureSkipVerify = false
	config.ServerName = serverName
	return config, nil
}
