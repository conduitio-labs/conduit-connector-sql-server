// Copyright © 2022 Meroxa, Inc & Yalantis.
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

package sqlserver

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/conduitio-labs/conduit-connector-sql-server/source"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	queryCreateTestTable       = `CREATE TABLE %s (ID INT, NAME VARCHAR(100))`
	queryDropTestTable         = `DROP TABLE %s`
	queryDropTestTrackingTable = `DROP TABLE CONDUIT_TRACKING_%s`
	queryIfExistTable          = `SELECT count(*) as ct
										FROM INFORMATION_SCHEMA.TABLES
										WHERE TABLE_TYPE = 'BASE TABLE'
										AND TABLE_NAME = ?`
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	counter int64
}

// GenerateRecord generates a random opencdc.Record.
func (d *driver) GenerateRecord(_ *testing.T, operation opencdc.Operation) opencdc.Record {
	atomic.AddInt64(&d.counter, 1)

	return opencdc.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			source.ConfigTable: d.Config.DestinationConfig[source.ConfigTable],
		},
		Key: opencdc.StructuredData{
			"ID": d.counter,
		},
		Payload: opencdc.Change{After: opencdc.RawData(
			fmt.Sprintf(
				`{"ID":%d,"NAME":"%s"}`, d.counter, gofakeit.Name(),
			),
		),
		},
	}
}

//nolint:paralleltest // we don't need paralleltest for the Acceptance tests.
func TestAcceptance(t *testing.T) {
	cfg := prepareConfig(t)

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest:        beforeTest(t, cfg),
			},
		},
	})
}

// beforeTest creates new table before each test.
func beforeTest(_ *testing.T, cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		table := randomIdentifier(t)
		t.Logf("table under test: %v", table)

		cfg[source.ConfigTable] = table

		err := prepareData(t, cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func prepareConfig(t *testing.T) map[string]string {
	conn := os.Getenv("SQL_SERVER_CONNECTION")
	if conn == "" {
		t.Skip("SQL_SERVER_CONNECTION env var must be set")

		return nil
	}

	return map[string]string{
		source.ConfigConnection:     conn,
		source.ConfigOrderingColumn: "ID",
		source.ConfigPrimaryKey:     "ID",
	}
}

func prepareData(t *testing.T, cfg map[string]string) error {
	db, err := sql.Open("mssql", cfg[source.ConfigConnection])
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTestTable, cfg[source.ConfigTable]))
	if err != nil {
		return err
	}

	db.Close()

	// drop table
	t.Cleanup(func() {
		db, err = sql.Open("mssql", cfg[source.ConfigConnection])
		if err != nil {
			t.Fatal(err)
		}

		queryDropTable := fmt.Sprintf(queryDropTestTable, cfg[source.ConfigTable])

		_, err = db.Exec(queryDropTable)
		if err != nil {
			t.Errorf("drop test table: %v", err)
		}

		queryDropTrackingTable := fmt.Sprintf(queryDropTestTrackingTable, cfg[source.ConfigTable])

		// check if table exist.
		rows, er := db.Query(queryIfExistTable, cfg[source.ConfigTable])
		if er != nil {
			t.Error(er)
		}

		defer rows.Close() //nolint:staticcheck,nolintlint

		for rows.Next() {
			var count int
			err = rows.Scan(&count)
			if err != nil {
				t.Error(er)
			}

			if count == 1 {
				// table exist, setup not needed.
				_, err = db.Exec(queryDropTrackingTable)
				if err != nil {
					t.Errorf("drop test tracking table: %v", err)
				}
			}
		}
		if rows.Err() != nil {
			t.Error(rows.Err())
		}

		if err = db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})

	return nil
}

func randomIdentifier(t *testing.T) string {
	return strings.ToUpper(fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000))
}
