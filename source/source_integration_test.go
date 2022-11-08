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

package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matryer/is"

	"github.com/conduitio-labs/conduit-connector-sql-server/config"
)

const (
	queryCreateTable = `
		CREATE TABLE %s (
			id int NOT NULL PRIMARY KEY,
			cl_bigint BIGINT,
			cl_tinyint TINYINT,
			cl_varchar VARCHAR(40),
			cl_text TEXT,
			cl_date DATE,
			cl_numeric NUMERIC(10,5),
			cl_decimal DECIMAL(10,5),
			cl_varbinary VARBINARY(100),
		    cl_float FLOAT
		)
`
	queryInsertSnapshotData = `
		INSERT INTO %s VALUES (1, 135345, 1, 'test', 'text', 
		                   '2018-09-09', 324.234, 234.234, CAST( 'A' AS VARBINARY), 194423.432),
							  (2, 236754, 1, 'test2', 'text2', 
		                   '2019-09-09', 424.234, 634.234, CAST( 'B' AS VARBINARY), 254423.542),
		                   	  (3, 3346454, 2, 'test3', 'text3', 
		                   '2019-10-10', 524.234, 534.234, CAST( 'C' AS VARBINARY), 364423.932)
`
	queryDropTable         = `DROP TABLE %s`
	queryDropTrackingTable = `DROP TABLE CONDUIT_TRACKING_%s`
)

func TestSource_Snapshot_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	db, err := sql.Open("mssql", cfg[config.KeyConnection])
	if err != nil {
		t.Fatal(err)
	}

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, tableName))
		if err != nil {
			t.Log(err)
		}

		_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTrackingTable, tableName))
		if err != nil {
			t.Log(err)
		}

		db.Close()
	})

	err = prepareData(ctx, db, tableName)
	if err != nil {
		t.Fatal(err)
	}

	s := New()

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// check first record.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedFirstRecord := map[string]any{"cl_bigint": 135345, "cl_date": "2018-09-09T00:00:00Z", "cl_decimal": "234.23400",
		"cl_float": 194423.432, "cl_numeric": "324.23400",
		"cl_text": "text", "cl_tinyint": 1, "cl_varbinary": "QQ==", "cl_varchar": "test", "id": 1}

	firstRecordBytes, err := json.Marshal(wantedFirstRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(firstRecordBytes, r.Payload.After.Bytes())

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func prepareConfigMap(table string) (map[string]string, error) {
	connection := os.Getenv("SQL_SERVER_CONNECTION")

	if connection == "" {
		return map[string]string{}, errors.New("SQL_SERVER_CONNECTION env var must be set")
	}

	return map[string]string{
		config.KeyConnection: connection,
		config.KeyTable:      table,
		KeyPrimaryKey:        "id",
		KeyOrderingColumn:    "id",
	}, nil
}

func prepareData(ctx context.Context, db *sql.DB, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, table))
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryInsertSnapshotData, table))
	if err != nil {
		return fmt.Errorf("insert_data: %w", err)
	}

	return nil
}

func randomIdentifier(t *testing.T) string {
	return strings.ToUpper(fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000))
}
