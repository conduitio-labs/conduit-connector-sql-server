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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
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

	queryInsertOneRow = `
		INSERT INTO %s VALUES (1, 135345, 1, 'test', 'text', 
		                   '2018-09-09', 324.234, 234.234, CAST( 'A' AS VARBINARY), 194423.432)
		
`
	queryUpdate = `
		UPDATE %s SET cl_varchar = 'update' WHERE id = 1
		
`
	queryDelete = `
		DELETE FROM %s WHERE id = 1
		
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

	db, err := sql.Open("mssql", cfg[ConfigConnection])
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

	wantedFirstRecord := map[string]any{"cl_bigint": 135345, "cl_date": "2018-09-09T00:00:00Z",
		"cl_decimal": "234.23400", "cl_float": 194423.432, "cl_numeric": "324.23400", "cl_text": "text",
		"cl_tinyint": 1, "cl_varbinary": "QQ==", "cl_varchar": "test", "id": 1}

	firstRecordBytes, err := json.Marshal(wantedFirstRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(firstRecordBytes, r.Payload.After.Bytes())

	// check second record.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedSecondRecord := map[string]any{"cl_bigint": 236754, "cl_date": "2019-09-09T00:00:00Z", "cl_decimal": "634.23400",
		"cl_float": 254423.542, "cl_numeric": "424.23400", "cl_text": "text2", "cl_tinyint": 1, "cl_varbinary": "Qg==",
		"cl_varchar": "test2", "id": 2}

	secondRecordBytes, err := json.Marshal(wantedSecondRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(secondRecordBytes, r.Payload.After.Bytes())

	// check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// let's continue from last processed position.
	err = s.Open(ctx, r.Position)
	if err != nil {
		t.Fatal(err)
	}

	// check third record.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedThirdRecord := map[string]any{"cl_bigint": 3346454, "cl_date": "2019-10-10T00:00:00Z", "cl_decimal": "534.23400",
		"cl_float": 364423.932, "cl_numeric": "524.23400", "cl_text": "text3", "cl_tinyint": 2, "cl_varbinary": "Qw==",
		"cl_varchar": "test3", "id": 3}

	wantedThirdBytes, err := json.Marshal(wantedThirdRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(wantedThirdBytes, r.Payload.After.Bytes())

	// check ErrBackoffRetry.
	_, err = s.Read(ctx)
	is.Equal(sdk.ErrBackoffRetry, err)

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Empty_Table(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	db, err := sql.Open("mssql", cfg[ConfigConnection])
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

	err = prepareEmptyTable(ctx, db, tableName)
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

	// check ErrBackoffRetry.
	_, err = s.Read(ctx)
	is.Equal(sdk.ErrBackoffRetry, err)

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Key_From_Config(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	// set key.
	cfg[ConfigPrimaryKey] = "cl_tinyint"

	db, err := sql.Open("mssql", cfg[ConfigConnection])
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

	// check ErrBackoffRetry.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedKey := map[string]any{"cl_tinyint": 1}

	wantedKeyBytes, err := json.Marshal(wantedKey)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(r.Key.Bytes(), wantedKeyBytes)

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Key_From_Table(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	// set empty key.
	cfg[ConfigPrimaryKey] = ""

	db, err := sql.Open("mssql", cfg[ConfigConnection])
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

	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedKey := map[string]any{"id": 1}

	wantedKeyBytes, err := json.Marshal(wantedKey)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(r.Key.Bytes(), wantedKeyBytes)

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_CDC_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	db, err := sql.Open("mssql", cfg[ConfigConnection])
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

	err = prepareEmptyTable(ctx, db, tableName)
	if err != nil {
		t.Fatal(err)
	}

	s := New()

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// insert second row.
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryInsertOneRow, tableName))
	if err != nil {
		t.Fatal(err)
	}

	// update row.
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryUpdate, tableName))
	if err != nil {
		t.Fatal(err)
	}

	// delete row.
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryDelete, tableName))
	if err != nil {
		t.Fatal(err)
	}

	// Check read from empty table.
	_, err = s.Read(ctx)
	is.Equal(sdk.ErrBackoffRetry, err)

	// check inserted data.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedRecord := map[string]any{"cl_bigint": 135345, "cl_date": "2018-09-09T00:00:00Z",
		"cl_decimal": "234.23400", "cl_float": 194423.432, "cl_numeric": "324.23400", "cl_text": nil,
		"cl_tinyint": 1, "cl_varbinary": "QQ==", "cl_varchar": "test", "id": 1}

	wantedRecordBytes, err := json.Marshal(wantedRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(wantedRecordBytes, r.Payload.After.Bytes())
	is.Equal(opencdc.OperationCreate, r.Operation)

	// check updated data.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedRecord = map[string]any{"cl_bigint": 135345, "cl_date": "2018-09-09T00:00:00Z",
		"cl_decimal": "234.23400", "cl_float": 194423.432, "cl_numeric": "324.23400", "cl_text": nil,
		"cl_tinyint": 1, "cl_varbinary": "QQ==", "cl_varchar": "update", "id": 1}

	wantedRecordBytes, err = json.Marshal(wantedRecord)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(wantedRecordBytes, r.Payload.After.Bytes())
	is.Equal(opencdc.OperationUpdate, r.Operation)

	// check deleted data.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	is.Equal(opencdc.OperationDelete, r.Operation)

	// check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Off(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tableName := randomIdentifier(t)

	cfg, err := prepareConfigMap(tableName)
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	db, err := sql.Open("mssql", cfg[ConfigConnection])
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

	// turn off snapshot
	cfg[ConfigSnapshot] = "false"

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// update.
	_, err = db.ExecContext(ctx, fmt.Sprintf(queryUpdate, tableName))
	if err != nil {
		t.Fatal(err)
	}

	// Check read. Snapshot data must be missed.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(r.Operation, opencdc.OperationUpdate) {
		t.Fatal(errors.New("not wanted type"))
	}

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
		ConfigConnection:     connection,
		ConfigTable:          table,
		ConfigPrimaryKey:     "id",
		ConfigOrderingColumn: "id",
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

func prepareEmptyTable(ctx context.Context, db *sql.DB, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, table))
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

func randomIdentifier(t *testing.T) string {
	return strings.ToUpper(fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000))
}
