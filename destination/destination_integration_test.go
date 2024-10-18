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

package destination

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

const (
	integrationTable = "conduit_integration_test_table"

	// queries.
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
	queryDropTable = `
	IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s')
  		 DROP TABLE %s;
`
)

func TestIntegrationDestination_Write_Insert_Success(t *testing.T) {
	var preparedID = 1

	ctx := context.Background()

	cfg, err := prepareConfig()
	if err != nil {
		t.Log(err)
		t.Skip(err)
	}

	db, err := sql.Open("mssql", cfg[ConfigConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	err = prepareTable(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[ConfigConnection]) //nolint:errcheck,nolintlint

	dest := New()

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	preparedData := map[string]any{
		"id":         preparedID,
		"cl_bigint":  321765482,
		"cl_tinyint": 2,
		"cl_varchar": "test",
		"cl_text":    "text_test",
		"cl_date": time.Date(
			2009, 11, 17, 20, 34, 58, 651387237, time.UTC),
		"cl_numeric":   1234.1234,
		"cl_decimal":   1234.1234,
		"cl_varbinary": []byte("some test"),
		"cl_float":     1234.1234,
	}

	count, err := dest.Write(ctx, []opencdc.Record{
		{
			Payload:   opencdc.Change{After: opencdc.StructuredData(preparedData)},
			Operation: opencdc.OperationSnapshot,
			Key:       opencdc.StructuredData{"id": "1"},
		},
	},
	)

	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	// check if row exist by id
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT id FROM %s", integrationTable))
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()

	var id int
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			t.Error(err)
		}
	}

	if err := rows.Err(); err != nil {
		t.Error("iterate rows error: %w", err)
	}

	if id != preparedID {
		t.Error(errors.New("id and prepared id not equal"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestIntegrationDestination_Write_Update_Success(t *testing.T) {
	var preparedVarchar = "updated_test"

	ctx := context.Background()

	cfg, err := prepareConfig()
	if err != nil {
		t.Log(err)
		t.Skip(err)
	}

	db, err := sql.Open("mssql", cfg[ConfigConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	err = prepareTable(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[ConfigConnection]) //nolint:errcheck,nolintlint

	dest := New()

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	preparedData := map[string]any{
		"id":         1,
		"cl_bigint":  321765482,
		"cl_tinyint": 2,
		"cl_varchar": "test",
		"cl_text":    "text_test",
		"cl_date": time.Date(
			2009, 11, 17, 20, 34, 58, 651387237, time.UTC),
		"cl_numeric":   1234.1234,
		"cl_decimal":   1234.1234,
		"cl_varbinary": []byte("some test"),
		"cl_float":     1234.1234,
	}

	count, err := dest.Write(ctx, []opencdc.Record{
		{
			Payload:   opencdc.Change{After: opencdc.StructuredData(preparedData)},
			Operation: opencdc.OperationSnapshot,
			Key:       opencdc.StructuredData{"id": "1"},
		},
	},
	)

	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	preparedData["cl_varchar"] = preparedVarchar

	_, err = dest.Write(ctx, []opencdc.Record{
		{
			Payload:   opencdc.Change{After: opencdc.StructuredData(preparedData)},
			Operation: opencdc.OperationUpdate,
			Key:       opencdc.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	// check if value was updated
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT cl_varchar FROM %s", integrationTable))
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()

	var clVarchar string
	for rows.Next() {
		err = rows.Scan(&clVarchar)
		if err != nil {
			t.Error(err)
		}
	}

	if err := rows.Err(); err != nil {
		t.Error("iterate rows error: %w", err)
	}

	if clVarchar != preparedVarchar {
		t.Error(errors.New("clVarchar and preparedVarchar not equal"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestIntegrationDestination_Write_Update_Composite_Keys_Success(t *testing.T) {
	var preparedVarchar = "updated_test"

	ctx := context.Background()

	cfg, err := prepareConfig()
	if err != nil {
		t.Log(err)
		t.Skip(err)
	}

	db, err := sql.Open("mssql", cfg[ConfigConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	err = prepareTable(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[ConfigConnection]) //nolint:errcheck,nolintlint

	dest := New()

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	preparedData := map[string]any{
		"id":         1,
		"cl_bigint":  321765482,
		"cl_tinyint": 2,
		"cl_varchar": "test",
		"cl_text":    "text_test",
		"cl_date": time.Date(
			2009, 11, 17, 20, 34, 58, 651387237, time.UTC),
		"cl_numeric":   1234.1234,
		"cl_decimal":   1234.1234,
		"cl_varbinary": []byte("some test"),
		"cl_float":     1234.1234,
	}

	count, err := dest.Write(ctx, []opencdc.Record{
		{
			Payload:   opencdc.Change{After: opencdc.StructuredData(preparedData)},
			Operation: opencdc.OperationSnapshot,
			Key:       opencdc.StructuredData{"id": "1"},
		},
	},
	)

	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	preparedData["cl_varchar"] = preparedVarchar

	_, err = dest.Write(ctx, []opencdc.Record{
		{
			Payload:   opencdc.Change{After: opencdc.StructuredData(preparedData)},
			Operation: opencdc.OperationUpdate,
			Key:       opencdc.StructuredData{"id": "1", "cl_tinyint": 2},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	// check if value was updated
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT cl_varchar FROM %s", integrationTable))
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()

	var clVarchar string
	for rows.Next() {
		err = rows.Scan(&clVarchar)
		if err != nil {
			t.Error(err)
		}
	}

	if err := rows.Err(); err != nil {
		t.Error("iterate rows error: %w", err)
	}

	if clVarchar != preparedVarchar {
		t.Error(errors.New("clVarchar and preparedVarchar not equal"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestIntegrationDestination_Write_Delete_Success(t *testing.T) {
	ctx := context.Background()

	cfg, err := prepareConfig()
	if err != nil {
		t.Log(err)
		t.Skip(err)
	}

	db, err := sql.Open("mssql", cfg[ConfigConnection])
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}

	err = prepareTable(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg[ConfigConnection]) //nolint:errcheck,nolintlint

	dest := New()

	err = dest.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = dest.Open(ctx)
	if err != nil {
		t.Error(err)
	}

	preparedData := map[string]any{
		"id":         1,
		"cl_bigint":  321765482,
		"cl_tinyint": 2,
		"cl_varchar": "test",
		"cl_text":    "text_test",
		"cl_date": time.Date(
			2009, 11, 17, 20, 34, 58, 651387237, time.UTC),
		"cl_numeric":   1234.1234,
		"cl_decimal":   1234.1234,
		"cl_varbinary": []byte("some test"),
		"cl_float":     1234.1234,
	}

	count, err := dest.Write(ctx, []opencdc.Record{
		{
			Payload:   opencdc.Change{After: opencdc.StructuredData(preparedData)},
			Operation: opencdc.OperationSnapshot,
			Key:       opencdc.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	count, err = dest.Write(ctx, []opencdc.Record{
		{
			Payload:   opencdc.Change{After: opencdc.StructuredData(preparedData)},
			Operation: opencdc.OperationDelete,
			Key:       opencdc.StructuredData{"id": "1"},
		},
	},
	)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Error(errors.New("count mismatched"))
	}

	err = dest.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}

	// check if row exist by id
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", integrationTable))
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			t.Error(err)
		}
	}

	if err := rows.Err(); err != nil {
		t.Error("iterate rows error: %w", err)
	}

	if count != 0 {
		t.Error(errors.New("count not zero"))
	}
}

func prepareConfig() (map[string]string, error) {
	conn := os.Getenv("SQL_SERVER_CONNECTION")
	if conn == "" {
		return nil, errors.New("missed env variable 'SQL_SERVER_CONNECTION'")
	}

	return map[string]string{
		ConfigConnection: conn,
		ConfigTable:      integrationTable,
	}, nil
}

func prepareTable(ctx context.Context, db *sql.DB) error {
	// for case if table exist
	_, err := db.ExecContext(ctx, fmt.Sprintf(queryDropTable, integrationTable, integrationTable))
	if err != nil {
		return err
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryCreateTable, integrationTable))
	if err != nil {
		return err
	}

	return nil
}

func clearData(ctx context.Context, connection string) error {
	db, err := sql.Open("mssql", connection)
	if err != nil {
		return fmt.Errorf("connect to sql server: %w", err)
	}

	defer db.Close()

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sql server: %w", err)
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(queryDropTable, integrationTable, integrationTable))
	if err != nil {
		return err
	}

	return nil
}
