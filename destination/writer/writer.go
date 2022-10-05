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

package writer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/huandu/go-sqlbuilder"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// metadata related.
	metadataTable = "sqlserver.table"
)

// Writer implements a writer logic for db2 destination.
type Writer struct {
	db        *sql.DB
	table     string
	keyColumn string
}

// Params is an incoming params for the NewWriter function.
type Params struct {
	DB        *sql.DB
	Table     string
	KeyColumn string
}

// NewWriter creates new instance of the Writer.
func NewWriter(ctx context.Context, params Params) (*Writer, error) {
	writer := &Writer{
		db:        params.DB,
		table:     params.Table,
		keyColumn: params.KeyColumn,
	}

	return writer, nil
}

// Close closes the underlying db connection.
func (w *Writer) Close(ctx context.Context) error {
	return w.db.Close()
}

// Delete deletes records by a key. First it looks in the sdk.Record.Key,
// if it doesn't find a key there it will use the default configured value for a key.
func (w *Writer) Delete(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	keyColumn, err := w.getKeyColumn(key)
	if err != nil {
		return fmt.Errorf("get key column: %w", err)
	}

	// return an error if we didn't find a value for the key
	keyValue, ok := key[keyColumn]
	if !ok {
		return ErrEmptyKey
	}

	query, args := w.buildDeleteQuery(tableName, keyColumn, keyValue)

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec delete: %w", err)
	}

	return nil
}

// Update updates records by a key. First it looks in the sdk.Record.Key,
// if it doesn't find a key there it will use the default configured value for a key.
func (w *Writer) Update(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrEmptyPayload
	}

	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	keyColumn, err := w.getKeyColumn(key)
	if err != nil {
		return fmt.Errorf("get key column: %w", err)
	}

	// return an error if we didn't find a value for the key
	keyValue, ok := key[keyColumn]
	if !ok {
		return ErrEmptyKey
	}

	query, args := w.buildUpdateQuery(tableName, keyColumn, keyValue, payload)

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec update: %w", err)
	}

	return nil
}

// getTableName returns either the records metadata value for table
// or the default configured value for table.
func (w *Writer) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataTable]
	if !ok {
		return w.table
	}

	return tableName
}

// Insert row to sql server db.
func (w *Writer) Insert(ctx context.Context, record sdk.Record) error {
	tableName := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrEmptyPayload
	}

	key, err := w.structurizeData(record.Key)
	if err != nil {
		// if the key is not structured, we simply ignore it
		// we'll try to insert just a payload in this case
		sdk.Logger(ctx).Debug().Msgf("structurize key during upsert: %v", err)
	}

	keyColumn, err := w.getKeyColumn(key)
	if err != nil {
		return fmt.Errorf("get key column: %w", err)
	}

	// if the record doesn't contain the key, insert the key if it's not empty.
	if _, ok := payload[keyColumn]; !ok {
		if _, ok := key[keyColumn]; ok {
			payload[keyColumn] = key[keyColumn]
		}
	}

	columns, values := w.extractColumnsAndValues(payload)

	query, args := w.buildInsertQuery(tableName, columns, values)

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec upsert: %w", err)
	}

	return nil
}

// buildDeleteQuery generates an SQL DELETE statement query,
// based on the provided table, keyColumn and keyValue.
func (w *Writer) buildDeleteQuery(table string, keyColumn string, keyValue any) (string, []any) {
	db := sqlbuilder.NewDeleteBuilder()

	db.DeleteFrom(table)
	db.Where(
		db.Equal(keyColumn, keyValue),
	)

	query, args := db.Build()

	return query, args
}

// getKeyColumn returns either the first key within the Key structured data
// or the default key configured value for key.
func (w *Writer) getKeyColumn(key sdk.StructuredData) (string, error) {
	if len(key) > 1 {
		return "", ErrCompositeKeysNotSupported
	}

	for k := range key {
		return k, nil
	}

	return w.keyColumn, nil
}

// structurizeData converts sdk.Data to sdk.StructuredData.
func (w *Writer) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

// extractColumnsAndValues turns the payload into slices of
// columns and values for inserting into db2.
func (w *Writer) extractColumnsAndValues(payload sdk.StructuredData) ([]string, []any) {
	var (
		columns []string
		values  []any
	)

	for key, value := range payload {
		columns = append(columns, key)
		values = append(values, value)
	}

	return columns, values
}

func (w *Writer) buildInsertQuery(table string, columns []string, values []any) (string, []any) {
	sb := sqlbuilder.NewInsertBuilder()

	sb.InsertInto(table)
	sb.Cols(columns...)
	sb.Values(values...)

	return sb.Build()
}

func (w *Writer) buildUpdateQuery(
	table, keyColumn string,
	keyValue any,
	payload map[string]any,
) (string, []any) {
	sb := sqlbuilder.NewUpdateBuilder()

	sb.Update(table)

	setVal := make([]string, 0)
	for key, val := range payload {
		setVal = append(setVal, sb.Assign(key, val))
	}

	sb.Set(setVal...)
	sb.Where(sb.Equal(keyColumn, keyValue))

	return sb.Build()
}
