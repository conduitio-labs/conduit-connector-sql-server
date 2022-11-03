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

package iterator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-sql-server/source/position"
)

// CDCIterator - cdc iterator.
type CDCIterator struct {
	db   *sqlx.DB
	rows *sqlx.Rows

	// table name.
	table string
	// trackingTable - tracking table name.
	trackingTable string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// key Name of column what iterator use for setting key in record.
	key string
	// batchSize size of batch.
	batchSize int
	// position last recorded position.
	position *position.Position
}

// NewCDCIterator create new cdc iterator.
func NewCDCIterator(
	ctx context.Context,
	db *sqlx.DB,
	table, trackingTable, key string,
	columns []string,
	batchSize int,
	position *position.Position,
) (*CDCIterator, error) {
	cdcIterator := &CDCIterator{
		db:            db,
		table:         table,
		trackingTable: trackingTable,
		columns:       columns,
		key:           key,
		batchSize:     batchSize,
		position:      position,
	}

	return cdcIterator, nil
}

// HasNext check ability to get next record.
func (i *CDCIterator) HasNext(ctx context.Context) (bool, error) {
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	if err := i.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return false, nil
}

// Next get new record.
func (i *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	id, ok := row[columnTrackingID].(int64)
	if !ok {
		return sdk.Record{}, ErrWrongTrackingIDType
	}

	operationType, ok := row[columnOperationType].(string)
	if !ok {
		return sdk.Record{}, ErrWrongTrackingIDType
	}

	pos := position.Position{
		IteratorType: position.TypeCDC,
		CDCID:        id,
		Time:         time.Now(),
	}

	convertedPosition, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	if _, ok = row[i.key]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	// delete tracking columns
	delete(row, columnOperationType)
	delete(row, columnTrackingID)
	delete(row, columnTimeCreated)

	transformedRowBytes, err := json.Marshal(row)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	i.position = &pos

	metadata := sdk.Metadata(map[string]string{metadataTable: i.table})
	metadata.SetCreatedAt(time.Now())

	switch operationType {
	case operationTypeInsert:
		return sdk.Util.Source.NewRecordCreate(convertedPosition, metadata,
			sdk.StructuredData{i.key: row[i.key]}, sdk.RawData(transformedRowBytes)), nil
	case operationTypeUpdate:
		return sdk.Util.Source.NewRecordUpdate(convertedPosition, metadata,
			sdk.StructuredData{i.key: row[i.key]}, nil, sdk.RawData(transformedRowBytes)), nil
	case operationTypeDelete:
		return sdk.Util.Source.NewRecordDelete(convertedPosition, metadata,
			sdk.StructuredData{i.key: row[i.key]}), nil
	default:
		return sdk.Record{}, ErrUnknownOperatorType
	}
}

// Stop shutdown iterator.
func (i *CDCIterator) Stop() error {
	if i.rows != nil {
		err := i.rows.Close()
		if err != nil {
			return err
		}
	}

	if i.db != nil {
		return i.db.Close()
	}

	return nil
}

// Ack check if record with position was recorded.
func (i *CDCIterator) Ack(ctx context.Context, pos *position.Position) error {
	return nil
}

// LoadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (i *CDCIterator) loadRows(ctx context.Context) error {
	selectBuilder := sqlbuilder.NewSelectBuilder()

	if len(i.columns) > 0 {
		// append additional columns
		selectBuilder.Select(append(i.columns,
			[]string{columnTrackingID, columnOperationType, columnTimeCreated}...)...)
	} else {
		selectBuilder.Select("*")
	}

	selectBuilder.From(i.trackingTable)

	if i.position != nil {
		selectBuilder.Where(
			selectBuilder.GreaterThan(columnTrackingID, i.position.CDCID),
		)
	}

	q, args := selectBuilder.
		OrderBy(columnTrackingID).
		Build()

	q = fmt.Sprintf("%s OFFSET 0 ROWS FETCH FIRST %d ROWS ONLY", q, i.batchSize)

	rows, err := i.db.QueryxContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("execute select query: %w", err)
	}

	i.rows = rows

	return nil
}
