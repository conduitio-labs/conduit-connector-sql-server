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

// SnapshotIterator - snapshot iterator.
type SnapshotIterator struct {
	db   *sqlx.DB
	rows *sqlx.Rows

	// table - table name.
	table string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// key Name of column what iterator use for setting key in record.
	key string
	// orderingColumn Name of column what iterator using for sorting data.
	orderingColumn string
	// maxValue from ordering column.
	maxValue any
	// batchSize size of batch.
	batchSize int
	// position last recorded position.
	position *position.Position
	// columnTypes column types from table.
	columnTypes map[string]string
}

func NewSnapshotIterator(
	ctx context.Context,
	db *sqlx.DB,
	table, orderingColumn, key string,
	columns []string,
	batchSize int,
	position *position.Position,
	columnTypes map[string]string,
) (*SnapshotIterator, error) {
	var err error

	snapshotIterator := &SnapshotIterator{
		db:             db,
		table:          table,
		columns:        columns,
		key:            key,
		orderingColumn: orderingColumn,
		batchSize:      batchSize,
		position:       position,
		columnTypes:    columnTypes,
	}

	err = snapshotIterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	if position != nil {
		snapshotIterator.maxValue = position.SnapshotMaxValue
	} else {
		err = snapshotIterator.setMaxValue(ctx)
		if err != nil {
			return nil, fmt.Errorf("set max value: %w", err)
		}
	}

	return snapshotIterator, nil
}

// HasNext check ability to get next record.
func (i *SnapshotIterator) HasNext(ctx context.Context) (bool, error) {
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	if err := i.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	// check new batch.
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	return false, nil
}

// Next get new record.
func (i *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	if _, ok := row[i.orderingColumn]; !ok {
		return sdk.Record{}, ErrOrderingColumnIsNotExist
	}

	pos := position.Position{
		IteratorType:             position.TypeSnapshot,
		SnapshotLastProcessedVal: row[i.orderingColumn],
		SnapshotMaxValue:         i.maxValue,
		Time:                     time.Now(),
	}

	convertedPosition, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	if _, ok := row[i.key]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	transformedRowBytes, err := json.Marshal(row)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	i.position = &pos

	metadata := sdk.Metadata(map[string]string{metadataTable: i.table})
	metadata.SetCreatedAt(time.Now())

	return sdk.Util.Source.NewRecordSnapshot(convertedPosition, metadata,
		sdk.StructuredData{i.key: row[i.key]}, sdk.RawData(transformedRowBytes)), nil
}

// Stop shutdown iterator.
func (i *SnapshotIterator) Stop(ctx context.Context) error {
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

// LoadRows selects a batch of rows from a database, based on the CombinedIterator's
// table, columns, orderingColumn, batchSize and the current position.
func (i *SnapshotIterator) loadRows(ctx context.Context) error {
	selectBuilder := sqlbuilder.NewSelectBuilder()

	if len(i.columns) > 0 {
		selectBuilder.Select(i.columns...)
	} else {
		selectBuilder.Select("*")
	}

	selectBuilder.From(i.table)

	if i.position != nil {
		selectBuilder.Where(
			selectBuilder.GreaterThan(i.orderingColumn, i.position.SnapshotLastProcessedVal),
			selectBuilder.LessEqualThan(i.orderingColumn, i.position.SnapshotMaxValue),
		)
	}

	q, args := selectBuilder.
		OrderBy(i.orderingColumn).
		Build()
	q = fmt.Sprintf("%s OFFSET 0 ROWS FETCH FIRST %d ROWS ONLY", q, i.batchSize)

	rows, err := i.db.QueryxContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("execute select query: %w", err)
	}

	i.rows = rows

	return nil
}

// getMaxValue get max value from ordered column.
func (i *SnapshotIterator) setMaxValue(ctx context.Context) error {
	rows, err := i.db.QueryxContext(ctx, fmt.Sprintf(queryGetMaxValue, i.orderingColumn, i.table))
	if err != nil {
		return fmt.Errorf("execute query get max value: %w", err)
	}
	defer rows.Close()

	var maxValue any
	for rows.Next() {
		err = rows.Scan(&maxValue)
		if err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
	}

	i.maxValue = maxValue

	return nil
}

// Ack check if record with position was recorded.
func (i *SnapshotIterator) Ack(ctx context.Context, p sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(p)).Msg("got ack")

	return nil
}
