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
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	_ "github.com/denisenkom/go-mssqldb" //nolint:revive,nolintlint

	"github.com/conduitio-labs/conduit-connector-sql-server/columntypes"
	"github.com/conduitio-labs/conduit-connector-sql-server/source/position"
)

// CombinedIterator combined iterator.
type CombinedIterator struct {
	cdc      *CDCIterator
	snapshot *SnapshotIterator

	// connection string.
	conn string

	// table - table name.
	table string
	// trackingTable - tracking table name.
	trackingTable string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// key Name of column what iterator use for setting key in record.
	key string
	// orderingColumn Name of column what iterator use for sorting data.
	orderingColumn string
	// batchSize size of batch.
	batchSize int
	// columnTypes column types from table.
	columnTypes map[string]string
}

// NewCombinedIterator - create new iterator.
func NewCombinedIterator(
	ctx context.Context,
	db *sqlx.DB,
	conn, table, key, orderingColumn string,
	columns []string,
	batchSize int,
	snapshot bool,
	sdkPosition sdk.Position,
) (*CombinedIterator, error) {
	var err error

	it := &CombinedIterator{
		conn:           conn,
		table:          table,
		columns:        columns,
		orderingColumn: orderingColumn,
		batchSize:      batchSize,
		trackingTable:  fmt.Sprintf(trackingTablePattern, table),
	}

	// set key field.
	err = it.setKey(ctx, db, key)
	if err != nil {
		return nil, fmt.Errorf("set key: %w", err)
	}

	it.checkColumnsField()

	// get column types for converting.
	it.columnTypes, err = columntypes.GetColumnTypes(ctx, db, table)
	if err != nil {
		return nil, fmt.Errorf("get table column types: %w", err)
	}

	// create tracking table, create triggers for cdc logic.
	err = it.SetupCDC(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("setup cdc: %w", err)
	}

	pos, err := position.ParseSDKPosition(sdkPosition)
	if err != nil {
		return nil, fmt.Errorf("parse position: %w", err)
	}

	if snapshot && (pos == nil || pos.IteratorType == position.TypeSnapshot) {
		it.snapshot, err = NewSnapshotIterator(ctx, db, it.table, it.orderingColumn, it.key, it.columns,
			it.batchSize, pos, it.columnTypes)
		if err != nil {
			return nil, fmt.Errorf("new shapshot iterator: %w", err)
		}
	} else {
		it.cdc, err = NewCDCIterator(ctx, db, it.table, it.trackingTable, it.key,
			it.columns, it.batchSize, pos, it.columnTypes)
		if err != nil {
			return nil, fmt.Errorf("new cdc iterator: %w", err)
		}
	}

	return it, nil
}

// SetupCDC - create tracking table, add columns, add triggers, set identity column.
func (c *CombinedIterator) SetupCDC(ctx context.Context, db *sqlx.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}

	defer tx.Rollback() // nolint:errcheck,nolintlint

	// check if table exist.
	rows, err := tx.QueryContext(ctx, queryIfTableExist, c.trackingTable)
	if err != nil {
		return fmt.Errorf("query exist table: %w", err)
	}

	defer rows.Close() //nolint:staticcheck,nolintlint

	for rows.Next() {
		var ct int
		er := rows.Scan(&ct)
		if er != nil {
			return fmt.Errorf("scan: %w", err)
		}

		if ct == 1 {
			// table exist, setup not needed.
			return nil
		}
	}

	// create tracking table with all columns from `table`.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryCreateTrackingTable, c.trackingTable, c.table))
	if err != nil {
		return fmt.Errorf("create tracking table: %w", err)
	}

	// add operation column.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryAddOperationTypeColumn, c.trackingTable, columnOperationType))
	if err != nil {
		return fmt.Errorf("add operation column: %w", err)
	}

	// add datetime column.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryAddODateTimeColumn, c.trackingTable, columnTimeCreated))
	if err != nil {
		return fmt.Errorf("add datetime column:: %w", err)
	}

	// add id column.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryAddIDColumn, c.trackingTable, columnTrackingID))
	if err != nil {
		return fmt.Errorf("add id column: %w", err)
	}

	columnNames := make([]string, 0)

	for key, val := range c.columnTypes {
		// sql-server doesn't support text, ntext, image column types inside triggers.
		// example of error:
		// mssql: Cannot use text, ntext, or image columns in the 'inserted' and 'deleted' tables.
		// connector excludes these columns.
		if val == columntypes.TextType || val == columntypes.NtextType || val == columntypes.ImageType {
			continue
		}

		columnNames = append(columnNames, key)
	}

	// add trigger to catch insert.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(triggerTemplate,
		getTriggerName(operationTypeInsert, c.table),
		c.table, operationTypeInsert, c.trackingTable, strings.Join(columnNames, ","), columnOperationType,
		strings.Join(columnNames, ","),
		operationTypeInsert, "inserted"))
	if err != nil {
		return fmt.Errorf("add trigger catch insert: %w", err)
	}

	// add trigger to catch update.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(triggerTemplate,
		getTriggerName(operationTypeUpdate, c.table),
		c.table, operationTypeUpdate, c.trackingTable, strings.Join(columnNames, ","), columnOperationType,
		strings.Join(columnNames, ","),
		operationTypeUpdate, "inserted"))
	if err != nil {
		return fmt.Errorf("add trigger catch update: %w", err)
	}

	// add trigger to catch delete.
	_, err = tx.ExecContext(ctx, fmt.Sprintf(triggerTemplate,
		getTriggerName(operationTypeDelete, c.table),
		c.table, operationTypeDelete, c.trackingTable, strings.Join(columnNames, ","), columnOperationType,
		strings.Join(columnNames, ","),
		operationTypeDelete, "deleted"))
	if err != nil {
		return fmt.Errorf("add trigger catch delete: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
// If the underlying snapshot iterator returns false, the combined iterator will try to switch to the cdc iterator.
func (c *CombinedIterator) HasNext(ctx context.Context) (bool, error) {
	switch {
	case c.snapshot != nil:
		hasNext, err := c.snapshot.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot has next: %w", err)
		}

		if !hasNext {
			if er := c.switchToCDCIterator(ctx); er != nil {
				return false, fmt.Errorf("switch to cdc iterator: %w", er)
			}

			return false, nil
		}

		return true, nil

	case c.cdc != nil:
		return c.cdc.HasNext(ctx)

	default:
		return false, nil
	}
}

// Next returns the next record.
func (c *CombinedIterator) Next(ctx context.Context) (sdk.Record, error) {
	switch {
	case c.snapshot != nil:
		return c.snapshot.Next(ctx)

	case c.cdc != nil:
		return c.cdc.Next(ctx)

	default:
		return sdk.Record{}, ErrNoInitializedIterator
	}
}

// Stop the underlying iterators.
func (c *CombinedIterator) Stop(ctx context.Context) error {
	if c.snapshot != nil {
		return c.snapshot.Stop(ctx)
	}

	if c.cdc != nil {
		return c.cdc.Stop(ctx)
	}

	return nil
}

// Ack collect tracking ids for removing.
func (c *CombinedIterator) Ack(ctx context.Context, rp sdk.Position) error {
	if c.cdc == nil {
		return nil
	}

	pos, err := position.ParseSDKPosition(rp)
	if err != nil {
		return fmt.Errorf("parse position: %w", err)
	}

	if pos.IteratorType == position.TypeCDC {
		return c.cdc.Ack(ctx, pos)
	}

	return nil
}

// switchToCDCIterator close snapshot iterator and run cdc iterator.
func (c *CombinedIterator) switchToCDCIterator(ctx context.Context) error {
	var err error

	err = c.snapshot.Stop(ctx)
	if err != nil {
		return fmt.Errorf("stop snaphot iterator: %w", err)
	}

	c.snapshot = nil

	db, err := sqlx.Open("mssql", c.conn)
	if err != nil {
		return err
	}

	c.cdc, err = NewCDCIterator(ctx, db, c.table, c.trackingTable, c.key,
		c.columns, c.batchSize, nil, c.columnTypes)
	if err != nil {
		return fmt.Errorf("new cdc iterator: %w", err)
	}

	return nil
}

// getPrimaryKeyField - get info about primary key field.
func (c *CombinedIterator) getPrimaryKeyFieldFromTable(ctx context.Context, db *sqlx.DB, table string) (string, error) {
	rows, err := db.QueryxContext(ctx, fmt.Sprintf(queryGetPrimaryKey, table, table))
	if err != nil {
		return "", fmt.Errorf("get primary key: %w", err)
	}

	var field string

	for rows.Next() {
		err = rows.Scan(&field)
		if err != nil {
			return "", fmt.Errorf("scan rows: %w", err)
		}
	}

	return field, nil
}

// setKey - set key field by priority:
// 1. Key from config.
// 2. Primary key from table.
// 3. Ordering column.
func (c *CombinedIterator) setKey(ctx context.Context, db *sqlx.DB, keyFromConfig string) error {
	if keyFromConfig != "" {
		c.key = keyFromConfig

		return nil
	}

	key, err := c.getPrimaryKeyFieldFromTable(ctx, db, c.table)
	if err != nil {
		return fmt.Errorf("get primary key from table: %w", err)
	}

	if key != "" {
		c.key = key

		return nil
	}

	c.key = c.orderingColumn

	return nil
}

// checkColumnsField check if key exist in custom columns, and if not add it.
func (c *CombinedIterator) checkColumnsField() {
	if c.columns == nil {
		return
	}

	for i := range c.columns {
		if c.columns[i] == c.key {
			return
		}
	}

	c.columns = append(c.columns, c.key)
}

func getTriggerName(operation, table string) string {
	return fmt.Sprintf("CONDUIT_TR_%s_%s", operation, table)
}
