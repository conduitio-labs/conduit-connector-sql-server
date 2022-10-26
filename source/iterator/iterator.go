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

	"github.com/conduitio-labs/conduit-connector-sql-server/columntypes"
	"github.com/conduitio-labs/conduit-connector-sql-server/source/position"
)

const (
	trackingTablePattern = "CONDUIT_TRACKING_%s"

	// tracking table columns.
	columnOperationType = "CONDUIT_OPERATION_TYPE"
	columnTimeCreated   = "CONDUIT_TRACKING_CREATED_DATE"
	columnTrackingID    = "CONDUIT_TRACKING_ID"
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
	sdkPosition sdk.Position,
) (*CombinedIterator, error) {
	var err error

	it := &CombinedIterator{
		conn:           conn,
		table:          table,
		columns:        columns,
		key:            key,
		orderingColumn: orderingColumn,
		batchSize:      batchSize,
		trackingTable:  fmt.Sprintf(trackingTablePattern, table),
	}

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

	if pos == nil || pos.IteratorType == position.TypeSnapshot {
		it.snapshot, err = NewSnapshotIterator(ctx, db, table, orderingColumn, key, columns,
			batchSize, pos, it.columnTypes)
		if err != nil {
			return nil, fmt.Errorf("new shapshot iterator: %w", err)
		}
	} else {
		it.cdc, err = NewCDCIterator(ctx, db, it.table, it.trackingTable, it.key,
			it.columns, it.batchSize, pos)
		if err != nil {
			return nil, fmt.Errorf("new shapshot iterator: %w", err)
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
	_, err = tx.ExecContext(ctx, fmt.Sprintf(queryAddGUIDColumn, c.trackingTable, columnTrackingID, c.table))
	if err != nil {
		return fmt.Errorf("add id column: %w", err)
	}

	columnNames := make([]string, 0)

	for key := range c.columnTypes {
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

func getTriggerName(operation, table string) string {
	return fmt.Sprintf("CONDUIT_TR_%s_%s", operation, table)
}
