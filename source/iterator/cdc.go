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

	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-sql-server/source/position"
)

// trackingTableService service for clearing tracking table.
type trackingTableService struct {
	// channel for getting stop signal.
	stopCh chan struct{}
	// channel for errors.
	errCh chan error
	// channel for notify that all queries finished and db can be closed.
	canCloseCh chan struct{}
	// idsForRemoving - ids of rows what need to clear.
	idsForRemoving []any
}

func newTrackingTableService() *trackingTableService {
	stopCh := make(chan struct{}, 1)
	canCloseCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	trackingIDsForRemoving := make([]any, 0)

	return &trackingTableService{
		stopCh:         stopCh,
		errCh:          errCh,
		idsForRemoving: trackingIDsForRemoving,
		canCloseCh:     canCloseCh,
	}
}

// CDCIterator - cdc iterator.
type CDCIterator struct {
	db *sqlx.DB

	// tableSrv service for clearing tracking table.
	tableSrv *trackingTableService

	// table - table name.
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
	table string, trackingTable, key string,
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
		tableSrv:      newTrackingTableService(),
	}

	return cdcIterator, nil
}
