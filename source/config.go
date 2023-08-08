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
	"github.com/conduitio-labs/conduit-connector-sql-server/config"
)

//go:generate paramgen -output=paramgen_src.go Config

const (
	// KeyOrderingColumn is a config name for an ordering column.
	KeyOrderingColumn = "orderingColumn"
	// KeyColumns is a config name for columns.
	KeyColumns = "columns"
	// KeyBatchSize is a config name for a batch size.
	KeyBatchSize = "batchSize"
	// KeyPrimaryKey is column name that records should use for their `Key` fields.
	KeyPrimaryKey string = "primaryKey"
	// KeySnapshot is a config name for snapshotMode.
	KeySnapshot = "snapshot"
)

// Config holds source specific configurable values.
type Config struct {
	config.Config

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"orderingColumn" validate:"required"`
	// Columns  list of column names that should be included in each Record's payload.
	Columns []string `json:"columns"`
	// BatchSize is the size of rows batch.
	BatchSize int `json:"batchSize" default:"1000" validate:"gt=1,lt=100000"`
	// Key - Column name that records should use for their `Key` fields.
	Key string `json:"primaryKey"`
	// Snapshot whether the plugin will take a snapshot of the entire table before starting cdc.
	Snapshot bool `default:"true"`
}
