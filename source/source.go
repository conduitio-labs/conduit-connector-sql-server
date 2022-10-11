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
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	_ "github.com/denisenkom/go-mssqldb" //nolint:revive,nolintlint

	"github.com/conduitio-labs/conduit-connector-sql-server/config"
)

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   Config
	iterator Iterator
}

// New initialises a new source.
func New() sdk.Source {
	return &Source{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyConnection: {
			Description: "Connection string to SQL Server",
			Required:    true,
			Default:     "",
		},
		config.KeyTable: {
			Description: "A name of the table that the connector should write to.",
			Required:    true,
			Default:     "",
		},
		KeyPrimaryKey: {
			Description: "A name of column that connector will use for create record key",
			Required:    true,
			Default:     "",
		},
		KeyOrderingColumn: {
			Description: "A name of a column that the connector will use for ordering rows.",
			Required:    true,
			Default:     "",
		},
		KeyColumns: {
			Description: "The list of column names that should be included in each Record's payload",
			Required:    false,
			Default:     "",
		},
		KeyBatchSize: {
			Description: "The size of rows batch",
			Required:    false,
			Default:     "1000",
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) error {
	cfg, err := Parse(cfgRaw)
	if err != nil {
		return err
	}

	s.config = cfg

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	db, err := sqlx.Open("mssql", s.config.Connection)
	if err != nil {
		return fmt.Errorf("connect to sql server: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sql server: %w", err)
	}

	// TODO implement combined iterator
	return nil
}

// Read gets the next object from the sql server.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("next: %w", err)
	}

	return r, nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (s *Source) Ack(ctx context.Context, p sdk.Position) error {
	return s.iterator.Ack(ctx, p)
}
