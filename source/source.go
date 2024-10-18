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

	"github.com/conduitio-labs/conduit-connector-sql-server/source/iterator"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/denisenkom/go-mssqldb" //nolint:revive,nolintlint
	"github.com/jmoiron/sqlx"
)

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   Config
	iterator Iterator
}

// New initialises a new source.
func New() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters returns a map of named config.Parameters that describe how to configure the Source.
func (s *Source) Parameters() config.Parameters {
	return s.config.Parameters()
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(_ context.Context, cfg config.Config) error {
	var sourceConfig Config
	err := sdk.Util.ParseConfig(ctx, cfg, &sourceConfig, New().Parameters())
	if err != nil {
		return err
	}

	s.config = sourceConfig

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp opencdc.Position) error {
	db, err := sqlx.Open("mssql", s.config.Connection)
	if err != nil {
		return fmt.Errorf("connect to sql server: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sql server: %w", err)
	}

	s.iterator, err = iterator.NewCombinedIterator(ctx, db, s.config.Connection, s.config.Table, s.config.Key,
		s.config.OrderingColumn, s.config.Columns, s.config.BatchSize, s.config.Snapshot, rp)
	if err != nil {
		return fmt.Errorf("create combined iterator: %w", err)
	}

	return nil
}

// Read gets the next object from the sql server.
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("next: %w", err)
	}

	return r, nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (s *Source) Ack(ctx context.Context, p opencdc.Position) error {
	return s.iterator.Ack(ctx, p)
}
