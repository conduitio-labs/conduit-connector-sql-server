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

//go:generate paramgen -output=paramgen_dest.go Config

package destination

import (
	"context"
	"database/sql"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"

	_ "github.com/denisenkom/go-mssqldb" //nolint:revive,nolintlint

	"github.com/conduitio-labs/conduit-connector-sql-server/config"
	"github.com/conduitio-labs/conduit-connector-sql-server/destination/writer"
)

// Destination SQL Server Connector persists records to a sql server database.
type Destination struct {
	sdk.UnimplementedDestination

	writer Writer
	config Config
}

type Config struct {
	config.Config
}

// New creates new instance of the Destination.
func New() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return d.config.Parameters()
}

// Configure parses and initializes the config.
func (d *Destination) Configure(_ context.Context, cfg map[string]string) error {
	var destConfig Config
	err := sdk.Util.ParseConfig(cfg, &destConfig)
	if err != nil {
		return err
	}

	d.config = destConfig

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	db, err := sql.Open("mssql", d.config.Connection)
	if err != nil {
		return fmt.Errorf("connect to sql server: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sql server: %w", err)
	}

	d.writer, err = writer.NewWriter(ctx, writer.Params{
		DB:    db,
		Table: d.config.Table,
	})

	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, record := range records {
		err := sdk.Util.Destination.Route(ctx, record,
			d.writer.Insert,
			d.writer.Update,
			d.writer.Delete,
			d.writer.Insert,
		)
		if err != nil {
			return i, fmt.Errorf("route %s: %w", record.Operation.String(), err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.writer != nil {
		return d.writer.Close(ctx)
	}

	return nil
}
