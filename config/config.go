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

package config

import (
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-sql-server/validator"
)

const (
	KeyConnection string = "connection"
	KeyTable      string = "table"
)

// Config contains configurable values
// shared between source and destination SQL Server connector.
type Config struct {
	// Connection string connection to SQL Server database.
	Connection string `validate:"required"`
	// Table is a name of the table that the connector should write to or read from.
	Table string `validate:"required,max=128"`
}

// Parse attempts to parse a provided map[string]string into a Config struct.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		Connection: cfg[KeyConnection],
		Table:      strings.ToUpper(cfg[KeyTable]),
	}

	if err := validator.Validate(&config); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}
