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
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-sql-server/config"
)

func TestParse(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    Config
		wantErr bool
	}{
		{
			name: "success, required and default fields",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					config.KeyTable:      "CLIENTS",
					KeyOrderingColumn:    "ID",
					KeyBatchSize:         "",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				BatchSize:      defaultBatchSize,
				Snapshot:       true,
			},
			wantErr: false,
		},
		{
			name: "success, custom batch size",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					config.KeyTable:      "CLIENTS",
					KeyOrderingColumn:    "ID",
					KeyBatchSize:         "50",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				BatchSize:      50,
				Snapshot:       true,
			},
			wantErr: false,
		},
		{
			name: "success, custom keys",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					config.KeyTable:      "CLIENTS",
					KeyPrimaryKeys:       "ID",
					KeyOrderingColumn:    "ID",
					KeyBatchSize:         "50",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				BatchSize:      50,
				Keys:           []string{"ID"},
				Snapshot:       true,
			},
			wantErr: false,
		},
		{
			name: "success, custom 2 keys",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					config.KeyTable:      "CLIENTS",
					KeyPrimaryKeys:       "ID,NAME",
					KeyOrderingColumn:    "ID",
					KeyBatchSize:         "50",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				BatchSize:      50,
				Keys:           []string{"ID", "NAME"},
				Snapshot:       true,
			},
			wantErr: false,
		},
		{
			name: "success, custom snapshot",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					config.KeyTable:      "CLIENTS",
					KeyOrderingColumn:    "ID",
					KeyBatchSize:         "50",
					KeySnapshot:          "false",
				},
			},
			want: Config{
				Config: config.Config{
					Connection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					Table:      "CLIENTS",
				},
				OrderingColumn: "ID",
				BatchSize:      50,
				Snapshot:       false,
			},
			wantErr: false,
		},
		{
			name: "failed, missed ordering column",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					config.KeyTable:      "CLIENTS",
					KeyBatchSize:         "50",
				},
			},
			wantErr: true,
		},
		{
			name: "success, snapshot error",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30",
					config.KeyTable:      "CLIENTS",
					KeyOrderingColumn:    "ID",
					KeyBatchSize:         "50",
					KeySnapshot:          "mode",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
