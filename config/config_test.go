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
	"reflect"
	"testing"
)

const (
	conn = "sqlserver://sa:password@0.0.0.0?database=mydb&connection+timeout=30"
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
			name: "success",
			args: args{
				cfg: map[string]string{
					KeyConnection: conn,
					KeyTable:      "CLIENTS",
				},
			},
			want: Config{
				Connection: conn,
				Table:      "CLIENTS",
			},
			wantErr: false,
		},
		{
			name: "fail, missed connection",
			args: args{
				cfg: map[string]string{
					KeyConnection: "",
					KeyTable:      "CLIENTS",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, missed table",
			args: args{
				cfg: map[string]string{
					KeyConnection: conn,
					KeyTable:      "",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid table",
			args: args{
				cfg: map[string]string{
					KeyConnection: conn,
					KeyTable: "somerealyrealyrealyorunrealynamewithalotcharactersandsomesymbolsbutwithsomesenceor" +
						"notanywayitismustbesomecharactersheretocheckthistestwithlongtablename",
				},
			},
			want:    Config{},
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
