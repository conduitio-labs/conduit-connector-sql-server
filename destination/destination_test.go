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

package destination

import (
	"context"
	"errors"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"

	"github.com/matryer/is"

	"github.com/conduitio-labs/conduit-connector-sql-server/config"
	"github.com/conduitio-labs/conduit-connector-sql-server/destination/mock"
	"github.com/conduitio-labs/conduit-connector-sql-server/destination/writer"
)

func TestDestination_Configure(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "sqlserver://sa:passwordx@0.0.0.0?database=mydb&connection+timeout=30",
					config.KeyTable:      "CLIENTS",
				},
			},
			wantErr: false,
		},
		{
			name: "fail, missing connection",
			args: args{
				cfg: map[string]string{
					config.KeyTable: "CLIENTS",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, missing table",
			args: args{
				cfg: map[string]string{
					config.KeyConnection: "sqlserver://sa:passwordx@0.0.0.0?database=mydb&connection+timeout=30",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := &Destination{}
			if err := d.Configure(context.Background(), tt.args.cfg); (err != nil) != tt.wantErr {
				t.Errorf("Destination.Configure() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDestination_Write(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Operation: sdk.OperationCreate,
			Key: sdk.StructuredData{
				"ID": 1,
			},
			Payload: sdk.Change{After: sdk.StructuredData{
				"ID":   1,
				"name": "test",
			},
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Insert(ctx, record).Return(nil)

		d := Destination{
			writer: w,
		}

		c, err := d.Write(ctx, []sdk.Record{record})
		is.NoErr(err)

		is.Equal(c, 1)
	})

	t.Run("success_update", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Operation: sdk.OperationUpdate,
			Key: sdk.StructuredData{
				"ID": 1,
			},
			Payload: sdk.Change{After: sdk.StructuredData{
				"ID":   1,
				"name": "test",
			},
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Update(ctx, record).Return(nil)

		d := Destination{
			writer: w,
		}

		c, err := d.Write(ctx, []sdk.Record{record})
		is.NoErr(err)

		is.Equal(c, 1)
	})

	t.Run("success_delete", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Operation: sdk.OperationDelete,
			Key: sdk.StructuredData{
				"ID": 1,
			},
			Payload: sdk.Change{After: sdk.StructuredData{
				"ID":   1,
				"name": "test",
			},
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Delete(ctx, record).Return(nil)

		d := Destination{
			writer: w,
		}

		c, err := d.Write(ctx, []sdk.Record{record})
		is.NoErr(err)

		is.Equal(c, 1)
	})

	t.Run("fail, empty payload", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		record := sdk.Record{
			Operation: sdk.OperationSnapshot,
			Position:  sdk.Position("1.0"),
			Key: sdk.StructuredData{
				"ID": 1,
			},
		}

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Insert(ctx, record).Return(writer.ErrEmptyPayload)

		d := Destination{
			writer: w,
		}

		_, err := d.Write(ctx, []sdk.Record{record})
		is.Equal(err != nil, true)
	})
}

func TestDestination_Teardown(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Close(ctx).Return(nil)

		d := Destination{
			writer: w,
		}

		err := d.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("success, writer is nil", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctx := context.Background()

		d := Destination{
			writer: nil,
		}

		err := d.Teardown(ctx)
		is.NoErr(err)
	})

	t.Run("fail, unexpected error", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Close(ctx).Return(errors.New("some error"))

		d := Destination{
			writer: w,
		}

		err := d.Teardown(ctx)
		is.Equal(err != nil, true)
	})
}
