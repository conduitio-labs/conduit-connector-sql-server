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
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"

	"github.com/conduitio-labs/conduit-connector-sql-server/source/mock"

	"github.com/conduitio-labs/conduit-connector-sql-server/source/position"
)

func TestSource_Read(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		st := make(sdk.StructuredData)
		st["key"] = "value"

		pos, _ := json.Marshal(position.Position{
			IteratorType:             position.TypeSnapshot,
			SnapshotLastProcessedVal: "1",
			CDCID:                    0,
			Time:                     time.Now(),
		})

		record := sdk.Record{
			Position: pos,
			Metadata: nil,
			Key:      st,
			Payload:  sdk.Change{After: st},
		}

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(record, nil)

		s := Source{
			iterator: it,
		}

		r, err := s.Read(ctx)
		if err != nil {
			t.Errorf("read error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(r, record) {
			t.Errorf("got = %v, want %v", r, record)
		}
	})

	t.Run("failed_has_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, errors.New("run query: failed"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})

	t.Run("failed_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}

func TestSource_Teardown(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop(ctx).Return(nil)

		s := Source{
			iterator: it,
		}
		err := s.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown error = \"%s\"", err.Error())
		}
	})

	t.Run("failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop(ctx).Return(errors.New("some error"))

		s := Source{
			iterator: it,
		}

		err := s.Teardown(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}
