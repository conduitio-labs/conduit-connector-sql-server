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

package position

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

func TestParseSDKPosition(t *testing.T) {
	snapshotPos := Position{
		IteratorType:             TypeSnapshot,
		SnapshotLastProcessedVal: 1,
		SnapshotMaxValue:         4,
		CDCID:                    0,
		Time: time.Date(
			2009, 11, 17, 20, 34, 58, 651387237, time.UTC),
	}

	wrongPosType := Position{
		IteratorType:             "i",
		SnapshotLastProcessedVal: 1,
		SnapshotMaxValue:         4,
		CDCID:                    0,
	}

	snapshotPosBytes, _ := json.Marshal(snapshotPos)

	wrongPosBytes, _ := json.Marshal(wrongPosType)

	tests := []struct {
		name        string
		in          opencdc.Position
		want        Position
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid position",
			in:   opencdc.Position(snapshotPosBytes),
			want: snapshotPos,
		},
		{
			name:        "unknown iterator type",
			in:          opencdc.Position(wrongPosBytes),
			wantErr:     true,
			expectedErr: errors.New("unknown iterator type : i").Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSDKPosition(tt.in)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)

					return
				}

				if err.Error() != tt.expectedErr {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.expectedErr, err.Error())

					return
				}

				return
			}
		})
	}
}
