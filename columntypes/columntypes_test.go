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

package columntypes

import (
	"testing"
)

func Test_parseToTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strValue string
		wantErr  bool
	}{
		{
			name:     "success RFC1123",
			strValue: "Sun, 12 Dec 2021 12:23:00 UTC",
			wantErr:  false,
		},
		{
			name:     "success RFC3339",
			strValue: "2014-11-12T11:45:26.371Z",
			wantErr:  false,
		},
		{
			name:     "failed",
			strValue: "test",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseToTime(tt.strValue)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}
		})
	}
}
