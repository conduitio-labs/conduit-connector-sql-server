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

//go:generate mockgen -package mock -source interface.go -destination mock/destination.go

package destination

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	Delete(ctx context.Context, record sdk.Record) error
	Insert(ctx context.Context, record sdk.Record) error
	Update(ctx context.Context, record sdk.Record) error
	Close(ctx context.Context) error
}
