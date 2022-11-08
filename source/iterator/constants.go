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

package iterator

const (
	trackingTablePattern = "CONDUIT_TRACKING_%s"

	// tracking table columns.
	columnOperationType = "CONDUIT_OPERATION_TYPE"
	columnTimeCreated   = "CONDUIT_TRACKING_CREATED_DATE"
	columnTrackingID    = "CONDUIT_TRACKING_ID"

	// metadata related.
	metadataTable = "sqlserver.table"

	// operation types.
	operationTypeInsert = "INSERT"
	operationTypeUpdate = "UPDATE"
	operationTypeDelete = "DELETE"

	// clearing buffer size.
	idsClearingBufferSize = 50

	// period after iterator clears table (seconds).
	clearingDuration = 5
)
