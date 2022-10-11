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
	queryGetMaxValue = `SELECT max(%s) FROM %s`

	queryIfTableExist = `SELECT count(*) as ct
										FROM INFORMATION_SCHEMA.TABLES
										WHERE TABLE_TYPE = 'BASE TABLE'
										AND TABLE_NAME = ?`

	queryCreateTrackingTable = `SELECT TOP 0 * INTO %s FROM %s`

	queryAddOperationTypeColumn = `ALTER TABLE %s ADD %s VARCHAR (10)`

	queryAddODateTimeColumn = `ALTER TABLE %s ADD %s datetime default getDate()`

	queryAddGUIDColumn = `ALTER TABLE %s ADD 
                             %s UNIQUEIDENTIFIER CONSTRAINT conduit_id_%s PRIMARY KEY DEFAULT NewId()`

	triggerTemplate = `
		CREATE TRIGGER %s 
		ON %s  
		AFTER  %s
		NOT FOR REPLICATION  
		AS  
		BEGIN 
			INSERT INTO 
			%s (%s,%s) 
		SELECT %s,'%s'
		FROM
			%s
		END
`
)
