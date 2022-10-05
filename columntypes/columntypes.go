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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// sql server date, time column types.
	dateType       = "date"
	datetime2      = "datetime2"
	datetime       = "datetime"
	datetimeOffset = "datetimeoffset"
	smallDatetime  = "smalldatetime"
	timeType       = "time"

	// sql server binary types.
	binary    = "binary"
	varbinary = "varbinary"
	image     = "image"
)

var (
	// querySchemaColumnTypes is a query that selects column names and
	// their data and column types from the information_schema.
	querySchemaColumnTypes = `
		SELECT COLUMN_NAME, DATA_TYPE 
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME='%s'
`
	// time layouts.
	layouts = []string{time.RFC3339, time.RFC3339Nano, time.Layout, time.ANSIC, time.UnixDate, time.RubyDate,
		time.RFC822, time.RFC822Z, time.RFC850, time.RFC1123, time.RFC1123Z, time.RFC3339, time.RFC3339,
		time.RFC3339Nano, time.Kitchen, time.Stamp, time.StampMilli, time.StampMicro, time.StampNano}
)

// Querier is a database querier interface needed for the GetColumnTypes function.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ConvertStructureData converts a sdk.StructureData values to a proper database types.
func ConvertStructureData(
	ctx context.Context,
	columnTypes map[string]string,
	data sdk.StructuredData,
) (sdk.StructuredData, error) {
	result := make(sdk.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = value

			continue
		}

		// sql server doesn't have json type or similar.
		// sql server string types can replace it.
		switch reflect.TypeOf(value).Kind() {
		case reflect.Map, reflect.Slice:
			bs, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("marshal: %w", err)
			}

			result[key] = string(bs)

			continue
		}

		// Converting value to time if it is string.
		switch columnTypes[strings.ToLower(key)] {
		case dateType, timeType, datetime2, datetime, datetimeOffset, smallDatetime:
			_, ok := value.(time.Time)
			if ok {
				result[key] = value

				continue
			}

			valueStr, ok := value.(string)
			if !ok {
				return nil, ErrValueIsNotAString
			}

			timeValue, err := parseToTime(valueStr)
			if err != nil {
				return nil, fmt.Errorf("convert value to time.Time: %w", err)
			}

			result[key] = timeValue
		case binary, varbinary, image:
			_, ok := value.([]byte)
			if ok {
				result[key] = value

				continue
			}

			valueStr, ok := value.(string)
			if !ok {
				return nil, ErrValueIsNotAString
			}

			result[key] = []byte(valueStr)

		default:
			result[key] = value
		}
	}

	return result, nil
}

// GetColumnTypes returns a map containing all table's columns and their database types.
func GetColumnTypes(ctx context.Context, querier Querier, tableName string) (map[string]string, error) {
	rows, err := querier.QueryContext(ctx, fmt.Sprintf(querySchemaColumnTypes, tableName))
	if err != nil {
		return nil, fmt.Errorf("query column types: %w", err)
	}

	columnTypes := make(map[string]string)
	for rows.Next() {
		var columnName, dataType string
		if er := rows.Scan(&columnName, &dataType); er != nil {
			return nil, fmt.Errorf("scan rows: %w", er)
		}

		columnTypes[columnName] = dataType
	}

	return columnTypes, nil
}

func parseToTime(val string) (time.Time, error) {
	for _, l := range layouts {
		timeValue, err := time.Parse(l, val)
		if err != nil {
			continue
		}

		return timeValue, nil
	}

	return time.Time{}, fmt.Errorf("%s - %w", val, ErrInvalidTimeLayout)
}
