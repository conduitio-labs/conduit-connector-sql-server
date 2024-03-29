// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-connector-sdk/tree/main/cmd/paramgen

package source

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func (Config) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		"batchSize": {
			Default:     "1000",
			Description: "batchSize is a size of rows batch.",
			Type:        sdk.ParameterTypeInt,
			Validations: []sdk.Validation{
				sdk.ValidationGreaterThan{Value: 1},
				sdk.ValidationLessThan{Value: 100000},
			},
		},
		"columns": {
			Default:     "",
			Description: "columns  list of column names that should be included in each Record's payload.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"connection": {
			Default:     "",
			Description: "connection string connection to SQL Server database.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"orderingColumn": {
			Default:     "",
			Description: "orderingColumn is a name of a column that the connector will use for ordering rows.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
		"primaryKey": {
			Default:     "",
			Description: "primaryKey - Column name that records should use for their `primaryKey` fields.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{},
		},
		"snapshot": {
			Default:     "true",
			Description: "snapshot whether the plugin will take a snapshot of the entire table before starting cdc.",
			Type:        sdk.ParameterTypeBool,
			Validations: []sdk.Validation{},
		},
		"table": {
			Default:     "",
			Description: "table is a name of the table that the connector should write to or read from.",
			Type:        sdk.ParameterTypeString,
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
		},
	}
}
