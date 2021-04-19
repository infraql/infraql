package iqlmodel

import (
	"infraql/internal/iql/iqlutil"

	"vitess.io/vitess/go/sqltypes"
)

type ITable interface {
	GetName() string
	KeyExists(string) bool
	GetKey(string) (interface{}, error)
	GetKeyAsSqlVal(string) (sqltypes.Value, error)
	GetRequiredParameters() map[string]Parameter
	FilterBy(func(interface{}) (ITable, error)) (ITable, error)
}

type Parameter struct {
	ID          string `json:"-"`
	Description string `json:"description"`
	Location    string `json:"location"`
	Type        string `json:"type"`
	Format      string `json:"format"`
	Pattern     string `json:"patern"`
	Required    bool   `json:"required"`
}

func (p *Parameter) ConditionIsValid(lhs string, rhs interface{}) bool {
	return iqlutil.ProviderTypeConditionIsValid(p.Type, lhs, rhs)
}
