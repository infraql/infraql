package iqlutil

import (
	"bytes"
	"encoding/json"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

func TranslateLikeToRegexPattern(likeString string) string {
	return "^" + strings.ReplaceAll(regexp.QuoteMeta(likeString), "%", ".*") + "$"
}

func SanitisePossibleTickEscapedTerm(term string) string {
	return strings.TrimSuffix(strings.TrimPrefix(term, "`"), "`")
}

func ProviderTypeConditionIsValid(providerType string, lhs string, rhs interface{}) bool {
	switch providerType {
	case "string":
		return reflect.TypeOf(rhs).String() == "string"
	case "object":
		return false
	case "array":
		return false
	case "int", "int32", "int64":
		return reflect.TypeOf(rhs).String() == "int"
	default:
		return false
	}
	return false
}

func PrettyPrintSomeJson(body []byte) ([]byte, error) {
	var prettyJSON bytes.Buffer
	err := json.Indent(&prettyJSON, body, "", "  ")
	if err != nil {
		return nil, err
	}
	return prettyJSON.Bytes(), nil
}

func GetSortedKeysStringMap(m map[string]string) []string {
	var retVal []string
	for k, _ := range m {
		retVal = append(retVal, k)
	}
	sort.Strings(retVal)
	return retVal
}
