package methodselect

import (
	"fmt"
	"infraql/internal/iql/metadata"
	"strings"
)

type IMethodSelector interface {
	GetMethod(resource *metadata.Resource, methodName string) (*metadata.Method, error)

	GetMethodForAction(resource *metadata.Resource, iqlAction string) (*metadata.Method, string, error)
}

func NewMethodSelector(provider string, version string) (IMethodSelector, error) {
	switch provider {
	case "google":
		return newGoogleMethodSelector(version)
	}
	return nil, fmt.Errorf("method selector for provider = '%s', api version = '%s' currently not supported", provider, version)
}

func newGoogleMethodSelector(version string) (IMethodSelector, error) {
	switch version {
	case "v1":
		return &DefaultGoogleMethodSelector{}, nil
	}
	return nil, fmt.Errorf("method selector for google, api version = '%s' currently not supported", version)
}

type DefaultGoogleMethodSelector struct {
}

func (sel *DefaultGoogleMethodSelector) GetMethodForAction(resource *metadata.Resource, iqlAction string) (*metadata.Method, string, error) {
	var methodName string
	switch strings.ToLower(iqlAction) {
	case "select":
		methodName = "list"
	case "delete":
		methodName = "delete"
	case "insert":
		methodName = "insert"
	default:
		return nil, "", fmt.Errorf("iql action = '%s' curently not supported, there is no method mapping possible for any resource", iqlAction)
	}
	m, err := sel.getMethodByName(resource, methodName)
	return m, methodName, err
}

func (sel *DefaultGoogleMethodSelector) GetMethod(resource *metadata.Resource, methodName string) (*metadata.Method, error) {
	return sel.getMethodByName(resource, methodName)
}

func (sel *DefaultGoogleMethodSelector) getMethodByName(resource *metadata.Resource, methodName string) (*metadata.Method, error) {
	m, ok := resource.Methods[methodName]
	if !ok {
		return nil, fmt.Errorf("no method = '%s' for resource = '%s'", methodName, resource.Name)
	}
	return &m, nil
}
