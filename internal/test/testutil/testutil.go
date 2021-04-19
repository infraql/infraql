package testutil

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

func CreateReadCloserFromString(s string) io.ReadCloser {
	return ioutil.NopCloser(strings.NewReader(s))
}

func StringEqualsFileContents(t *testing.T, s string, filePath string) bool {
	fileContents, err := ioutil.ReadFile(filePath)
	if err == nil {
		t.Logf("file contents for testing = %s", string(fileContents))
		return s == string(fileContents)
	}
	return false
}
