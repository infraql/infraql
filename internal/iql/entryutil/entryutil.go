package entryutil

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"infraql/internal/iql/dto"
	"infraql/internal/iql/handler"
	"infraql/internal/iql/iqlerror"

	"infraql/internal/pkg/preprocessor"

	lrucache "vitess.io/vitess/go/cache" 
)

func BuildHandlerContext(runtimeCtx dto.RuntimeCtx, rdr io.Reader, lruCache *lrucache.LRUCache) (*handler.HandlerContext, error) {
	var err error
	var prepRd, externalTmplRdr io.Reader
	pp := preprocessor.NewPreprocessor(preprocessor.TripleLessThanToken, preprocessor.TripleGreaterThanToken)
	iqlerror.PrintErrorAndExitOneIfNil(pp, "preprocessor error")
	if runtimeCtx.TemplateCtxFilePath == "" {
		prepRd, err = pp.Prepare(rdr, runtimeCtx.InfilePath)
	} else {
		externalTmplRdr, err = os.Open(runtimeCtx.TemplateCtxFilePath)
		iqlerror.PrintErrorAndExitOneIfError(err)
		prepRd = rdr
		err = pp.PrepareExternal(strings.Trim(strings.ToLower(filepath.Ext(runtimeCtx.TemplateCtxFilePath)), "."), externalTmplRdr, runtimeCtx.TemplateCtxFilePath)
	}
	iqlerror.PrintErrorAndExitOneIfError(err)
	ppRd, err := pp.Render(prepRd)
	iqlerror.PrintErrorAndExitOneIfError(err)
	var bb []byte
	bb, err = ioutil.ReadAll(ppRd)
	iqlerror.PrintErrorAndExitOneIfError(err)
	return handler.GetHandlerCtx(strings.TrimSpace(string(bb)), runtimeCtx, lruCache)
}