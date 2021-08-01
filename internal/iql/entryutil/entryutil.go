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
	"infraql/internal/iql/sqlengine"

	"infraql/internal/pkg/preprocessor"
	"infraql/internal/pkg/txncounter"

	lrucache "vitess.io/vitess/go/cache"
)

func BuildSQLEngine(runtimeCtx dto.RuntimeCtx) (sqlengine.SQLEngine, error) {
	sqlCfg := sqlengine.NewSQLEngineConfig(runtimeCtx)
	return sqlengine.NewSQLEngine(sqlCfg)
}

func GetTxnCounterManager(handlerCtx handler.HandlerContext) (*txncounter.TxnCounterManager, error) {
	genId, err := handlerCtx.SQLEngine.GetCurrentGenerationId()
	if err != nil {
		genId, err = handlerCtx.SQLEngine.GetNextGenerationId()
		if err != nil {
			return nil, err
		}
	}
	sessionId, err := handlerCtx.SQLEngine.GetNextSessionId(genId)
	if err != nil {
		return nil, err
	}
	return txncounter.NewTxnCounterManager(genId, sessionId), nil
}

func BuildHandlerContext(runtimeCtx dto.RuntimeCtx, rdr io.Reader, lruCache *lrucache.LRUCache, sqlEngine sqlengine.SQLEngine) (handler.HandlerContext, error) {
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
	return handler.GetHandlerCtx(strings.TrimSpace(string(bb)), runtimeCtx, lruCache, sqlEngine)
}
