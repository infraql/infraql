
# Infraql Developer Guide

## Concurrency considerations

In server mode, a thread pool issues one thread to handle each connection.

The following are single threaded:

  - Lexical and Syntax Analysis.
  - Semantic Analysis.
  - Execution of a single, childless primitive. 
  - Execution of primitives a, b where a > b or b < a in the partial ordering of the plan DAG.  Although it need not be the same thread executing each, they will be strictly sequential.

The following are potentially multi threaded:

  - Plan optimization.
  - Execution of sibling primitives.

## Rebuilding Parser

```bash
make -C vitess.io/vitess/go/vt/sqlparser
```

If you need to add new AST node types, make sure to add them to [go/vt/sqlparser/ast.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/sqlparser/ast.go) and then regenerate the file [go/vt/sqlparser/rewriter.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/sqlparser/rewriter.go) as follows:

```
cd vitess.io/vitess/go/vt/sqlparser

go run ./visitorgen/main -input=ast.go -output=rewriter.go
```

## Outstading required Uplifts

### High level Tech debt / bugs for later

Really high level stuff:

  - Cache system -> db (redis????).

### Cache

  - Cache size limitations and rotation policy.
  - Cache persistence format from simple json -> db (redis????).
  - Re-use vitess LRU Cache???

### Data Model

  - Need reasoned view of tables / joins / rows.
  - Migrate repsonses to MySQL server type *a la* Vitess.
  - DML operations to use similar response filtering to metadata ops.

### Execution model

  - Failure modes and possible multiple errors... how to communicate cause and final state to user.  Need some overall philosophy that is extensible to transactions.
  - Need reasoned view of primitives and optimisations, default extensible method map aproach.
  - Parallelisation of "atomic" DML ops.

### Presentation layer

  - MySQL client Server POC.
  - Readlines up arrow bug when line loner than one window width.

## Tests

Building locally or in cloud will automatically:

1. Run `gotest` tests.
2. Build the executable.
3. Run integration tests.

### gotest

Test coverage is sparse.  Regressions are mitigated by `gotest` integration testing in the [driver](/internal/iql/driver/driver_integration_test.go) and [infraql](/infraql/main_integration_test.go) packages.  Some testing functionality is supported through convenience functionality inside the [test](/internal/test) packages.

#### Point in time gotest coverage

If not already done, then install 'cover' with `go get golang.org/x/tools/cmd/cover`.  
Then: `go test -cover ../...`.

### Integration testing

Integration testing is driven from [test/python/main.py](/test/python/main.py), and via config-driven [generators](/test/test-generators/live-integration/integration.json).  In the first instance, this did not not call any remote backends; rather calling the `infraql` executable to run queries against cached provider discovery data.    

One can run local integration tests against remote backends; simple, extensible example as follows:

1. place a service account key file in `test/assets/secrets/google/sa-key.json`.
2. place a jsonnet context file in `test/assets/input/live-integration/template-context/local/network-crud/network-crud.jsonnet`; something similar to `test/assets/input/live-integration/template-context/example.jsonnet` with the name of a project for whoch the service account has network create and view privileges will suffice.
3. `cd build`
4. `cmake -DLIVE_INTEGRATION_TESTS=live-integration ..`
5. `cmake --build .`


To stop running live integration tests:

- `cmake -DLIVE_INTEGRATION_TESTS=live-integration ..`
- `cmake --build .`

**TODO**: instrument **REAL** integration tests as part of github actions workflow(s).

## Cross Compilation locally

`cmake` can cross-compile, provided dependencies are met.

### From mac

In order to support windows compilation:

```
brew install mingw-w64
```

In order to support linux compilation:

```
export HOMEBREW_BUILD_FROM_SOURCE=1
brew install FiloSottile/musl-cross/musl-cross
```

## Testing latest build from CI system

### On mac

Download and unzip.  For the sake of example, let us consider the executable `~/Downloads/infraql`.

First:
```
chmod +x ~/Downloads/infraql
```

Then, on OSX > 10, you will need to whitelist the executable for execution even though it was not signed by an identifie developer.  Least harmful way to do this is try and execute some command (below is one candidate), and then open `System Settings` > `Security & Privacy` and there should be some UI to allow execution of the untrusted `infraql` file.  At least this works on High Sierra `v1.2.1`.

Then, run test commands, such as:
```
~/Downloads/infraql --keyfilepath=$HOME/moonlighting/infraql-original/keys/sa-key.json exec "select group_concat(substr(name, 0, 5)) || ' lalala' as cc from google.compute.disks where project = 'lab-kr-network-01' and zone = 'australia-southeast1-b';" -o text
```

## Notes on vitess

Vitess implements mysql client and sql driver interfaces.  The server backend listens over HTTP and RPC and implements methods for:

  - "Execute"; execute a simple, single query.
  - "StreamExecute"; tailored to execute a query returning a large result set.
  - "ExecuteBatch"; execution of multiple queries inside a txn.

Vitess maintains an LRU cache of query plans, searchable by query plaintext.  This model will likely work better for vitess thatn infraql; in the former routing is the main concern, in the latter "hotspots" in finer granularity is indicated.

If we do choose to leverage vitess' server implementation, we may implement the vitess vtgate interface [as per vtgate/vtgateservice/interface.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/vtgate/vtgateservice/interface.go).

### Low level vitess notes

The various `main()` functions:

  - [line 34 cmd/vtctld/main.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/cmd/vtctld/main.go)
  - [line 52 cmd/vtgate/vtgate.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/cmd/vtgate/vtgate.go) 
  - [line 106 cmd/vtcombo/main.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/cmd/vtcombo/main.go)

...aggregate all the requisite setup for the server.

[Run(); line 33 run.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/servenv/run.go) sets up RPC and HTTP servers.

[Init(); line 133 in vtgate.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/vtgate/vtgate.go) initialises the VT server singleton.

Init() calls [NewExecutor(); line 108 in executor.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/vtgate/executor.go), a one-per-server object which includes an LRU cache of plans.

In terms of handling individual queries:

  - VTGate sessions [vtgateconn.go line 46](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/servenv/run.go) are passed in per request.
  - On the client side, [conn.Query(); line 284 in driver.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/vitessdriver/driver.go) calls (for example) `conn.session.StreamExecute()`.
  - Server side, [Conn.handleNextCommand(); line 759 mysql/conn.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/mysql/conn.go)
  - Server side, vt software; [VTGate.StreamExecute(); line 301 in vtgate.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/vtgate/vtgate.go).
  - Then, (either directly or indirectly) [Executor.StreamExecute(); line 1128 in executor.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/vtgate/executor.go) handles synchronous `streaming` queries, and calls `Executor.getPlan()`.
  - [Executor.getPlan(); in particular line 1352 in executor.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/vtgate/executor.go)
is the guts of query processing.
  - [Build(); line 265 in builder.go](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/vtgate/planbuilder/builder.go) is the driver for plan building.
