<!-- language: lang-none -->

![Platforms](https://img.shields.io/badge/platform-windows%20macos%20linux-brightgreen)
![Go](https://github.com/infraql/infraql/workflows/Go/badge.svg)
![License](https://img.shields.io/github/license/infraql/infraql)
![Lines](https://img.shields.io/tokei/lines/github/infraql/infraql)  
[![InfraQL](https://docs.infraql.io/img/infraql-banner.png)](https://infraql.io/)  


# Deploy, Manage and Query Cloud Infrastructure using SQL

[[Documentation](https://docs.infraql.io/)]  [[Developer Guide](https://github.com/infraql/infraql/blob/develop/developer_guide.md)]

## Cloud infrastructure coding using SQL

> InfraQL allows you to create, modify and query the state of services and resources across all three major public cloud providers (Google, AWS and Azure) using a common, widely known DSL...SQL.

----
## Its as easy as...
    use google; SELECT * FROM compute.instance WHERE zone = 'australia-southeast1-b' AND project = 'my-project' ;

----

## Design

`infraql` generalizes the idea of infrastructure / computing reources into a `provider`, `service`, `resource` heirarchy that can be queried with SQL semantics, plus some imperative operations which are not canonical SQL.  Potentially any infrastructure: computing, orchestration, storage, SAAS, PAAS offerings etc can be managed with `infraql`, athough the primary driver is cloud infrastructure management.  Multi-provider queries are to be a first class citizen in `infraql`.

Considering query execution in a bottom-up manner from backend execution to frontend source code processing, the strategic design for `infraql` is:

  - Backend **Execution** of queries through `Primitive` interfaces that encapsulate access and mutation operations against arbitrary APIs.  `Primitive`s may act on any particular API, eg: http, SDK, IPC, specific wire protocol.  Potentially variegated (eg: part http API, part SDK).
  - A `Plan` object includes a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph) of `Primitive`s.  `Plan`s may be optimized and cached a la [vitess](https://github.com/vitessio/vitess).  Logically, the `Plan`, once initialized, is matured in the following sequential phases:
    1. **Intermediate Code Generation**; for now no formal language is defined.  Simply objects and function pointers of `infraql`, encapsulated in `Primitives`.
    2. **Code Optimization**; parallelization of independent operations, removal of redundant operations.
    3. **Code Generation**; final calls against whatever backend, eg http API. 
  - **Semantic Analysis** of queries is a phase that accepts an AST as input and:
    - creates a symbol table.
    - analyzes provider heirarchies and API(s) required to complete the query.  Typically these would be sourced by downloading and cacheing provider discovery documents.
    - performs type checking, scope (label) analysis.
    - creates a `Planbuilder` object and decorates it during analysis.
    - **may** generate some primitives.
    - generates, at the very least, a `Plan` stub.
  - **Lexical and Syntax analysis**; using the machinery from Vitess, which is a lex / yacc style grammar, processed with golang libraries to emulate lex and yacc.  The [sqlparser](https://github.com/infraql/vitess/blob/feature/infraql-develop/go/vt/sqlparser) module, originally from [vitess](https://github.com/vitessio/vitess) contains the implementation.  The output is an AST.

The semantic analysis and latter phases are sensitive to the type and structure of provider backends.

`infraql` supports specific API versions for providers, API upgrades may require `infraql` reversioning.

---

## Providers

Starting off with Google.  Other providers to follow.

---

## Build

With cmake:

```bash
cd build
cmake ..
cmake --build .
```


Executable `build/infraql` will be created.


## Run

```bash
./build/infraql --help

```

## Examples

```
./infraql exec "show extended services from google where title = 'Service Directory API';"
```

More examples in [/examples.md](/examples.md).

---

## Developers

[/developer_guide.md](/developer_guide.md).

## Testing

[/test/README.md](/test/README.md).

## Acknowledgements

Forks of the following support our work:

  - [vitess](https://vitess.io/)
  - [readline](https://github.com/chzyer/readline)
  - [color](https://github.com/fatih/color)

We gratefully acknowledge these pieces of work.

## License

See [/LICENSE](/LICENSE)
