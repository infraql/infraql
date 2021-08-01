module infraql

go 1.16

require (
	github.com/coreos/etcd v3.3.13+incompatible // indirect
	github.com/fatih/color v1.9.0
	github.com/go-delve/delve v1.6.1 // indirect
	github.com/google/go-jsonnet v0.17.0
	github.com/infraql/go-sqlite3 v0.0.1-infraqlalpha
	github.com/magiconair/properties v1.8.5
	github.com/mattn/go-isatty v0.0.13 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/olekukonko/tablewriter v0.0.0-20180130162743-b8a9be070da4
	github.com/peterh/liner v1.2.1 // indirect
	github.com/pkg/profile v0.0.0-20170413231811-06b906832ed0 // indirect
	github.com/prometheus/tsdb v0.7.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.8.1
	go.starlark.net v0.0.0-20210602144842-1cdb82c9e17a // indirect
	golang.org/x/arch v0.0.0-20210502124803-cbf565b21d1e // indirect
	golang.org/x/oauth2 v0.0.0-20210402161424-2e8d93401602
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	readline v0.0.0-00010101000000-000000000000
	vitess.io/vitess v0.0.8-rc2
)

replace readline => github.com/infraql/readline v0.0.0-20210418072316-6e4ad520d2b4

replace github.com/fatih/color => github.com/infraql/color v1.10.1-0.20210418074258-4aa529ee76ed

replace vitess.io/vitess => github.com/infraql/vitess v0.0.8-rc2
