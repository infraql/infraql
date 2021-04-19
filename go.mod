module infraql

go 1.13

require (
	github.com/fatih/color v1.9.0
	github.com/google/go-jsonnet v0.17.0
	github.com/magiconair/properties v1.8.1
	github.com/olekukonko/tablewriter v0.0.0-20180130162743-b8a9be070da4
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.6.3
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	readline v0.0.0-00010101000000-000000000000
	vitess.io/vitess v0.0.0-20200623183837-9438d77d1a90
)

replace readline => github.com/infraql/readline v0.0.0-20210418072316-6e4ad520d2b4

replace github.com/fatih/color => github.com/infraql/color v1.10.1-0.20210418074258-4aa529ee76ed

replace vitess.io/vitess => github.com/infraql/vitess v0.0.2-0.20210418065154-d6737d4243d4
