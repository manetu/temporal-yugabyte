// The MIT License
//
// Copyright (c) 2025 Manetu Inc.  All rights reserved.
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"fmt"
	"github.com/manetu/temporal-yugabyte/driver"
	stdlog "log"
	"os"
	"path"
	"strings"
	"text/template"
	_ "time/tzdata" // embed tzdata as a fallback

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/build"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"      // needed to load mysql plugin
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql" // needed to load postgresql plugin
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"     // needed to load sqlite plugin
	"go.temporal.io/server/temporal"
	"go.uber.org/automaxprocs/maxprocs"
)

// main entry point for the temporal server
func main() {
	app := buildCLI()
	_ = app.Run(os.Args)
}

// buildCLI is the main entry point for the temporal server
func buildCLI() *cli.App {
	app := cli.NewApp()
	app.Name = "temporal"
	app.Usage = "Temporal server"
	app.Version = headers.ServerVersion
	app.ArgsUsage = " "
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "root",
			Aliases: []string{"r"},
			Value:   ".",
			Usage:   "root directory of execution environment (deprecated)",
			EnvVars: []string{config.EnvKeyRoot},
		},
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Value:   "config",
			Usage:   "config dir path relative to root (deprecated)",
			EnvVars: []string{config.EnvKeyConfigDir},
		},
		&cli.StringFlag{
			Name:    "env",
			Aliases: []string{"e"},
			Value:   "development",
			Usage:   "runtime environment (deprecated)",
			EnvVars: []string{config.EnvKeyEnvironment},
		},
		&cli.StringFlag{
			Name:    "zone",
			Aliases: []string{"az"},
			Usage:   "availability zone (deprecated)",
			EnvVars: []string{config.EnvKeyAvailabilityZone, config.EnvKeyAvailabilityZoneTypo},
		},
		&cli.StringFlag{
			Name:    "config-file",
			Usage:   "path to config file (absolute or relative to current working directory)",
			EnvVars: []string{config.EnvKeyConfigFile},
		},
		&cli.BoolFlag{
			Name:    "allow-no-auth",
			Usage:   "allow no authorizer",
			EnvVars: []string{config.EnvKeyAllowNoAuth},
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:      "validate-dynamic-config",
			Usage:     "Validate a dynamic config file[s] with known keys and types",
			ArgsUsage: "<file> ...",
			Action: func(c *cli.Context) error {
				total := 0
				for _, fileName := range c.Args().Slice() {
					contents, err := os.ReadFile(fileName)
					if err != nil {
						return err
					}
					result := dynamicconfig.LoadYamlFile(contents)
					total += len(result.Errors)
					fmt.Println(fileName)
					t := template.Must(template.New("").Parse(
						"{{range .Errors}}  error: {{.}}\n" +
							"{{end}}{{range .Warnings}}  warning: {{.}}\n" +
							"{{end}}",
					))
					_ = t.Execute(os.Stdout, result)
				}
				if total > 0 {
					return fmt.Errorf("%d total errors", total)
				}
				return nil
			},
		},
		{
			Name:      "start",
			Usage:     "Start Temporal server",
			ArgsUsage: " ",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:    "services",
					Aliases: []string{"s"},
					Usage:   "comma separated list of services to start. Deprecated",
					Hidden:  true,
				},
				&cli.StringSliceFlag{
					Name:    "service",
					Aliases: []string{"svc"},
					Value:   cli.NewStringSlice(temporal.DefaultServices...),
					Usage:   "service(s) to start",
				},
			},
			Before: func(c *cli.Context) error {
				if c.Args().Len() > 0 {
					return cli.Exit("ERROR: start command doesn't support arguments. Use --service flag instead.", 1)
				}

				if c.IsSet("config-file") && (c.IsSet("config") || c.IsSet("env") || c.IsSet("zone") || c.IsSet("root")) {
					return cli.Exit("ERROR: can not use --config, --env, --zone, or --root with --config-file", 1)
				}

				if _, err := maxprocs.Set(); err != nil {
					stdlog.Println(fmt.Sprintf("WARNING: failed to set GOMAXPROCS: %v.", err))
				}
				return nil
			},
			Action: func(c *cli.Context) error {
				services := c.StringSlice("service")
				allowNoAuth := c.Bool("allow-no-auth")

				// For backward compatibility to support old flag format (i.e. `--services=frontend,history,matching`).
				if c.IsSet("services") {
					stdlog.Println("WARNING: --services flag is deprecated. Specify multiply --service flags instead.")
					services = strings.Split(c.String("services"), ",")
				}

				var cfg *config.Config
				var err error

				switch {
				case c.IsSet("config-file"):
					cfg, err = config.Load(config.WithConfigFile(c.String("config-file")))
				case c.IsSet("config") || c.IsSet("env") || c.IsSet("zone"):
					cfg, err = config.Load(
						config.WithEnv(c.String("env")),
						config.WithConfigDir(path.Join(c.String("root"), c.String("config"))),
						config.WithZone(c.String("zone")),
					)
				default:
					cfg, err = config.Load(config.WithEmbedded())
				}

				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to load configuration: %v.", err), 1)
				}

				logger := log.NewZapLogger(log.BuildZapLogger(cfg.Log))
				logger.Info("Build info.",
					tag.Time("git-time", build.InfoData.GitTime),
					tag.String("git-revision", build.InfoData.GitRevision),
					tag.Bool("git-modified", build.InfoData.GitModified),
					tag.String("go-arch", build.InfoData.GoArch),
					tag.String("go-os", build.InfoData.GoOs),
					tag.String("go-version", build.InfoData.GoVersion),
					tag.Bool("cgo-enabled", build.InfoData.CgoEnabled),
					tag.String("server-version", headers.ServerVersion),
					tag.Bool("debug-mode", debug.Enabled),
				)

				authorizer, err := authorization.GetAuthorizerFromConfig(
					&cfg.Global.Authorization,
				)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to instantiate authorizer. Error: %v", err), 1)
				}
				if authorization.IsNoopAuthorizer(authorizer) && !allowNoAuth {
					logger.Warn(
						"Not using any authorizer and flag `--allow-no-auth` not detected. " +
							"Future versions will require using the flag `--allow-no-auth` " +
							"if you do not want to set an authorizer.",
					)
				}

				claimMapper, err := authorization.GetClaimMapperFromConfig(&cfg.Global.Authorization, logger)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to instantiate claim mapper: %v.", err), 1)
				}
				s, err := temporal.NewServer(
					temporal.ForServices(services),
					temporal.WithConfig(cfg),
					temporal.WithCustomDataStoreFactory(&driver.MetaFactory{}),
					temporal.WithLogger(logger),
					temporal.InterruptOn(temporal.InterruptCh()),
					temporal.WithAuthorizer(authorizer),
					temporal.WithClaimMapper(func(cfg *config.Config) authorization.ClaimMapper {
						return claimMapper
					}),
				)
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to create server. Error: %v.", err), 1)
				}

				err = s.Start()
				if err != nil {
					return cli.Exit(fmt.Sprintf("Unable to start server. Error: %v", err), 1)
				}
				return cli.Exit("All services are stopped.", 0)
			},
		},
	}
	return app
}
