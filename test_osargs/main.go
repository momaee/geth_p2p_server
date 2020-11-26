package main

import (
	"fmt"
	"os"
	"sort"

	"./internal/debug"
	"./internal/flags"
	"./internal/openrpc"

	"github.com/ethereum/go-ethereum/console/prompt"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"gopkg.in/urfave/cli.v1"
)

var (
	// Git SHA1 commit hash of the release (set via linker flags)
	gitCommit = ""
	gitDate   = ""
	// The app that holds all commands and flags.
	app       = flags.NewApp(gitCommit, gitDate, "the ETC Core Go-Ethereum command line interface")
	nodeFlags = []cli.Flag{
		// utils.IdentityFlag,
	}
)

func init() {
	// Initialize the CLI app and start Geth
	app.Action = geth
	app.HideVersion = true // we have a command to print the version
	app.Copyright = "Copyright 2013-2020 The core-geth and go-ethereum Authors"
	app.Commands = []cli.Command{
		// See chaincmd.go:
		// initCommand,
		// importCommand,
		// exportCommand,
		// importPreimagesCommand,
		// exportPreimagesCommand,
		// copydbCommand,
		// removedbCommand,
		// dumpCommand,
		// dumpGenesisCommand,
		// inspectCommand,
		// See accountcmd.go:
		// accountCommand,
		// walletCommand,
		// See consolecmd.go:
		// consoleCommand,
		// attachCommand,
		// javascriptCommand,
		// See misccmd.go:
		// makecacheCommand,
		// makedagCommand,
		// versionCommand,
		// licenseCommand,
		// See config.go
		// dumpConfigCommand,
		// See retesteth.go
		// retestethCommand,
		// See cmd/utils/flags_legacy.go
		// utils.ShowDeprecated,
	}
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Flags = append(app.Flags, nodeFlags...)

	app.Before = func(ctx *cli.Context) error {
		return debug.Setup(ctx)
	}
	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		prompt.Stdin.Close() // Resets terminal mode.
		return nil
	}

	if err := rpc.SetDefaultOpenRPCSchemaRaw(openrpc.OpenRPCSchema); err != nil {
		log.Crit("Setting OpenRPC default", "error", err)
	}
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Println("err")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// geth is the main entry point into the system if no special subcommand is ran.
// It creates a default node based on the command line arguments and runs it in
// blocking mode, waiting for it to be shut down.
func geth(ctx *cli.Context) error {
	if args := ctx.Args(); len(args) > 0 {
		return fmt.Errorf("invalid command: %q", args[0])
	}

	// // prepare(ctx)
	// stack, backend := makeFullNode(ctx)
	// defer stack.Close()

	// startNode(ctx, stack, backend)
	// stack.Wait()
	return nil
}
