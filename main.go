package main

import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/gocql/gocql"

	"./internal/debug"
	"./internal/flags"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/console/prompt"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discv5"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/nat"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/params/types/genesisT"
	"gopkg.in/urfave/cli.v1"
)

const (
	defaultMaxPeers        = 100000
	defaultMaxPendingPeers = 100
	defaultETHListenAddr   = 30303
	defaultETCListenAddr   = 30301
	defaultETHNodeDatabase = "eth_nodes"
	defaultETCNodeDatabase = "etc_nodes"
	defaultDialRatio       = 2

	defaultCassandraHost        = "127.0.0.1"
	defaultCassandraPort        = 9042
	defaultCassandraETHKeySpace = "eth"
	defaultCassandraETCKeySpace = "etc"
	defaultConsistency          = gocql.Quorum
)

var (
	genesis *genesisT.Genesis
	// Git SHA1 commit hash of the release (set via linker flags)
	gitCommit = ""
	gitDate   = ""
	// The app that holds all commands and flags.
	app       = flags.NewApp(gitCommit, gitDate, "the AFRA ETH/ETC command line interface")
	nodeFlags = []cli.Flag{
		utils.ClassicFlag,
		utils.ListenPortFlag,
		// utils.MaxPeersFlag,
		// utils.MaxPendingPeersFlag,
		cassandraHostFlag,
		cassandraPortFlag,
		cassandraKeySpaceFlag,
	}
	cassandraHostFlag = cli.StringFlag{
		Name:  "cassandra.addr",
		Usage: "Cassandra server listening ip address",
		Value: defaultCassandraHost,
	}
	cassandraPortFlag = cli.IntFlag{
		Name:  "cassandra.port",
		Usage: "Cassandra server listening port",
		Value: defaultCassandraPort,
	}
	cassandraKeySpaceFlag = cli.StringFlag{
		Name:  "cassandra.keyspace",
		Usage: "Cassandra server keyspace. (default: \"eth\" for ethereum mainnet and \"etc\" for ethereum classic mainnet)",
		Value: defaultCassandraETHKeySpace,
	}
)

// AppHelpFlagGroups is the application flags, grouped by functionality.
var AppHelpFlagGroups = []flags.FlagGroup{
	{
		Name: "ETHEREUM",
		Flags: []cli.Flag{
			utils.ClassicFlag,
		},
	},
	{
		Name: "NETWORKING",
		Flags: []cli.Flag{
			utils.ListenPortFlag,
			// utils.MaxPeersFlag,
			// utils.MaxPendingPeersFlag,
		},
	},
	{
		Name:  "LOGGING AND DEBUGGING",
		Flags: debug.Flags,
	},
	{
		Name: "CASSANDRA",
		Flags: []cli.Flag{
			cassandraHostFlag,
			cassandraPortFlag,
			cassandraKeySpaceFlag,
		},
	},
	{
		Name: "MISC",
		Flags: []cli.Flag{
			cli.HelpFlag,
		},
	},
}

type gethConfig struct {
	// Configuration of peer-to-peer networking.
	p2p       p2p.Config
	cassandra cassandraConfig
}

type cassandraConfig struct {
	host        string
	port        string
	username    string
	password    string
	keySpace    string
	consistency gocql.Consistency
}

func init() {
	// Initialize the CLI app and start Geth
	app.Action = geth
	app.HideVersion = true // we have a command to print the version
	app.Copyright = "Copyright 2013-2020 The core-geth and go-ethereum Authors"
	app.Commands = []cli.Command{}
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Flags = append(app.Flags, nodeFlags...)
	app.Flags = append(app.Flags, debug.Flags...)

	app.Before = func(ctx *cli.Context) error {
		return debug.Setup(ctx)
	}
	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		prompt.Stdin.Close() // Resets terminal mode.
		return nil
	}
	editCli()

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
	cfg := makeConfig(ctx)

	if err := startNode(cfg); err != nil {
		return err
	}
	return nil
}

func makeConfig(ctx *cli.Context) *gethConfig {
	// Load defaults.
	nodekey, _ := crypto.GenerateKey()
	cfg := gethConfig{
		p2p: p2p.Config{
			MaxPeers:        defaultMaxPeers,
			MaxPendingPeers: defaultMaxPendingPeers,
			PrivateKey:      nodekey,
			Name:            common.MakeName("Geth", "v1.9.23"),
			ListenAddr:      fmt.Sprintf(":%d", defaultETHListenAddr),
			Protocols:       protocols(),
			NAT:             nat.Any(),
			NodeDatabase:    defaultETHNodeDatabase,
			DialRatio:       defaultDialRatio,
		},
		cassandra: cassandraConfig{
			host:        defaultCassandraHost,
			keySpace:    defaultCassandraETHKeySpace,
			consistency: defaultConsistency,
			port:        fmt.Sprintf(":%d", defaultCassandraPort),
		},
	}
	genesis = params.DefaultGenesisBlock()
	if ctx.GlobalBool(utils.ClassicFlag.Name) {
		cfg.p2p.ListenAddr = fmt.Sprintf(":%d", defaultETCListenAddr)
		cfg.p2p.NodeDatabase = defaultETCNodeDatabase
		cfg.cassandra.keySpace = defaultCassandraETCKeySpace
		genesis = params.DefaultClassicGenesisBlock()
	}
	setBootstrapNodes(ctx, &cfg.p2p)
	setBootstrapNodesV5(ctx, &cfg.p2p)
	if ctx.GlobalIsSet(utils.ListenPortFlag.Name) {
		cfg.p2p.ListenAddr = fmt.Sprintf(":%d", ctx.GlobalInt(utils.ListenPortFlag.Name))
	}
	if ctx.GlobalIsSet(cassandraHostFlag.Name) {
		cfg.cassandra.host = ctx.GlobalString(cassandraHostFlag.Name)
	}
	if ctx.GlobalIsSet(cassandraPortFlag.Name) {
		cfg.cassandra.port = fmt.Sprintf(":%d", ctx.GlobalInt(cassandraPortFlag.Name))
	}
	if ctx.GlobalIsSet(cassandraKeySpaceFlag.Name) {
		cfg.cassandra.keySpace = ctx.GlobalString(cassandraKeySpaceFlag.Name)
	}

	return &cfg
}

// Protocols returns all the currently configured
// network protocols to start.
func protocols() []p2p.Protocol {
	protos := make([]p2p.Protocol, len(ProtocolVersions))
	for i, vsn := range ProtocolVersions {
		protos[i] = makeProtocol(vsn)
	}
	return protos
}

func makeProtocol(version uint) p2p.Protocol {
	length, ok := protocolLengths[version]
	if !ok {
		panic("makeProtocol for unknown version")
	}

	return p2p.Protocol{
		Name:    protocolName,
		Version: version,
		Length:  length,
		// See geth.go
		Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
			return runPeer(newPeer(int(version), p, rw, nil))
		},
		NodeInfo: func() interface{} {
			return nodeInfo()
		},
		//todo peerinfo
		// PeerInfo: func(id enode.ID) interface{} {
		// 	if p := pm.peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
		// 		return p.Info()
		// 	}
		// 	return nil
		// },
	}
}

// setBootstrapNodes creates a list of bootstrap nodes from the command line
// flags, reverting to pre-configured ones if none have been specified.
func setBootstrapNodes(ctx *cli.Context, cfg *p2p.Config) {
	urls := params.MainnetBootnodes
	if ctx.GlobalBool(utils.ClassicFlag.Name) {
		urls = params.ClassicBootnodes
	}

	if cfg.BootstrapNodes != nil {
		return // already set, don't apply defaults.
	}

	cfg.BootstrapNodes = make([]*enode.Node, 0, len(urls))
	for _, url := range urls {
		if url != "" {
			node, err := enode.Parse(enode.ValidSchemes, url)
			if err != nil {
				log.Crit("Bootstrap URL invalid", "enode", url, "err", err)
				continue
			}
			cfg.BootstrapNodes = append(cfg.BootstrapNodes, node)
		}
	}
}

// setBootstrapNodesV5 creates a list of bootstrap nodes from the command line
// flags, reverting to pre-configured ones if none have been specified.
func setBootstrapNodesV5(ctx *cli.Context, cfg *p2p.Config) {
	urls := params.MainnetBootnodes
	if ctx.GlobalBool(utils.ClassicFlag.Name) {
		urls = params.ClassicBootnodes
	}

	if cfg.BootstrapNodesV5 != nil {
		return // already set, don't apply defaults.
	}

	cfg.BootstrapNodesV5 = make([]*discv5.Node, 0, len(urls))
	for _, url := range urls {
		if url != "" {
			node, err := discv5.ParseNode(url)
			if err != nil {
				log.Error("Bootstrap URL invalid", "enode", url, "err", err)
				continue
			}
			cfg.BootstrapNodesV5 = append(cfg.BootstrapNodesV5, node)
		}
	}
}

func editCli() {
	// Override the default app help template
	cli.AppHelpTemplate = flags.AppHelpTemplate

	// Override the default app help printer, but only for the global app help
	originalHelpPrinter := cli.HelpPrinter
	cli.HelpPrinter = func(w io.Writer, tmpl string, data interface{}) {
		if tmpl == flags.AppHelpTemplate {
			// Iterate over all the flags and add any uncategorized ones
			categorized := make(map[string]struct{})
			for _, group := range AppHelpFlagGroups {
				for _, flag := range group.Flags {
					categorized[flag.String()] = struct{}{}
				}
			}
			deprecated := make(map[string]struct{})
			for _, flag := range utils.DeprecatedFlags {
				deprecated[flag.String()] = struct{}{}
			}
			// Only add uncategorized flags if they are not deprecated
			var uncategorized []cli.Flag
			for _, flag := range data.(*cli.App).Flags {
				if _, ok := categorized[flag.String()]; !ok {
					if _, ok := deprecated[flag.String()]; !ok {
						uncategorized = append(uncategorized, flag)
					}
				}
			}
			if len(uncategorized) > 0 {
				// Append all ungategorized options to the misc group
				miscs := len(AppHelpFlagGroups[len(AppHelpFlagGroups)-1].Flags)
				AppHelpFlagGroups[len(AppHelpFlagGroups)-1].Flags = append(AppHelpFlagGroups[len(AppHelpFlagGroups)-1].Flags, uncategorized...)

				// Make sure they are removed afterwards
				defer func() {
					AppHelpFlagGroups[len(AppHelpFlagGroups)-1].Flags = AppHelpFlagGroups[len(AppHelpFlagGroups)-1].Flags[:miscs]
				}()
			}
			// Render out custom usage screen
			originalHelpPrinter(w, tmpl, flags.HelpData{App: data, FlagGroups: AppHelpFlagGroups})
		} else if tmpl == flags.CommandHelpTemplate {
			// Iterate over all command specific flags and categorize them
			categorized := make(map[string][]cli.Flag)
			for _, flag := range data.(cli.Command).Flags {
				if _, ok := categorized[flag.String()]; !ok {
					categorized[flags.FlagCategory(flag, AppHelpFlagGroups)] = append(categorized[flags.FlagCategory(flag, AppHelpFlagGroups)], flag)
				}
			}

			// sort to get a stable ordering
			sorted := make([]flags.FlagGroup, 0, len(categorized))
			for cat, flgs := range categorized {
				sorted = append(sorted, flags.FlagGroup{Name: cat, Flags: flgs})
			}
			sort.Sort(flags.ByCategory(sorted))

			// add sorted array to data and render with default printer
			originalHelpPrinter(w, tmpl, map[string]interface{}{
				"cmd":              data,
				"categorizedFlags": sorted,
			})
		} else {
			originalHelpPrinter(w, tmpl, data)
		}
	}
}
