package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/params/types/ctypes"
	"github.com/ethereum/go-ethereum/params/types/genesisT"
	"github.com/ethereum/go-ethereum/rpc"

	mapset "github.com/deckarep/golang-set"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discv5"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/nat"
	"github.com/ethereum/go-ethereum/params"
)

const messageId = 0

type Message string

// Constants to match up protocol versions and messages
const (
	eth63 = 63
	eth64 = 64
	eth65 = 65
)

// protocolName is the official short name of the protocol used during capability negotiation.
const protocolName = "eth"

// ProtocolVersions are the supported versions of the eth protocol (first is primary).
var ProtocolVersions = []uint{eth65, eth64, eth63}

// protocolLengths are the number of implemented message corresponding to different protocol versions.
var protocolLengths = map[uint]uint64{eth65: 17, eth64: 17, eth63: 17}

const protocolMaxMsgSize = 10 * 1024 * 1024 // Maximum cap on the size of a protocol message

const (
	// maxQueuedBlocks is the maximum number of block propagations to queue up before
	// dropping broadcasts. There's not much point in queueing stale blocks, so a few
	// that might cover uncles should be enough.
	maxQueuedBlocks = 4

	// maxQueuedBlockAnns is the maximum number of block announcements to queue up before
	// dropping broadcasts. Similarly to block propagations, there's no point to queue
	// above some healthy uncle limit, so use that.
	maxQueuedBlockAnns = 4
)

var (
	genesis      *genesisT.Genesis = params.DefaultGenesisBlock()
	genesisBlock *types.Block
	head         *types.Block
	headHash     common.Hash
	headNumber   uint64
	headTd       *big.Int = new(big.Int)
)

// func MyProtocol() p2p.Protocol {
// 	return p2p.Protocol{
// 		Name:    "eth",
// 		Version: 63,
// 		Length:  17,
// 		Run:     msgHandler,
// 	}
// }

func main() {
	fmt.Println("test ")
	genesisBlock = core.GenesisToBlock(genesis, nil)
	if genesisBlock.Number().Sign() != 0 {
		fmt.Println("can't commit genesis block with number > 0")
		return
	}
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	nodekey, _ := crypto.GenerateKey()
	myConfig := p2p.Config{
		MaxPeers:   1000,
		PrivateKey: nodekey,
		Name:       "my node name",
		ListenAddr: ":30300",
		// Protocols:  []p2p.Protocol{MyProtocol()},
		Protocols: Protocols(),
		NAT:       nat.Any(),
		// NoDiscovery: true,
		NodeDatabase: "mynodes",
	}
	setBootstrapNodes(&myConfig)
	setBootstrapNodesV5(&myConfig)

	srv := p2p.Server{
		Config: myConfig,
	}

	if err := srv.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	select {}
}

func msgHandler(peer *p2p.Peer, ws p2p.MsgReadWriter) error {
	for {
		msg, err := ws.ReadMsg()
		if err != nil {
			return err
		}
		fmt.Println()
		fmt.Println("peer:", peer)
		fmt.Println("ID:", peer.ID())
		fmt.Println("Node:", peer.Node())
		fmt.Println("RemoteAddr:", peer.RemoteAddr())
		fmt.Println("LocalAddr:", peer.LocalAddr())

		fmt.Println("msg:", msg)
		fmt.Println("code:", msg.Code)
		fmt.Println("size:", msg.Size)
		fmt.Println("receivedat:", msg.ReceivedAt)
		var myMessage Message
		err = msg.Decode(&myMessage)
		if err != nil {
			fmt.Println("err", err)
			// handle decode error
			continue
		}
		fmt.Println("recv2:", myMessage)
		switch myMessage {
		case "foo":
			err := p2p.SendItems(ws, messageId, "bar")
			if err != nil {
				return err
			}
		default:
			fmt.Println("recv:", myMessage)
		}
	}
}

// Protocols returns all the currently configured
// network protocols to start.
func Protocols() []p2p.Protocol {
	protos := make([]p2p.Protocol, len(ProtocolVersions))
	for i, vsn := range ProtocolVersions {
		protos[i] = makeProtocol(vsn)
		// protos[i].Attributes = []enr.Entry{s.currentEthEntry()}
		// protos[i].DialCandidates = s.dialCandidates
	}
	return protos
}

// RegisterProtocols adds backend's protocols to the node's p2p server.
func registerProtocols(cfg *p2p.Config, protocols []p2p.Protocol) {
	cfg.Protocols = append(cfg.Protocols, protocols...)
}

// setBootstrapNodes creates a list of bootstrap nodes from the command line
// flags, reverting to pre-configured ones if none have been specified.
func setBootstrapNodes(cfg *p2p.Config) {
	urls := MainnetBootnodes

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

// MainnetBootnodes are the enode URLs of the P2P bootstrap nodes running on
// the main Ethereum network.
var MainnetBootnodes = []string{
	// Ethereum Foundation Go Bootnodes
	"enode://d860a01f9722d78051619d1e2351aba3f43f943f6f00718d1b9baa4101932a1f5011f16bb2b1bb35db20d6fe28fa0bf09636d26a87d31de9ec6203eeedb1f666@18.138.108.67:30303",   // bootnode-aws-ap-southeast-1-001
	"enode://22a8232c3abc76a16ae9d6c3b164f98775fe226f0917b0ca871128a74a8e9630b458460865bab457221f1d448dd9791d24c4e5d88786180ac185df813a68d4de@3.209.45.79:30303",     // bootnode-aws-us-east-1-001
	"enode://ca6de62fce278f96aea6ec5a2daadb877e51651247cb96ee310a318def462913b653963c155a0ef6c7d50048bba6e6cea881130857413d9f50a621546b590758@34.255.23.113:30303",   // bootnode-aws-eu-west-1-001
	"enode://279944d8dcd428dffaa7436f25ca0ca43ae19e7bcf94a8fb7d1641651f92d121e972ac2e8f381414b80cc8e5555811c2ec6e1a99bb009b3f53c4c69923e11bd8@35.158.244.151:30303",  // bootnode-aws-eu-central-1-001
	"enode://8499da03c47d637b20eee24eec3c356c9a2e6148d6fe25ca195c7949ab8ec2c03e3556126b0d7ed644675e78c4318b08691b7b57de10e5f0d40d05b09238fa0a@52.187.207.27:30303",   // bootnode-azure-australiaeast-001
	"enode://103858bdb88756c71f15e9b5e09b56dc1be52f0a5021d46301dbbfb7e130029cc9d0d6f73f693bc29b665770fff7da4d34f3c6379fe12721b5d7a0bcb5ca1fc1@191.234.162.198:30303", // bootnode-azure-brazilsouth-001
	"enode://715171f50508aba88aecd1250af392a45a330af91d7b90701c436b618c86aaa1589c9184561907bebbb56439b8f8787bc01f49a7c77276c58c1b09822d75e8e8@52.231.165.108:30303",  // bootnode-azure-koreasouth-001
	"enode://5d6d7cd20d6da4bb83a1d28cadb5d409b64edf314c0335df658c1a54e32c7c4a7ab7823d57c39b6a757556e68ff1df17c748b698544a55cb488b52479a92b60f@104.42.217.25:30303",   // bootnode-azure-westus-001
}

// setBootstrapNodesV5 creates a list of bootstrap nodes from the command line
// flags, reverting to pre-configured ones if none have been specified.
func setBootstrapNodesV5(cfg *p2p.Config) {
	urls := MainnetBootnodes

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

func makeProtocol(version uint) p2p.Protocol {
	length, ok := protocolLengths[version]
	if !ok {
		panic("makeProtocol for unknown version")
	}

	return p2p.Protocol{
		Name:    protocolName,
		Version: version,
		Length:  length,
		Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
			return runPeer(newPeer(int(version), p, rw, nil))
		},
		// Run: msgHandler,
		NodeInfo: func() interface{} {
			return fakeNodeInfo()
		},
		// PeerInfo: func(id enode.ID) interface{} {
		// 	if p := pm.peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
		// 		return p.Info()
		// 	}
		// 	return nil
		// },
	}
}

// NodeInfo retrieves some protocol metadata about the running host node.
func fakeNodeInfo() (*eth.NodeInfo, error) {
	if err := updateHeadfromRpc(); err != nil {
		fmt.Println("Update head from rpc failed", "err", err)
		return nil, err
	}
	return &eth.NodeInfo{
		Network:    1,
		Difficulty: big.NewInt(3221496830632519),
		Genesis:    common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
		Config:     genesis.Config,
		Head:       common.HexToHash("0x65f5c9e2639cc2ddc7e23bd023b9fb84c5a83b3800d193eab915aa87a85eae9e"),
	}, nil
}

// propEvent is a block propagation, waiting for its turn in the broadcast queue.
type propEvent struct {
	block *types.Block
	td    *big.Int
}

type peer struct {
	id string

	*p2p.Peer
	rw p2p.MsgReadWriter

	version  int         // Protocol version negotiated
	syncDrop *time.Timer // Timed connection dropper if sync progress isn't validated in time

	head common.Hash
	td   *big.Int
	lock sync.RWMutex

	knownBlocks     mapset.Set        // Set of block hashes known to be known by this peer
	queuedBlocks    chan *propEvent   // Queue of blocks to broadcast to the peer
	queuedBlockAnns chan *types.Block // Queue of blocks to announce to the peer

	knownTxs    mapset.Set                           // Set of transaction hashes known to be known by this peer
	txBroadcast chan []common.Hash                   // Channel used to queue transaction propagation requests
	txAnnounce  chan []common.Hash                   // Channel used to queue transaction announcement requests
	getPooledTx func(common.Hash) *types.Transaction // Callback used to retrieve transaction from txpool

	term chan struct{} // Termination channel to stop the broadcaster
}

func newPeer(version int, p *p2p.Peer, rw p2p.MsgReadWriter, getPooledTx func(hash common.Hash) *types.Transaction) *peer {
	return &peer{
		Peer:            p,
		rw:              rw,
		version:         version,
		id:              fmt.Sprintf("%x", p.ID().Bytes()[:8]),
		knownTxs:        mapset.NewSet(),
		knownBlocks:     mapset.NewSet(),
		queuedBlocks:    make(chan *propEvent, maxQueuedBlocks),
		queuedBlockAnns: make(chan *types.Block, maxQueuedBlockAnns),
		txBroadcast:     make(chan []common.Hash),
		txAnnounce:      make(chan []common.Hash),
		getPooledTx:     getPooledTx,
		term:            make(chan struct{}),
	}
}

func runPeer(p *peer) error {
	p.Log().Info("Ethereum peer connected handle", "name", p.Name(), "fullID", p.Node().ID().String(), "urlv4", p.Node().URLv4())
	// if !p.Peer.Info().Network.Trusted {
	// 	p.Log().Warn("Disconnected")
	// 	return p2p.DiscTooManyPeers
	// }
	// p.Log().Info("Ethereum peer connected handle", "name", p.Name(), "localAddress", p.LocalAddr().String(), "remoteAddress", p.RemoteAddr().String(), "fullID", p.Node().ID().String(), "urlv4", p.Node().URLv4())

	//todo: chainconfig
	if err := updateHeadfromRpc(); err != nil {
		p.Log().Error("Update head from rpc failed", "err", err)
		return err
	}

	if err := p.Handshake(1, headTd, headHash, genesis.Hash(), forkid.NewID(pm.blockchain), pm.forkFilter); err != nil {
		p.Log().Debug("Ethereum handshake failed", "err", err)
		return err
	}

	return nil
}

type BlockfromRpc struct {
	Number     *hexutil.Big
	Difficulty string
	Hash       common.Hash
}

func updateHeadfromRpc() error {
	client, _ := rpc.Dial("http://192.168.1.104:8545")
	if client == nil {
		fmt.Println("couldn't create rpc")
		return errors.New("could not connect to client rpc")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var lastBlock BlockfromRpc
	err := client.CallContext(ctx, &lastBlock, "eth_getBlockByNumber", "latest", false)
	if err != nil {
		fmt.Println("can't get latest block:", err)
		return errors.New("could not get lates block")
	}
	headTd.SetString(lastBlock.Difficulty[2:], 16)
	headHash = lastBlock.Hash
	headNumber = lastBlock.Number.ToInt().Uint64()
	return nil
}

type BlockchainForkid struct {
}

func (bc *BlockchainForkid) Config() ctypes.ChainConfigurator {
	return genesis.Config
}

func (bc *BlockchainForkid) Genesis() *types.Block {
	return genesisBlock
}

func (bc *BlockchainForkid) CurrentHeader() *types.Header {
	if err := updateHeadfromRpc(); err != nil {
		fmt.Println("Update head from rpc failed", "err", err)
		return nil
	}
	return &types.Header{}
}
