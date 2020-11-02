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

	handshakeTimeout = 5 * time.Second
)

var (
	genesis      *genesisT.Genesis = params.DefaultGenesisBlock()
	genesisBlock *types.Block
	head         *types.Block
	headHash     common.Hash
	headNumber   uint64
	headTd       *big.Int = new(big.Int)
)

var (
	syncChallengeTimeout = 15 * time.Second // Time allowance for a node to reply to the sync progress challenge
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
		MaxPeers:   10000,
		PrivateKey: nodekey,
		Name:       "my node name",
		ListenAddr: ":30300",
		// Protocols:  []p2p.Protocol{MyProtocol()},
		Protocols: Protocols(),
		NAT:       nat.Any(),
		// NoDiscovery: true,
		NodeDatabase: "mynodes",
		DialRatio:    1,
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
func fakeNodeInfo() *eth.NodeInfo {
	if err := updateHeadfromRpc(); err != nil {
		fmt.Println("Update head from rpc failed", "err", err)
		return nil
	}
	return &eth.NodeInfo{
		Network:    1,
		Difficulty: big.NewInt(3221496830632519),
		Genesis:    common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
		Config:     genesis.Config,
		Head:       common.HexToHash("0x65f5c9e2639cc2ddc7e23bd023b9fb84c5a83b3800d193eab915aa87a85eae9e"),
	}
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
	p.Log().Debug("Ethereum peer connected handle", "name", p.Name(), "fullID", p.Node().ID().String(), "urlv4", p.Node().URLv4())
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
	var blockchainForkid BlockchainForkid

	forkID := forkid.NewID(blockchainForkid.Config(), blockchainForkid.Genesis().Hash(), blockchainForkid.CurrentHeader().Number.Uint64())

	if err := p.handshake(1, headTd, headHash, genesisBlock.Hash(), forkID, forkid.NewFilter(&blockchainForkid)); err != nil {
		p.Log().Debug("Ethereum handshake failed", "err", err)
		return err
	}
	p.Log().Error("Ethereum handshake done")

	// Request the peer's checkpoint header for chain height/weight validation
	if err := p.requestHeadersByNumber(headNumber, 1, 0, false); err != nil { ////////////////// todo if else body. todo headNumber or sth else?
		return err
	}

	// Start a timer to disconnect if the peer doesn't reply in time
	p.syncDrop = time.AfterFunc(syncChallengeTimeout, func() {
		p.Log().Warn("Checkpoint challenge timed out, dropping", "addr", p.RemoteAddr(), "type", p.Name())
		// pm.removePeer(p.id)
	})
	// Make sure it's cleaned up if the peer dies off
	defer func() {
		if p.syncDrop != nil {
			p.syncDrop.Stop()
			p.syncDrop = nil
		}
	}()
	for {
		if err := handleMsg(p); err != nil {
			p.Log().Debug("Ethereum message handling failed", "err", err)
			return err
		}
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
	return &types.Header{
		Difficulty: headTd,
		Number:     new(big.Int).SetUint64(headNumber),
	}
}

// statusData63 is the network packet for the status message for eth/63.
type statusData63 struct {
	ProtocolVersion uint32
	NetworkId       uint64
	TD              *big.Int
	CurrentBlock    common.Hash
	GenesisBlock    common.Hash
}

// statusData is the network packet for the status message for eth/64 and later.
type statusData struct {
	ProtocolVersion uint32
	NetworkID       uint64
	TD              *big.Int
	Head            common.Hash
	Genesis         common.Hash
	ForkID          forkid.ID
}

// eth protocol message codes
const (
	StatusMsg          = 0x00
	NewBlockHashesMsg  = 0x01
	TransactionMsg     = 0x02
	GetBlockHeadersMsg = 0x03
	BlockHeadersMsg    = 0x04
	GetBlockBodiesMsg  = 0x05
	BlockBodiesMsg     = 0x06
	NewBlockMsg        = 0x07
	GetNodeDataMsg     = 0x0d
	NodeDataMsg        = 0x0e
	GetReceiptsMsg     = 0x0f
	ReceiptsMsg        = 0x10

	// New protocol message codes introduced in eth65
	//
	// Previously these message ids were used by some legacy and unsupported
	// eth protocols, reown them here.
	NewPooledTransactionHashesMsg = 0x08
	GetPooledTransactionsMsg      = 0x09
	PooledTransactionsMsg         = 0x0a
)

// Handshake executes the eth protocol handshake, negotiating version number,
// network IDs, difficulties, head and genesis blocks.
func (p *peer) handshake(network uint64, td *big.Int, head common.Hash, genesis common.Hash, forkID forkid.ID, forkFilter forkid.Filter) error {
	// Send out own handshake in a new thread
	errc := make(chan error, 2)

	var (
		status63 statusData63 // safe to read after two values have been received from errc
		status   statusData   // safe to read after two values have been received from errc
	)
	go func() {
		switch {
		case p.version == eth63:
			errc <- p2p.Send(p.rw, StatusMsg, &statusData63{
				ProtocolVersion: uint32(p.version),
				NetworkId:       network,
				TD:              td,
				CurrentBlock:    head,
				GenesisBlock:    genesis,
			})
		case p.version >= eth64:
			errc <- p2p.Send(p.rw, StatusMsg, &statusData{
				ProtocolVersion: uint32(p.version),
				NetworkID:       network,
				TD:              td,
				Head:            head,
				Genesis:         genesis,
				ForkID:          forkID,
			})
		default:
			panic(fmt.Sprintf("unsupported eth protocol version: %d", p.version))
		}
	}()
	go func() {
		switch {
		case p.version == eth63:
			errc <- p.readStatusLegacy(network, &status63, genesis)
		case p.version >= eth64:
			errc <- p.readStatus(network, &status, genesis, forkFilter)
		default:
			panic(fmt.Sprintf("unsupported eth protocol version: %d", p.version))
		}
	}()
	timeout := time.NewTimer(handshakeTimeout)
	defer timeout.Stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				return err
			}
		case <-timeout.C:
			return p2p.DiscReadTimeout
		}
	}
	switch {
	case p.version == eth63:
		p.td, p.head = status63.TD, status63.CurrentBlock
	case p.version >= eth64:
		p.td, p.head = status.TD, status.Head
	default:
		panic(fmt.Sprintf("unsupported eth protocol version: %d", p.version))
	}
	return nil
}

func (p *peer) readStatusLegacy(network uint64, status *statusData63, genesis common.Hash) error {
	msg, err := p.rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Code != StatusMsg {
		return errResp(ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, StatusMsg)
	}
	if msg.Size > protocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, protocolMaxMsgSize)
	}
	// Decode the handshake and make sure everything matches
	if err := msg.Decode(&status); err != nil {
		return errResp(ErrDecode, "msg %v: %v", msg, err)
	}
	if status.GenesisBlock != genesis {
		return errResp(ErrGenesisMismatch, "%x (!= %x)", status.GenesisBlock[:], genesis[:8])
	}
	if status.NetworkId != network {
		return errResp(ErrNetworkIDMismatch, "%d (!= %d)", status.NetworkId, network)
	}
	if int(status.ProtocolVersion) != p.version {
		return errResp(ErrProtocolVersionMismatch, "%d (!= %d)", status.ProtocolVersion, p.version)
	}
	return nil
}

func errResp(code errCode, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

const (
	ErrMsgTooLarge = iota
	ErrDecode
	ErrInvalidMsgCode
	ErrProtocolVersionMismatch
	ErrNetworkIDMismatch
	ErrGenesisMismatch
	ErrForkIDRejected
	ErrNoStatusMsg
	ErrExtraStatusMsg
)

type errCode int

func (p *peer) readStatus(network uint64, status *statusData, genesis common.Hash, forkFilter forkid.Filter) error {
	msg, err := p.rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Code != StatusMsg {
		return errResp(ErrNoStatusMsg, "readStatus first msg has code %x (!= %x)", msg.Code, StatusMsg)
	}
	if msg.Size > protocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "readStatus %v > %v", msg.Size, protocolMaxMsgSize)
	}
	// Decode the handshake and make sure everything matches
	if err := msg.Decode(&status); err != nil {
		return errResp(ErrDecode, "readStatus msg %v: %v", msg, err)
	}
	if status.NetworkID != network {
		return errResp(ErrNetworkIDMismatch, "readStatus %d (!= %d)", status.NetworkID, network)
	}
	if int(status.ProtocolVersion) != p.version {
		return errResp(ErrProtocolVersionMismatch, "readStatus %d (!= %d)", status.ProtocolVersion, p.version)
	}
	if status.Genesis != genesis {
		return errResp(ErrGenesisMismatch, " %readStatus x (!= %x)", status.Genesis, genesis)
	}
	if err := forkFilter(status.ForkID); err != nil {
		return errResp(ErrForkIDRejected, "readStatus %v", err)
	}
	return nil
}

func (e errCode) String() string {
	return errorToString[int(e)]
}

// XXX change once legacy code is out
var errorToString = map[int]string{
	ErrMsgTooLarge:             "Message too long",
	ErrDecode:                  "Invalid message",
	ErrInvalidMsgCode:          "Invalid message code",
	ErrProtocolVersionMismatch: "Protocol version mismatch",
	ErrNetworkIDMismatch:       "Network ID mismatch",
	ErrGenesisMismatch:         "Genesis mismatch",
	ErrForkIDRejected:          "Fork ID rejected",
	ErrNoStatusMsg:             "No status message",
	ErrExtraStatusMsg:          "Extra status message",
}

// RequestHeadersByNumber fetches a batch of blocks' headers corresponding to the
// specified header query, based on the number of an origin block.
func (p *peer) requestHeadersByNumber(origin uint64, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromnum", origin, "skip", skip, "reverse", reverse)
	return p2p.Send(p.rw, GetBlockHeadersMsg, &getBlockHeadersData{Origin: hashOrNumber{Number: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse})
}

// getBlockHeadersData represents a block header query.
type getBlockHeadersData struct {
	Origin  hashOrNumber // Block from which to retrieve headers
	Amount  uint64       // Maximum number of headers to retrieve
	Skip    uint64       // Blocks to skip between consecutive headers
	Reverse bool         // Query direction (false = rising towards latest, true = falling towards genesis)
}

// hashOrNumber is a combined field for specifying an origin block.
type hashOrNumber struct {
	Hash   common.Hash // Block hash from which to retrieve headers (excludes Number)
	Number uint64      // Block hash from which to retrieve headers (excludes Hash)
}

// handleMsg is invoked whenever an inbound message is received from a remote
// peer. The remote connection is torn down upon returning any error.
func handleMsg(p *peer) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
	/////////////////// < mahmoodian > ///////////////////
	// p.Log().Info("Ethereum peer connected", "name", p.Name(), "localAddress", p.LocalAddr().String(), "remoteAddress", p.RemoteAddr().String(), "test", p.String())
	/////////////////////////////////////////////////////
	msg, err := p.rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Size > protocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, protocolMaxMsgSize)
	}
	defer msg.Discard()

	// Handle the message depending on its contents
	switch {
	case msg.Code == StatusMsg:
		////////////////// < mahmoodian > ///////////////
		p.Log().Info("new Msg", "code", "StatusMsg")
		/////////////////////////////////////////////////
		// Status messages should never arrive after the handshake
		return errResp(ErrExtraStatusMsg, "uncontrolled status message")

	// Block header query, collect the requested headers and reply
	case msg.Code == GetBlockHeadersMsg:
		p.Log().Info("new Msg", "code", "GetBlockHeadersMsg")
	case msg.Code == BlockHeadersMsg:
		p.Log().Info("new Msg", "code", "BlockHeadersMsg")

	case msg.Code == GetBlockBodiesMsg:
		p.Log().Info("new Msg", "code", "GetBlockBodiesMsg")

	case msg.Code == BlockBodiesMsg:
		p.Log().Info("new Msg", "code", "BlockBodiesMsg")

	case p.version >= eth63 && msg.Code == GetNodeDataMsg:
		p.Log().Info("new Msg", "code", "GetNodeDataMsg")

	case p.version >= eth63 && msg.Code == NodeDataMsg:
		p.Log().Info("new Msg", "code", "NodeDataMsg")

	case p.version >= eth63 && msg.Code == GetReceiptsMsg:
		p.Log().Info("new Msg", "code", "GetReceiptsMsg")

	case p.version >= eth63 && msg.Code == ReceiptsMsg:
		p.Log().Info("new Msg", "code", "ReceiptsMsg")

	case msg.Code == NewBlockHashesMsg:
		p.Log().Info("new Msg", "code", "NewBlockHashesMsg")

	case msg.Code == NewBlockMsg:
		p.Log().Info("new Msg", "code", "NewBlockMsg")

	case msg.Code == NewPooledTransactionHashesMsg && p.version >= eth65:
		p.Log().Info("new Msg", "code", "NewPooledTransactionHashesMsg")

	case msg.Code == GetPooledTransactionsMsg && p.version >= eth65:
		p.Log().Info("new Msg", "code", "GetPooledTransactionsMsg")

	case msg.Code == TransactionMsg || (msg.Code == PooledTransactionsMsg && p.version >= eth65):
		p.Log().Info("new Msg", "code", "TransactionMsg || (msg.Code == PooledTransactionsMsg && p.version >= eth65)")

	default:
		p.Log().Info("new Msg", "code", "default")

	}
	return nil
}
