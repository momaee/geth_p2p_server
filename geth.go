package main

import (
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/params/types/coregeth"
	"github.com/ethereum/go-ethereum/params/types/ctypes"
	"github.com/ethereum/go-ethereum/params/types/goethereum"
	"github.com/ethereum/go-ethereum/params/vars"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/gocql/gocql"

	"./cassandra_lib"

	mapset "github.com/deckarep/golang-set"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
)

// Constants to match up protocol versions and messages
const (
	eth63 = 63
	eth64 = 64
	eth65 = 65
)

// protocolName is the official short name of the protocol used during capability negotiation.
const protocolName = "eth"

const protocolMaxMsgSize = 10 * 1024 * 1024 // Maximum cap on the size of a protocol message

const (
	maxKnownTxs    = 32768 // Maximum transactions hashes to keep in the known list (prevent DOS)
	maxKnownBlocks = 1024  // Maximum block hashes to keep in the known list (prevent DOS)

	// maxQueuedBlocks is the maximum number of block propagations to queue up before
	// dropping broadcasts. There's not much point in queueing stale blocks, so a few
	// that might cover uncles should be enough.
	maxQueuedBlocks = 4

	// maxQueuedBlockAnns is the maximum number of block announcements to queue up before
	// dropping broadcasts. Similarly to block propagations, there's no point to queue
	// above some healthy uncle limit, so use that.
	maxQueuedBlockAnns = 4

	handshakeTimeout = 5 * time.Second

	maxRecordedBlocks = maxKnownBlocks
	maxRecordedTxs    = 50 * maxKnownTxs
)

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

// ProtocolVersions are the supported versions of the eth protocol (first is primary).
var ProtocolVersions = []uint{eth65, eth64, eth63}

// protocolLengths are the number of implemented message corresponding to different protocol versions.
var protocolLengths = map[uint]uint64{eth65: 17, eth64: 17, eth63: 17}

var syncChallengeTimeout = 15 * time.Second // Time allowance for a node to reply to the sync progress challenge

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

var (
	genesisBlock     *types.Block
	headHash         common.Hash
	headNumber       uint64   = 0
	headTd           *big.Int = new(big.Int)
	checkpoint       *ctypes.TrustedCheckpoint
	checkpointNumber uint64       // Block number for the sync progress validator to cross reference
	checkpointHash   common.Hash  // Block hash for the sync progress validator to cross reference
	mux              sync.RWMutex //mux for updating headData
	cluster          *gocql.ClusterConfig
	session          *gocql.Session
	blocksSet        mapset.Set = mapset.NewSet()
	txsSet           mapset.Set = mapset.NewSet()
)

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

type BlockfromRpc struct {
	Number     *hexutil.Big
	Difficulty string
	Hash       common.Hash
}

type BlockchainForkid struct {
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

type errCode int

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

// newBlockHashesData is the network packet for the block announcements.
type newBlockHashesData []struct {
	Hash   common.Hash // Hash of one particular block being announced
	Number uint64      // Number of one particular block being announced
}

// newBlockData is the network packet for the block propagation message.
type newBlockData struct {
	Block *types.Block
	TD    *big.Int
}

// PeerInfo represents a short summary of the Ethereum sub-protocol metadata known
// about a connected peer.
type PeerInfo struct {
	Version    int      `json:"version"`    // Ethereum protocol version negotiated
	Difficulty *big.Int `json:"difficulty"` // Total difficulty of the peer's blockchain
	Head       string   `json:"head"`       // SHA3 hash of the peer's best owned block
}

func startNode(cfg *gethConfig) error {
	var err error
	genesisBlock = core.GenesisToBlock(genesis, nil)
	if genesisBlock.Number().Sign() != 0 {
		log.Error("can't commit genesis block with number > 0")
		return errors.New("can't commit genesis block with number > 0")
	}
	if p, ok := genesis.Config.(*coregeth.CoreGethChainConfig); ok {
		checkpoint = p.TrustedCheckpoint
	} else if p, ok := genesis.Config.(*goethereum.ChainConfig); ok {
		checkpoint = p.TrustedCheckpoint
	}
	//todo checkpoint is nil for classic
	// If we have trusted checkpoints, enforce them on the chain
	if checkpoint != nil {
		checkpointNumber = (checkpoint.SectionIndex+1)*vars.CHTFrequency - 1
		checkpointHash = checkpoint.SectionHead
		log.Debug("checkpoints", "checkpointNumber", checkpointNumber, "checkpointHash", checkpointHash.Hex())
	} else {
		log.Warn("Continuing without any checkpoints")
	}
	updateHead(genesisBlock.Header())

	// connect to the cluster
	cluster = gocql.NewCluster(cfg.cassandra.host + cfg.cassandra.port)
	cluster.Keyspace = cfg.cassandra.keySpace
	cluster.Consistency = cfg.cassandra.consistency
	session, err = cluster.CreateSession()
	if err != nil {
		log.Warn("Couldn't create cassandra session", "err", err)
	} else {
		log.Info("Cassandra session created")
	}
	defer session.Close()

	srv := p2p.Server{
		Config: cfg.p2p,
	}
	if err := srv.Start(); err != nil {
		log.Error("Could not start p2p server", "err", err)
		os.Exit(1)
	}

	select {}
}

// RegisterProtocols adds backend's protocols to the node's p2p server.
func registerProtocols(cfg *p2p.Config, protocols []p2p.Protocol) {
	cfg.Protocols = append(cfg.Protocols, protocols...)
}

// NodeInfo retrieves some protocol metadata about the running host node.
func nodeInfo() *eth.NodeInfo {
	return &eth.NodeInfo{
		Network:    1,
		Difficulty: headTd,
		Genesis:    genesisBlock.Hash(),
		Config:     genesis.Config,
		Head:       headHash,
	}
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
	p.Log().Debug("Ethereum peer connected handle", "name", p.Name(), "fullID", p.Node().ID().String(), "urlv4", p.Node().URLv4(), "ip", p.RemoteAddr().String())

	var blockchainForkid BlockchainForkid

	forkID := forkid.NewID(blockchainForkid.Config(), blockchainForkid.Genesis().Hash(), blockchainForkid.CurrentHeader().Number.Uint64())

	if err := p.handshake(1, headTd, headHash, genesisBlock.Hash(), forkID, forkid.NewFilter(&blockchainForkid)); err != nil {
		p.Log().Debug("Ethereum handshake failed", "err", err)
		return err
	}
	p.Log().Info("Ethereum handshake done")
	if checkpointHash != (common.Hash{}) {
		// Request the peer's checkpoint header for chain height/weight validation
		if err := p.requestHeadersByNumber(checkpointNumber, 1, 0, false); err != nil {
			p.Log().Warn("requestHeadersByNumber failed", "err", err)
			return err
		}
		//todo do we neeed removepeer function
		// Start a timer to disconnect if the peer doesn't reply in time
		p.syncDrop = time.AfterFunc(syncChallengeTimeout, func() {
			p.Log().Warn("Checkpoint challenge timed out, dropping", "addr", p.RemoteAddr(), "type", p.Name())
			removePeer(p)
		})
		// Make sure it's cleaned up if the peer dies off
		defer func() {
			if p.syncDrop != nil {
				p.syncDrop.Stop()
				p.syncDrop = nil
			}
		}()

	}
	for {
		if err := handleMsg(p); err != nil {
			p.Log().Warn("Ethereum message handling failed", "err", err)
			return err
		}
	}
}

func removePeer(p *peer) {
	p.Log().Debug("Removing Ethereum peer", "peer", p.RemoteAddr())
	// Hard disconnect at the networking layer
	p.Peer.Disconnect(p2p.DiscUselessPeer)
}

func updateHead(header *types.Header) {
	mux.Lock()
	defer mux.Unlock()
	if header.Number.Uint64() > headNumber {
		headHash = header.Hash()
		headNumber = header.Number.Uint64()
		headTd = header.Difficulty
		log.Debug("head updated. ", "headNumber", headNumber, "headHash", headHash.Hex(), "headTd", headTd)
	}

}

// func updateHeadfromRpc() error {
// 	client, _ := rpc.Dial("http://192.168.1.104:8545")
// 	if client == nil {
// 		fmt.Println("couldn't create rpc")
// 		return errors.New("could not connect to client rpc")
// 	}
// 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()
// 	var lastBlock BlockfromRpc
// 	err := client.CallContext(ctx, &lastBlock, "eth_getBlockByNumber", "latest", false)
// 	if err != nil {
// 		fmt.Println("can't get latest block:", err)
// 		return errors.New("could not get lates block")
// 	}
// 	headTd.SetString(lastBlock.Difficulty[2:], 16)
// 	headHash = lastBlock.Hash
// 	headNumber = lastBlock.Number.ToInt().Uint64()
// 	return nil
// }

func (bc *BlockchainForkid) Config() ctypes.ChainConfigurator {
	return genesis.Config
}

func (bc *BlockchainForkid) Genesis() *types.Block {
	return genesisBlock
}

func (bc *BlockchainForkid) CurrentHeader() *types.Header {
	return &types.Header{
		Difficulty: headTd,
		Number:     new(big.Int).SetUint64(headNumber),
	}
}

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

// RequestHeadersByNumber fetches a batch of blocks' headers corresponding to the
// specified header query, based on the number of an origin block.
func (p *peer) requestHeadersByNumber(origin uint64, amount int, skip int, reverse bool) error {
	p.Log().Debug("Fetching batch of headers", "count", amount, "fromnum", origin, "skip", skip, "reverse", reverse)
	return p2p.Send(p.rw, GetBlockHeadersMsg, &getBlockHeadersData{Origin: hashOrNumber{Number: origin}, Amount: uint64(amount), Skip: uint64(skip), Reverse: reverse})
}

// handleMsg is invoked whenever an inbound message is received from a remote
// peer. The remote connection is torn down upon returning any error.
func handleMsg(p *peer) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
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
		p.Log().Debug("new Msg", "code", "StatusMsg")
		// Status messages should never arrive after the handshake
		return errResp(ErrExtraStatusMsg, "uncontrolled status message")

	// Block header query, collect the requested headers and reply
	case msg.Code == GetBlockHeadersMsg:
		p.Log().Debug("new Msg", "code", "GetBlockHeadersMsg")
		// Decode the complex header query
		var query getBlockHeadersData
		if err := msg.Decode(&query); err != nil {
			p.Log().Warn("GetBlockHeadersMsg failed", "err", err)
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		p.Log().Debug("GetBlockHeadersMsg", "msg", query)

		var headers []*types.Header

		return p2p.Send(p.rw, BlockHeadersMsg, headers)

	case msg.Code == BlockHeadersMsg:
		p.Log().Debug("new Msg", "code", "BlockHeadersMsg")
		// A batch of headers arrived to one of our previous requests
		var headers []*types.Header
		if err := msg.Decode(&headers); err != nil {
			p.Log().Warn("BlockHeadersMsg faild", "err", err)
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		p.Log().Debug("BlockHeadersMsg", "len", len(headers))

		for i, header := range headers {
			// Validate and mark the remote transaction
			if header == nil {
				return errResp(ErrDecode, "block header %d is nil", i)
			}
			p.Log().Debug("BlockHeadersMsg", "number", header.Number, "hash", header.Hash().Hex())
		}

		// If no headers were received, but we're expecting a checkpoint header, consider it that //todo
		if len(headers) == 0 && p.syncDrop != nil {
			// Stop the timer either way, decide later to drop or not
			p.syncDrop.Stop()
			p.syncDrop = nil
		}
		// If we have no checkpoint let every peer connects us.
		if checkpoint == nil && p.syncDrop != nil {
			// Stop the timer either way, decide later to drop or not
			p.syncDrop.Stop()
			p.syncDrop = nil
		}
		// Filter out any explicitly requested headers, deliver the rest to the downloader
		filter := len(headers) == 1
		if filter {
			// If it's a potential sync progress check, validate the content and advertised chain weight
			if p.syncDrop != nil && headers[0].Number.Uint64() == checkpointNumber {
				// Disable the sync drop timer
				p.syncDrop.Stop()
				p.syncDrop = nil

				// Validate the header and either drop the peer or continue
				if headers[0].Hash() != checkpointHash {
					return errors.New("checkpoint hash mismatch")
				}
				updateHead(headers[0])
				return nil
			}
		}

	case msg.Code == GetBlockBodiesMsg:
		p.Log().Debug("new Msg", "code", "GetBlockBodiesMsg")

	case msg.Code == BlockBodiesMsg:
		p.Log().Debug("new Msg", "code", "BlockBodiesMsg")

	case p.version >= eth63 && msg.Code == GetNodeDataMsg:
		p.Log().Debug("new Msg", "code", "GetNodeDataMsg")

	case p.version >= eth63 && msg.Code == NodeDataMsg:
		p.Log().Debug("new Msg", "code", "NodeDataMsg")

	case p.version >= eth63 && msg.Code == GetReceiptsMsg:
		p.Log().Debug("new Msg", "code", "GetReceiptsMsg")

	case p.version >= eth63 && msg.Code == ReceiptsMsg:
		p.Log().Debug("new Msg", "code", "ReceiptsMsg")

	case msg.Code == NewBlockHashesMsg:
		p.Log().Debug("new Msg", "code", "NewBlockHashesMsg")
		var announces newBlockHashesData
		if err := msg.Decode(&announces); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		// Mark the hashes as present at the remote node
		for _, block := range announces {
			p.MarkBlock(block.Hash)
			p.Log().Debug("NewBlockHashesMsg", "number", block.Number, "hash", block.Hash.Hex())
			p.addBlockHash(block.Hash)
		}
		// // Schedule all the unknown hashes for retrieval ///todo does we need a fetcher??
		// unknown := make(newBlockHashesData, 0, len(announces))
		// for _, block := range announces {
		// 	if !pm.blockchain.HasBlock(block.Hash, block.Number) {
		// 		unknown = append(unknown, block)
		// 	}
		// }
		// for _, block := range unknown {
		// 	pm.blockFetcher.Notify(p.id, block.Hash, block.Number, time.Now(), p.RequestOneHeader, p.RequestBodies)
		// }

	case msg.Code == NewBlockMsg:
		p.Log().Debug("new Msg", "code", "NewBlockMsg")
		// Retrieve and decode the propagated block
		var request newBlockData
		if err := msg.Decode(&request); err != nil {
			p.Log().Warn("NewBlockMsg Decode faild", "err", err)
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		if hash := types.CalcUncleHash(request.Block.Uncles()); hash != request.Block.UncleHash() {
			p.Log().Warn("Propagated block has invalid uncles", "have", hash, "exp", request.Block.UncleHash())
			break // TODO(karalabe): return error eventually, but wait a few releases
		}
		if hash := types.DeriveSha(request.Block.Transactions(), trie.NewStackTrie(nil)); hash != request.Block.TxHash() {
			p.Log().Warn("Propagated block has invalid body", "have", hash, "exp", request.Block.TxHash())
			break // TODO(karalabe): return error eventually, but wait a few releases
		}
		if err := request.sanityCheck(); err != nil {
			p.Log().Warn("NewBlockMsg sanityCheck faild", "err", err)
			return err
		}
		request.Block.ReceivedAt = msg.ReceivedAt
		request.Block.ReceivedFrom = p

		// Mark the peer as owning the block and schedule it for import
		p.MarkBlock(request.Block.Hash())
		// pm.blockFetcher.Enqueue(p.id, request.Block) //todo

		// Assuming the block is importable by the peer, but possibly not yet done so,
		// calculate the head hash and TD that the peer truly must have.
		var (
			trueHead = request.Block.ParentHash()
			trueTD   = new(big.Int).Sub(request.TD, request.Block.Difficulty())
		)
		// Update the peer's total difficulty if better than the previous
		if _, td := p.Head(); trueTD.Cmp(td) > 0 {
			p.SetHead(trueHead, trueTD)
			// pm.chainSync.handlePeerEvent(p)
		}
		updateHead(request.Block.Header())
		p.Log().Debug("NewBlockMsg", "number", request.Block.Number(), "hash", request.Block.Hash().Hex(), "td", request.TD)
		p.addBlock(request.Block)

	case msg.Code == NewPooledTransactionHashesMsg && p.version >= eth65:
		p.Log().Debug("new Msg", "code", "NewPooledTransactionHashesMsg")
		var hashes []common.Hash
		if err := msg.Decode(&hashes); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Schedule all the unknown hashes for retrieval
		for _, hash := range hashes {
			p.markTransaction(hash)
			p.Log().Debug("NewPooledTransactionHashesMsg", "hash", hash.Hex())
			p.addTxHash(hash)
		}
		// pm.txFetcher.Notify(p.id, hashes)
	case msg.Code == GetPooledTransactionsMsg && p.version >= eth65:
		p.Log().Debug("new Msg", "code", "GetPooledTransactionsMsg")

	case msg.Code == TransactionMsg || (msg.Code == PooledTransactionsMsg && p.version >= eth65):
		p.Log().Debug("new Msg", "code", "TransactionMsg || (msg.Code == PooledTransactionsMsg && p.version >= eth65)")
		// Transactions can be processed, parse all of them and deliver to the pool
		var txs []*types.Transaction
		if err := msg.Decode(&txs); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		for i, tx := range txs {
			// Validate and mark the remote transaction
			if tx == nil {
				return errResp(ErrDecode, "transaction %d is nil", i)
			}
			p.Log().Debug("TransactionMsg", "hash", tx.Hash().Hex())
			p.markTransaction(tx.Hash())
			p.addTx(tx)
		}
		// pm.txFetcher.Enqueue(p.id, txs, msg.Code == PooledTransactionsMsg)
	default:
		p.Log().Info("new Msg", "code", "default")

	}
	return nil
}

func (p *peer) addBlockHash(hash common.Hash) error {
	if blocksSet.Contains(hash) {
		return nil
	}

	// If we reached the memory allowance, drop a previously blocks hash
	for blocksSet.Cardinality() >= maxRecordedBlocks {
		poped := blocksSet.Pop()
		log.Debug("poped from blocksSet", "hash", poped.(common.Hash).Hex())

	}
	log.Debug("adding to blocksSet", "size", blocksSet.Cardinality(), "hash", hash.Hex())
	blocksSet.Add(hash)

	if err := checkSession(); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(p.RemoteAddr().String())
	if err != nil {
		return err
	}
	if err := cassandra_lib.AddBlockHash(session, hash, host); err != nil {
		p.Log().Warn("Couldn't insert block hash into cassandra", "err", err)
		return err
	}
	p.Log().Info("BlockHash inserted to cassandra", "block", hash.Hex(), "ip", p.RemoteAddr())
	return nil
}

func (p *peer) addBlock(block *types.Block) error {
	if blocksSet.Contains(block.Hash()) {
		return nil
	}

	// If we reached the memory allowance, drop a previously blocks hash
	for blocksSet.Cardinality() >= maxRecordedBlocks {
		poped := blocksSet.Pop()
		log.Debug("poped from blocksSet", "hash", poped.(common.Hash).Hex())

	}
	log.Debug("adding to blocksSet", "size", blocksSet.Cardinality(), "hash", block.Hash().Hex())
	blocksSet.Add(block.Hash())

	if err := checkSession(); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(p.RemoteAddr().String())
	if err != nil {
		return err
	}
	if err := cassandra_lib.AddBlock(session, block, host); err != nil {
		p.Log().Warn("Couldn't insert block into cassandra", "err", err)
		return err
	}
	p.Log().Info("Block inserted to cassandra", "block", block.Hash().Hex(), "ip", p.RemoteAddr())
	return nil
}

func (p *peer) addTx(tx *types.Transaction) error {
	if txsSet.Contains(tx.Hash()) {
		return nil
	}
	// If we reached the memory allowance, drop a previously blocks hash
	for txsSet.Cardinality() >= maxRecordedTxs {
		poped := txsSet.Pop()
		log.Debug("poped from txsSet", "hash", poped.(common.Hash).Hex())

	}
	log.Debug("adding to txsSet", "size", txsSet.Cardinality(), "hash", tx.Hash().Hex())
	txsSet.Add(tx.Hash())

	if err := checkSession(); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(p.RemoteAddr().String())
	if err != nil {
		return err
	}
	if err := cassandra_lib.AddTx(session, tx, host); err != nil {
		p.Log().Warn("Couldn't insert tx into cassandra", "err", err)
		return err
	}
	p.Log().Info("Tx inserted to cassandra", "tx", tx.Hash().Hex(), "ip", p.RemoteAddr())
	return nil
}

func (p *peer) addTxHash(hash common.Hash) error {
	if txsSet.Contains(hash) {
		return nil
	}
	// If we reached the memory allowance, drop a previously blocks hash
	for txsSet.Cardinality() >= maxRecordedTxs {
		poped := txsSet.Pop()
		log.Debug("poped from txsSet", "hash", poped.(common.Hash).Hex())
	}
	log.Debug("adding to txsSet", "size", txsSet.Cardinality(), "hash", hash.Hex())
	txsSet.Add(hash)

	if err := checkSession(); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(p.RemoteAddr().String())
	if err != nil {
		return err
	}
	if err := cassandra_lib.AddTxHash(session, hash, host); err != nil {
		p.Log().Warn("Couldn't insert tx hash into cassandra", "err", err)
		return err
	}
	p.Log().Info("TxHash inserted to cassandra", "tx", hash.Hex(), "ip", p.RemoteAddr())
	return nil
}

func checkSession() error {
	if session == nil {
		var err error
		log.Warn("Cassandra session is nil. trying to  create session")
		session, err = cluster.CreateSession()
		if err != nil {
			log.Warn("Couldn't create cassandra session", "err", err)
			return err
		}
		log.Info("Cassandra session created")
		return nil
	}
	return nil
}

// MarkTransaction marks a transaction as known for the peer, ensuring that it
// will never be propagated to this particular peer.
func (p *peer) markTransaction(hash common.Hash) {
	// If we reached the memory allowance, drop a previously known transaction hash
	for p.knownTxs.Cardinality() >= maxKnownTxs {
		p.knownTxs.Pop()
	}
	p.knownTxs.Add(hash)
}

// MarkBlock marks a block as known for the peer, ensuring that the block will
// never be propagated to this particular peer.
func (p *peer) MarkBlock(hash common.Hash) {
	// If we reached the memory allowance, drop a previously known block hash
	for p.knownBlocks.Cardinality() >= maxKnownBlocks {
		p.knownBlocks.Pop()
	}
	p.knownBlocks.Add(hash)
}

// sanityCheck verifies that the values are reasonable, as a DoS protection
func (request *newBlockData) sanityCheck() error {
	if err := request.Block.SanityCheck(); err != nil {
		return err
	}
	//TD at mainnet block #7753254 is 76 bits. If it becomes 100 million times
	// larger, it will still fit within 100 bits
	if tdlen := request.TD.BitLen(); tdlen > 100 {
		return fmt.Errorf("too large block TD: bitlen %d", tdlen)
	}
	return nil
}

// Head retrieves a copy of the current head hash and total difficulty of the
// peer.
func (p *peer) Head() (hash common.Hash, td *big.Int) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	copy(hash[:], p.head[:])
	return hash, new(big.Int).Set(p.td)
}

// SetHead updates the head hash and total difficulty of the peer.
func (p *peer) SetHead(hash common.Hash, td *big.Int) {
	p.lock.Lock()
	defer p.lock.Unlock()

	copy(p.head[:], hash[:])
	p.td.Set(td)
}

// EncodeRLP is a specialized encoder for hashOrNumber to encode only one of the
// two contained union fields.
func (hn *hashOrNumber) EncodeRLP(w io.Writer) error {
	if hn.Hash == (common.Hash{}) {
		return rlp.Encode(w, hn.Number)
	}
	if hn.Number != 0 {
		return fmt.Errorf("both origin hash (%x) and number (%d) provided", hn.Hash, hn.Number)
	}
	return rlp.Encode(w, hn.Hash)
}

// DecodeRLP is a specialized decoder for hashOrNumber to decode the contents
// into either a block hash or a block number.
func (hn *hashOrNumber) DecodeRLP(s *rlp.Stream) error {
	_, size, _ := s.Kind()
	origin, err := s.Raw()
	if err == nil {
		switch {
		case size == 32:
			err = rlp.DecodeBytes(origin, &hn.Hash)
		case size <= 8:
			err = rlp.DecodeBytes(origin, &hn.Number)
		default:
			err = fmt.Errorf("invalid input size %d for origin", size)
		}
	}
	return err
}

// Info gathers and returns a collection of metadata known about a peer.
func (p *peer) Info() *PeerInfo {
	hash, td := p.Head()

	return &PeerInfo{
		Version:    p.version,
		Difficulty: td,
		Head:       hash.Hex(),
	}
}
