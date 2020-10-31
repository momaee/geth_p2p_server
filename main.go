package main

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type Block struct {
	Number     *hexutil.Big
	Difficulty string
	Hash       common.Hash
}

func main() {
	// Connect the client.
	client, _ := rpc.Dial("http://192.168.1.104:8545")
	if client == nil {
		fmt.Println("couldn't create rpc")
		return
	}
	// subch := make(chan Block)

	// Ensure that subch receives the latest block.
	// go func() {
	// 	for i := 0; ; i++ {
	// 		if i > 0 {
	// 			time.Sleep(2 * time.Second)
	// 		}
	// 		subscribeBlocks(client, subch)
	// 	}
	// }()
	td := new(big.Int)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var lastBlock Block
	err := client.CallContext(ctx, &lastBlock, "eth_getBlockByNumber", "latest", false)
	if err != nil {
		fmt.Println("can't get latest block:", err)
		return
	}
	td.SetString(lastBlock.Difficulty[2:], 16)
	fmt.Println("latest block:", lastBlock.Number)
	fmt.Println("latest block:", lastBlock.Difficulty)
	fmt.Println(fmt.Sprintf("%x", td)) // or %X or upper case
	fmt.Println("latest block:", lastBlock.Hash.Hex())

	// Print events from the subscription as they arrive.
	// for block := range subch {
	// 	fmt.Println("latest block:", block.Number)
	// }
}

// subscribeBlocks runs in its own goroutine and maintains
// a subscription for new blocks.
func subscribeBlocks(client *rpc.Client, subch chan Block) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Subscribe to new blocks.
	sub, err := client.EthSubscribe(ctx, subch, "newHeads")
	if err != nil {
		fmt.Println("subscribe error:", err)
		return
	}

	// The connection is established now.
	// Update the channel with the current block.
	var lastBlock Block
	err = client.CallContext(ctx, &lastBlock, "eth_getBlockByNumber", "latest", false)
	if err != nil {
		fmt.Println("can't get latest block:", err)
		return
	}
	subch <- lastBlock

	// The subscription will deliver events to the channel. Wait for the
	// subscription to end for any reason, then loop around to re-establish
	// the connection.
	fmt.Println("connection lost: ", <-sub.Err())
}
