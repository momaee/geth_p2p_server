/* Before you execute the program, Launch `cqlsh` and execute:
create keyspace eth with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
create table eth.ipaddresses(hash ascii PRIMARY KEY, ip ascii);
*/

package cassandra_lib

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/gocql/gocql"
)

var mutex = &sync.RWMutex{}

//ErrNoSession called when session is nil
const ErrNoSession = "Session is nil"

//AddBlock adds whole block to cassandra
func AddBlock(session *gocql.Session, block *types.Block, ip string) error {
	return AddBlockHash(session, block.Hash(), ip)
}

// AddBlockHash adds block hash to cassandra
func AddBlockHash(session *gocql.Session, hash common.Hash, ip string) error {
	if session == nil {
		return errors.New(ErrNoSession)
	}
	mutex.Lock()
	defer mutex.Unlock()
	// insert a hash
	if err := session.Query(`INSERT INTO ipaddresses (hash, ip) VALUES (?, ?) IF NOT EXISTS`,
		hash.Hex(), ip).Exec(); err != nil {
		return err
	}
	return nil
}

func test() {
	// connect to the cluster
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println("could not create session")
		return
	}
	defer session.Close()

	// insert a tweet
	if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
		"me", gocql.TimeUUID(), "hello world").Exec(); err != nil {
		log.Fatal(err)
	}

	var id gocql.UUID
	var text string

	/* Search for a specific set of records whose 'timeline' column matches
	 * the value 'me'. The secondary index that we created earlier will be
	 * used for optimizing the search */
	if err := session.Query(`SELECT id, text FROM tweet WHERE timeline = ? LIMIT 1`,
		"me").Consistency(gocql.One).Scan(&id, &text); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Tweet:", id, text)

	// list all tweets
	iter := session.Query(`SELECT id, text FROM tweet WHERE timeline = ?`, "me").Iter()
	for iter.Scan(&id, &text) {
		fmt.Println("TweetT:", id, text)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}
