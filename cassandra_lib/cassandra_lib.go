/* Before you execute the program, Launch `cqlsh` and execute:
create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
create table example.tweet(timeline text, id UUID, text text, PRIMARY KEY(id));
create index on example.tweet(timeline);
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

var mutex = &sync.Mutex{}

const ErrNoSession = "Session is nil"

//AddBlock adds whole block to cassandra
func AddBlock(session *gocql.Session, block *types.Block) error {
	if session == nil {
		return errors.New(ErrNoSession)
	}
	mutex.Lock()
	defer mutex.Unlock()
	// insert a tweet
	if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
		"me", gocql.TimeUUID(), block.Hash().Hex()).Exec(); err != nil {
		return err
	}
	return nil
}

func AddBlockHashNumber(session *gocql.Session, hash common.Hash, number uint64) error {
	if session == nil {
		return errors.New(ErrNoSession)
	}
	mutex.Lock()
	defer mutex.Unlock()
	// insert a tweet
	if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
		"me", gocql.TimeUUID(), hash.Hex()).Exec(); err != nil {
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
