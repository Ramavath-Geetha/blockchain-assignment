package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
	"bufio"
	"os"
	"github.com/syndtr/goleveldb/leveldb"
)
//defines a custom type struct BlockStatus as an integer (int) 
//declares two constants Committed and Pending with the values 0 and 1, respectively.
type BlockStatus int

const (
	Committed BlockStatus = iota
	Pending
)

// marshaling refers to the process of converting Go data structures into a JSON representation, 
// while unmarshaling is the opposite process of converting JSON data into Go data structures.
type Txn struct {
	BlockNumber int    `json:"blockNumber"`
	Key         string `json:"key"`
	Value       Value  `json:"value"`
	Valid       bool   `json:"valid"`
	Hash        string `json:"hash"`
}
//that represents the value associated with a transaction. 
//key names for marshaling/unmarshaling JSON.

type Value struct {
	Val int     `json:"val"`
	Ver float64 `json:"ver"`
}


//represents a block in a blockchain
type Block struct {
	BlockNumber    int         `json:"blockNumber"`
	PrevBlockHash  string      `json:"prevBlockHash"`
	Txns           []Txn       `json:"txns"`
	Timestamp      int64       `json:"timestamp"`
	BlockStatus    BlockStatus `json:"blockStatus"`
}

//interface with 2 methods 
type BlockInterface interface {
	PushTxns(txns []Txn) error
	UpdateBlockStatus(status BlockStatus) error
}

//Defines a struct BlockImpl that represents the implementation of the BlockInterface
//representing the leveldb database
type BlockImpl struct {
	db *leveldb.DB
}

//Defines a constructor function NewBlockImpl 
//creates a new instance of BlockImpl with the provided LevelDB database and returns a pointer to it.
func NewBlockImpl(db *leveldb.DB) *BlockImpl {
	return &BlockImpl{db: db}
}

//Implements the PushTxns method for the BlockImpl struct. It takes a slice of transactions (txns) 
//as input and performs various operations related to transactions and LevelDB database.



func (b *BlockImpl) PushTxns(txns []Txn) error {
	
	startTime := time.Now()

	var wg sync.WaitGroup
	//A loop is started over the range of txns, which represents each transaction in the input slice.
	for i := range txns {
		//wg.Add(1) is called to add a counter to the WaitGroup to indicate that a goroutine is about to be executed.
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			hash := sha256.Sum256([]byte(fmt.Sprintf("%v", txns[i])))
			txns[i].Hash = fmt.Sprintf("%x", hash)

			if val, err := b.db.Get([]byte(txns[i].Key), nil); err == nil {
				fmt.Println(string(val))
				var value Value
				if err := json.Unmarshal(val, &value); err == nil {
					if value.Ver == txns[i].Value.Ver {
				    //If they match, the transaction is considered valid, and the Valid field of the transaction is set to true
						txns[i].Valid = true
					//	If the transaction is valid, the updated value is stored back into the database using 
						b.db.Put([]byte(txns[i].Key), []byte(fmt.Sprintf(`{"val":%d,"ver":%f}`, txns[i].Value.Val, txns[i].Value.Ver)), nil)
					} else {
						//If the version check fails or any other error occurs during the process, the Valid field of the transaction is set to false.
						txns[i].Valid = false
					}
				}
			}
		}(i)
	}
	wg.Wait()

    
	//The duration of the block processing is calculated by subtracting the startTime from the current time using time.Since(startTime).
	duration := time.Since(startTime)
	//processing time is converted to seconds using duration.Seconds()
    seconds := duration.Seconds()
	fmt.Printf("Block Number: %d\n", txns[0].BlockNumber)
	fmt.Printf("Block Processing Time: %.6f seconds\n", seconds)

	return nil
}

func (b *BlockImpl) UpdateBlockStatus(status BlockStatus) error {
	return nil
}

func writeBlockToFile(blockChannel chan Block) {
	file, err := os.OpenFile("./db/ledger.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for block := range blockChannel {
		blockJSON, err := json.Marshal(block)
		if err != nil {
			log.Fatal(err)
		}

		if _, err := file.WriteString(string(blockJSON) + "\n"); err != nil {
			log.Fatal(err)
		}
	}
}

func getBlockByNumber(blockNumber int) (*Block, error) {
	file, err := os.OpenFile("./db/ledger.txt", os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		blockJSON := scanner.Text()
		var blk Block
		if err := json.Unmarshal([]byte(blockJSON), &blk); err != nil {
			return nil, err
		}
		if blk.BlockNumber == blockNumber {
			return &blk, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("block not found")
}

func getAllBlocks() ([]Block, error) {
	file, err := os.OpenFile("./db/ledger.txt", os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var blocks []Block

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		blockJSON := scanner.Text()
		var block Block
		if err := json.Unmarshal([]byte(blockJSON), &block); err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return blocks, nil
}

func main() {
	db, err := leveldb.OpenFile("./db", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

    //Setup LevelDB entries
    for i:=1; i<=1000; i++{
        key := fmt.Sprintf("SIM%d",i)
        value := fmt.Sprintf(`{"val": %d, "ver": 1.0}`,i)
        err = db.Put([]byte(key),[]byte(value),nil)
        if err != nil{
            log.Println("Error putting value into LeveDB:",err)
        }
    }

	blockImpl := NewBlockImpl(db)

	blockChannel := make(chan Block)
	go writeBlockToFile(blockChannel)

	txns := []Txn{
		{BlockNumber: 1, Key: "SIM1", Value: Value{Val: 2, Ver: 1.0}},
		{BlockNumber: 1, Key: "SIM2", Value: Value{Val: 3, Ver: 1.0}},
		{BlockNumber: 1, Key: "SIM3", Value: Value{Val: 4, Ver: 2.0}},
	}

	block := Block{
		BlockNumber:   1,
		PrevBlockHash: "1234567",
		Txns:          txns,
		Timestamp:     time.Now().Unix(),
		BlockStatus:   Pending,
	}

	blockChannel <- block

	blockNumber := 1
	blk, err := getBlockByNumber(blockNumber)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Block Number: %d\n", blk.BlockNumber)
	fmt.Printf("Previous Block Hash: %s\n", blk.PrevBlockHash)
	// Print other block details as needed

	t := time.Unix(blk.Timestamp, 0)
	fmt.Printf("Timestamp: %v\n", t.Format("2006-01-02 15:04:05"))

	blks, err := getAllBlocks()
	if err != nil {
		log.Fatal(err)
	}

    block.BlockStatus = Committed
	for _, block := range blks {
		fmt.Printf("Block Number: %d\n", block.BlockNumber)
        fmt.Printf("BlockStatus: %d\n",block.BlockStatus)
		fmt.Printf("Previous Block Hash: %s\n", block.PrevBlockHash)
		fmt.Printf("Timestamp: %v\n", time.Unix(block.Timestamp, 0).Format("2006-01-02 15:04:05"))
		// Print other block details as needed
		fmt.Println()
	}

	if err := blockImpl.PushTxns(block.Txns); err != nil {
		log.Fatal(err)
	}

	blockJSON, err := json.Marshal(block)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(blockJSON))
}





































