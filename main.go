package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type BlockStatus string

const (
	Committed BlockStatus = "Committed"
	Pending   BlockStatus = "Pending"
)
type Txn struct {
	BlockNumber int    `json:"blockNumber"`
	Key         string `json:"key"`
	Value       Value  `json:"value"`
	Valid       bool   `json:"valid"`
	Hash        string `json:"hash"`
}

type Value struct {
	Val int     `json:"val"`
	Ver float64 `json:"ver"`
}

type Block struct {
	BlockNumber   int          `json:"blockNumber"`
	PrevBlockHash string       `json:"prevBlockHash"`
	Txns          []Txn        `json:"txns"`
	Timestamp     int64        `json:"timestamp"`
	BlockStatus   BlockStatus  `json:"blockStatus"`
}

type BlockInterface interface {
	PushTxns(txns []Txn) error
	UpdateBlockStatus(status BlockStatus) error
}

type BlockImpl struct {
	db *leveldb.DB
}

func NewBlockImpl(db *leveldb.DB) *BlockImpl {
	return &BlockImpl{db: db}
}

func (b *BlockImpl) PushTxns(block *Block, txns []Txn) error {
	startTime := time.Now()

	batch := new(leveldb.Batch)

	var wg sync.WaitGroup
	var mu sync.Mutex // Mutex for synchronizing access to the txns slice

	for i := range txns {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hash := sha256.Sum256([]byte(fmt.Sprintf("%v", txns[i])))
			txns[i].Hash = fmt.Sprintf("%x", hash)

			if val, err := b.db.Get([]byte(txns[i].Key), nil); err == nil {
				var dbValue Value
				if err := json.Unmarshal(val, &dbValue); err == nil {
					mu.Lock() // Acquire the mutex before modifying the txns slice
					defer mu.Unlock() // Release the mutex after modifying the txns slice

					if dbValue.Ver == txns[i].Value.Ver {
						txns[i].Valid = true
						valueJSON, err := json.Marshal(txns[i].Value)
						if err != nil {
							log.Println("Error marshaling value:", err)
						}
						batch.Put([]byte(txns[i].Key), valueJSON)
					} else {
						txns[i].Valid = false
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Commit the batch operation
	if err := b.db.Write(batch, nil); err != nil {
		return err
	}

	block.BlockStatus = Committed

	duration := time.Since(startTime)
	seconds := duration.Seconds()
	fmt.Printf("Block Number: %d\n", txns[0].BlockNumber)
	fmt.Printf("Block Processing Time: %.6f seconds\n", seconds)

	return nil
}


func (b *BlockImpl) UpdateBlockStatus(status BlockStatus) error {
	return nil
}

func writeBlockToFile(data []byte) {
	file, err := os.OpenFile("./db/ledger.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(string(data) + "\n"); err != nil {
		log.Fatal(err)
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

	blockImpl := NewBlockImpl(db)

	for j := 1; j <= 10; j++ {
		var txns []Txn
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("SIM%d", i)
			value := Value{
				Val: rand.Intn(1000) + 1,
				Ver: float64(rand.Intn(2) + 1),
			}
			txn := Txn{
				BlockNumber: j,
				Key:         key,
				Value:       value,
			}
			txns = append(txns, txn)
		}

		block := Block{
			BlockNumber:   j,
			PrevBlockHash: "1234567",
			Txns:          txns,
			Timestamp:     time.Now().Unix(),
			BlockStatus:   Pending,
		}

		if err := blockImpl.PushTxns(&block, block.Txns); err != nil {
			log.Fatal(err)
		}

		blockJSON, err := json.Marshal(block)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(blockJSON))
		writeBlockToFile(blockJSON)
	}
}
