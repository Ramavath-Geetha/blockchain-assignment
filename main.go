package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type BlockStatus int

const (
	Committed BlockStatus = iota
	Pending
)

type Txn struct {
	BlockNumber int    `json:"blockNumber"`
	Key         string `json:"sim"`
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
	Hash          string       `json:"hash"`
}

type BlockInterface interface {
	PushTxns(block *Block, txns []Txn, blockChannel chan *Block) error
	UpdateBlockStatus(status BlockStatus) error
}

type BlockImpl struct {
	db *leveldb.DB
}

func NewBlockImpl(db *leveldb.DB) *BlockImpl {
	return &BlockImpl{db: db}
}

func (b *BlockImpl) PushTxns(block *Block, txns []Txn, blockChannel chan *Block) error {
	var wg sync.WaitGroup
	for i := range txns {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hash := sha256.Sum256([]byte(fmt.Sprintf("%v", txns[i])))
			txns[i].Hash = fmt.Sprintf("%x", hash)
			if val, err := b.db.Get([]byte(txns[i].Key), nil); err == nil {
				var dbValue Value
				if err := json.Unmarshal(val, &dbValue); err == nil {
					if dbValue.Ver == txns[i].Value.Ver {
						txns[i].Valid = true
						valueJSON, err := json.Marshal(txns[i].Value)
						if err != nil {
							log.Println("Error marshaling value:", err)
						}
						b.db.Put([]byte(txns[i].Key), valueJSON, nil)
					} else {
						txns[i].Valid = false
					}
				}
			}
		}(i)
	}
	wg.Wait()

	if block.BlockNumber > 1 {
		prevBlock, err := getBlockByNumber("./db/ledger.txt", block.BlockNumber-1)
		if err == nil {
			block.PrevBlockHash = prevBlock.Hash
		} else {
			log.Println("Error fetching previous block:", err)
		}
	}
	block.Hash = CalculateBlockHash(block)
	block.BlockStatus = Committed

	blockChannel <- block

	return nil
}

func (b *BlockImpl) UpdateBlockStatus(status BlockStatus) error {
	return nil
}

func CalculateBlockHash(block *Block) string {
	blockJSON, err := json.Marshal(block)
	if err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(blockJSON))
}

func writeBlockToFile(blockJSON []byte) {
	file, err := os.OpenFile("./db/ledger.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString(string(blockJSON) + "\n"); err != nil {
		log.Fatal(err)
	}
}

func getBlockByNumber(filePath string, blockNumber int) (*Block, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	for scanner.Scan() {
		var block Block
		if err := json.Unmarshal([]byte(scanner.Text()), &block); err != nil {
			return nil, err
		}
		if block.BlockNumber == blockNumber {
			if block.BlockNumber > 1 {
				prevBlock, err := getBlockByNumber(filePath, block.BlockNumber-1)
				if err == nil {
					block.PrevBlockHash = prevBlock.Hash
				} else {
					log.Println("Error fetching previous block:", err)
				}
			}
			return &block, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("block not found")
}

func fetchAllBlocks(filePath string) ([]*Block, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var blocks []*Block

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	for scanner.Scan() {
		var block Block
		if err := json.Unmarshal([]byte(scanner.Text()), &block); err != nil {
			return nil, err
		}
		if block.BlockNumber > 1 {
			prevBlock, err := getBlockByNumber(filePath, block.BlockNumber-1)
			if err == nil {
				block.PrevBlockHash = prevBlock.Hash
			} else {
				log.Println("Error fetching previous block:", err)
			}
		}
		blocks = append(blocks, &block)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return blocks, nil
}

func main() {
	totalTransactionsEnv := "10000"

	transactionsPerBlockEnv := os.Getenv("TRANSACTIONS_PER_BLOCK")

	totalTransactions, err := strconv.Atoi(totalTransactionsEnv)
	if err != nil {
		log.Fatal("Invalid value for TOTAL_TRANSACTIONS:", totalTransactionsEnv)
	}

	transactionsPerBlock, err := strconv.Atoi(transactionsPerBlockEnv)
	if err != nil {
		log.Fatal("Invalid value for TRANSACTIONS_PER_BLOCK:", transactionsPerBlockEnv)
	}

	numBlocks := totalTransactions / transactionsPerBlock

	db, err := leveldb.OpenFile("db", nil)
	if err != nil {
		log.Fatal("Error opening LevelDB:", err)
	}
	defer db.Close()

	for i := 1; i <= totalTransactions; i++ {
		key := fmt.Sprintf("SIM%d", i)
		value := fmt.Sprintf(`{"val": %d, "ver": 1.0}`, i)
		err = db.Put([]byte(key), []byte(value), nil)
		if err != nil {
			log.Println("Error putting value into LevelDB:", err)
		}
	}

	blockChannel := make(chan *Block)

	blockImpl := NewBlockImpl(db)

	go func() {
		file, err := os.OpenFile("./db/ledger.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		for {
			receivedBlock := <-blockChannel
			blockJSON, err := json.Marshal(receivedBlock)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := file.WriteString(string(blockJSON) + "\n"); err != nil {
				log.Fatal(err)
			}
		}
	}()

	for j := 1; j <= numBlocks; j++ {
		var txns []Txn
		for i := 1; i <= transactionsPerBlock; i++ {
			sim := fmt.Sprintf("SIM%d", (j-1)*transactionsPerBlock+i)
			value := Value{Val: rand.Intn(100),
				Ver: roundToNearest(rand.Float64()*4.0 + 1.0)}
			txns = append(txns, Txn{
				BlockNumber: j,
				Key:         sim,
				Value:       value,
				Valid:       false,
			})
		}

		block := &Block{
			BlockNumber:   j,
			PrevBlockHash: "0000000",
			Txns:          txns,
			Timestamp:     time.Now().UnixNano(),
			BlockStatus:   Pending,
			Hash:          "",
		}

		fmt.Printf("Processing Block Number: %d\n", j)
		startTime := time.Now()
		blockImpl.PushTxns(block, txns, blockChannel)
		processingTime := time.Since(startTime)
		fmt.Printf("Processing Time for Block Number %d: %s\n", j, processingTime.String())
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter the block number you want to fetch (1-10), or enter 'all' to fetch all blocks: ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		if text == "all" {
			blocks, err := fetchAllBlocks("./db/ledger.txt")
			if err != nil {
				fmt.Println("Error fetching blocks:", err)
				continue
			}
			for _, block := range blocks {
				blockJSON, err := json.MarshalIndent(block, "", "  ")
				if err != nil {
					fmt.Println("Error marshaling block:", err)
					continue
				}
				fmt.Println(string(blockJSON))
			}
		} else {
			blockNumber, err := strconv.Atoi(text)
			if err != nil || blockNumber < 1 || blockNumber > numBlocks {
				fmt.Printf("Invalid input. Please enter a number between 1 and %d, or 'all' to fetch all blocks.\n", numBlocks)
				continue
			}
			block, err := getBlockByNumber("./db/ledger.txt", blockNumber)
			if err != nil {
				fmt.Println("Error fetching block:", err)
				continue
			}
			blockJSON, err := json.MarshalIndent(block, "", "  ")
			if err != nil {
				fmt.Println("Error marshaling block:", err)
				continue
			}
			fmt.Println(string(blockJSON))
		}
	}
}

func roundToNearest(x float64) float64 {
	return math.Round(x)
}
