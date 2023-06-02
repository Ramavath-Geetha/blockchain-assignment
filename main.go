package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type BlockStatus string

const (
	Committed BlockStatus = "Committed"
	Pending   BlockStatus = "Pending"
)

type Txn struct {
	ID    string
	Value struct {
		Val float64
		Ver float64
	}
	Valid bool
}

type Block struct {
	BlockNumber      int
	Txns             []Txn
	Timestamp        time.Time
	BlockStatus      BlockStatus
	PreviousBlockHash string
}

type BlockInterface interface {
	PushValidTxns(txns []map[string]map[string]float64, db *leveldb.DB) error
	UpdateBlockStatus(status BlockStatus)
}

func calculateHash(txn Txn, ch chan Txn) {
	// Calculate hash of txn
	// Update txn ID and valid status
	ch <- txn
}

func (b *Block) PushValidTxns(txns []map[string]map[string]float64, db *leveldb.DB, maxTxns int, ch chan *Block) error {
	// Create channel to receive calculated hashes
	hashCh := make(chan Txn)

	count := 0
	for _, txn := range txns {
		if count >= maxTxns {
			break
		}
		for key, value := range txn {
			// Get the current value from LevelDB
			data, err := db.Get([]byte(key), nil)
			if err != nil {
				return err
			}

			var currentVal struct {
				Val float64
				Ver float64
			}
			json.Unmarshal(data, &currentVal)

			// Check if the version matches
			valid := currentVal.Ver == value["ver"]

			// Add the transaction to the block
			go calculateHash(Txn{
				ID: key,
				Value: struct {
					Val float64
					Ver float64
				}{
					Val: value["val"],
					Ver: value["ver"],
				},
				Valid: valid,
			}, hashCh)

			// If the transaction is valid, update the value in LevelDB
			if valid {
				currentVal.Val = value["val"]
				currentVal.Ver += 1.0
				newData, _ := json.Marshal(currentVal)
				err = db.Put([]byte(key), newData, nil)
				if err != nil {
					return err
				}
			}

			count++
			if count >= maxTxns {
				break
			}
		}
	}

	// Wait for all goroutines to finish and update the block's transactions
	for i := 0; i < count; i++ {
		txn := <-hashCh
		b.Txns = append(b.Txns, txn)
	}

	// Update block status
	b.UpdateBlockStatus(Pending)

	// Send block to the channel for committing blocks
	ch <- b

	return nil
}

func commitBlock(commitCh chan *Block, fileCh chan *Block) {
	// Wait for block to be ready to commit
	block := <-commitCh

	// Update block status
	block.UpdateBlockStatus(Committed)

	// Send block to the channel for writing to a file
	fileCh <- block
}

func (b *Block) UpdateBlockStatus(status BlockStatus) {
	b.BlockStatus = status
}

func setupLevelDB() (*leveldb.DB, error) {
	db, err := leveldb.OpenFile("path/to/db", nil)
	if err != nil {
		return nil, err
	}

	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("SIM%d", i)
		value := fmt.Sprintf(`{"val": %d, "ver": 1.0}`, i)
		err = db.Put([]byte(key), []byte(value), nil)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func writeBlockToFile(block *Block, file *os.File) error {
	ledger, err := json.MarshalIndent(block, "", "  ")
	if err != nil {
		return err
	}

	_, err = file.Write(ledger)
	if err != nil {
		return err
	}

	_, err = file.WriteString("\n\n")
	if err != nil {
		return err
	}

	return nil
}

func readBlockFromFile(file *os.File, blockNumber int) (*Block, error) {
	decoder := json.NewDecoder(file)

	for {
		// Read a line from the file
		block := &Block{}
		err := decoder.Decode(block)
		if err != nil {
			return nil, err
		}

		// Check if the block number matches
		if block.BlockNumber == blockNumber {
			return block, nil
		}
	}
}

func readAllBlocksFromFile(file *os.File) ([]*Block, error) {
	decoder := json.NewDecoder(file)
	blocks := make([]*Block, 0)

	for {
		// Read a line from the file
		block := &Block{}
		err := decoder.Decode(block)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

func main() {
	db, err := setupLevelDB()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	txns := []map[string]map[string]float64{
		{"SIM1": {"val": 2, "ver": 1.0}},
		{"SIM2": {"val": 3, "ver": 1.0}},
		{"SIM3": {"val": 4, "ver": 2.0}},
	}

	block := &Block{
		BlockNumber:      1,
		Txns:             []Txn{},
		Timestamp:        time.Now(),
		BlockStatus:      Pending,
		PreviousBlockHash: "0xabc123",
	}

	// Create channel for committing blocks
	commitCh := make(chan *Block)
	// Create channel for writing blocks to a file
	fileCh := make(chan *Block)

	go commitBlock(commitCh, fileCh)

	err = block.PushValidTxns(txns, db, 5, commitCh)
	if err != nil {
		panic(err)
	}

	// Wait for block to be written to a file
	committedBlock := <-fileCh

	// Open the file for writing blocks
	file, err := os.OpenFile("blocks.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Write the block to the file
	err = writeBlockToFile(committedBlock, file)
	if err != nil {
		panic(err)
	}

	fmt.Println("Block processing time:", time.Since(block.Timestamp))
}
