// Package kvstore
//
// @author: xwc1125
package kvstore

import (
	"bytes"
	"encoding/binary"
	"github.com/chain5j/chain5j-pkg/codec/rlp"
	"github.com/chain5j/chain5j-pkg/types"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/models/statetype"
	"github.com/chain5j/logger"
)

// ReadCanonicalHash 读取规范区块头hash
func ReadCanonicalHash(db ChainDbReader, number uint64) types.Hash {
	data, _ := db.Get(headerHashKey(number))
	if len(data) == 0 {
		return types.Hash{}
	}
	return types.BytesToHash(data)
}

// WriteCanonicalHash 写入规范区块头hash。
func WriteCanonicalHash(db ChainDbWriter, hash types.Hash, number uint64) {
	if err := db.Put(headerHashKey(number), hash.Bytes()); err != nil {
		logger.Crit("Failed to store number to hash mapping", "err", err)
	}
}

// DeleteCanonicalHash 移除区块高度到hash的键值映射。
func DeleteCanonicalHash(db ChainDbDeleter, number uint64) {
	if err := db.Delete(headerHashKey(number)); err != nil {
		logger.Crit("Failed to delete number to hash mapping", "err", err)
	}
}

// ReadHeaderNumber 根据区块hash检索区块高度值
func ReadHeaderNumber(db ChainDbReader, hash types.Hash) *uint64 {
	data, _ := db.Get(headerNumberKey(hash))
	if len(data) != 8 {
		return nil
	}
	number := binary.BigEndian.Uint64(data)
	return &number
}

// ReadHeader 读取header
func ReadHeader(db ChainDbReader, hash types.Hash, number uint64) *models.Header {
	data := ReadHeaderRLP(db, hash, number)
	if len(data) == 0 {
		return nil
	}
	header := new(models.Header)
	if err := rlp.Decode(bytes.NewReader(data), header); err != nil {
		logger.Error("Invalid block header RLP", "hash", hash, "err", err)
		return nil
	}
	return header
}

// HasHeader 检查区块头是否存在
func HasHeader(db ChainDbReader, hash types.Hash, number uint64) bool {
	if has, err := db.Has(headerKey(number, hash)); !has || err != nil {
		return false
	}
	return true
}

// WriteHeader 写入区块头
func WriteHeader(db ChainDbWriter, header *models.Header) {
	var (
		hash    = header.Hash()
		height  = header.Height
		encoded = encodeBlockNumber(height)
	)

	key := headerNumberKey(hash)
	if err := db.Put(key, encoded); err != nil {
		logger.Crit("Failed to store hash to number mapping", "err", err)
	}

	// Write the encoded header
	data, err := rlp.EncodeToBytes(header)
	if err != nil {
		logger.Crit("Failed to RLP encode header", "err", err)
	}

	key = headerKey(height, hash)
	if err := db.Put(key, data); err != nil {
		logger.Crit("Failed to store header", "err", err)
	}
}

// DeleteHeader removes all block header data associated with a hash.
func DeleteHeader(db ChainDbDeleter, hash types.Hash, number uint64) {
	if err := db.Delete(headerKey(number, hash)); err != nil {
		logger.Crit("Failed to delete header", "err", err)
	}
	if err := db.Delete(headerNumberKey(hash)); err != nil {
		logger.Crit("Failed to delete hash to number mapping", "err", err)
	}
}

// ReadHeadHeaderHash 读取当前区块头hash
func ReadHeadHeaderHash(db ChainDbReader) types.Hash {
	data, _ := db.Get(headHeaderKey)
	if len(data) == 0 {
		return types.Hash{}
	}
	return types.BytesToHash(data)
}

// WriteHeadHeaderHash 写入当前区块头hash
func WriteHeadHeaderHash(db ChainDbWriter, hash types.Hash) {
	if err := db.Put(headHeaderKey, hash.Bytes()); err != nil {
		logger.Crit("Failed to store last header's hash", "err", err)
	}
}

// ReadHeadBlockHash 读取当前区块hash
func ReadHeadBlockHash(db ChainDbReader) types.Hash {
	data, _ := db.Get(headBlockKey)
	if len(data) == 0 {
		return types.Hash{}
	}
	return types.BytesToHash(data)
}

// WriteHeadBlockHash 写入当前区块hash
func WriteHeadBlockHash(db ChainDbWriter, hash types.Hash) {
	if err := db.Put(headBlockKey, hash.Bytes()); err != nil {
		logger.Crit("Failed to store last block's hash", "err", err)
	}
}

// ReadHeaderRLP retrieves a block header in its raw RLP database encoding.
func ReadHeaderRLP(db ChainDbReader, hash types.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get(headerKey(number, hash))
	return data
}

// ReadBlock retrieves an entire block corresponding to the hash, assembling it
// back from the stored header and body. If either the header or body could not
// be retrieved nil is returned.
//
// Note, due to concurrent download of header and block body the header and thus
// canonical hash can be stored in the database but the body data not (yet).
func ReadBlock(db ChainDbReader, hash types.Hash, number uint64) *models.Block {
	header := ReadHeader(db, hash, number)
	if header == nil {
		return nil
	}

	body := ReadBody(db, hash, number)
	if body == nil {
		return nil
	}
	return models.NewBlock(header, body.Txs, nil)
}

// WriteBlock serializes a block into the database, header and body separately.
func WriteBlock(db ChainDbWriter, block *models.Block) {
	WriteBody(db, block.Hash(), block.Height(), block.Body())
	WriteHeader(db, block.Header())
}

// DeleteBlock removes all block data associated with a hash.
func DeleteBlock(db ChainDbDeleter, hash types.Hash, number uint64) {
	DeleteHeader(db, hash, number)
	DeleteBody(db, hash, number)
}

// HasBody verifies the existence of a block body corresponding to the hash.
func HasBody(db ChainDbReader, hash types.Hash, number uint64) bool {
	if has, err := db.Has(blockBodyKey(number, hash)); !has || err != nil {
		return false
	}
	return true
}

// ReadBody retrieves the block body corresponding to the hash.
func ReadBody(db ChainDbReader, hash types.Hash, number uint64) *models.Body {
	data := ReadBodyRLP(db, hash, number)
	if len(data) == 0 {
		return nil
	}
	body := new(models.Body)
	if err := rlp.Decode(bytes.NewReader(data), body); err != nil {
		logger.Error("Invalid block body RLP", "hash", hash, "err", err)
		return nil
	}
	return body
}

// WriteBody store a block body into the database.
func WriteBody(db ChainDbWriter, hash types.Hash, number uint64, body *models.Body) {
	data, err := rlp.EncodeToBytes(body)
	if err != nil {
		logger.Crit("Failed to RLP encode body", "err", err)
	}

	WriteBodyRLP(db, hash, number, data)
}

// WriteReceipts stores all the transaction receipts belonging to a block.
func WriteReceipts(db ChainDbWriter, hash types.Hash, number uint64, receipts statetype.Receipts) {
	// Convert the receipts into their storage form and serialize them
	storageReceipts := make([]*statetype.ReceiptForStorage, len(receipts))
	for i, receipt := range receipts {
		storageReceipts[i] = (*statetype.ReceiptForStorage)(receipt)
	}
	bytes, err := rlp.EncodeToBytes(storageReceipts)
	if err != nil {
		logger.Crit("Failed to encode block receipts", "err", err)
	}
	// Store the flattened receipt slice
	if err := db.Put(blockReceiptsKey(number, hash), bytes); err != nil {
		logger.Crit("Failed to store block receipts", "err", err)
	}
}

// ReadReceipts retrieves all the transaction receipts belonging to a block.
func ReadReceipts(db ChainDbReader, hash types.Hash, number uint64) statetype.Receipts {
	// Retrieve the flattened receipt slice
	data, _ := db.Get(blockReceiptsKey(number, hash))
	if len(data) == 0 {
		return nil
	}
	// Convert the receipts from their storage form to their internal representation
	storageReceipts := []*statetype.ReceiptForStorage{}
	if err := rlp.DecodeBytes(data, &storageReceipts); err != nil {
		logger.Error("Invalid receipt array RLP", "hash", hash, "err", err)
		return nil
	}
	receipts := make(statetype.Receipts, len(storageReceipts))
	for i, receipt := range storageReceipts {
		receipts[i] = (*statetype.Receipt)(receipt)
	}
	return receipts
}

// DeleteBody removes all block body data associated with a hash.
func DeleteBody(db ChainDbDeleter, hash types.Hash, number uint64) {
	if err := db.Delete(blockBodyKey(number, hash)); err != nil {
		logger.Crit("Failed to delete block body", "err", err)
	}
}

// ReadBodyRLP retrieves the block body (transactions and uncles) in RLP encoding.
func ReadBodyRLP(db ChainDbReader, hash types.Hash, number uint64) rlp.RawValue {
	data, _ := db.Get(blockBodyKey(number, hash))
	return data
}

// WriteBodyRLP stores an RLP encoded block body into the database.
func WriteBodyRLP(db ChainDbWriter, hash types.Hash, number uint64, rlp rlp.RawValue) {
	if err := db.Put(blockBodyKey(number, hash), rlp); err != nil {
		logger.Crit("Failed to store block body", "err", err)
	}
}

// TxLookupEntry is a positional metadata to help looking up the data content of
// a transaction or receipt given only its hash.
type TxLookupEntry struct {
	BlockHash  types.Hash   // 区块Hash
	BlockIndex uint64       // 区块Index
	TxType     types.TxType // 交易类型
	TxIndex    uint64       // 交易index
}

// WriteTxLookupEntries stores a positional metadata for every transaction from
// a block, enabling hash based transaction and receipt lookups.
func WriteTxLookupEntries(db ChainDbWriter, block *models.Block) {
	for _, txs := range block.Transactions().Data() {
		for j, tx := range txs {
			entry := TxLookupEntry{
				BlockHash:  block.Hash(),
				BlockIndex: block.Height(),
				TxType:     txs[0].TxType(),
				TxIndex:    uint64(j),
			}
			data, err := rlp.EncodeToBytes(entry)
			if err != nil {
				logger.Crit("Failed to encode transaction lookup entry", "err", err)
			}
			if err := db.Put(txLookupKey(tx.Hash()), data); err != nil {
				logger.Crit("Failed to store transaction lookup entry", "err", err)
			}
		}
	}
}

// ReadTxLookupEntry retrieves the positional metadata associated with a transaction
// hash to allow retrieving the transaction or receipt by hash.
func ReadTxLookupEntry(db ChainDbReader, hash types.Hash) (blockHash types.Hash, blockIndex uint64, txType types.TxType, txIndex uint64) {
	data, _ := db.Get(txLookupKey(hash))
	if len(data) == 0 {
		return types.Hash{}, 0, types.TxTypeUnknown, 0
	}
	var entry TxLookupEntry
	if err := rlp.DecodeBytes(data, &entry); err != nil {
		logger.Error("Invalid transaction lookup entry RLP", "hash", hash, "err", err)
		return types.Hash{}, 0, types.TxTypeUnknown, 0
	}
	return entry.BlockHash, entry.BlockIndex, entry.TxType, entry.TxIndex
}

// ReadTransaction retrieves a specific transaction from the database, along with
// its added positional metadata.
func ReadTransaction(db ChainDbReader, hash types.Hash) (models.Transaction, types.Hash, uint64, uint64) {
	blockHash, blockNumber, txType, txIndex := ReadTxLookupEntry(db, hash)
	if blockHash == (types.Hash{}) {
		return nil, types.Hash{}, 0, 0
	}
	body := ReadBody(db, blockHash, blockNumber)
	if body == nil || body.Txs.Len() <= int(txIndex) {
		logger.Error("Transaction referenced missing", "number", blockNumber, "hash", blockHash, "index", txIndex)
		return nil, types.Hash{}, 0, 0
	}
	return body.Txs.GetTx(txType, uint(txIndex)), blockHash, blockNumber, txIndex
}
