// Package kvstore
//
// @author: xwc1125
package kvstore

import (
	"context"
	"errors"
	"github.com/chain5j/chain5j-pkg/database/kvstore"
	"github.com/chain5j/chain5j-pkg/types"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/chain5j-protocol/models/statetype"
	"github.com/chain5j/chain5j-protocol/protocol"
	"github.com/chain5j/logger"
)

var (
	_ protocol.Database = new(kvStore)
)

type kvStore struct {
	log logger.Logger
	db  kvstore.Database
}

func NewKvStore(rootCtx context.Context, opts ...option) (protocol.Database, error) {
	k := &kvStore{
		log: logger.New("kvStore"),
	}
	if err := apply(k, opts...); err != nil {
		logger.Error("kvstore apply options err", "err", err)
		return nil, err
	}
	return k, nil
}

func (k *kvStore) Start() error {
	return nil
}
func (k *kvStore) Stop() error {
	return nil
}

func (k *kvStore) LatestHeader() (*models.Header, error) {
	// 获取最新的区块头hash及最新的区块头
	lastBlockHash := ReadHeadHeaderHash(k.db)
	if lastBlockHash == types.EmptyHash {
		return nil, errors.New("current header not exist")
	}
	if latestHeader, err := k.GetHeaderByHash(lastBlockHash); err != nil {
		return nil, err
	} else {
		return latestHeader, nil
	}
}
func (k *kvStore) GetHeader(hash types.Hash, height uint64) (*models.Header, error) {
	// 根据hash及number获取header
	header := ReadHeader(k.db, hash, height)
	if header == nil {
		return nil, errors.New("header is not exist")
	}
	return header, nil
}
func (k *kvStore) GetHeaderByHash(hash types.Hash) (*models.Header, error) {
	// 根据hash读取区块高度
	height := ReadHeaderNumber(k.db, hash)
	if height == nil {
		return nil, errors.New("header is not exist")
	}
	return k.GetHeader(hash, *height)
}
func (k *kvStore) GetHeaderByHeight(height uint64) (*models.Header, error) {
	// 根据区块高度读取规范区块头hash
	hash := ReadCanonicalHash(k.db, height)
	if hash == (types.EmptyHash) {
		return nil, errors.New("header is not exist")
	}
	return k.GetHeader(hash, height)
}
func (k *kvStore) GetHeaderHeight(hash types.Hash) (*uint64, error) {
	return ReadHeaderNumber(k.db, hash), nil
}
func (k *kvStore) HasHeader(hash types.Hash, height uint64) (bool, error) {
	return HasHeader(k.db, hash, height), nil
}

func (k *kvStore) CurrentBlock() (*models.Block, error) {
	// 获取最新的区块头hash及最新的区块头
	lastBlockHash := ReadHeadBlockHash(k.db)
	if lastBlockHash == types.EmptyHash {
		return nil, errors.New("latest block no exist")
	}
	latestBlock, err := k.GetBlockByHash(lastBlockHash)
	if err != nil {
		return nil, err
	}
	return latestBlock, nil
}
func (k *kvStore) GetBlock(hash types.Hash, height uint64) (*models.Block, error) {
	block := ReadBlock(k.db, hash, height)
	if block == nil {
		return nil, errors.New("block is not exist")
	}
	return block, nil
}
func (k *kvStore) GetBlockByHash(hash types.Hash) (*models.Block, error) {
	// 根据hash读取区块高度
	height := ReadHeaderNumber(k.db, hash)
	if height == nil {
		return nil, errors.New("block is not exist")
	}
	return k.GetBlock(hash, *height)
}
func (k *kvStore) GetBlockByHeight(height uint64) (*models.Block, error) {
	hash := ReadCanonicalHash(k.db, height)
	if hash == (types.Hash{}) {
		return nil, errors.New("block is not exist")
	}
	return k.GetBlock(hash, height)
}
func (k *kvStore) HasBlock(hash types.Hash, height uint64) (bool, error) {
	return HasBody(k.db, hash, height), nil
}

func (k *kvStore) ChainConfig() (*models.ChainConfig, error) {
	return ReadChainConfigLatest(k.db)
}
func (k *kvStore) GetChainConfig(hash types.Hash, height uint64) (*models.ChainConfig, error) {
	return ReadChainConfigByHash(k.db, hash)
}
func (k *kvStore) GetChainConfigByHeight(height uint64) (*models.ChainConfig, error) {
	return ReadChainConfigByHeight(k.db, height)
}
func (k *kvStore) GetChainConfigByHash(hash types.Hash) (*models.ChainConfig, error) {
	return ReadChainConfigByHash(k.db, hash)
}

func (k *kvStore) GetCanonicalHash(height uint64) (bHash types.Hash, err error) {
	return ReadCanonicalHash(k.db, height), nil
}
func (k *kvStore) LatestBlockHash() (bHash types.Hash, err error) {
	return ReadHeadBlockHash(k.db), nil
}
func (k *kvStore) LatestHeaderHash() (bHash types.Hash, err error) {
	return ReadHeadHeaderHash(k.db), nil
}

func (k *kvStore) GetBody(hash types.Hash, height uint64) (*models.Body, error) {
	return ReadBody(k.db, hash, height), nil
}

func (k *kvStore) GetTransaction(hash types.Hash) (tx models.Transaction, blockHash types.Hash, blockHeight uint64, txIndex uint64, err error) {
	tx, blockHash, blockHeight, txIndex = ReadTransaction(k.db, hash)
	return
}
func (k *kvStore) GetReceipts(bHash types.Hash, height uint64) (statetype.Receipts, error) {
	return ReadReceipts(k.db, bHash, height), nil
}

func (k *kvStore) WriteBlock(block *models.Block) (err error) {
	WriteBlock(k.db, block)
	return nil
}
func (k *kvStore) WriteHeader(header *models.Header) (err error) {
	WriteHeader(k.db, header)
	return nil
}
func (k *kvStore) WriteChainConfig(bHash types.Hash, height uint64, chainConfig *models.ChainConfig) error {
	return WriteChainConfig(k.db, bHash, height, chainConfig)
}
func (k *kvStore) WriteLatestBlockHash(bHash types.Hash) error {
	WriteHeadBlockHash(k.db, bHash)
	return nil
}
func (k *kvStore) WriteLatestHeaderHash(bHash types.Hash) error {
	WriteHeadHeaderHash(k.db, bHash)
	return nil
}
func (k *kvStore) WriteCanonicalHash(bHash types.Hash, height uint64) error {
	WriteCanonicalHash(k.db, bHash, height)
	return nil
}
func (k *kvStore) WriteTxsLookup(block *models.Block) error {
	batch := k.db.NewBatch()
	WriteTxLookupEntries(batch, block)
	batch.Write()
	return nil
}
func (k *kvStore) WriteReceipts(bHash types.Hash, height uint64, receipts statetype.Receipts) error {
	WriteReceipts(k.db, bHash, height, receipts)
	return nil
}

func (k *kvStore) DeleteBlock(blockAbs []models.BlockAbstract, currentHeight, desHeight uint64) error {
	batch := k.db.NewBatch()
	if blockAbs != nil {
		for _, a := range blockAbs {
			// 删除body
			DeleteBody(batch, a.Hash, a.Height)
			// 删除header
			DeleteHeader(batch, a.Hash, a.Height)
			// 删除
		}
	}
	// 回滚标准库的区块高度
	if currentHeight > desHeight {
		for i := currentHeight; i > desHeight; i-- {
			DeleteCanonicalHash(batch, i)
		}
	}
	return batch.Write()
}
