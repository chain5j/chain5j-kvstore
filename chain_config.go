// Package kvstore
//
// @author: xwc1125
package kvstore

import (
	"errors"
	"github.com/chain5j/chain5j-pkg/codec"
	"github.com/chain5j/chain5j-pkg/collection/maps/hashmap"
	"github.com/chain5j/chain5j-pkg/types"
	"github.com/chain5j/chain5j-protocol/models"
	"github.com/chain5j/logger"
)

// ReadChainConfigLatest 读取最新的区块链配置
func ReadChainConfigLatest(db ChainDbReader) (*models.ChainConfig, error) {
	bHash, err := db.Get(chainConfigLatestPrefix)
	if len(bHash) == 0 {
		return nil, err
	}
	return ReadChainConfigByHash(db, types.BytesToHash(bHash))
}

func ReadChainConfigByHeight(db ChainDbReader, height uint64) (*models.ChainConfig, error) {
	bHash, err := db.Get(chainConfigHeightKey(height))
	if len(bHash) == 0 {
		return nil, err
	}
	return ReadChainConfigByHash(db, types.BytesToHash(bHash))
}

// ReadChainConfigByHash 读取区块链配置，hash为创世区块hash
func ReadChainConfigByHash(db ChainDbReader, bHash types.Hash) (*models.ChainConfig, error) {
	data, err := db.Get(chainConfigKey(bHash))
	if len(data) == 0 {
		return nil, err
	}
	var cfc = models.ChainConfig{
		Consensus: &models.ConsensusConfig{
			Name: "",
			Data: hashmap.NewHashMap(true),
		},
	}
	if err = codec.Coder().Decode(data, &cfc); err != nil {
		return nil, err
	}
	return &cfc, nil
}

// WriteChainConfig 写入链配置
func WriteChainConfig(db ChainDbWriter, bHash types.Hash, height uint64, cfg *models.ChainConfig) error {
	if cfg == nil {
		return errors.New("chain config is empty")
	}

	bytes, err := codec.Coder().Encode(cfg)
	if err != nil {
		return err
	}

	if err := db.Put(chainConfigKey(bHash), bytes); err != nil {
		logger.Crit("Failed to store chain config", "err", err)
	}
	if err := db.Put(chainConfigHeightKey(height), bHash.Bytes()); err != nil {
		logger.Crit("Failed to store chain config height", "err", err)
	}
	if err := db.Put(chainConfigLatestPrefix, bHash.Bytes()); err != nil {
		logger.Crit("Failed to store chain config hash", "err", err)
	}
	return nil
}
