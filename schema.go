// Package kvstore
//
// @author: xwc1125
package kvstore

import (
	"encoding/binary"
	"github.com/chain5j/chain5j-pkg/types"
)

var (
	headHeaderKey = []byte("LastHeader") // 已知header的hash
	headBlockKey  = []byte("LastBlock")  // 已知区块的hash

	headerPrefix       = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	headerHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian) 根据hash检索区块高度

	blockBodyPrefix     = []byte("b") // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockReceiptsPrefix = []byte("r") // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	txLookupPrefix = []byte("l") // txLookupPrefix + hash -> transaction/receipt lookup metadata

	chainConfigPrefix       = []byte("cc-")       // 链配置前缀
	chainConfigLatestPrefix = []byte("cc-latest") // 最新链配置前缀
)

// headerNumberKey = headerNumberPrefix + hash
// 通过hash获取区块高度
func headerNumberKey(hash types.Hash) []byte {
	return append(headerNumberPrefix, hash.Bytes()...)
}

// headerHashKey = headerPrefix + num (uint64 big endian) + headerHashSuffix
// 通过区块高度获取区块头hash
func headerHashKey(number uint64) []byte {
	return append(append(headerPrefix, encodeBlockNumber(number)...), headerHashSuffix...)
}

// headerKey = headerPrefix + num (uint64 big endian) + hash
// 根据区块高度和hash获取区块头
func headerKey(number uint64, hash types.Hash) []byte {
	return append(append(headerPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}

// encodeBlockNumber encodes a block number as big endian uint64
// 将uint64转换为bytes
func encodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

// blockBodyKey = blockBodyPrefix + num (uint64 big endian) + hash
// 通过区块高度和hash获取区块体
func blockBodyKey(number uint64, hash types.Hash) []byte {
	return append(append(blockBodyPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}

// txLookupKey = txLookupPrefix + hash
// 通过交易hash查找区块信息
func txLookupKey(hash types.Hash) []byte {
	return append(txLookupPrefix, hash.Bytes()...)
}

// chainConfigKey = configPrefix + hash
// 通过hash获取链配置信息
func chainConfigKey(hash types.Hash) []byte {
	return append(chainConfigPrefix, hash.Bytes()...)
}

// chainConfigHeightKey = configPrefix + height
func chainConfigHeightKey(height uint64) []byte {
	return append(chainConfigPrefix, encodeBlockNumber(height)...)
}

// blockReceiptsKey = blockReceiptsPrefix + num (uint64 big endian) + hash
// 通过高度和hash获取收据信息
func blockReceiptsKey(number uint64, hash types.Hash) []byte {
	return append(append(blockReceiptsPrefix, encodeBlockNumber(number)...), hash.Bytes()...)
}
