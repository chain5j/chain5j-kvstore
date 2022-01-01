// Package kvstore
//
// @author: xwc1125
package kvstore

// ChainDbReader wraps the Has and Get method of a backing data store.
type ChainDbReader interface {
	Has(key []byte) (bool, error)
	Get(key []byte) ([]byte, error)
}

// ChainDbWriter wraps the Put method of a backing data store.
type ChainDbWriter interface {
	Put(key []byte, value []byte) error
}

// ChainDbDeleter wraps the Delete method of a backing data store.
type ChainDbDeleter interface {
	Delete(key []byte) error
}
