// Package kvstore
//
// @author: xwc1125
package kvstore

import (
	"fmt"
	"github.com/chain5j/chain5j-pkg/database/kvstore"
)

const (
	DefaultTxStorePath    = "txdata"
	DefaultBlockStorePath = "chaindata"
	DefaultCrudStorePath  = "cruddata"
)

// option 单个选项
type option func(ops *kvStore) error

func apply(f *kvStore, opts ...option) error {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(f); err != nil {
			return fmt.Errorf("option apply err:%v", err)
		}
	}
	return nil
}

// WithDB kv数据库
func WithDB(db kvstore.Database) option {
	return func(ops *kvStore) error {
		ops.db = db
		return nil
	}
}
