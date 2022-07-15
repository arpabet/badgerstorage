/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package badgerstorage

import (
	"github.com/dgraph-io/badger/v2"
)

func OpenDatabase(dataDir string, options ...Option) (*badger.DB, error) {

	opts := badger.DefaultOptions(dataDir)
	opts.ValueLogMaxEntries = ValueLogMaxEntries

	for _, opt := range options {
		opt.apply(&opts)
	}

	return badger.Open(opts)

}

