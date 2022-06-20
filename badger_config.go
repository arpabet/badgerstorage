/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
*/

package badgerstorage

import (
	"errors"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"go.uber.org/zap"
	"path/filepath"
	"time"
)

var (
	ValueLogMaxEntries  = uint32(1024 * 1024 * 1024)
	KeyRotationDuration = time.Hour * 24 * 7
	MaxPendingWrites = 4096

	ErrInvalidKeySize   = errors.New("invalid key size")
	ErrCanceled         = errors.New("operation was canceled")
	ErrDatabaseExist    = errors.New("database exist")
	ErrDatabaseNotExist = errors.New("database not exist")
	ErrItemNotExist     = errors.New("item not exist")
)

// Option configures reconciler using the functional options paradigm
// popularized by Rob Pike and Dave Cheney. If you're unfamiliar with this style,
// see https://commandcenter.blogspot.com/2014/01/self-referential-functions-and-design.html and
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis.
type Option interface {
	apply(*badger.Options)
}

// OptionFunc implements Option interface.
type optionFunc func(*badger.Options)

// apply the configuration to the provided config.
func (fn optionFunc) apply(r *badger.Options) {
	fn(r)
}

// option that do nothing
func WithNope() Option {
	return optionFunc(func(opts *badger.Options) {
	})
}

func WithReadOnly() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.ReadOnly = true
	})
}

func WithInMemory() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.InMemory = true
	})
}

func WithNumVersionsToKeep(num int) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.NumVersionsToKeep = num
	})
}

func WithSyncWrites() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.SyncWrites = true
	})
}

func WithDataDir(dataDir string) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.Dir = dataDir
		opts.ValueDir = dataDir
	})
}

func WithKeyValueDir(dataDir string) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.Dir = filepath.Join(dataDir, "key")
		opts.ValueDir = filepath.Join(dataDir, "value")
	})
}

func WithKeyMemoryMap() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.TableLoadingMode = options.MemoryMap
	})
}

func WithKeyFileIO() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.TableLoadingMode = options.FileIO
	})
}

func WithKeyRAM() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.TableLoadingMode = options.LoadToRAM
	})
}

func WithValueMemoryMap() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.ValueLogLoadingMode = options.MemoryMap
	})
}

func WithValueFileIO() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.ValueLogLoadingMode = options.FileIO
	})
}

func WithValueRAM() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.ValueLogLoadingMode = options.LoadToRAM
	})
}

func WithEncryptionKey(storageKey []byte) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.EncryptionKey = storageKey
		opts.EncryptionKeyRotationDuration = KeyRotationDuration
	})
}

func WithTruncate() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.Truncate = true
	})
}

func WithCompression(zstd bool) Option {
	return optionFunc(func(opts *badger.Options) {
		if zstd {
			opts.Compression = options.ZSTD
			opts.ZSTDCompressionLevel = 9
		} else {
			opts.Compression = options.None
		}
	})
}

func WithSnappy() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.Compression = options.Snappy
	})
}

func WithLogger(debug bool) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.Logger = NewLogger(debug)
	})
}

func WithZapLogger(log *zap.Logger, debug bool) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.Logger = NewZapLogger(log, debug)
	})
}


// Fine tuning options.

func WithMaxTableSize(size int64) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.MaxTableSize = size
	})
}

func WithLevelSizeMultiplier(value int) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.LevelSizeMultiplier = value
	})
}

func WithMaxLevels(value int) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.MaxLevels = value
	})
}

func WithValueThreshold(threshold int) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.ValueThreshold = threshold
	})
}

func WithNumMemtables(value int) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.NumMemtables = value
	})
}

// Changing BlockSize across DB runs will not break badger. The block size is
// read from the block index stored at the end of the table.
func WithBlockSize(value int) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.BlockSize = value
	})
}

func WithBloomFalsePositive(value float64) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.BloomFalsePositive = value
	})
}

func WithKeepL0InMemory() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.KeepL0InMemory = true
	})
}

func WithBlockCacheSize(value int64) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.BlockCacheSize = value
	})
}

func WithIndexCacheSize(value int64) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.IndexCacheSize = value
	})
}

func WithLoadBloomsOnOpen() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.LoadBloomsOnOpen = true
	})
}

func WithNumLevelZeroTables(value int) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.NumLevelZeroTables = value
	})
}

func WithNumLevelZeroTablesStall(value int) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.NumLevelZeroTablesStall = value
	})
}

func WithLevelOneSize(value int64) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.LevelOneSize = value
	})
}

func WithValueLogFileSize(value int64) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.ValueLogFileSize = value
	})
}

func WithValueLogMaxEntries(value uint32) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.ValueLogMaxEntries = value
	})
}

func WithNumCompactors(value int) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.NumCompactors = value
	})
}

func WithCompactL0OnClose() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.CompactL0OnClose = true
	})
}

func WithLogRotatesToFlush(value int32) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.LogRotatesToFlush = value
	})
}

// When set, checksum will be validated for each entry read from the value log file.
func WithVerifyValueChecksum() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.VerifyValueChecksum = true
	})
}

// BypassLockGaurd will bypass the lock guard on badger. Bypassing lock
// guard can cause data corruption if multiple badger instances are using
// the same directory. Use this options with caution.
func WithLBypassLockGuard() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.BypassLockGuard = true
	})
}

// ChecksumVerificationMode decides when db should verify checksums for SSTable blocks.
func WithChecksumVerificationMode(mode options.ChecksumVerificationMode) Option {
	return optionFunc(func(opts *badger.Options) {
		opts.ChecksumVerificationMode = mode
	})
}

// DetectConflicts determines whether the transactions would be checked for
// conflicts. The transactions can be processed at a higher rate when
// conflict detection is disabled.
func WithDetectConflicts() Option {
	return optionFunc(func(opts *badger.Options) {
		opts.DetectConflicts = true
	})
}
