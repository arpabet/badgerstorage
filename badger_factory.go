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
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"os"
	"path/filepath"
)

func DatabaseExist(dataDir string) bool {
	_, err := os.Stat(dataDir)
	return err == nil
}

func CreateDatabase(conf *BadgerConfig) error {

	var log badger.Logger

	if conf.Log != nil {
		log = NewZapLogger(conf.Log, conf.Debug)
	} else {
		log = NewLogger(conf.Debug)
	}

	log.Infof("Create database on folder %s\n", conf.DataDir)

	if DatabaseExist(conf.DataDir) {
		log.Infof("Data directory is not empty %s\n", conf.DataDir)

		if conf.Action != DeleteIfExist {
			return ErrDatabaseExist
		}

		err := os.RemoveAll(conf.DataDir)
		if err != nil {
			log.Errorf("Error delete directory '%s', %v\n", conf.DataDir, err)
			return err
		}

	}

	if err := createAllDirectories(conf, log); err != nil {
		return err
	}

	db, err := OpenDatabase(conf)
	if err != nil {
		return err
	}
	return db.Close()
}

func createAllDirectories(conf *BadgerConfig, log badger.Logger) error {

	keyDir := filepath.Join(conf.DataDir, "key")
	valueDir := filepath.Join(conf.DataDir, "value")

	log.Infof("Create directories: %s, %s, %s\n", conf.DataDir, keyDir, valueDir)
	if err := createDirIfNeeded(conf.DataDir, conf.DirPerm); err != nil {
		log.Errorf("Error create directory '%s', %v\n", conf.DataDir, err)
		return err
	}

	if err := createDirIfNeeded(keyDir, conf.DirPerm); err != nil {
		log.Errorf("Error create directory '%s', %v\n", keyDir, err)
		return err
	}

	if err := createDirIfNeeded(valueDir, conf.DirPerm); err != nil {
		log.Errorf("Error create directory '%s', %v\n", valueDir, err)
		return err
	}
	return nil
}

func OpenDatabase(conf *BadgerConfig) (*badger.DB, error) {

	var log badger.Logger

	if conf.Log != nil {
		log = NewZapLogger(conf.Log, conf.Debug)
	} else {
		log = NewLogger(conf.Debug)
	}

	if !DatabaseExist(conf.DataDir) {

		if conf.Action == CreateIfNotExist {
			if err := createAllDirectories(conf, log); err != nil {
				return nil, err
			}
		} else {
			return nil, ErrDatabaseNotExist
		}

	}

	keyDir := filepath.Join(conf.DataDir, "key")
	valueDir := filepath.Join(conf.DataDir, "value")

	opts := badger.DefaultOptions(conf.DataDir)
	opts.Logger = log
	if conf.UseZSTD {
		opts.Compression = options.ZSTD
		opts.ZSTDCompressionLevel = 9
	} else {
		opts.Compression = options.None
	}
	opts.ValueLogMaxEntries = ValueLogMaxEntries
	opts.Dir = keyDir
	opts.ValueDir = valueDir
	opts.TableLoadingMode = options.MemoryMap
	opts.ValueLogLoadingMode = options.MemoryMap
	if conf.StorageKey != nil {

		if len(conf.StorageKey) != KeySize {
			return nil, ErrInvalidKeySize
		}

		opts.EncryptionKey = conf.StorageKey
		opts.EncryptionKeyRotationDuration = KeyRotationDuration
	}
	opts.Truncate = conf.TruncateDB

	return badger.Open(opts)

}

func createDirIfNeeded(dirname string, dirperm os.FileMode) error {

	_, err := os.Stat(dirname)
	exist := err == nil
	if exist {
		return nil
	}

	if dirperm == 0 {
		dirperm = DefaultDirPerm
	}

	err = os.Mkdir(dirname, dirperm)
	if err != nil {
		return err
	}

	return os.Chmod(dirname, dirperm)
}
