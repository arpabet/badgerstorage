/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package badgerstorage

import (
	"github.com/dgraph-io/badger/v2"
	"github.com/pkg/errors"
	"go.arpabet.com/storage"
	"io"
	"os"
	"time"
)

type badgerStorage struct {
	name string
	db *badger.DB
}

func New(name string, dataDir string, options ...Option) (storage.ManagedStorage, error) {

	if name == "" {
		return nil, errors.New("empty bean name")
	}

	db, err := OpenDatabase(dataDir, options...)
	if err != nil {
		return nil, wrapError(err)
	}

	return &badgerStorage{name: name, db: db}, nil
}

func FromDB(name string, db *badger.DB) storage.ManagedStorage {
	return &badgerStorage{name: name, db: db}
}

func (t *badgerStorage) BeanName() string {
	return t.name
}

func (t *badgerStorage) Destroy() error {
	return t.db.Close()
}

func (t *badgerStorage) Get() *storage.GetOperation {
	return &storage.GetOperation{Storage: t}
}

func (t *badgerStorage) Set() *storage.SetOperation {
	return &storage.SetOperation{Storage: t}
}

func (t *badgerStorage) CompareAndSet() *storage.CompareAndSetOperation {
	return &storage.CompareAndSetOperation{Storage: t}
}

func (t *badgerStorage) Increment() *storage.IncrementOperation {
	return &storage.IncrementOperation{Storage: t, Initial: 0, Delta: 1}
}

func (t *badgerStorage) Remove() *storage.RemoveOperation {
	return &storage.RemoveOperation{Storage: t}
}

func (t *badgerStorage) Enumerate() *storage.EnumerateOperation {
	return &storage.EnumerateOperation{Storage: t}
}

func (t *badgerStorage) GetRaw(key []byte, ttlPtr *int, versionPtr *int64, required bool) ([]byte, error) {
	return t.getImpl(key, ttlPtr, versionPtr, required)
}

func (t *badgerStorage) SetRaw(key, value []byte, ttlSeconds int) error {

	txn := t.db.NewTransaction(true)
	defer txn.Discard()

	entry := &badger.Entry{Key: key, Value: value, UserMeta: byte(0x0)}

	if ttlSeconds > 0 {
		entry.ExpiresAt = uint64(time.Now().Unix() + int64(ttlSeconds))
	}

	err := txn.SetEntry(entry)

	if err != nil {
		return errors.Errorf("badger put entry error, %v", err)
	}

	return wrapError(txn.Commit())

}

func (t *badgerStorage) DoInTransaction(key []byte, cb func(entry *storage.RawEntry) bool) error {

	txn := t.db.NewTransaction(true)
	defer txn.Discard()

	rawEntry := &storage.RawEntry {
		Key: key,
		Ttl: storage.NoTTL,
		Version: 0,
	}

	item, err := txn.Get(key)
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return err
		}
	} else {
		rawEntry.Value, err = item.ValueCopy(nil)
		if err != nil {
			return errors.Errorf("badger fetch value failed: %v", err)
		}
		rawEntry.Ttl = getTtl(item)
		rawEntry.Version = int64(item.Version())
	}

	if !cb(rawEntry) {
		return ErrCanceled
	}

	entry := &badger.Entry{
		Key: key,
		Value: rawEntry.Value,
		UserMeta: byte(0x0)}

	if rawEntry.Ttl > 0 {
		entry.ExpiresAt = uint64(time.Now().Unix() + int64(rawEntry.Ttl))
	}

	err = txn.SetEntry(entry)
	if err != nil {
		return errors.Errorf("badger set entry error, %v", err)
	}

	return wrapError(txn.Commit())
}

func (t *badgerStorage) CompareAndSetRaw(key, value []byte, ttlSeconds int, version int64) (bool, error) {

	txn := t.db.NewTransaction(true)
	defer txn.Discard()

	entry := &badger.Entry{Key: key, Value: value, UserMeta: byte(0x0)}

	if ttlSeconds > 0 {
		entry.ExpiresAt = uint64(time.Now().Unix() + int64(ttlSeconds))
	}

	item, err := txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			if version != 0 { // for non exist item version is 0
				return false, nil
			}
		} else {
			return false, err
		}
	} else if item.Version() != uint64(version) {
		return false, nil
	}

	err = txn.SetEntry(entry)

	if err != nil {
		return false, errors.Errorf("badger put entry error, %v", err)
	}

	return true, wrapError(txn.Commit())

}

func (t *badgerStorage) RemoveRaw(key []byte) error {

	txn := t.db.NewTransaction(true)
	defer txn.Discard()

	err := txn.Delete(key)

	if err != nil {
		return errors.Errorf("badger delete entry error, %v", err)
	}
	return wrapError(txn.Commit())
}

func (t *badgerStorage) getImpl(key []byte, ttlPtr *int, versionPtr *int64, required bool) ([]byte, error) {

	txn := t.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get(key)
	if err != nil {

		if err == badger.ErrKeyNotFound {
			if required {
				return nil, os.ErrNotExist
			} else {
				return nil, nil
			}
		} else {
			return nil, errors.Errorf("badger get value failed: %v", err)
		}

	}

	data, err := item.ValueCopy(nil)
	if err != nil {
		return nil, errors.Errorf("badger fetch value failed: %v", err)
	}

	if ttlPtr != nil {
		*ttlPtr = getTtl(item)
	}

	if versionPtr != nil {
		*versionPtr = int64(item.Version())
	}

	return data, nil
}

func (t *badgerStorage) EnumerateRaw(prefix, seek []byte, batchSize int, onlyKeys bool, cb func(entry *storage.RawEntry) bool) error {

	options := badger.IteratorOptions{
		PrefetchValues: !onlyKeys,
		PrefetchSize:   batchSize,
		Reverse:        false,
		AllVersions:    false,
		Prefix:         prefix,
	}

	txn := t.db.NewTransaction(false)
	defer txn.Discard()

	iter := txn.NewIterator(options)
	defer iter.Close()

	for iter.Seek(seek); iter.Valid(); iter.Next() {

		item := iter.Item()
		key := item.Key()
		var value []byte
		if !onlyKeys {
			var err error
			value, err = item.ValueCopy(nil)
			if err != nil {
				return errors.Errorf("badger failed to copy value for key %v", key)
			}
		}
		rw := storage.RawEntry{
			Key:     key,
			Value:   value,
			Ttl:     getTtl(item),
			Version: int64(item.Version()),
		}
		if !cb(&rw) {
			break
		}
	}

	return nil
}

func getTtl(item *badger.Item) int {
	expiresAt := item.ExpiresAt()
	if expiresAt == 0 {
		return 0
	}
	val := int(expiresAt - uint64(time.Now().Unix()))
	if val == 0 {
		val = -1
	}
	return val
}

func (t *badgerStorage) Compact(discardRatio float64) error {
	return wrapError(t.db.RunValueLogGC(discardRatio))
}

func (t *badgerStorage) Backup(w io.Writer, since uint64) (uint64, error) {
	newSince, err := t.db.Backup(w, since)
	return newSince, wrapError(err)
}

func (t *badgerStorage) Restore(r io.Reader) error {
	return wrapError(t.db.Load(r, MaxPendingWrites))
}

func (t *badgerStorage) DropAll() error {
	return wrapError(t.db.DropAll())
}

func (t *badgerStorage) DropWithPrefix(prefix []byte) error {
	return wrapError(t.db.DropPrefix(prefix))
}

func wrapError(err error) error {
	if err != nil {
		return errors.Errorf("badger error, %v", err)
	}
	return err
}

func (t *badgerStorage) Instance() interface{} {
	return t.db
}
