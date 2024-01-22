package indextree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/smartbch/moeingads/types"
)

var _ = badger.BlockCache

type BadgerDB struct {
	db *badger.DB
	// ro     *gorocksdb.ReadOptions
	// wo     *gorocksdb.WriteOptions
	// woSync *gorocksdb.WriteOptions
	batch *badgerDBBatch
	mtx   sync.Mutex
}

func NewBadgerDB(name string, dir string) (IKVDB, error) {
	// default rocksdb option, good enough for most cases, including heavy workloads.
	// 64MB table cache, 32MB write buffer
	// compression: snappy as default, need to -lsnappy to enable.
	// bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	// bbto.SetBlockCache(gorocksdb.NewLRUCache(64 * 1024 * 1024))
	// bbto.SetFilterPolicy(gorocksdb.NewBloomFilter(10))

	// opts := gorocksdb.NewDefaultOptions()
	// opts.SetBlockBasedTableFactory(bbto)
	// SetMaxOpenFiles to 4096 seems to provide a reliable performance boost
	// opts.SetMaxOpenFiles(4096)
	// opts.SetCreateIfMissing(true)
	// opts.IncreaseParallelism(runtime.NumCPU())
	// 1.5GB maximum memory use for writebuffer.
	// opts.OptimizeLevelStyleCompaction(512 * 1024 * 1024)

	dbPath := filepath.Join(dir, name+".db")
	opts := badger.DefaultOptions(dbPath)
	return NewBadgerDBWithOptions(opts)
}

func NewBadgerDBWithOptions(opts badger.Options) (IKVDB, error) {
	// dbPath := filepath.Join(dir, name+".db")
	// filter := HeightCompactionFilter{}
	// opts.SetCompactionFilter(&filter) // use a customized compaction filter
	// opts.SetCompression(gorocksdb.NoCompression)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	// ro := gorocksdb.NewDefaultReadOptions()
	// wo := gorocksdb.NewDefaultWriteOptions()
	// woSync := gorocksdb.NewDefaultWriteOptions()
	// woSync.SetSync(true)
	database := &BadgerDB{
		db: db,
		// ro:     ro,
		// wo:     wo,
		// woSync: woSync,
		// filter: &filter,
	}
	return database, nil
}

func (db *BadgerDB) CurrBatch() types.Batch {
	return db.batch
}

func (db *BadgerDB) LockBatch() {
	db.mtx.Lock()
}

func (db *BadgerDB) UnlockBatch() {
	db.mtx.Unlock()
}

func (db *BadgerDB) CloseOldBatch() {
	if db.batch != nil {
		db.batch.WriteSync()
		db.batch.Close()
		db.batch = nil
	}
}

func (db *BadgerDB) OpenNewBatch() {
	db.batch = db._newBatch()
}

func (db *BadgerDB) SetPruneHeight(h uint64) {
	// do nothing
}

func (db *BadgerDB) GetPruneHeight() (uint64, bool) {
	// return db.filter.pruneHeight, db.filter.pruneEnable
	return 0, false
}

// Implements DB.
func (db *BadgerDB) Get(key []byte) []byte {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	var val []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err == nil && val == nil {
			val = []byte{}
		}
		return err
	})
	if err != nil {
		panic(err)
	}
	return val
}

// Implements DB.
func (db *BadgerDB) Has(key []byte) bool {
	bytes := db.Get(key)
	return bytes != nil
}

// Implements DB.
func (db *BadgerDB) Set(key []byte, value []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	if value == nil {
		panic("Nil Value for RocksDB")
	}
	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	if err != nil {
		panic(err)
	}
}

func (db *BadgerDB) sync() {
	err := db.db.Sync()
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *BadgerDB) SetSync(key []byte, value []byte) {
	db.Set(key, value)
	db.sync()
}

// Implements DB.
func (db *BadgerDB) Delete(key []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	if err != nil {
		panic(err)
	}
}

// Implements DB.
func (db *BadgerDB) DeleteSync(key []byte) {
	db.Delete(key)
	db.sync()
}

func (db *BadgerDB) DB() *badger.DB {
	return db.db
}

// Implements DB.
func (db *BadgerDB) Close() {
	// db.ro.Destroy()
	// db.wo.Destroy()
	// db.woSync.Destroy()
	db.db.Close()
}

// Implements DB.
func (db *BadgerDB) Print() {
	itr := db.Iterator(nil, nil)
	defer itr.Close()
	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
}

// Implements DB.
func (db *BadgerDB) Stats() map[string]string {
	return nil
}

//----------------------------------------
// Batch

// Implements DB.
func (db *BadgerDB) NewBatch() types.Batch {
	return db._newBatch()
}

func (db *BadgerDB) _newBatch() *badgerDBBatch {
	wb := &badgerDBBatch{
		db:         db,
		batch:      db.db.NewWriteBatch(),
		firstFlush: make(chan struct{}, 1),
	}
	wb.firstFlush <- struct{}{}
	return wb
}

type badgerDBBatch struct {
	db    *BadgerDB
	batch *badger.WriteBatch

	firstFlush chan struct{}
}

// Implements Batch.
func (mBatch *badgerDBBatch) Set(key, value []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	if value == nil {
		panic("Nil Value for RocksDB")
	}
	if mBatch.batch == nil {
		panic("errBatchClosed")
	}
	mBatch.batch.Set(append([]byte{}, key...), append([]byte{}, value...))
}

// Implements Batch.
func (mBatch *badgerDBBatch) Delete(key []byte) {
	if len(key) == 0 {
		panic("Empty Key for RocksDB")
	}
	if mBatch.batch == nil {
		panic("errBatchClosed")
	}
	mBatch.batch.Delete(append([]byte{}, key...))
}

// Implements Batch.
func (mBatch *badgerDBBatch) Write() {
	if mBatch.batch == nil {
		panic("errBatchClosed")
	}

	select {
	case <-mBatch.firstFlush:
		err := mBatch.batch.Flush()
		if err != nil {
			panic(err)
		}
	default:
		panic("batch already flushed")
	}

	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	mBatch.Close()
}

// Implements Batch.
func (mBatch *badgerDBBatch) WriteSync() {
	mBatch.Write()
	mBatch.db.sync()
}

// Implements Batch.
func (mBatch *badgerDBBatch) Close() {
	if mBatch.batch != nil {
		select {
		case <-mBatch.firstFlush: // a Flush after Cancel panics too
		default:
		}
		mBatch.batch.Cancel()
		mBatch.batch = nil
	}
}

//----------------------------------------
// Iterator

func (db *BadgerDB) Iterator(start, end []byte) types.Iterator {
	opts := badger.DefaultIteratorOptions
	return db.iteratorOpts(start, end, opts)
}

func (db *BadgerDB) ReverseIterator(start, end []byte) types.Iterator {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	return db.iteratorOpts(end, start, opts)
}

func (b *BadgerDB) iteratorOpts(start, end []byte, opts badger.IteratorOptions) *badgerDBIterator {
	// if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
	// 	panic("errKeyEmpty")
	// }
	txn := b.db.NewTransaction(false)
	iter := txn.NewIterator(opts)
	iter.Rewind()
	iter.Seek(start)
	if opts.Reverse && iter.Valid() && bytes.Equal(iter.Item().Key(), start) {
		// If we're going in reverse, our starting point was "end",
		// which is exclusive.
		iter.Next()
	}
	return &badgerDBIterator{
		reverse: opts.Reverse,
		start:   start,
		end:     end,

		txn:  txn,
		iter: iter,
	}
}

var _ types.Iterator = (*badgerDBIterator)(nil)

type badgerDBIterator struct {
	reverse    bool
	start, end []byte

	txn  *badger.Txn
	iter *badger.Iterator

	lastErr error
}

func (itr *badgerDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *badgerDBIterator) Valid() bool {
	if !itr.iter.Valid() {
		return false
	}
	if len(itr.end) > 0 {
		key := itr.iter.Item().Key()
		if c := bytes.Compare(key, itr.end); (!itr.reverse && c >= 0) || (itr.reverse && c < 0) {
			// We're at the end key, or past the end.
			return false
		}
	}
	return true
}

func (itr *badgerDBIterator) Key() []byte {
	itr.assertIsValid()
	return itr.iter.Item().KeyCopy(nil)
}

func (itr *badgerDBIterator) Value() []byte {
	itr.assertIsValid()
	val, err := itr.iter.Item().ValueCopy(nil)
	if err != nil {
		itr.lastErr = err
	}
	return val
}

func (itr badgerDBIterator) Next() {
	itr.assertIsValid()
	itr.iter.Next()
}

func (itr *badgerDBIterator) Close() {
	itr.iter.Close()
	itr.txn.Discard()
}

func (itr *badgerDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("iterator is invalid")
	}
}
