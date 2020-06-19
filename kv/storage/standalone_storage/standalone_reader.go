package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandaloneReader is a implementation of storage.StorageReader
type StandaloneReader struct {
	txn *badger.Txn
}

func NewStandaloneReader(txn *badger.Txn) *StandaloneReader {
	return &StandaloneReader{
		txn,
	}
}

func (sr *StandaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(sr.txn, cf, key)
}

// IterCF is a func
func (sr *StandaloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandaloneReader) Close() {
	sr.txn.Discard()
}
