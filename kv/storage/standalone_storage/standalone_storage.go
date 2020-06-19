package standalone_storage

import (
	badger "github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	opts badger.Options
	db   *badger.DB
}

// NewStandAloneStorage is a func
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = opts.Dir

	return &StandAloneStorage{
		opts,
		nil,
	}
}

// Start is a func
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	var err error
	s.db, err = badger.Open(s.opts)
	if err != nil {
		return err
	}
	return nil
}

// Stop is a func
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.db.Close(); err != nil {
		return err
	}
	return nil
}

// Reader is a func
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return NewStandaloneReader(txn), nil
	// return nil, nil
}

// Write is a func
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// TODO: no save point is used
	var wb engine_util.WriteBatch
	for _, mod := range batch {
		wb.Reset()
		switch mod.Data.(type) {
		case storage.Put:
			wb.SetCF(mod.Cf(), mod.Key(), mod.Value())
			break
		case storage.Delete:
			wb.DeleteCF(mod.Cf(), mod.Key())
			break
		}
		if err := wb.WriteToDB(s.db); err != nil {
			return err
		}
	}
	return nil
}
