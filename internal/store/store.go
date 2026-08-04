package store

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/pranesh/meshflow/pkg/events"
	"github.com/pranesh/meshflow/pkg/workflow"
	"go.etcd.io/bbolt"
)

var (
	bucketWorkflows  = []byte("workflows")
	bucketRuns       = []byte("runs")
	bucketEventLog   = []byte("eventlog")
	bucketMeta       = []byte("meta")
)

type Store struct {
	db   *bbolt.DB
	path string
}

func Open(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dbPath := filepath.Join(dataDir, "meshflow.db")

	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}

	s := &Store{db: db, path: dbPath}

	if err := s.initBuckets(); err != nil {
		db.Close()
		return nil, err
	}

	slog.Info("bolt store opened", "path", dbPath)
	return s, nil
}

func (s *Store) initBuckets() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		for _, b := range [][]byte{bucketWorkflows, bucketRuns, bucketEventLog, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return fmt.Errorf("create bucket %s: %w", b, err)
			}
		}
		return nil
	})
}

func (s *Store) SaveWorkflow(w *workflow.Workflow, yamlData []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketWorkflows)
		return b.Put([]byte(w.Name), yamlData)
	})
}

func (s *Store) LoadWorkflows() (map[string][]byte, error) {
	result := make(map[string][]byte)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketWorkflows)
		return b.ForEach(func(k, v []byte) error {
			val := make([]byte, len(v))
			copy(val, v)
			result[string(k)] = val
			return nil
		})
	})
	return result, err
}

func (s *Store) SaveRun(run *workflow.WorkflowRun) error {
	data, err := json.Marshal(run)
	if err != nil {
		return fmt.Errorf("marshal run: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRuns)
		return b.Put([]byte(run.ID), data)
	})
}

func (s *Store) LoadRuns() (map[string]*workflow.WorkflowRun, error) {
	result := make(map[string]*workflow.WorkflowRun)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRuns)
		return b.ForEach(func(k, v []byte) error {
			var run workflow.WorkflowRun
			if err := json.Unmarshal(v, &run); err != nil {
				slog.Warn("failed to unmarshal run", "key", string(k), "error", err)
				return nil
			}
			result[string(k)] = &run
			return nil
		})
	})
	return result, err
}

func (s *Store) DeleteRun(runID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketRuns)
		return b.Delete([]byte(runID))
	})
}

func (s *Store) AppendEvent(evt events.Event) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketEventLog)
		key := []byte(fmt.Sprintf("%s:%s", evt.Timestamp.Format("20060102T150405.000000"), evt.ID))
		return b.Put(key, data)
	})
}

func (s *Store) LoadEvents(since time.Time) ([]events.Event, error) {
	var result []events.Event
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketEventLog)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var evt events.Event
			if err := json.Unmarshal(v, &evt); err != nil {
				continue
			}
			result = append(result, evt)
		}
		return nil
	})
	return result, err
}

func (s *Store) CompactEvents(retainCount int) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketEventLog)
		c := b.Cursor()
		total := 0
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			total++
		}
		if total <= retainCount {
			return nil
		}
		toDelete := total - retainCount
		i := 0
		for k, _ := c.First(); k != nil && i < toDelete; k, _ = c.Next() {
			if err := b.Delete(k); err != nil {
				return err
			}
			i++
		}
		slog.Info("compacted event log", "deleted", toDelete, "retained", retainCount)
		return nil
	})
}

func (s *Store) Close() error {
	slog.Info("closing bolt store", "path", s.path)
	return s.db.Close()
}
