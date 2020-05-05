package main

import (
	"fmt"
	"github.com/prologic/bitcask"
	"github.com/tidwall/buntdb"
	"github.com/xujiajun/nutsdb"
	"go.etcd.io/bbolt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	valLength                = 200
	numberOfValuesForWriting = 10000
)

type Dictionary []string

func getDictionary() (Dictionary, error) {
	wordsBytes, err := ioutil.ReadFile("/usr/share/dict/words")
	if err != nil {
		return nil, fmt.Errorf("cannot read file /usr/share/dict/words: %w", err)
	}
	return strings.Split(strings.Trim(string(wordsBytes), "\n"), "\n"), nil
}

func (d Dictionary) generateKeysAndVals() ([][]byte, [][]byte) {
	n := len(d)
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	r := rand.New(rand.NewSource(100))

	// generate same slice every bench
	rands := make([]int, n)
	for i := 0; i < n; i++ {
		rands[i] = r.Intn(len(d))
	}
	// generate keys
	for i := 0; i < n; i++ {
		keys[i] = []byte(d[rands[i]])
		vals[i] = make([]byte, valLength)
		r.Read(vals[i])
	}
	return keys, vals
}

func generatePseudoRandomIndices() []int {
	res := make([]int, numberOfValuesForWriting)
	for i := 0; i < numberOfValuesForWriting; i++ {
		res[i] = rand.Intn(numberOfValuesForWriting)
	}
	return res
}

// bucket name: test
func BenchmarkBBolt(b *testing.B) {
	getDbWithTestBucket := func(name string, b *testing.B) *bbolt.DB {
		db, err := bbolt.Open(filepath.Join("data", name), os.ModePerm, nil)
		if err != nil {
			b.Fatal("Error opening db:", err)
		}
		// prepare bucket for tests
		if err = db.Update(func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucket([]byte("test"))
			return err
		}); err != nil {
			b.Fatal(err)
		}
		return db
	}
	closeDb := func(db *bbolt.DB, b *testing.B) {
		path := db.Path()
		if err := db.Close(); err != nil {
			b.Fatal("Error on closing db:", err)
		}
		if err := os.Remove(path); err != nil {
			b.Fatal("Error on removing db:", err)
		}
	}
	dict, err := getDictionary()
	if err != nil {
		b.Fatal("Cannot get dictionary:", err)
	}
	keys, vals := dict.generateKeysAndVals()

	// write
	b.Run("write", func(b *testing.B) {
		db := getDbWithTestBucket("bbolt-read.db", b)
		defer closeDb(db, b)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte("test"))
				key := keys[i%len(dict)]
				val := vals[i%len(dict)]
				return b.Put(key, val)
			}); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})

	// read
	b.Run("read", func(b *testing.B) {
		db := getDbWithTestBucket("bbolt-read.db", b)
		defer closeDb(db, b)
		// insert values for reading
		err := db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("test"))
			if err != nil {
				return err
			}
			for i := 0; i < numberOfValuesForWriting; i++ {
				err = b.Put(keys[i], vals[i])
				if err != nil {
					log.Println("ERROR KEY:", keys[i])
					return fmt.Errorf("error (i: %d, key:'%s', val:'%s'): %w", i, keys[i], vals[i], err)
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal("Error on preparing db:", err)
		}

		// generate indices for keys
		indices := generatePseudoRandomIndices()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := db.View(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte("test"))
				key := keys[indices[i%numberOfValuesForWriting]]
				_ = b.Get(key)
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})

	// read-write
	b.Run("write-read", func(b *testing.B) {
		db := getDbWithTestBucket("bbolt-write-read.db", b)
		defer closeDb(db, b)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte("test"))
				key := keys[i%len(dict)]
				val := vals[i%len(dict)]
				return b.Put(key, val)
			}); err != nil {
				b.Fatal(err)
			}
			if err := db.View(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte("test"))
				key := keys[i%len(dict)]
				b.Get(key)
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})
}

func BenchmarkBuntDbDisk(b *testing.B) {
	getDb := func(name string, b *testing.B) *buntdb.DB {
		db, err := buntdb.Open(filepath.Join("data", name))
		if err != nil {
			b.Fatal("Error opening db:", err)
		}
		err = db.SetConfig(buntdb.Config{
			SyncPolicy:           0,
			AutoShrinkPercentage: 0,
			AutoShrinkMinSize:    0,
			AutoShrinkDisabled:   false,
			OnExpired:            nil,
			OnExpiredSync:        nil,
		})
		if err != nil {
			b.Fatal("Error on set config db:", err)
		}
		return db
	}
	closeDb := func(db *buntdb.DB, name string, b *testing.B) {
		if err := db.Close(); err != nil {
			b.Fatal("Error on closing db:", err)
		}
		if err := os.Remove(filepath.Join("data", name)); err != nil {
			b.Fatal("Error on removing db:", err)
		}
	}
	dict, err := getDictionary()
	if err != nil {
		b.Fatal("Cannot get dictionary:", err)
	}
	keys, vals := dict.generateKeysAndVals()

	//write
	b.Run("write", func(b *testing.B) {
		db := getDb("buntdb-write.db", b)
		defer closeDb(db, "buntdb-write.db", b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := db.Update(func(tx *buntdb.Tx) error {
				key := string(keys[i%len(dict)])
				val := string(vals[i%len(dict)])
				_, _, err := tx.Set(key, val, nil)
				return err
			})
			if err != nil {
				b.Fatal("Error on writing:", err)
			}
		}

		b.StopTimer()
	})

	//read
	b.Run("read", func(b *testing.B) {
		db := getDb("buntdb-read.db", b)
		defer closeDb(db, "buntdb-read.db", b)
		// insert values for reading
		err := db.Update(func(tx *buntdb.Tx) error {
			for i := 0; i < 10000; i++ {
				_, _, err = tx.Set(string(keys[i]), string(vals[i]), nil)
				if err != nil {
					return fmt.Errorf("error on set: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal("Error on preparing db:", err)
		}

		// generate indices for keys
		indices := generatePseudoRandomIndices()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := db.View(func(tx *buntdb.Tx) error {
				key := string(keys[indices[i%numberOfValuesForWriting]])
				_, err := tx.Get(key)
				return err
			})
			if err != nil {
				b.Fatal("Error on reading:", err)
			}
		}

		b.StopTimer()
	})

	//write-read
	b.Run("write-read", func(b *testing.B) {
		db := getDb("buntdb-write-read.db", b)
		defer closeDb(db, "buntdb-write-read.db", b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := db.Update(func(tx *buntdb.Tx) error {
				key := string(keys[i%len(dict)])
				val := string(vals[i%len(dict)])
				_, _, err = tx.Set(key, val, nil)
				return err
			})
			if err != nil {
				b.Fatal("Error on writing:", err)
			}
			err = db.View(func(tx *buntdb.Tx) error {
				key := string(keys[i%len(dict)])
				_, err := tx.Get(key)
				return err
			})
			if err != nil {
				b.Fatal("Error on reading:", err)
			}
		}

		b.StopTimer()
	})
}

func BenchmarkBuntDbMemory(b *testing.B) {
	getDb := func(b *testing.B) *buntdb.DB {
		db, err := buntdb.Open(":memory:")
		if err != nil {
			b.Fatal("Error opening db:", err)
		}
		return db
	}
	closeDb := func(db *buntdb.DB, b *testing.B) {
		if err := db.Close(); err != nil {
			b.Fatal("Error on closing db:", err)
		}
	}
	dict, err := getDictionary()
	if err != nil {
		b.Fatal("Cannot get dictionary:", err)
	}
	keys, vals := dict.generateKeysAndVals()

	//write
	b.Run("write", func(b *testing.B) {
		db := getDb(b)
		defer closeDb(db, b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := db.Update(func(tx *buntdb.Tx) error {
				key := string(keys[i%len(dict)])
				val := string(vals[i%len(dict)])
				_, _, err := tx.Set(key, val, nil)
				return err
			})
			if err != nil {
				b.Fatal("Error on writing:", err)
			}
		}

		b.StopTimer()
	})

	//read
	b.Run("read", func(b *testing.B) {
		db := getDb(b)
		defer closeDb(db, b)
		// insert values for reading
		err := db.Update(func(tx *buntdb.Tx) error {
			for i := 0; i < 10000; i++ {
				_, _, err = tx.Set(string(keys[i]), string(vals[i]), nil)
				if err != nil {
					return fmt.Errorf("error on set: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal("Error on preparing db:", err)
		}

		// generate indices for keys
		indices := generatePseudoRandomIndices()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := db.View(func(tx *buntdb.Tx) error {
				key := string(keys[indices[i%numberOfValuesForWriting]])
				_, err := tx.Get(key)
				return err
			})
			if err != nil {
				b.Fatal("Error on reading:", err)
			}
		}

		b.StopTimer()
	})

	//write-read
	b.Run("write-read", func(b *testing.B) {
		db := getDb(b)
		defer closeDb(db, b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := db.Update(func(tx *buntdb.Tx) error {
				key := string(keys[i%len(dict)])
				val := string(vals[i%len(dict)])
				_, _, err = tx.Set(key, val, nil)
				return err
			})
			if err != nil {
				b.Fatal("Error on writing:", err)
			}
			err = db.View(func(tx *buntdb.Tx) error {
				key := string(keys[i%len(dict)])
				_, err := tx.Get(key)
				return err
			})
			if err != nil {
				b.Fatal("Error on reading:", err)
			}
		}

		b.StopTimer()
	})
}

func BenchmarkBitcaskSync(b *testing.B) {
	getDb := func(name string, b *testing.B) *bitcask.Bitcask {
		db, err := bitcask.Open(filepath.Join("data", name), bitcask.WithSync(true))
		if err != nil {
			b.Fatal("Error opening db:", err)
		}
		return db
	}
	closeDb := func(db *bitcask.Bitcask, name string, b *testing.B) {
		if err := db.Close(); err != nil {
			b.Fatal("Error on closing db:", err)
		}
		if err := os.RemoveAll(filepath.Join("data", name)); err != nil {
			b.Fatal("Error on removing db:", err)
		}
	}
	dict, err := getDictionary()
	if err != nil {
		b.Fatal("Cannot get dictionary:", err)
	}
	keys, vals := dict.generateKeysAndVals()

	//write
	b.Run("write", func(b *testing.B) {
		db := getDb("bitcask-write.db", b)
		defer closeDb(db, "bitcask-write.db", b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := keys[i%len(dict)]
			val := vals[i%len(dict)]
			err = db.Put(key, val)
			if err != nil {
				log.Fatal("Error on put:", err)
			}
		}

		b.StopTimer()
	})

	//read
	b.Run("read", func(b *testing.B) {
		db := getDb("bitcask-read.db", b)
		defer closeDb(db, "bitcask-read.db", b)
		// insert values for reading
		for i := 0; i < 10000; i++ {
			err = db.Put(keys[i], vals[i])
			if err != nil {
				log.Fatal("Error on put data before read:", err)
			}
		}

		// generate indices for keys
		indices := generatePseudoRandomIndices()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := keys[indices[i%numberOfValuesForWriting]]
			val, err := db.Get(key)
			if err != nil {
				b.Fatal("Error on reading:", err, val)
			}
		}

		b.StopTimer()
	})

	//write-read
	b.Run("write-read", func(b *testing.B) {
		db := getDb("bitcask-write-read.db", b)
		defer closeDb(db, "bitcask-write-read.db", b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := keys[i%len(dict)]
			val := vals[i%len(dict)]
			err = db.Put(key, val)
			if err != nil {
				b.Fatal("Error on writing:", err)
			}
			_, err := db.Get(key)
			if err != nil {
				b.Fatal("Error on reading:", err)
			}
		}

		b.StopTimer()
	})
}

func BenchmarkNutsDb(b *testing.B) {
	getDb := func(name string, b *testing.B) *nutsdb.DB {
		opts := nutsdb.DefaultOptions
		opts.Dir = filepath.Join("data", name)
		db, err := nutsdb.Open(opts)
		if err != nil {
			b.Fatal("Error opening db:", err)
		}
		return db
	}
	closeDb := func(db *nutsdb.DB, name string, b *testing.B) {
		if err := db.Close(); err != nil {
			b.Fatal("Error on closing db:", err)
		}
		if err := os.RemoveAll(filepath.Join("data", name)); err != nil {
			b.Fatal("Error on removing db:", err)
		}
	}
	dict, err := getDictionary()
	if err != nil {
		b.Fatal("Cannot get dictionary:", err)
	}
	keys, vals := dict.generateKeysAndVals()

	//write
	b.Run("write", func(b *testing.B) {
		db := getDb("nutsdb-write.db", b)
		defer closeDb(db, "nutsdb-write.db", b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err = db.Update(func(tx *nutsdb.Tx) error {
				key := keys[i%len(dict)]
				val := vals[i%len(dict)]
				err = tx.Put("b", key, val, 0)
				return err
			})
			if err != nil {
				log.Fatal("Error on put:", err)
			}
		}

		b.StopTimer()
	})

	//read
	b.Run("read", func(b *testing.B) {
		db := getDb("nutsdb-read.db", b)
		defer closeDb(db, "nutsdb-read.db", b)
		// insert values for reading
		for i := 0; i < 10000; i++ {
			err = db.Update(func(tx *nutsdb.Tx) error {
				key := keys[i]
				val := vals[i]
				err = tx.Put("b", key, val, 0)
				return err
			})
			if err != nil {
				log.Fatal("Error on put before reading:", err)
			}
		}

		// generate indices for keys
		indices := generatePseudoRandomIndices()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err = db.View(func(tx *nutsdb.Tx) error {
				key := keys[indices[i%numberOfValuesForWriting]]
				_, err = tx.Get("b", key)
				return err
			})
			if err != nil {
				log.Fatal("Error on read:", err)
			}
		}

		b.StopTimer()
	})

	//write-read
	b.Run("write-read", func(b *testing.B) {
		db := getDb("nutsdb-write-read.db", b)
		defer closeDb(db, "nutsdb-write-read.db", b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := keys[i%len(dict)]
			val := vals[i%len(dict)]
			err = db.Update(func(tx *nutsdb.Tx) error {
				err = tx.Put("b", key, val, 0)
				return err
			})
			if err != nil {
				log.Fatal("Error on put:", err)
			}
			err = db.View(func(tx *nutsdb.Tx) error {
				_, err = tx.Get("b", key)
				return err
			})
			if err != nil {
				log.Fatal("Error on get:", err)
			}
		}

		b.StopTimer()
	})
}
