package main

import (
	"github.com/xujiajun/nutsdb"
	"log"
	"path/filepath"
)

func main() {
	opts := nutsdb.DefaultOptions
	opts.Dir = filepath.Join("data", "nutsdb")
	db, err := nutsdb.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Update(func(tx *nutsdb.Tx) error {
		err = tx.Put("b", []byte("key"), []byte("val"), 0)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	/*db, err := buntdb.Open("data/buntdb-write.db")
	if err != nil {
		log.Fatal(err)
	}
	db.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get("dynamic")
		log.Println(val)
		return err
	})*/
	/*db, err := bbolt.Open("data/bbolt.db", os.ModePerm, nil)
	if err != nil {
		log.Fatal("Error on opening:", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		if err != nil {
			return err
		}
		val := b.Get([]byte("test"))
		log.Println(string(val))
		err = b.Put([]byte("test"), []byte("val"))
		if err != nil {
			return err
		}
		val = b.Get([]byte("test"))
		log.Println(string(val))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}*/
}
