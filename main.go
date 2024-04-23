package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	bolt "go.etcd.io/bbolt"

	"github.com/ahrtr/etcd-data-rebuilder/internel/common"
)

func main() {
	output := flag.String("output", "new_db", "The new db file path")
	flag.Parse()

	if flag.NArg() == 0 {
		log.Fatal("Must provide the original db file path")
	}
	if flag.NArg() > 3 {
		log.Fatalf("Too many arguments: %d", flag.NArg())
	}

	dbPath := flag.Args()[0]
	pageSize, err := getPageSize(dbPath)
	if err != nil {
		log.Fatalf("Failed to get page size from %q: %v", dbPath, err)
	}
	log.Printf("pageSize: %d\n", pageSize)

	if err := rebuild(dbPath, *output, int64(pageSize)); err != nil {
		log.Fatalf("Failed to rebuild data: %v", err)
	}

	log.Printf("Rebuilding data file %q successfully\n", *output)
}

func rebuild(srcdb, dstdb string, pageSize int64) error {
	// open the target bbolt db
	db, err := bolt.Open(dstdb, 0600, nil)
	if err != nil {
		return fmt.Errorf("failed to open bbolt db: %w", err)
	}
	defer db.Close()

	// open the source bbolt db file
	f, err := os.OpenFile(srcdb, os.O_RDONLY, 0400)
	if err != nil {
		return fmt.Errorf("failed to open the source db file %q: %w", srcdb, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to get source db file's FileInfo: %w", err)
	}
	fileSize := fi.Size()
	if fileSize%pageSize != 0 {
		log.Printf("The source db file size %d can't be divided by the pageSize %d\n", fileSize, pageSize)
	}

	// scan the source db file, and rebuild the target bbolt db
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("key"))
		if err != nil {
			return err
		}
		var pageID int64 = 2
		for {
			offset := pageID * pageSize
			if offset+pageSize >= fileSize {
				break
			}

			// Note it also verifies the pageID and overflow fields when reading page data.
			p, _, err := ReadPage(f, pageSize, pageID, fileSize)
			if err != nil {
				log.Printf("Read page [ID: %d] failed: %v\n", pageID, err)
				pageID++ // We don't know the overflow, so increment by 1.
				continue
			}

			// Verify the page type field.
			if !p.IsValidPageType() {
				log.Printf("Page [ID: %d] has unexpected page type [%x]\n", pageID, p.Flags())
				pageID += int64(p.Overflow() + 1) // Can we trust the overflow in this case?
				continue
			}

			// We only read leaf pages, because data is only included in leaf pages.
			if !p.IsLeafPage() {
				pageID += int64(p.Overflow() + 1)
				continue
			}

			for i := uint16(0); i < p.Count(); i++ {
				e := p.LeafPageElement(i)

				if e.IsBucketEntry() {
					// TODO: handle inline bucket case
				} else {
					k, v := e.Key(), e.Value()

					if isRevision(k) {
						if err := b.Put(k, v); err != nil {
							return err
						}
					}
				}
			}

			pageID += int64(p.Overflow() + 1)
		}

		return nil
	})
}

// ReadPage reads page data of the given page ID from the data file.
// Note it also verifies that the page ID and overflow fields.
func ReadPage(f *os.File, pageSize, pageID, fileSize int64) (*common.Page, []byte, error) {
	// Read one block into buffer.
	buf := make([]byte, pageSize)
	if n, err := f.ReadAt(buf, pageID*pageSize); err != nil {
		return nil, nil, err
	} else if n != len(buf) {
		return nil, nil, io.ErrUnexpectedEOF
	}

	p := common.LoadPage(buf)
	if p.Id() != common.Pgid(pageID) {
		return nil, nil, fmt.Errorf("unexpected Page ID: %d, want: %d", p.Id(), pageID)
	}

	// Read overflow if present
	overflowN := p.Overflow()
	if overflowN == 0 {
		return p, buf, nil
	}
	if (pageID+int64(overflowN)+1)*pageSize > fileSize {
		return nil, nil, fmt.Errorf("page [ID: %d, overflow: %d] exceeds the file size: %d", pageID, overflowN, fileSize)
	}

	// Re-read entire Page (with overflow) into buffer.
	buf = make([]byte, int64(1+overflowN)*pageSize)
	if n, err := f.ReadAt(buf, pageID*pageSize); err != nil {
		return nil, nil, err
	} else if n != len(buf) {
		return nil, nil, io.ErrUnexpectedEOF
	}
	p = common.LoadPage(buf)

	return p, buf, nil
}

func isRevision(key []byte) bool {
	klen := len(key)
	if klen != 17 && klen != 18 {
		return false
	}

	if key[8] != '_' {
		return false
	}

	if klen == 18 && key[17] != 't' {
		return false
	}

	return true
}
