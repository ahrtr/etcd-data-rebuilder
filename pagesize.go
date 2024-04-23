package main

import (
	"fmt"
	"io"
	"os"

	berrors "github.com/ahrtr/etcd-data-rebuilder/errors"
	"github.com/ahrtr/etcd-data-rebuilder/internel/common"
)

func getPageSize(db string) (int, error) {
	f, err := os.OpenFile(db, os.O_RDONLY, 0400)
	if err != nil {
		return 0, fmt.Errorf("failed to open the db file %q: %w", db, err)
	}
	defer f.Close()

	var (
		meta0CanRead, meta1CanRead bool
	)

	// Read the first meta page to determine the page size.
	if pgSize, canRead, err := getPageSizeFromFirstMeta(f); err != nil {
		// We cannot read the page size from page 0, but can read page 0.
		meta0CanRead = canRead
	} else {
		return pgSize, nil
	}

	// Read the second meta page to determine the page size.
	if pgSize, canRead, err := getPageSizeFromSecondMeta(f); err != nil {
		// We cannot read the page size from page 1, but can read page 1.
		meta1CanRead = canRead
	} else {
		return pgSize, nil
	}

	// If we can't read the page size from both pages, but can read
	// either page, then we assume it's the same as the OS or the one
	// given, since that's how the page size was chosen in the first place.
	//
	// If both pages are invalid, and (this OS uses a different page size
	// from what the database was created with or the given page size is
	// different from what the database was created with), then we are out
	// of luck and cannot access the database.
	if meta0CanRead || meta1CanRead {
		return common.DefaultPageSize, nil
	}

	return 0, berrors.ErrInvalid
}

func getPageSizeFromFirstMeta(f *os.File) (int, bool, error) {
	var buf [0x1000]byte
	var metaCanRead bool
	if bw, err := f.ReadAt(buf[:], 0); err == nil && bw == len(buf) {
		metaCanRead = true
		if m := common.LoadPage(buf[:]).Meta(); m.Validate() == nil {
			return int(m.PageSize()), metaCanRead, nil
		}
	}
	return 0, metaCanRead, berrors.ErrInvalid
}

// getPageSizeFromSecondMeta reads the pageSize from the second meta page
func getPageSizeFromSecondMeta(f *os.File) (int, bool, error) {
	var (
		fileSize    int64
		metaCanRead bool
	)

	// get the db file size
	if info, err := f.Stat(); err != nil {
		return 0, metaCanRead, err
	} else {
		fileSize = info.Size()
	}

	// We need to read the second meta page, so we should skip the first page;
	// but we don't know the exact page size yet, it's chicken & egg problem.
	// The solution is to try all the possible page sizes, which starts from 1KB
	// and until 16MB (1024<<14) or the end of the db file
	for i := 0; i <= 14; i++ {
		var buf [0x1000]byte
		var pos int64 = 1024 << uint(i)
		if pos >= fileSize-1024 {
			break
		}
		bw, err := f.ReadAt(buf[:], pos)
		if (err == nil && bw == len(buf)) || (err == io.EOF && int64(bw) == (fileSize-pos)) {
			metaCanRead = true
			if m := common.LoadPage(buf[:]).Meta(); m.Validate() == nil {
				return int(m.PageSize()), metaCanRead, nil
			}
		}
	}

	return 0, metaCanRead, berrors.ErrInvalid
}
