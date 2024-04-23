package common

import (
	"hash/fnv"
	"unsafe"

	"github.com/ahrtr/etcd-data-rebuilder/errors"
)

type Meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     InBucket
	freelist Pgid
	pgid     Pgid
	txid     Txid
	checksum uint64
}

// Validate checks the marker bytes and version of the meta page to ensure it matches this binary.
func (m *Meta) Validate() error {
	if m.magic != Magic {
		return errors.ErrInvalid
	} else if m.version != Version {
		return errors.ErrVersionMismatch
	} else if m.checksum != m.Sum64() {
		return errors.ErrChecksum
	}
	return nil
}

// Sum64 generates the checksum for the meta.
func (m *Meta) Sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(Meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

func (m *Meta) PageSize() uint32 {
	return m.pageSize
}
