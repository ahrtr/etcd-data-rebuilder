package common

import "unsafe"

type Pgid uint64

const LeafPageElementSize = unsafe.Sizeof(leafPageElement{})

const (
	BranchPageFlag   = 0x01
	LeafPageFlag     = 0x02
	MetaPageFlag     = 0x04
	FreelistPageFlag = 0x10
)

const (
	BucketLeafFlag = 0x01
)

type Page struct {
	id       Pgid
	flags    uint16
	count    uint16
	overflow uint32
}

// Meta returns a pointer to the metadata section of the page.
func (p *Page) Meta() *Meta {
	return (*Meta)(UnsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))
}

func (p *Page) IsBranchPage() bool {
	return p.flags == BranchPageFlag
}

func (p *Page) IsLeafPage() bool {
	return p.flags == LeafPageFlag
}

func (p *Page) IsMetaPage() bool {
	return p.flags == MetaPageFlag
}

func (p *Page) IsFreelistPage() bool {
	return p.flags == FreelistPageFlag
}

func (p *Page) IsValidPageType() bool {
	return p.IsBranchPage() || p.IsLeafPage() || p.IsMetaPage() || p.IsFreelistPage()
}

func (p *Page) Id() Pgid {
	return p.id
}

func (p *Page) Flags() uint16 {
	return p.flags
}

func (p *Page) Count() uint16 {
	return p.count
}

func (p *Page) Overflow() uint32 {
	return p.overflow
}

// leafPageElement represents a node on a leaf page.
type leafPageElement struct {
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

// LeafPageElement retrieves the leaf node by index
func (p *Page) LeafPageElement(index uint16) *leafPageElement {
	return (*leafPageElement)(UnsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		LeafPageElementSize, int(index)))
}

// Key returns a byte slice of the node key.
func (n *leafPageElement) Key() []byte {
	i := int(n.pos)
	j := i + int(n.ksize)
	return UnsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

// Value returns a byte slice of the node value.
func (n *leafPageElement) Value() []byte {
	i := int(n.pos) + int(n.ksize)
	j := i + int(n.vsize)
	return UnsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

func (n *leafPageElement) IsBucketEntry() bool {
	return n.flags&uint32(BucketLeafFlag) != 0
}
