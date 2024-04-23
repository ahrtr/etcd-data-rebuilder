package common

import "os"

// Version represents the data file format version.
const Version uint32 = 2

// Magic represents a marker value to indicate that a file is a Bolt DB.
const Magic uint32 = 0xED0CDAED

const pageMaxAllocSize = 0xFFFFFFF

// Txid represents the internal transaction identifier.
type Txid uint64

// DefaultPageSize is the default page size for db which is set to the OS page size.
var DefaultPageSize = os.Getpagesize()
