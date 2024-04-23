package common

import "unsafe"

func LoadPage(buf []byte) *Page {
	return (*Page)(unsafe.Pointer(&buf[0]))
}
