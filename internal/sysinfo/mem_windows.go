//go:build windows

package sysinfo

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

func totalMemoryBytes() (uint64, bool) {
	var st windows.MemStatusEx
	st.Length = uint32(unsafe.Sizeof(st))
	if err := windows.GlobalMemoryStatusEx(&st); err != nil {
		return 0, false
	}
	if st.TotalPhys == 0 {
		return 0, false
	}
	return st.TotalPhys, true
}

func availableMemoryBytes() (uint64, bool) {
	var st windows.MemStatusEx
	st.Length = uint32(unsafe.Sizeof(st))
	if err := windows.GlobalMemoryStatusEx(&st); err != nil {
		return 0, false
	}
	return st.AvailPhys, true
}
