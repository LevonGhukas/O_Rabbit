package sysinfo

// FileDescriptors returns the current number of open file descriptors and the soft limit.
// ok=false if the information is unavailable.
func FileDescriptors() (used, limit uint64, ok bool) {
	return fileDescriptors()
}
