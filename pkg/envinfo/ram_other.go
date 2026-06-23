//go:build !linux && !darwin

package envinfo

// totalRAMBytes is unavailable on this platform; report 0 (unknown).
func totalRAMBytes() uint64 { return 0 }
