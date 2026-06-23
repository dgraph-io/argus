//go:build darwin

package envinfo

import "golang.org/x/sys/unix"

// totalRAMBytes reads hw.memsize via sysctl.
func totalRAMBytes() uint64 {
	v, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0
	}
	return v
}
