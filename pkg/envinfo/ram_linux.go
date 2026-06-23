//go:build linux

package envinfo

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// totalRAMBytes reads MemTotal from /proc/meminfo (kB) and returns bytes.
func totalRAMBytes() uint64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line) // "MemTotal:  16384256 kB"
		if len(fields) >= 2 {
			if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				return kb * 1024
			}
		}
		break
	}
	return 0
}
