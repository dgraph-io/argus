package output

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"

	"github.com/dgraph-io/argus/pkg/metrics"
)

// CSVWriter writes metrics to a CSV file.
type CSVWriter struct {
	file   *os.File
	writer *csv.Writer
	mu     sync.Mutex
	closed bool
}

// NewCSVWriter creates a new CSV metrics writer.
func NewCSVWriter(path string) (*CSVWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %w", err)
	}

	w := csv.NewWriter(f)
	cw := &CSVWriter{
		file:   f,
		writer: w,
	}

	// Write header
	header := []string{
		"timestamp",
		"operation_type",
		"duration_ms",
		"success",
		"error",
		"item_count",
		"memory_alloc_mb",
		"heap_alloc_mb",
		"gc_pauses_us",
		"gc_count",
	}
	if err := w.Write(header); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to write CSV header: %w", err)
	}
	w.Flush()

	return cw, nil
}

// WriteRecord writes a single metrics record to CSV.
func (w *CSVWriter) WriteRecord(rec metrics.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("CSV writer is closed")
	}

	errStr := ""
	if rec.Error != "" {
		errStr = rec.Error
	}

	row := []string{
		rec.Timestamp.Format("2006-01-02T15:04:05.000"),
		rec.OperationType,
		fmt.Sprintf("%.2f", float64(rec.Duration.Milliseconds())),
		fmt.Sprintf("%v", rec.Success),
		errStr,
		fmt.Sprintf("%d", rec.ItemCount),
		fmt.Sprintf("%.2f", float64(rec.MemoryAlloc)/1024/1024),
		fmt.Sprintf("%.2f", float64(rec.MemoryHeapAlloc)/1024/1024),
		fmt.Sprintf("%d", rec.GCPauses),
		fmt.Sprintf("%d", rec.GCCount),
	}

	if err := w.writer.Write(row); err != nil {
		return fmt.Errorf("failed to write CSV row: %w", err)
	}
	w.writer.Flush()

	return w.writer.Error()
}

// WriteRecords writes multiple records to CSV.
func (w *CSVWriter) WriteRecords(records []metrics.Record) error {
	for _, rec := range records {
		if err := w.WriteRecord(rec); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the CSV file.
func (w *CSVWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.writer.Flush()
	w.closed = true
	return w.file.Close()
}
