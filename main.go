package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Result holds extracted data from matching lines
type Result struct {
	LineNumber  int
	UUID        string
	RequestUUID string
	Fields      map[string]string // Dynamic fields based on filters
}

// Filter represents a key-value filter condition
type Filter struct {
	Key   string
	Value string
}

// WorkItem represents a line to be processed
type WorkItem struct {
	LineNumber int
	Line       string
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <file_path> <key1=val1> [key2=val2] ...\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s app.log ageOfLocation=0 subscriberState=4\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]
	filters, err := parseFilters(os.Args[2:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing filters: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Fprintf(os.Stderr, "\n[!] Interrupt received, shutting down gracefully...\n")
		cancel()
	}()

	startTime := time.Now()
	results, err := searchLogFile(ctx, filePath, filters)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	duration := time.Since(startTime)

	displayResults(results, filters, duration)
}

func parseFilters(args []string) ([]Filter, error) {
	filters := make([]Filter, 0, len(args))
	for _, arg := range args {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid filter format: %s (expected key=value)", arg)
		}
		filters = append(filters, Filter{Key: parts[0], Value: parts[1]})
	}
	return filters, nil
}

func searchLogFile(ctx context.Context, filePath string, filters []Filter) ([]Result, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	numWorkers := runtime.NumCPU()
	workChan := make(chan WorkItem, numWorkers*2)
	resultChan := make(chan Result, numWorkers*2)

	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, &wg, workChan, resultChan, filters)
	}

	var collectorWg sync.WaitGroup
	results := make([]Result, 0)
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		for result := range resultChan {
			results = append(results, result)
		}
	}()

	scanner := bufio.NewScanner(file)
	maxCapacity := 10 * 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	lineNumber := 0
	scannedLines := 0
	matchedLines := 0

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			close(workChan)
			wg.Wait()
			close(resultChan)
			collectorWg.Wait()
			return results, fmt.Errorf("operation cancelled")
		default:
		}

		lineNumber++
		scannedLines++
		line := scanner.Text()

		if !fastFilter(line, filters) {
			continue
		}

		matchedLines++

		select {
		case workChan <- WorkItem{LineNumber: lineNumber, Line: line}:
		case <-ctx.Done():
			close(workChan)
			wg.Wait()
			close(resultChan)
			collectorWg.Wait()
			return results, fmt.Errorf("operation cancelled")
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	close(workChan)
	wg.Wait()
	close(resultChan)
	collectorWg.Wait()

	fmt.Fprintf(os.Stderr, "[*] Scanned: %d lines | Fast-matched: %d lines | Results: %d\n",
		scannedLines, matchedLines, len(results))

	return results, nil
}

func fastFilter(line string, filters []Filter) bool {
	if !strings.Contains(line, "{") {
		return false
	}
	for _, filter := range filters {
		if !strings.Contains(line, `"`+filter.Key+`"`) {
			return false
		}
	}
	return true
}

func worker(ctx context.Context, wg *sync.WaitGroup, workChan <-chan WorkItem, resultChan chan<- Result, filters []Filter) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-workChan:
			if !ok {
				return
			}

			jsonStart := strings.Index(item.Line, "{")
			if jsonStart == -1 {
				continue
			}

			var data map[string]interface{}
			if err := json.Unmarshal([]byte(item.Line[jsonStart:]), &data); err != nil {
				continue
			}

			if !matchFiltersRecursive(data, filters) {
				continue
			}

			// Extract UUID and requestUuid separately
			uuid := findInJSON(data, "uuid")
			requestUuid := findInJSON(data, "requestUuid")

			// Extract all filter keys
			fields := make(map[string]string)
			for _, filter := range filters {
				fields[filter.Key] = findInJSON(data, filter.Key)
			}

			result := Result{
				LineNumber:  item.LineNumber,
				UUID:        uuid,
				RequestUUID: requestUuid,
				Fields:      fields,
			}

			select {
			case resultChan <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func matchFiltersRecursive(data map[string]interface{}, filters []Filter) bool {
	for _, filter := range filters {
		if !findKeyValueInJSON(data, filter.Key, filter.Value) {
			return false
		}
	}
	return true
}

func findKeyValueInJSON(data interface{}, key, value string) bool {
	switch v := data.(type) {
	case map[string]interface{}:
		if val, ok := v[key]; ok {
			if fmt.Sprintf("%v", val) == value {
				return true
			}
		}
		for _, val := range v {
			if findKeyValueInJSON(val, key, value) {
				return true
			}
		}
	case []interface{}:
		for _, item := range v {
			if findKeyValueInJSON(item, key, value) {
				return true
			}
		}
	}
	return false
}

func findInJSON(data interface{}, keys ...string) string {
	for _, key := range keys {
		if val := findKeyInJSON(data, key); val != "" {
			return val
		}
	}
	return "-"
}

func findKeyInJSON(data interface{}, key string) string {
	switch v := data.(type) {
	case map[string]interface{}:
		if val, ok := v[key]; ok {
			return fmt.Sprintf("%v", val)
		}
		for _, val := range v {
			if result := findKeyInJSON(val, key); result != "" {
				return result
			}
		}
	case []interface{}:
		for _, item := range v {
			if result := findKeyInJSON(item, key); result != "" {
				return result
			}
		}
	}
	return ""
}

func displayResults(results []Result, filters []Filter, duration time.Duration) {
	if len(results) == 0 {
		fmt.Println("\nNo matching records found.")
		return
	}

	// Calculate column widths
	colWidths := map[string]int{
		"LINE":         5,
		"UUID":         36,
		"REQUEST_UUID": 36,
	}

	// Add filter keys as columns
	for _, filter := range filters {
		colWidths[filter.Key] = len(filter.Key)
		if colWidths[filter.Key] < 10 {
			colWidths[filter.Key] = 10
		}
	}

	// Calculate max widths from data
	for _, r := range results {
		if len(r.UUID) > colWidths["UUID"] {
			colWidths["UUID"] = len(r.UUID)
		}
		if len(r.RequestUUID) > colWidths["REQUEST_UUID"] {
			colWidths["REQUEST_UUID"] = len(r.RequestUUID)
		}
		for key, val := range r.Fields {
			if len(val) > colWidths[key] {
				colWidths[key] = len(val)
			}
		}
	}

	// Calculate total width
	totalWidth := colWidths["LINE"] + colWidths["UUID"] + colWidths["REQUEST_UUID"] + 10
	for _, filter := range filters {
		totalWidth += colWidths[filter.Key] + 3
	}

	// Print header
	fmt.Println()
	fmt.Println(strings.Repeat("=", totalWidth))

	// Print column names
	fmt.Printf("| %-*s | %-*s | %-*s",
		colWidths["LINE"], "LINE",
		colWidths["UUID"], "UUID",
		colWidths["REQUEST_UUID"], "REQUEST_UUID")
	for _, filter := range filters {
		fmt.Printf(" | %-*s", colWidths[filter.Key], strings.ToUpper(filter.Key))
	}
	fmt.Println(" |")
	fmt.Println(strings.Repeat("=", totalWidth))

	// Print rows
	for _, r := range results {
		fmt.Printf("| %-*d | %-*s | %-*s",
			colWidths["LINE"], r.LineNumber,
			colWidths["UUID"], truncate(r.UUID, colWidths["UUID"]),
			colWidths["REQUEST_UUID"], truncate(r.RequestUUID, colWidths["REQUEST_UUID"]))
		for _, filter := range filters {
			val := r.Fields[filter.Key]
			fmt.Printf(" | %-*s", colWidths[filter.Key], truncate(val, colWidths[filter.Key]))
		}
		fmt.Println(" |")
	}

	// Print footer
	fmt.Println(strings.Repeat("=", totalWidth))
	fmt.Printf("\nTotal Results: %d | Processing Time: %v\n", len(results), duration.Round(time.Millisecond))
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}
