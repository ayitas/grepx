# GrepX

High-performance CLI tool for searching and filtering large log files (50GB+) with JSON parsing and dynamic table output.

## Features

- Zero external dependencies (Go standard library only)
- Concurrent processing with worker pool
- Memory efficient streaming (does not load full file into RAM)
- Dynamic table output based on filter keywords
- Recursive JSON search across all nested levels
- Graceful shutdown handling

## Installation

```bash
git clone <repository-url>
cd grepx

# Standard build
go build -o grepx

# Optimized build (smaller binary)
go build -ldflags="-s -w" -o grepx
