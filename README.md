# Trawl

Trawl is a fast, multithreaded command-line tool for searching files, directories, and file contents.

The goal of the project is simple: start from the users current directory, traverse the directory tree using a pool of worker threads, and stream matches to the terminal as they are discovered.

```bash
trawl "search term"
```

## Goals

- Search directory names, file names, and file contents
- Traverse the filesystem concurrently using a worker thread pool
- Stream results to the terminal in real time
- Highlight matching text and show compact context around matches
- Skip binary files efficiently
- Keep memory usage low while searching large directory trees
- Make effective use of available CPU and I/O resources

## Implementation

Trawl is written in Rust.

The initial implementation favors simplicity and uses buffered file I/O and a shared work queue. Performance optimizations such as memory-mapped files and more advanced search algorithms may be explored later based on benchmarks.

## Status

Early development.