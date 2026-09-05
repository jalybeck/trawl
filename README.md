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

## Downloads

Latest automatically built binaries. The table updates whenever a new git tag triggers a
build and publishes release packages; older versions are not kept.

<!-- BUILD_TABLE_START -->
| Package | Platform | Built |
| --- | --- | --- |
| [trawl_win_x64.zip](https://github.com/jalybeck/trawl/releases/latest/download/trawl_win_x64.zip) | Windows x64 | 2026-09-05 19:26 UTC |
| [trawl_linux_x64.tar.gz](https://github.com/jalybeck/trawl/releases/latest/download/trawl_linux_x64.tar.gz) | Linux x64 | 2026-09-05 19:26 UTC |
<!-- BUILD_TABLE_END -->
