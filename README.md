# Trawl

Trawl is a fast, multithreaded command-line tool for searching files, directories, and file contents.

The goal of the project is simple: start from the users current directory, traverse the directory tree using a pool of worker threads, and stream matches to the terminal as they are discovered.

```bash
trawl "search term"
```

## Filtering by file size

Use `-s` (or `--size`) to filter both file name and content matches by file size.
The same filter works with `-nc` (names only):

```bash
trawl "search term" -s "<5kb"
trawl "search term" -s "> 2.5MB" -nc
trawl "search term" -s ">=1KB" -s "<=10MiB"
```

Supported comparisons are `<`, `<=`, `>`, `>=`, and `=`. A size without a
comparison means equality. Decimal values and spaces inside the quoted filter
are supported, and units are case-insensitive. No unit means bytes; `B`, `KB`,
`MB`, `GB`, and `TB` use powers of 1000, while `KiB`, `MiB`, `GiB`, and `TiB` use
powers of 1024. Repeated filters must all match.

Quote filters containing `<` or `>` so your shell passes them to Trawl.
With a size filter, directories are still traversed but only matching files
are reported. The filter also applies to a file supplied with `-p`.

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

Stable.

## Downloads

Latest automatically built binaries. The table updates whenever a new git tag triggers a
build and publishes release packages; older versions are not kept.

<!-- BUILD_TABLE_START -->
| Package | Platform | Built |
| --- | --- | --- |
| [trawl_win_x64.zip](https://github.com/jalybeck/trawl/releases/latest/download/trawl_win_x64.zip) | Windows x64 | 2026-09-19 07:38 UTC |
| [trawl_linux_x64.tar.gz](https://github.com/jalybeck/trawl/releases/latest/download/trawl_linux_x64.tar.gz) | Linux x64 | 2026-09-19 07:38 UTC |
<!-- BUILD_TABLE_END -->
