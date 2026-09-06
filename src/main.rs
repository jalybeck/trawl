use colored::{Color, Colorize};
use indicatif::ProgressBar;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::Duration;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Worker {
    _id: usize,
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(_id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let handle = thread::spawn(move || {
            loop {
                // lock is released immediately after the message is received (recv returns the value)
                let message = receiver.lock().unwrap().recv();

                match message {
                    Ok(job) => job(),
                    Err(_) => break, // channel closed -> pool is being destroyed, exit the loop
                }
            }
        });

        Worker {
            _id,
            handle: Some(handle),
        }
    }
}

struct Threadpool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
    pending: Arc<(Mutex<usize>, Condvar)>,
}

impl Threadpool {
    fn new() -> Threadpool {
        let num_threads = thread::available_parallelism().unwrap().get();
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(num_threads);
        for id in 0..num_threads {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        Threadpool {
            workers,
            sender: Some(sender),
            pending: Arc::new((Mutex::new(0), Condvar::new())),
        }
    }

    fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // "Add(1)" -- increment the pending count before the job is sent to the queue (avoids race condition)
        *self.pending.0.lock().unwrap() += 1;

        let pending = Arc::clone(&self.pending);
        let job: Job = Box::new(move || {
            // Execute the job and catch any panics to prevent the thread from crashing
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                eprintln!("trawl: worker task panicked: {:?}", payload);
            }

            let (lock, cvar) = pending.as_ref();
            let mut count = lock.lock().unwrap();
            *count -= 1; // "Done()"
            if *count == 0 {
                cvar.notify_all();
            }
        });

        self.sender.as_ref().unwrap().send(job).unwrap();
    }

    /// Block until the queue and all its spawned subtasks have been processed.
    fn wait(&self) {
        let (lock, cvar) = &*self.pending;
        let mut count = lock.lock().unwrap();
        while *count != 0 {
            count = cvar.wait(count).unwrap();
        }
    }
}

impl Drop for Threadpool {
    fn drop(&mut self) {
        drop(self.sender.take()); // close the channel -> workers exit the loop

        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                handle.join().unwrap();
            }
        }
    }
}

const CONTEXT: usize = 20; // how many characters of context to show around a match

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Encoding {
    Utf8Bom,
    Utf16Le,
    Utf16Be,
    Utf32Le,
    Utf32Be,
    None,
}

impl Encoding {
    fn bom_len(self) -> usize {
        match self {
            Encoding::Utf8Bom => 3,
            Encoding::Utf16Le | Encoding::Utf16Be => 2,
            Encoding::Utf32Le | Encoding::Utf32Be => 4,
            Encoding::None => 0,
        }
    }
}

// UTF-32 LE's BOM (FF FE 00 00) starts with the same two bytes as UTF-16 LE's (FF FE),
// so it must be checked first or it would always be misdetected as UTF-16 LE.
fn detect_encoding(bytes: &[u8]) -> Encoding {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        Encoding::Utf8Bom
    } else if bytes.starts_with(&[0x00, 0x00, 0xFE, 0xFF]) {
        Encoding::Utf32Be
    } else if bytes.starts_with(&[0xFF, 0xFE, 0x00, 0x00]) {
        Encoding::Utf32Le
    } else if bytes.starts_with(&[0xFF, 0xFE]) {
        Encoding::Utf16Le
    } else if bytes.starts_with(&[0xFE, 0xFF]) {
        Encoding::Utf16Be
    } else {
        Encoding::None
    }
}

fn decode_utf16(bytes: &[u8], big_endian: bool) -> String {
    let units = bytes.chunks_exact(2).map(|c| {
        let arr = [c[0], c[1]];
        if big_endian {
            u16::from_be_bytes(arr)
        } else {
            u16::from_le_bytes(arr)
        }
    });
    char::decode_utf16(units)
        .map(|r| r.unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect()
}

fn decode_utf32(bytes: &[u8], big_endian: bool) -> String {
    bytes
        .chunks_exact(4)
        .map(|c| {
            let arr = [c[0], c[1], c[2], c[3]];
            let code = if big_endian {
                u32::from_be_bytes(arr)
            } else {
                u32::from_le_bytes(arr)
            };
            char::from_u32(code).unwrap_or(char::REPLACEMENT_CHARACTER)
        })
        .collect()
}

fn is_hidden(entry: &std::fs::DirEntry) -> bool {
    let name = entry.file_name();
    if name.to_string_lossy().starts_with('.') {
        return true;
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
        if let Ok(metadata) = entry.metadata() {
            if metadata.file_attributes() & FILE_ATTRIBUTE_HIDDEN != 0 {
                return true;
            }
        }
    }

    false
}

const EXCLUDED_DIRS: &[&str] = &[
    "target",       // Rust
    "node_modules", // Node.js / JS / TS
    "dist",         // JS/TS build output (webpack, vite, jne.)
    "build",        // Common (C/C++, Gradle, jne.)
    "out",          // Common build output
    "bin",          // .NET / C, kääntötulokset
    "obj",          // .NET
    "vendor",       // Go / PHP / Ruby dependencies
    "__pycache__",  // Python bytecode cache
    "venv",         // Python virtual environment
    "env",          // Python virtual environment (common alternative)
];

// Uses entry.metadata() (not file_type()): on WSL/DrvFs the readdir d_type fast path
// file_type() relies on can misreport Windows junctions/reparse points as directories,
// while metadata() always performs a real lstat-equivalent call and reports them correctly.
// On Windows, std's is_symlink() only recognizes the SYMLINK reparse tag, not MOUNT_POINT
// (junctions, e.g. the legacy `Application Data` -> `AppData\Roaming` alias) - so check the
// raw FILE_ATTRIBUTE_REPARSE_POINT bit directly to catch every reparse point, not just symlinks.
fn is_symlink(entry: &std::fs::DirEntry) -> bool {
    let Ok(metadata) = entry.metadata() else {
        return false;
    };

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        return metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0;
    }

    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

fn is_excluded_dir(entry: &std::fs::DirEntry) -> bool {
    let Ok(file_type) = entry.file_type() else {
        return false;
    };
    if !file_type.is_dir() {
        return false;
    }

    let name = entry.file_name();
    EXCLUDED_DIRS.contains(&name.to_string_lossy().as_ref())
}

fn is_cloud_placeholder(entry: &std::fs::DirEntry) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS: u32 = 0x00400000; // Onedrive: remote content not locally available
        const FILE_ATTRIBUTE_RECALL_ON_OPEN: u32 = 0x00040000; // Onedrive: remote content will be available on open
        const FILE_ATTRIBUTE_OFFLINE: u32 = 0x00001000; // HSM (Hierarchical Storage Management) / offline content

        if let Ok(metadata) = entry.metadata() {
            let attrs = metadata.file_attributes();
            return attrs
                & (FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
                    | FILE_ATTRIBUTE_RECALL_ON_OPEN
                    | FILE_ATTRIBUTE_OFFLINE)
                != 0;
        }
    }

    #[cfg(not(windows))]
    {
        let _ = entry; // muilla alustoilla tätä ongelmaa ei ole
    }

    false
}

// Byte-level ASCII case folding is safe on UTF-8: multi-byte sequence bytes always have the
// high bit set (>= 0x80), so folding only ever touches standalone ASCII bytes and can never
// shift a match off a char boundary. Non-ASCII letters (e.g. "é") are still compared exactly.
fn find_pattern(haystack: &str, pattern: &str, case_sensitive: bool) -> Option<usize> {
    if case_sensitive {
        return haystack.find(pattern);
    }

    let hay = haystack.as_bytes();
    let pat = pattern.as_bytes();
    if pat.is_empty() {
        return Some(0);
    }
    if pat.len() > hay.len() {
        return None;
    }
    (0..=hay.len() - pat.len()).find(|&i| hay[i..i + pat.len()].eq_ignore_ascii_case(pat))
}

fn contains_pattern(haystack: &str, pattern: &str, case_sensitive: bool) -> bool {
    find_pattern(haystack, pattern, case_sensitive).is_some()
}

fn highlight_all(text: &str, pattern: &str, base: Color, case_sensitive: bool) -> String {
    let mut result = String::new();
    let mut start = 0;

    while let Some(pos) = find_pattern(&text[start..], pattern, case_sensitive) {
        let abs_pos = start + pos;
        if abs_pos > start {
            result.push_str(&text[start..abs_pos].color(base).to_string());
        }
        result.push_str(
            &text[abs_pos..abs_pos + pattern.len()]
                .red()
                .bold()
                .to_string(),
        );
        start = abs_pos + pattern.len();
    }
    if start < text.len() {
        result.push_str(&text[start..].color(base).to_string());
    }
    result
}

fn handle_path(
    path: PathBuf,
    pool: Arc<Threadpool>,
    pattern: Arc<String>,
    tx: mpsc::Sender<String>,
    cmd_options: Arc<CmdOptions>,
) {
    if path.is_dir() {
        if let Ok(entries) = std::fs::read_dir(&path) {
            for entry in entries.flatten() {
                if is_symlink(&entry) {
                    continue;
                }
                if !cmd_options.has(CmdOption::Hidden) && is_hidden(&entry) {
                    continue;
                }
                if !cmd_options.has(CmdOption::Excluded) && (is_excluded_dir(&entry) || is_cloud_placeholder(&entry)) {
                    continue;
                }

                let entry_path = entry.path();
                let case_sensitive = cmd_options.has(CmdOption::CaseSensitive);

                // Check if the file name contains the pattern before scheduling it for processing
                let file_name = entry.file_name().to_string_lossy().into_owned();
                if contains_pattern(&file_name, &pattern, case_sensitive) {
                    let full_path = entry_path.to_string_lossy();
                    let _ = tx.send(highlight_all(&full_path, &pattern, Color::Cyan, case_sensitive));
                }

                // The block shadows the outer Arc/Sender bindings with clones scoped to just
                // this argument, so `pool.execute` below still refers to the original `pool`.
                pool.execute({
                    let pool = Arc::clone(&pool);
                    let pattern = Arc::clone(&pattern);
                    let tx = tx.clone();
                    let cmd_options = Arc::clone(&cmd_options);
                    move || handle_path(entry_path, pool, pattern, tx, cmd_options)
                });
            }
        }
    } else {
        search_file(&path, &pattern, tx, cmd_options.has(CmdOption::CaseSensitive));
    }
}

fn search_file(path: &Path, pattern: &str, tx: mpsc::Sender<String>, case_sensitive: bool) {
    let Ok(file) = File::open(path) else { return };
    let mut reader = BufReader::new(file);

    let encoding = match reader.fill_buf() {
        Ok(peek) => detect_encoding(peek),
        Err(_) => return,
    };

    if encoding == Encoding::None {
        // git-style binary file detection: skip files containing null bytes
        match reader.fill_buf() {
            Ok(peek) if peek.contains(&0) => return,
            Ok(_) => {}
            Err(_) => return,
        }
        search_utf8_lines(&mut reader, path, pattern, &tx, case_sensitive);
        return;
    }

    // BOM'd encodings can't be decoded line-by-line as raw bytes, so read+decode the whole file.
    let Ok(bytes) = std::fs::read(path) else { return };
    let bom_len = encoding.bom_len();
    if bytes.len() < bom_len {
        return;
    }
    let body = &bytes[bom_len..];
    let text = match encoding {
        Encoding::Utf8Bom => String::from_utf8_lossy(body).into_owned(),
        Encoding::Utf16Le => decode_utf16(body, false),
        Encoding::Utf16Be => decode_utf16(body, true),
        Encoding::Utf32Le => decode_utf32(body, false),
        Encoding::Utf32Be => decode_utf32(body, true),
        Encoding::None => unreachable!(),
    };

    for (i, line) in text.lines().enumerate() {
        process_line(path, pattern, i + 1, line, &tx, case_sensitive);
    }
}

fn search_utf8_lines(
    reader: &mut BufReader<File>,
    path: &Path,
    pattern: &str,
    tx: &mpsc::Sender<String>,
    case_sensitive: bool,
) {
    let mut buf = Vec::new(); // reused for each line to avoid repeated allocations
    let mut line_no = 0usize;

    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf) {
            Ok(0) => break, // EOF
            Ok(n) => n,
            Err(_) => break, // read error, exit this file
        };
        line_no += 1;

        let line = String::from_utf8_lossy(&buf);
        let line = line.trim_end_matches(['\r', '\n']);

        process_line(path, pattern, line_no, line, tx, case_sensitive);
    }
}

fn process_line(
    path: &Path,
    pattern: &str,
    line_no: usize,
    line: &str,
    tx: &mpsc::Sender<String>,
    case_sensitive: bool,
) {
    if let Some(pos) = find_pattern(line, pattern, case_sensitive) {
        let from = floor_char_boundary(line, pos.saturating_sub(CONTEXT));
        let to = ceil_char_boundary(line, (pos + pattern.len() + CONTEXT).min(line.len()));

        let before = &line[from..pos];
        let matched = &line[pos..pos + pattern.len()];
        let after = &line[pos + pattern.len()..to];

        let prefix = if from == 0 { "" } else { "..." };
        let suffix = if to == line.len() { "" } else { "..." };

        let path_str = path.display().to_string();
        let _ = tx.send(format!(
            "{}:{}: {}{}{}{}{}",
            highlight_all(&path_str, pattern, Color::BrightBlue, case_sensitive),
            line_no.to_string().yellow(),
            prefix,
            before,
            matched.red().bold(),
            after,
            suffix
        ));
    }
}

fn floor_char_boundary(s: &str, index: usize) -> usize {
    let mut idx = index.min(s.len());
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    idx
}

fn ceil_char_boundary(s: &str, index: usize) -> usize {
    let mut idx = index.min(s.len());
    while idx < s.len() && !s.is_char_boundary(idx) {
        idx += 1;
    }
    idx
}

fn format_duration(d: Duration) -> String {
    let total_secs = d.as_secs();
    if total_secs >= 60 {
        let minutes = total_secs / 60;
        let seconds = total_secs % 60;
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{:?}", d) // alle minuutin: käytä olemassa olevaa ns/µs/ms/s-vaihtelua
    }
}

#[derive(PartialEq)]
enum CmdOption {
    Hidden,
    Excluded,
    All,
    CaseSensitive,
}

struct CmdOptions {
    options: Vec<CmdOption>
}

impl CmdOptions {
    fn has(&self, option: CmdOption) -> bool {
        // CaseSensitive is deliberately excluded from the --all shorthand: search is
        // case-insensitive by default and must be opted into explicitly via -c.
        let all_applies = matches!(option, CmdOption::Hidden | CmdOption::Excluded);
        self.options
            .iter()
            .any(|o| *o == option || (all_applies && *o == CmdOption::All))
    }
}

const USAGE: &str = "\
Usage: trawl \"<keyword>\" [options]

Options:
  -h, --hidden         Also search hidden files and directories
  -e, --excluded       Also search common build/dependency directories (target, node_modules, dist, ...)
  -a, --all            Shorthand for --hidden --excluded
  -c, --case-sensitive Case-sensitive search (default: case-insensitive)
";

fn handle_args() -> (std::path::PathBuf, String, CmdOptions) {
    let Ok(cwd) = std::env::current_dir() else {
        eprintln!("Could not get current directory. exiting.");
        std::process::exit(1);
    };

    let Some(pattern) = std::env::args().nth(1) else {
        eprint!("{USAGE}");
        std::process::exit(1);
    };
    
    // Extract commandline options
    let mut options = Vec::new();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "-h" | "--hidden" => options.push(CmdOption::Hidden),
            "-e" | "--excluded" => options.push(CmdOption::Excluded),
            "-a" | "--all" => options.push(CmdOption::All),
            "-c" | "--case-sensitive" => options.push(CmdOption::CaseSensitive),
            _ => {}
        }
    }

    (cwd, pattern, CmdOptions { options })
}

fn main() {
    let (cwd, pattern, cmd_options) = handle_args();
    let pattern = Arc::new(pattern);
    let cmd_options = Arc::new(cmd_options);

    let start_time = std::time::Instant::now();

    let pb = Arc::new(ProgressBar::new_spinner());
    pb.set_message("Trawling...");

    // No enable_steady_tick(): that spawns indicatif's own background redraw thread,
    // which would again touch the terminal concurrently with the printer thread below.
    // Instead, this single printer thread both prints and ticks the spinner itself.
    let (tx, rx) = mpsc::channel::<String>();
    let printer = thread::spawn({
        let pb = Arc::clone(&pb);
        move || {
            loop {
                match rx.recv_timeout(Duration::from_millis(80)) {
                    Ok(line) => pb.println(line),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
                pb.tick();
            }
        }
    });

    let pool = Arc::new(Threadpool::new());

    // Start processing from the current working directory.
    handle_path(cwd, Arc::clone(&pool), pattern, tx, cmd_options);

    pool.wait(); // Wait until all jobs and their subtasks are processed

    printer.join().unwrap(); // all senders are dropped by now, so the channel is closed

    pb.finish_and_clear();

    let duration = start_time.elapsed();
    println!("Trawling completed in: {}", format_duration(duration));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn write_temp_file(name_hint: &str, bytes: &[u8]) -> PathBuf {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let mut path = std::env::temp_dir();
        path.push(format!(
            "trawl_test_{}_{}_{}.txt",
            std::process::id(),
            name_hint,
            id
        ));
        File::create(&path).unwrap().write_all(bytes).unwrap();
        path
    }

    // colored decides at runtime whether to emit ANSI escapes based on TTY detection, which
    // would otherwise make substring assertions on the search output flaky under `cargo test`.
    fn run_search(bytes: &[u8], pattern: &str, name_hint: &str) -> Vec<String> {
        run_search_with_case(bytes, pattern, name_hint, true)
    }

    fn run_search_with_case(
        bytes: &[u8],
        pattern: &str,
        name_hint: &str,
        case_sensitive: bool,
    ) -> Vec<String> {
        colored::control::set_override(false);
        let path = write_temp_file(name_hint, bytes);
        let (tx, rx) = mpsc::channel();
        search_file(&path, pattern, tx, case_sensitive);
        let results: Vec<String> = rx.iter().collect();
        let _ = std::fs::remove_file(&path);
        results
    }

    fn utf16_bytes(text: &str, big_endian: bool, bom: bool) -> Vec<u8> {
        let mut out = Vec::new();
        if bom {
            out.extend_from_slice(if big_endian {
                &[0xFE, 0xFF]
            } else {
                &[0xFF, 0xFE]
            });
        }
        for unit in text.encode_utf16() {
            out.extend_from_slice(&if big_endian {
                unit.to_be_bytes()
            } else {
                unit.to_le_bytes()
            });
        }
        out
    }

    fn utf32_bytes(text: &str, big_endian: bool, bom: bool) -> Vec<u8> {
        let mut out = Vec::new();
        if bom {
            out.extend_from_slice(if big_endian {
                &[0x00, 0x00, 0xFE, 0xFF]
            } else {
                &[0xFF, 0xFE, 0x00, 0x00]
            });
        }
        for ch in text.chars() {
            let code = ch as u32;
            out.extend_from_slice(&if big_endian {
                code.to_be_bytes()
            } else {
                code.to_le_bytes()
            });
        }
        out
    }

    #[test]
    fn detects_encodings_by_bom() {
        assert_eq!(detect_encoding(&[0xEF, 0xBB, 0xBF, b'h']), Encoding::Utf8Bom);
        assert_eq!(detect_encoding(&[0xFF, 0xFE, b'h', 0]), Encoding::Utf16Le);
        assert_eq!(detect_encoding(&[0xFE, 0xFF, 0, b'h']), Encoding::Utf16Be);
        assert_eq!(detect_encoding(&[0xFF, 0xFE, 0x00, 0x00]), Encoding::Utf32Le);
        assert_eq!(detect_encoding(&[0x00, 0x00, 0xFE, 0xFF]), Encoding::Utf32Be);
        assert_eq!(detect_encoding(b"plain text"), Encoding::None);
    }

    #[test]
    fn searches_plain_utf8_without_bom() {
        let results = run_search(b"hello needle world\n", "needle", "plain");
        assert_eq!(results.len(), 1);
        assert!(results[0].contains(":1:"));
    }

    #[test]
    fn searches_utf8_with_bom() {
        let mut bytes = vec![0xEF, 0xBB, 0xBF];
        bytes.extend_from_slice(b"line one\nfind needle here\nline three\n");
        let results = run_search(&bytes, "needle", "utf8bom");
        assert_eq!(results.len(), 1);
        assert!(results[0].contains(":2:"));
    }

    #[test]
    fn does_not_leak_bom_char_into_first_line() {
        let mut bytes = vec![0xEF, 0xBB, 0xBF];
        bytes.extend_from_slice(b"needle at start\n");
        let results = run_search(&bytes, "needle", "utf8bom_start");
        assert_eq!(results.len(), 1);
        // The BOM char (U+FEFF) must not end up glued onto the matched line.
        assert!(!results[0].contains('\u{feff}'));
        assert!(results[0].contains(":1:"));
    }

    #[test]
    fn searches_utf16_le() {
        let bytes = utf16_bytes("line one\nfind needle here\nline three\n", false, true);
        let results = run_search(&bytes, "needle", "utf16le");
        assert_eq!(results.len(), 1);
        assert!(results[0].contains(":2:"));
    }

    #[test]
    fn searches_utf16_be() {
        let bytes = utf16_bytes("line one\nfind needle here\nline three\n", true, true);
        let results = run_search(&bytes, "needle", "utf16be");
        assert_eq!(results.len(), 1);
        assert!(results[0].contains(":2:"));
    }

    #[test]
    fn searches_utf32_le() {
        let bytes = utf32_bytes("line one\nfind needle here\nline three\n", false, true);
        let results = run_search(&bytes, "needle", "utf32le");
        assert_eq!(results.len(), 1);
        assert!(results[0].contains(":2:"));
    }

    #[test]
    fn searches_utf32_be() {
        let bytes = utf32_bytes("line one\nfind needle here\nline three\n", true, true);
        let results = run_search(&bytes, "needle", "utf32be");
        assert_eq!(results.len(), 1);
        assert!(results[0].contains(":2:"));
    }

    #[test]
    fn utf16_multiple_matches_on_different_lines() {
        let bytes = utf16_bytes("needle one\nno match\nneedle two\n", true, true);
        let results = run_search(&bytes, "needle", "utf16be_multi");
        assert_eq!(results.len(), 2);
        assert!(results[0].contains(":1:"));
        assert!(results[1].contains(":3:"));
    }

    #[test]
    fn plain_binary_file_without_bom_is_skipped() {
        let bytes = vec![b'a', b'b', 0u8, b'c', b'd'];
        let results = run_search(&bytes, "ab", "binary");
        assert!(results.is_empty());
    }

    #[test]
    fn find_pattern_case_insensitive_basic() {
        assert_eq!(find_pattern("Hello World", "world", false), Some(6));
        assert_eq!(find_pattern("Hello World", "world", true), None);
        assert_eq!(find_pattern("Hello World", "WORLD", false), Some(6));
    }

    #[test]
    fn find_pattern_case_insensitive_leaves_non_ascii_bytes_intact() {
        // "caf\u{e9}" (non-ASCII 'e' with acute accent) must still match exactly, unaffected by folding.
        assert_eq!(find_pattern("café bar", "café", false), Some(0));
        assert_eq!(find_pattern("café bar", "CAFÉ", false), None);
    }

    #[test]
    fn cmd_option_all_does_not_imply_case_sensitive() {
        let opts = CmdOptions {
            options: vec![CmdOption::All],
        };
        assert!(opts.has(CmdOption::Hidden));
        assert!(opts.has(CmdOption::Excluded));
        assert!(!opts.has(CmdOption::CaseSensitive));
    }

    #[test]
    fn search_is_case_insensitive_by_default() {
        let results = run_search_with_case(b"Hello NEEDLE world\n", "needle", "ci_default", false);
        assert_eq!(results.len(), 1);
        assert!(results[0].contains(":1:"));
    }

    #[test]
    fn search_with_case_sensitive_flag_rejects_different_case() {
        let results = run_search_with_case(b"Hello NEEDLE world\n", "needle", "cs_flag", true);
        assert!(results.is_empty());
    }

    #[test]
    fn case_insensitive_search_works_across_all_encodings() {
        let text = "Find NEEDLE here\n";

        let mut utf8bom = vec![0xEF, 0xBB, 0xBF];
        utf8bom.extend_from_slice(text.as_bytes());
        assert_eq!(
            run_search_with_case(&utf8bom, "needle", "ci_utf8bom", false).len(),
            1
        );

        let utf16le = utf16_bytes(text, false, true);
        assert_eq!(
            run_search_with_case(&utf16le, "needle", "ci_utf16le", false).len(),
            1
        );

        let utf16be = utf16_bytes(text, true, true);
        assert_eq!(
            run_search_with_case(&utf16be, "needle", "ci_utf16be", false).len(),
            1
        );

        let utf32le = utf32_bytes(text, false, true);
        assert_eq!(
            run_search_with_case(&utf32le, "needle", "ci_utf32le", false).len(),
            1
        );

        let utf32be = utf32_bytes(text, true, true);
        assert_eq!(
            run_search_with_case(&utf32be, "needle", "ci_utf32be", false).len(),
            1
        );
    }
}
