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
    // crossbeam's Receiver is a lock-free MPMC, so it's cloned directly instead of
    // wrapping a single receiver in Arc<Mutex<..>> (no shared-lock contention on dequeue).
    fn new(_id: usize, receiver: crossbeam_channel::Receiver<Job>) -> Worker {
        let handle = thread::spawn(move || {
            while let Ok(job) = receiver.recv() {
                job();
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
    sender: Option<crossbeam_channel::Sender<Job>>,
    pending: Arc<(Mutex<usize>, Condvar)>,
}

impl Threadpool {
    fn new() -> Threadpool {
        let num_threads = thread::available_parallelism().unwrap().get();
        let (sender, receiver) = crossbeam_channel::unbounded();

        let mut workers = Vec::with_capacity(num_threads);
        for id in 0..num_threads {
            workers.push(Worker::new(id, receiver.clone()));
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

    // Cheap single-byte pre-filter avoids the full eq_ignore_ascii_case comparison
    // (and its slice/iterator overhead) at almost every position; profiling showed
    // this loop dominating CPU time under case-insensitive search.
    let first_lower = pat[0].to_ascii_lowercase();
    let first_upper = pat[0].to_ascii_uppercase();
    let last = hay.len() - pat.len();
    (0..=last)
        .filter(|&i| hay[i] == first_lower || hay[i] == first_upper)
        .find(|&i| hay[i..i + pat.len()].eq_ignore_ascii_case(pat))
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

fn format_file_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    let mut unit = 0;
    let mut divisor = 1_u128;
    while u128::from(bytes) >= divisor * 1_000 && unit + 1 < UNITS.len() {
        divisor *= 1_000;
        unit += 1;
    }
    if unit == 0 {
        return format!("{bytes} B");
    }
    let mut tenths = (u128::from(bytes) * 10 + divisor / 2) / divisor;
    // Rounding 999.95 KB should display 1 MB, rather than 1000 KB.
    if tenths >= 10_000 && unit + 1 < UNITS.len() {
        divisor *= 1_000;
        unit += 1;
        tenths = (u128::from(bytes) * 10 + divisor / 2) / divisor;
    }
    if tenths.is_multiple_of(10) {
        format!("{} {}", tenths / 10, UNITS[unit])
    } else {
        format!("{}.{} {}", tenths / 10, tenths % 10, UNITS[unit])
    }
}

fn format_match_path(
    path: &Path,
    pattern: &str,
    base: Color,
    case_sensitive: bool,
    file_size: Option<u64>,
) -> String {
    let mut label = highlight_all(&path.to_string_lossy(), pattern, base, case_sensitive);
    if let Some(bytes) = file_size {
        let size_label = format!("({})", format_file_size(bytes));
        label.push(' ');
        label.push_str(&size_label.bright_magenta().to_string());
    }
    label
}

// Entries get grouped into chunks and scheduled as one job per chunk instead of one job per
// entry - cuts down the number of channel sends/receives for directories with many files.
const BATCH_SIZE: usize = 64;

fn handle_path(
    path: PathBuf,
    pool: Arc<Threadpool>,
    pattern: Arc<String>,
    tx: mpsc::Sender<String>,
    cmd_options: Arc<CmdOptions>,
) {
    if path.is_dir() {
        if let Ok(entries) = std::fs::read_dir(&path) {
            let mut batch: Vec<PathBuf> = Vec::with_capacity(BATCH_SIZE);

            for entry in entries.flatten() {
                if is_symlink(&entry) {
                    continue;
                }
                if !cmd_options.has(CmdOption::Hidden) && is_hidden(&entry) {
                    continue;
                }
                if !cmd_options.has(CmdOption::Excluded)
                    && (is_excluded_dir(&entry) || is_cloud_placeholder(&entry))
                {
                    continue;
                }

                let entry_path = entry.path();
                let case_sensitive = cmd_options.has(CmdOption::CaseSensitive);
                let mut matches_size = true;
                let mut file_size = None;
                if !cmd_options.size_filters.is_empty() {
                    let Ok(metadata) = entry.metadata() else {
                        continue;
                    };
                    if metadata.is_dir() {
                        // Always traverse directories, but only report files with a size filter.
                        matches_size = false;
                    } else if !metadata.is_file() || !cmd_options.matches_size(metadata.len()) {
                        continue;
                    } else {
                        file_size = Some(metadata.len());
                    }
                }

                // Check if the file name contains the pattern before scheduling it for processing
                let file_name = entry.file_name().to_string_lossy().into_owned();
                if matches_size && contains_pattern(&file_name, &pattern, case_sensitive) {
                    let _ = tx.send(format_match_path(
                        &entry_path,
                        &pattern,
                        Color::Cyan,
                        case_sensitive,
                        file_size,
                    ));
                }

                batch.push(entry_path);
                if batch.len() == BATCH_SIZE {
                    schedule_batch(
                        &pool,
                        std::mem::take(&mut batch),
                        &pattern,
                        &tx,
                        &cmd_options,
                    );
                }
            }

            if !batch.is_empty() {
                schedule_batch(&pool, batch, &pattern, &tx, &cmd_options);
            }
        }
    } else if !cmd_options.has(CmdOption::NoContent) {
        let mut file_size = None;
        if !cmd_options.size_filters.is_empty() {
            let Ok(metadata) = path.metadata() else {
                return;
            };
            if !metadata.is_file() || !cmd_options.matches_size(metadata.len()) {
                return;
            }
            file_size = Some(metadata.len());
        }
        search_file(
            &path,
            &pattern,
            tx,
            cmd_options.has(CmdOption::CaseSensitive),
            file_size,
        );
    }
}

// Schedules one job that processes every path in `batch` sequentially. Each entry's
// `handle_path` call is caught individually so one panicking entry doesn't abort the rest
// of the batch (the pool's own catch_unwind around the whole job is a last-resort net only).
fn schedule_batch(
    pool: &Arc<Threadpool>,
    batch: Vec<PathBuf>,
    pattern: &Arc<String>,
    tx: &mpsc::Sender<String>,
    cmd_options: &Arc<CmdOptions>,
) {
    let pool_clone = Arc::clone(pool);
    let pattern = Arc::clone(pattern);
    let tx = tx.clone();
    let cmd_options = Arc::clone(cmd_options);

    pool.execute(move || {
        for entry_path in batch {
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handle_path(entry_path, Arc::clone(&pool_clone), Arc::clone(&pattern), tx.clone(), Arc::clone(&cmd_options));
            })) {
                eprintln!("trawl: worker task panicked: {:?}", payload);
            }
        }
    });
}

fn search_file(
    path: &Path,
    pattern: &str,
    tx: mpsc::Sender<String>,
    case_sensitive: bool,
    file_size: Option<u64>,
) {
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
        search_utf8_lines(&mut reader, path, pattern, &tx, case_sensitive, file_size);
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
        process_line(path, pattern, i + 1, line, &tx, case_sensitive, file_size);
    }
}

fn search_utf8_lines(
    reader: &mut BufReader<File>,
    path: &Path,
    pattern: &str,
    tx: &mpsc::Sender<String>,
    case_sensitive: bool,
    file_size: Option<u64>,
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

        // Binary content can start anywhere in a file (e.g. a PDF's plain-text header followed
        // by compressed streams), so re-check for NUL bytes / invalid UTF-8 on every line and
        // bail out of the whole file the moment either is hit - not just on the first buffered
        // chunk. Without this, from_utf8_lossy() can silently reassemble a pattern's exact UTF-8
        // bytes out of garbage binary data (e.g. searching "©" matched inside random PDF bytes).
        if buf.contains(&0) {
            return;
        }
        let Ok(line) = std::str::from_utf8(&buf) else {
            return;
        };
        let line = line.trim_end_matches(['\r', '\n']);

        process_line(path, pattern, line_no, line, tx, case_sensitive, file_size);
    }
}

fn process_line(
    path: &Path,
    pattern: &str,
    line_no: usize,
    line: &str,
    tx: &mpsc::Sender<String>,
    case_sensitive: bool,
    file_size: Option<u64>,
) {
    if let Some(pos) = find_pattern(line, pattern, case_sensitive) {
        let from = floor_char_boundary(line, pos.saturating_sub(CONTEXT));
        let to = ceil_char_boundary(line, (pos + pattern.len() + CONTEXT).min(line.len()));

        let before = &line[from..pos];
        let matched = &line[pos..pos + pattern.len()];
        let after = &line[pos + pattern.len()..to];

        let prefix = if from == 0 { "" } else { "..." };
        let suffix = if to == line.len() { "" } else { "..." };

        let _ = tx.send(format!(
            "{}:{}: {}{}{}{}{}",
            format_match_path(
                path,
                pattern,
                Color::BrightBlue,
                case_sensitive,
                file_size,
            ),
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
    NoContent,
}

#[derive(Debug, PartialEq, Eq)]
enum SizeComparison {
    Less,
    LessOrEqual,
    Equal,
    GreaterOrEqual,
    Greater,
}

#[derive(Debug, PartialEq, Eq)]
struct SizeFilter {
    comparison: SizeComparison,
    bytes: u64,
    fractional_byte: bool,
}

impl SizeFilter {
    fn parse(input: &str) -> Result<Self, String> {
        let input = input.trim();
        let (comparison, value) = if let Some(value) = input.strip_prefix("<=") {
            (SizeComparison::LessOrEqual, value)
        } else if let Some(value) = input.strip_prefix(">=") {
            (SizeComparison::GreaterOrEqual, value)
        } else if let Some(value) = input.strip_prefix('<') {
            (SizeComparison::Less, value)
        } else if let Some(value) = input.strip_prefix('>') {
            (SizeComparison::Greater, value)
        } else if let Some(value) = input.strip_prefix('=') {
            (SizeComparison::Equal, value)
        } else {
            (SizeComparison::Equal, input)
        };
        let value = value.trim();
        let number_end = value
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(value.len());
        let (number, unit) = value.split_at(number_end);
        let invalid = || format!("invalid size filter {input:?}; use e.g. \"<5KB\" or \"> 2.5MB\"");
        if number.is_empty() || !number.bytes().any(|b| b.is_ascii_digit()) {
            return Err(invalid());
        }
        let multiplier: u128 = match unit.trim().to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "kb" => 1_000,
            "mb" => 1_000_000,
            "gb" => 1_000_000_000,
            "tb" => 1_000_000_000_000,
            "kib" => 1 << 10,
            "mib" => 1 << 20,
            "gib" => 1 << 30,
            "tib" => 1 << 40,
            _ => return Err(invalid()),
        };
        let (whole, fractional) = number.split_once('.').unwrap_or((number, ""));
        // Integer arithmetic keeps comparisons accurate even above f64's exact range.
        let digits = format!("{whole}{fractional}");
        let numerator = digits
            .parse::<u128>()
            .map_err(|_| invalid())?
            .checked_mul(multiplier)
            .ok_or_else(invalid)?;
        let precision = u32::try_from(fractional.len()).map_err(|_| invalid())?;
        let denominator = 10_u128.checked_pow(precision).ok_or_else(invalid)?;
        let bytes = u64::try_from(numerator / denominator).map_err(|_| invalid())?;
        let fractional_byte = numerator % denominator != 0;
        if bytes == u64::MAX && fractional_byte {
            return Err(invalid());
        }
        Ok(Self {
            comparison,
            bytes,
            fractional_byte,
        })
    }

    fn matches(&self, bytes: u64) -> bool {
        use std::cmp::Ordering;
        let ordering = match bytes.cmp(&self.bytes) {
            Ordering::Equal if self.fractional_byte => Ordering::Less,
            ordering => ordering,
        };
        match self.comparison {
            SizeComparison::Less => ordering == Ordering::Less,
            SizeComparison::LessOrEqual => ordering != Ordering::Greater,
            SizeComparison::Equal => ordering == Ordering::Equal,
            SizeComparison::GreaterOrEqual => ordering != Ordering::Less,
            SizeComparison::Greater => ordering == Ordering::Greater,
        }
    }
}

struct CmdOptions {
    options: Vec<CmdOption>,
    size_filters: Vec<SizeFilter>,
}

impl CmdOptions {
    fn matches_size(&self, bytes: u64) -> bool {
        self.size_filters.iter().all(|filter| filter.matches(bytes))
    }

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
  -nc, --no-content    Only match file/directory names, don't search file contents
  -s, --size <filter>  Filter files by size, e.g. \"<5KB\" or \"> 2.5MB\" (also with -nc)
                      Show rounded file sizes after file names
  -p, --path <path>    Search starting from <path> instead of the current directory

Size filters: <, <=, >, >=, = (default); B, KB, MB, GB, TB (powers of 1000),
              KiB, MiB, GiB, TiB (powers of 1024). Units are case-insensitive.
              Quote filters containing < or >. Repeat -s to combine limits.
              Directories are traversed but not reported when filtering by size.
  ";

fn handle_args() -> (std::path::PathBuf, String, CmdOptions) {
    let Ok(cwd) = std::env::current_dir() else {
        eprintln!("Could not get current directory. exiting.");
        std::process::exit(1);
    };

    parse_args(std::env::args().skip(1), cwd).unwrap_or_else(|error| {
        eprintln!("trawl: {error}");
        eprint!("{USAGE}");
        std::process::exit(1);
    })
}

fn parse_args(
    mut args: impl Iterator<Item = String>,
    cwd: PathBuf,
) -> Result<(PathBuf, String, CmdOptions), String> {
    let pattern = args.next().ok_or("a search keyword is required")?;

    // Extract commandline options
    let mut options = Vec::new();
    let mut size_filters = Vec::new();
    let mut custom_path: Option<String> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--hidden" => options.push(CmdOption::Hidden),
            "-e" | "--excluded" => options.push(CmdOption::Excluded),
            "-a" | "--all" => options.push(CmdOption::All),
            "-c" | "--case-sensitive" => options.push(CmdOption::CaseSensitive),
            "-nc" | "--no-content" => options.push(CmdOption::NoContent),
            "-s" | "--size" => {
                let mut filter = args.next().ok_or("-s requires a size filter")?;
                if matches!(filter.as_str(), "<" | "<=" | ">" | ">=" | "=") {
                    let value = args
                        .next()
                        .ok_or("-s requires a size after the comparison")?;
                    filter.push_str(&value);
                }
                size_filters.push(SizeFilter::parse(&filter)?);
            }
            "-p" | "--path" => {
                let path = args.next().ok_or("-p requires a path argument")?;
                custom_path = Some(path);
            }
            _ => {}
        }
    }

    let start_path = match custom_path {
        Some(p) => PathBuf::from(p),
        None => cwd,
    };

    Ok((
        start_path,
        pattern,
        CmdOptions {
            options,
            size_filters,
        },
    ))
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

    #[test]
    fn size_filters_parse_units_decimals_and_comparison_boundaries() {
        for (input, threshold) in [
            ("<5kb", 5_000),
            ("> 2.5MB", 2_500_000),
            ("<= 1.5 KiB", 1_536),
            (">=1GB", 1_000_000_000),
            ("=2MiB", 2_097_152),
            ("3GiB", 3_221_225_472),
            ("1tb", 1_000_000_000_000),
            ("1TiB", 1_099_511_627_776),
            (" 42 B ", 42),
            ("42", 42),
        ] {
            let filter = SizeFilter::parse(input).unwrap();
            assert_eq!(filter.bytes, threshold, "{input}");
            assert!(!filter.fractional_byte, "{input}");
        }
        for (input, expected) in [
            ("<10", [true, false, false]),
            ("<=10", [true, true, false]),
            ("=10", [false, true, false]),
            (">=10", [false, true, true]),
            (">10", [false, false, true]),
        ] {
            let filter = SizeFilter::parse(input).unwrap();
            assert_eq!(
                [filter.matches(9), filter.matches(10), filter.matches(11)],
                expected,
                "{input}"
            );
        }
        assert!(SizeFilter::parse("<.5B").unwrap().matches(0));
        assert!(!SizeFilter::parse("<.5B").unwrap().matches(1));
        assert!(!SizeFilter::parse(">=10.1B").unwrap().matches(10));
        assert!(SizeFilter::parse(">=10.1B").unwrap().matches(11));
        assert!(!SizeFilter::parse("=10.1B").unwrap().matches(10));
        let large = SizeFilter::parse(">9007199254740992B").unwrap();
        assert!(!large.matches(9_007_199_254_740_992));
        assert!(large.matches(9_007_199_254_740_993));
        assert!(
            SizeFilter::parse("18446744073709551615B")
                .unwrap()
                .matches(u64::MAX)
        );
    }

    #[test]
    fn size_filters_reject_invalid_values() {
        for input in [
            "",
            "<",
            "KB",
            "<KB",
            "-1KB",
            ">-1",
            "NaN",
            "inf",
            "1.2.3MB",
            "5XB",
            "5 KB junk",
            "1 2KB",
            "<<5KB",
            "=>5KB",
            "2,5MB",
            "18446744073709551616B",
            "18446744073709551615.1B",
            "999999999999999999999999999999999999999TB",
        ] {
            assert!(SizeFilter::parse(input).is_err(), "{input:?}");
        }
    }

    #[test]
    fn size_arguments_accept_combined_and_separate_comparisons() {
        let cwd = PathBuf::from("root");
        let args = [
            "needle",
            "-s",
            ">",
            "2.5MB",
            "--size",
            "<=10MiB",
            "-nc",
            "-a",
            "-p",
            "elsewhere",
        ];
        let (path, pattern, options) =
            parse_args(args.into_iter().map(String::from), cwd.clone()).unwrap();
        assert_eq!(path, PathBuf::from("elsewhere"));
        assert_eq!(pattern, "needle");
        assert!(options.has(CmdOption::NoContent));
        assert!(options.has(CmdOption::Hidden));
        assert!(!options.matches_size(2_500_000));
        assert!(options.matches_size(2_500_001));
        assert!(options.matches_size(10_485_760));
        assert!(!options.matches_size(10_485_761));
        for args in [
            vec!["needle", "-s"],
            vec!["needle", "-s", ">"],
            vec!["needle", "--size", "invalid"],
            vec!["needle", "-s", "-nc"],
        ] {
            assert!(parse_args(args.into_iter().map(String::from), cwd.clone()).is_err());
        }
    }

    fn run_tree_search(path: &Path, filter: Option<&str>, no_content: bool) -> Vec<String> {
        let pool = Arc::new(Threadpool::new());
        let (tx, rx) = mpsc::channel();
        let options = CmdOptions {
            options: if no_content {
                vec![CmdOption::NoContent]
            } else {
                Vec::new()
            },
            size_filters: filter
                .map(|filter| vec![SizeFilter::parse(filter).unwrap()])
                .unwrap_or_default(),
        };
        handle_path(
            path.to_path_buf(),
            Arc::clone(&pool),
            Arc::new("needle".to_owned()),
            tx,
            Arc::new(options),
        );
        pool.wait();
        rx.iter().collect()
    }

    #[test]
    fn size_filter_covers_names_contents_subdirectories_and_direct_files() {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let root =
            std::env::temp_dir().join(format!("trawl_size_test_{}_{}", std::process::id(), id));
        std::fs::create_dir(&root).unwrap();
        let nested = root.join("needle_directory");
        std::fs::create_dir(&nested).unwrap();
        let small = root.join("needle_small.txt");
        let large = root.join("needle_large.txt");
        std::fs::write(&small, b"needle").unwrap();
        let mut large_content = b"needle large content".to_vec();
        large_content.resize(1_549, b' ');
        std::fs::write(&large, large_content).unwrap();
        std::fs::write(nested.join("plain.txt"), b"needle").unwrap();
        std::fs::write(root.join("empty.txt"), b"").unwrap();

        let normal = run_tree_search(&root, Some("<10B"), false);
        assert_eq!(normal.len(), 3, "{normal:?}");
        assert!(
            normal.iter().any(|line| line.contains("plain.txt (6 B):1:")),
            "{normal:?}"
        );
        assert!(
            !normal.iter().any(|line| line.contains("needle_large.txt")),
            "{normal:?}"
        );
        let names = run_tree_search(&root, Some("<10B"), true);
        assert_eq!(names.len(), 1, "{names:?}");
        assert!(names[0].ends_with("needle_small.txt (6 B)"));
        let larger = run_tree_search(&root, Some(">6B"), true);
        assert_eq!(larger.len(), 1, "{larger:?}");
        assert!(larger[0].ends_with("needle_large.txt (1.5 KB)"));
        let unfiltered = run_tree_search(&root, None, true);
        assert_eq!(unfiltered.len(), 3, "{unfiltered:?}");
        assert!(
            unfiltered
                .iter()
                .any(|line| line.ends_with("needle_directory"))
        );
        assert!(run_tree_search(&small, Some("<6B"), false).is_empty());
        assert_eq!(run_tree_search(&small, Some("<=6B"), false).len(), 1);
        assert!(run_tree_search(&root, Some("=0B"), false).is_empty());

        // Remove only the uniquely created fixture and its known children.
        std::fs::remove_file(small).unwrap();
        std::fs::remove_file(large).unwrap();
        std::fs::remove_file(nested.join("plain.txt")).unwrap();
        std::fs::remove_file(root.join("empty.txt")).unwrap();
        std::fs::remove_dir(nested).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

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
        search_file(&path, pattern, tx, case_sensitive, None);
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
    fn binary_content_appearing_after_valid_text_lines_is_skipped() {
        // Simulates a file (e.g. a PDF) whose first buffered chunk is plain-text-like and only
        // turns into binary garbage further in - the NUL byte here would be missed by a check
        // that only inspects the first peeked chunk.
        let mut bytes = b"%PDF-1.4\nsome header text\n".to_vec();
        bytes.extend_from_slice(&[0x00, 0x01, 0x02, b'\n']);
        bytes.extend_from_slice(b"needle should not be reached\n");
        let results = run_search(&bytes, "needle", "binary_later_null");
        assert!(results.is_empty());
    }

    #[test]
    fn copyright_symbol_does_not_leak_from_invalid_utf8_binary_data() {
        // Regression test: searching a multi-byte pattern like '©' (UTF-8 bytes C2 A9) must not
        // match when those exact bytes happen to occur inside otherwise-invalid UTF-8 binary
        // data with no NUL byte present - from_utf8_lossy() used to silently paper over the
        // invalid bytes around it and let the match through.
        let mut bytes = b"%PDF-1.4\n".to_vec();
        bytes.extend_from_slice(&[0xFF, 0xC2, 0xA9, 0xFE, b'\n']);
        let results = run_search(&bytes, "\u{a9}", "pdf_copyright_no_null");
        assert!(results.is_empty());
    }

    #[test]
    fn copyright_symbol_still_matches_in_real_utf8_text() {
        let results = run_search("price © 2024 example\n".as_bytes(), "\u{a9}", "real_copyright");
        assert_eq!(results.len(), 1);
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
            size_filters: Vec::new(),
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
