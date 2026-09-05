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

            let (lock, cvar) = &*pending;
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

fn has_bom(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0xEF, 0xBB, 0xBF])       // UTF-8 BOM
        || bytes.starts_with(&[0x00, 0x00, 0xFE, 0xFF]) // UTF-32 BE (check before UTF-16!)
        || bytes.starts_with(&[0xFF, 0xFE])      // UTF-16 LE / UTF-32 LE
        || bytes.starts_with(&[0xFE, 0xFF]) // UTF-16 BE
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

fn highlight_all(text: &str, pattern: &str, base: Color) -> String {
    let mut result = String::new();
    let mut start = 0;

    while let Some(pos) = text[start..].find(pattern) {
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

fn handle_path(path: PathBuf, pool: Arc<Threadpool>, pattern: Arc<String>, pb: Arc<ProgressBar>) {
    if path.is_dir() {
        if let Ok(entries) = std::fs::read_dir(&path) {
            for entry in entries.flatten() {
                if is_hidden(&entry) || is_excluded_dir(&entry) || is_cloud_placeholder(&entry) {
                    continue;
                }

                let entry_path = entry.path();
                let pool2 = Arc::clone(&pool);
                let pattern2 = Arc::clone(&pattern);
                let pb2 = Arc::clone(&pb);

                // Check if the file name contains the pattern before scheduling it for processing
                let file_name = entry.file_name().to_string_lossy().into_owned();
                if file_name.contains(&*pattern) {
                    let full_path = entry_path.to_string_lossy();
                    pb.println(highlight_all(&full_path, &pattern, Color::Cyan));
                }

                pool.execute(move || handle_path(entry_path, pool2, pattern2, pb2));
            }
        }
    } else {
        search_file(&path, &pattern, pb);
    }
}

fn search_file(path: &Path, pattern: &str, pb: Arc<ProgressBar>) {
    let Ok(file) = File::open(path) else { return };
    let mut reader = BufReader::new(file);

    // git-style binary file detection: skip files containing null bytes
    match reader.fill_buf() {
        Ok(peek) if has_bom(peek) => {}
        Ok(peek) if peek.contains(&0) => return,
        Ok(_) => {}
        Err(_) => return,
    }

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

        if let Some(pos) = line.find(pattern) {
            let from = floor_char_boundary(line, pos.saturating_sub(CONTEXT));
            let to = ceil_char_boundary(line, (pos + pattern.len() + CONTEXT).min(line.len()));

            let before = &line[from..pos];
            let matched = &line[pos..pos + pattern.len()];
            let after = &line[pos + pattern.len()..to];

            let prefix = if from == 0 { "" } else { "..." };
            let suffix = if to == line.len() { "" } else { "..." };

            let path_str = path.display().to_string();
            pb.println(format!(
                "{}:{}: {}{}{}{}{}",
                highlight_all(&path_str, pattern, Color::Blue),
                line_no.to_string().yellow(),
                prefix,
                before,
                matched.red().bold(),
                after,
                suffix
            ));
        }
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

fn main() {
    let Ok(cwd) = std::env::current_dir() else {
        eprintln!("Could not get current directory. exiting.");
        std::process::exit(1);
    };
        
    let Some(pattern) = std::env::args().nth(1) else {
        eprintln!("Usage: trawl \"<keyword>\"");
        std::process::exit(1);
    };

    let pattern = Arc::new(pattern);

    let start_time = std::time::Instant::now();

    let pb = Arc::new(ProgressBar::new_spinner());
    pb.enable_steady_tick(Duration::from_millis(80));
    pb.set_message("Trawling...");

    let pool = Arc::new(Threadpool::new());

    // Start processing from the current working directory.
    handle_path(cwd, Arc::clone(&pool), pattern, Arc::clone(&pb));

    pool.wait(); // Wait until all jobs and their subtasks are processed

    pb.finish_and_clear();

    let duration = start_time.elapsed();
    println!("Trawling completed in: {}", format_duration(duration));
}
