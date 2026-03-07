use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::Parser;
use lru::LruCache;
use notify::{RecursiveMode, Watcher};
use tokio::sync::OnceCell;
use tokio::time::Instant;
use tokio_stream::StreamExt;

use start::Start;
use utils::PathMetadata;
use utils::Utils;

mod file_operations;
mod macros;
mod start;
mod utils;

/// Sync changes from a directory to another
#[derive(Parser, Debug)]
#[command(author, version, long_about = None)]
pub struct Args {
    /// Path of the directory to watch changes from
    #[arg(index(1), required(true))]
    source_dir: PathBuf,
    /// Path of the directory to write changes to
    #[arg(index(2), required(true))]
    target_dir: PathBuf,
    /// Exclude file or dir from the <SOURCE_DIR>, can be used multiple times
    #[arg(long, short)]
    exclude: Vec<PathBuf>,
    /// Exclude files with names ending by a tilde `~`
    #[arg(long, visible_alias("no-tmp"))]
    no_temporary_editor_files: bool,
    /// Ignore creation events
    #[arg(long, visible_alias("no-create"))]
    no_creation_events: bool,
    /// Exclude `.git`, `.idea` dirs + enables `no-temporary-editor-files`
    #[arg(long, visible_alias("ide"))]
    ide_mode: bool,
    /// Display the time spent copying the file
    #[arg(long, visible_alias("stats"))]
    statistics: bool,
    /// Maximum number of files to copy concurrently
    #[arg(long, default_value_t = 1)]
    copy_parallelism: usize,
    /// Set the log level to trace
    #[arg(long)]
    trace: bool,
}

pub static LOG_TRACE: OnceCell<bool> = OnceCell::const_new();

#[tokio::main]
async fn main() {
    if let Err(e) = Start::parse_args().await {
        err!("{}", e);
        return;
    }

    if let Err(e) = init_event_loop().await {
        err!("{}", e);
    };
}

async fn init_event_loop() -> notify::Result<()> {
    let (mut watcher, mut rx) = Start::fs_watcher()?;

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    watcher.watch(&Utils::args().source_dir, RecursiveMode::Recursive)?;

    let mut file_store: LruCache<PathBuf, PathMetadata> =
        LruCache::new(NonZeroUsize::new(32_768).unwrap());

    let mut rename_from: Option<PathBuf> = None;

    info!(
        "Ready - Waiting for changes on '{}'",
        Utils::fmt_path(&Utils::args().source_dir)
    );
    while let Some(res) = rx.next().await {
        match res {
            Ok(event) => {
                let emit_time = Instant::now();
                trace!("{:?}", event);

                Utils::handle_event(event, &mut file_store, emit_time, &mut rename_from).await
            }
            Err(e) => err!("watch error: {:?}", e),
        }
    }

    Ok(())
}
