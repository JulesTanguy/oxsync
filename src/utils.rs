use core::fmt::Debug;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[cfg(windows)]
use std::path::{Component, Prefix};

use blake3::Hash;
use lru::LruCache;
use notify::event::{ModifyKind, RenameMode};
use notify::{Event, EventKind};
use tokio::fs;
use tokio::sync::OnceCell;
use tokio::time::Instant;

use crate::file_operations::FileOperationsManager;
use crate::{err, info, warn, Args};

pub struct Utils;

static ARGS: OnceCell<Args> = OnceCell::const_new();
static EXCLUDED_PATHS: OnceCell<Vec<PathBuf>> = OnceCell::const_new();

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PathType {
    File,
    Dir,
}

#[derive(Debug, Clone)]
pub struct PathMetadata {
    pub path_type: PathType,
    pub hash: Option<Hash>,
    pub last_change: SystemTime,
}

impl Utils {
    pub fn set_args(args: Args) {
        ARGS.set(args).unwrap();
    }
    pub fn args<'a>() -> &'a Args {
        ARGS.get().unwrap()
    }

    pub fn set_excluded_paths(excluded_paths: Vec<PathBuf>) {
        EXCLUDED_PATHS.set(excluded_paths).unwrap();
    }
    pub fn excluded_paths<'a>() -> &'a Vec<PathBuf> {
        EXCLUDED_PATHS.get().unwrap()
    }

    pub fn fmt_path(path: &Path) -> String {
        let path_str = path.to_string_lossy();
        if let Some(stripped) = path_str.strip_prefix(r"\\?\") {
            stripped.to_string()
        } else {
            path_str.into_owned()
        }
    }

    /// See `https://github.com/dherman/verbatim`
    pub fn path_to_verbatim(path: &Path) -> PathBuf {
        #[cfg(not(windows))]
        {
            path.to_path_buf()
        }

        #[cfg(windows)]
        {
            let mut components = path.components();
            match components.next() {
                Some(Component::Prefix(prefix)) => {
                    let new_prefix = match prefix.kind() {
                        Prefix::Disk(letter) => {
                            let new_prefix_string =
                                format!(r"\\?\{}:\", String::from_utf8_lossy(&[letter]));
                            let new_prefix = Path::new(&new_prefix_string).to_path_buf();
                            new_prefix
                        }
                        _ => return path.to_path_buf(),
                    };
                    new_prefix.join(components)
                }
                Some(other) => Path::new(r"\\?\").join(Path::new(&other)).join(components),
                _ => path.to_path_buf(),
            }
        }
    }

    pub async fn copy_file(
        src_path: &Path,
        dest_path: &Path,
        path_str: &str,
        emit_time: Instant,
    ) -> Result<(), ()> {
        if let Err(err) = fs::copy(src_path, dest_path).await {
            err!("failed to copy '{}', error: {}", path_str, err.to_string());
            Err(())
        } else {
            Self::print_action("copied", "file", path_str, &emit_time);
            Ok(())
        }
    }

    pub async fn create_dirs(
        dest_path: &Path,
        path_str: &str,
        emit_time: &Instant,
        dependency: bool,
    ) -> Result<(), ()> {
        if let Err(err) = fs::create_dir_all(&dest_path).await {
            if dependency {
                err!(
                    "failed to create dirs for '{}', error: {}",
                    path_str,
                    err.to_string()
                );
            } else {
                err!("failed to copy '{}', error: {}", path_str, err.to_string());
            }

            Err(())
        } else {
            if !dependency {
                Self::print_action("created", "dir", path_str, emit_time);
            }
            Ok(())
        }
    }

    pub fn get_destination_path_and_dirs(relative_path: &Path) -> (PathBuf, PathBuf) {
        let dest_path = Self::get_destination_path(relative_path);
        let dirs = dest_path.parent().unwrap().to_path_buf();

        (dest_path, dirs)
    }

    pub fn get_destination_path(relative_path: &Path) -> PathBuf {
        Path::new(&Self::args().target_dir).join(relative_path)
    }

    pub async fn handle_event(
        event: Event,
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        rename_from: &mut Option<PathBuf>,
    ) {
        match event.kind {
            EventKind::Create(_) => {
                if !Utils::args().no_creation_events {
                    FileOperationsManager::create(file_store, emit_time, event).await;
                }
            }
            EventKind::Modify(kind) => match kind {
                ModifyKind::Name(rename) => match rename {
                    RenameMode::From => {
                        FileOperationsManager::rename(file_store, emit_time, event, rename_from)
                            .await;
                    }
                    RenameMode::To => {
                        FileOperationsManager::rename(file_store, emit_time, event, rename_from)
                            .await;
                    }
                    _ => {
                        FileOperationsManager::copy(file_store, emit_time, event).await;
                    }
                },
                _ => {
                    FileOperationsManager::copy(file_store, emit_time, event).await;
                }
            },
            EventKind::Remove(_) => {
                FileOperationsManager::remove(file_store, emit_time, event).await;
            }
            EventKind::Access(_) => {}
            _ => {
                warn!("Unknown event: {:?}", event)
            }
        }
    }

    pub fn print_action(action_verb: &str, type_path: &str, path_str: &str, emit_time: &Instant) {
        if Utils::args().statistics {
            let elapsed = emit_time.elapsed().as_micros();
            if elapsed >= 1000 {
                info!(
                    "{} '{}' {} in {} ms",
                    type_path,
                    path_str,
                    action_verb,
                    elapsed / 1000
                );
            } else {
                info!(
                    "{} '{}' {} in {} μs",
                    type_path, path_str, action_verb, elapsed
                );
            }

            return;
        }

        info!("{} '{}' {}", type_path, path_str, action_verb);
    }
}
