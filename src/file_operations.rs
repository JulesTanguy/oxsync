use std::path::{Path, PathBuf};
use std::time::SystemTime;

use blake3::{hash, Hash};
use lru::LruCache;
use notify::event::{ModifyKind, RenameMode};
use notify::Event;
use notify::EventKind::Modify;
use tokio::fs;
use tokio::time::Instant;

use crate::utils::{PathType, Utils};
use crate::{err, info, PathMetadata};

pub(crate) struct FileOperationsManager;

impl FileOperationsManager {
    pub async fn copy(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        event: Event,
    ) {
        // "paths" length is always 1 on Windows
        for src_path in event.paths {
            let v_path = Utils::path_to_verbatim(&src_path);

            if is_in_excluded_paths(&v_path) {
                continue;
            }

            let path_str = v_path
                .strip_prefix(&Utils::args().source_dir)
                .unwrap()
                .to_str()
                .unwrap();

            if Utils::args().no_temporary_editor_files && path_str.ends_with('~') {
                continue;
            }

            let relative_path = v_path.strip_prefix(&Utils::args().source_dir).unwrap();
            let (dest_path, dirs) = Utils::get_destination_path_and_dirs(relative_path);

            if let Some(path_metadata) = file_store.get(&v_path) {
                match path_metadata.path_type {
                    PathType::Dir => {
                        if !dest_path.is_dir()
                            && Utils::create_dirs(&dest_path, path_str, &emit_time, false)
                                .await
                                .is_ok()
                        {
                            Self::write_in_file_store(file_store, v_path, PathType::Dir, None)
                                .await;
                        }
                    }
                    PathType::File => {
                        let current_hash = if let Ok(file_content) = fs::read(&v_path).await {
                            Some(hash(&file_content))
                        } else {
                            None
                        };

                        if current_hash.is_none() {
                            Self::create_depends_dirs(dirs, path_str, file_store, &emit_time).await;

                            if Utils::copy_file(&v_path, &dest_path, path_str, emit_time)
                                .await
                                .is_ok()
                            {
                                Self::write_in_file_store(file_store, v_path, PathType::File, None)
                                    .await;
                            }
                            continue;
                        }

                        let file_is_identical = current_hash == path_metadata.hash;
                        let last_change_superior_to_one_sec = SystemTime::now()
                            .duration_since(path_metadata.last_change)
                            .unwrap()
                            .as_millis()
                            > 1000;

                        if file_is_identical && last_change_superior_to_one_sec {
                            info!("file '{}' not copied : content is identical", path_str);
                        } else if file_is_identical {
                        } else {
                            Self::create_depends_dirs(dirs, path_str, file_store, &emit_time).await;

                            if Utils::copy_file(&v_path, &dest_path, path_str, emit_time)
                                .await
                                .is_ok()
                            {
                                Self::write_in_file_store(
                                    file_store,
                                    v_path,
                                    PathType::File,
                                    current_hash,
                                )
                                .await;
                            }
                        }
                    }
                }
                continue;
            }

            if v_path.is_file() {
                Self::create_depends_dirs(dirs, path_str, file_store, &emit_time).await;

                if Utils::copy_file(&v_path, &dest_path, path_str, emit_time)
                    .await
                    .is_ok()
                {
                    let current_hash = if let Ok(file_content) = fs::read(&v_path).await {
                        Some(hash(&file_content))
                    } else {
                        None
                    };

                    Self::write_in_file_store(file_store, v_path, PathType::File, current_hash)
                        .await;
                }
                continue;
            }

            if v_path.is_dir()
                && !dest_path.is_dir()
                && Utils::create_dirs(&dest_path, path_str, &emit_time, false)
                    .await
                    .is_ok()
            {
                Self::write_in_file_store(file_store, v_path, PathType::Dir, None).await;
            }
        }
    }

    pub async fn remove(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        event: Event,
    ) {
        // "paths" length is always 1 on Windows
        for src_path in event.paths {
            let v_path = Utils::path_to_verbatim(&src_path);

            if is_in_excluded_paths(&v_path) {
                continue;
            }
            let path_str = v_path
                .strip_prefix(&Utils::args().source_dir)
                .unwrap()
                .to_str()
                .unwrap();

            if Utils::args().no_temporary_editor_files && path_str.ends_with('~') {
                continue;
            }

            let relative_path = v_path.strip_prefix(&Utils::args().source_dir).unwrap();
            let dest_path = Utils::get_destination_path(relative_path);

            if !dest_path.exists() {
                return;
            } else if dest_path.is_file() {
                if let Err(err) = fs::remove_file(dest_path).await {
                    handle_remove_err(err, path_str, PathType::File);
                } else {
                    Utils::print_action("deleted", "file", path_str, &emit_time);
                };
                file_store.pop(&v_path);
            } else if dest_path.is_dir() {
                if let Err(err) = fs::remove_dir_all(dest_path).await {
                    handle_remove_err(err, path_str, PathType::Dir);
                } else {
                    Utils::print_action("deleted", "dir", path_str, &emit_time);
                };
                file_store.pop(&v_path);
            } else {
                err!("remove error: '{}' is not a file or a directory", path_str);
            }
        }
    }

    pub async fn rename(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        event: Event,
        rename_from: &mut Option<PathBuf>,
    ) {
        // "paths" length is always 1 on Windows
        for src_path in event.paths {
            let v_path = Utils::path_to_verbatim(&src_path);

            if is_in_excluded_paths(&v_path) {
                continue;
            }

            let path_str = v_path
                .strip_prefix(&Utils::args().source_dir)
                .unwrap()
                .to_str()
                .unwrap();

            if Utils::args().no_temporary_editor_files && path_str.ends_with('~') {
                continue;
            }

            let relative_path = v_path.strip_prefix(&Utils::args().source_dir).unwrap();
            let (dest_path, _) = Utils::get_destination_path_and_dirs(relative_path);

            match event.kind {
                Modify(ModifyKind::Name(RenameMode::From)) => {
                    *rename_from = Some(dest_path);
                }
                Modify(ModifyKind::Name(RenameMode::To)) => {
                    if rename_from.is_some() {
                        let old_path = rename_from.take().unwrap();

                        if fs::rename(&old_path, dest_path).await.is_ok() {
                            let path_type;
                            let path_type_str;
                            if v_path.is_file() {
                                path_type = PathType::File;
                                path_type_str = "file";
                            } else if v_path.is_dir() {
                                path_type = PathType::Dir;
                                path_type_str = "dir";
                            } else {
                                err!("'{}' is not a file or a directory", path_str);
                                return;
                            };

                            Utils::print_action("renamed", path_type_str, path_str, &emit_time);

                            if let Some(mut metadata) = file_store.pop(&old_path) {
                                metadata.last_change = SystemTime::now();
                                file_store.put(v_path, metadata);
                            } else {
                                let metadata = PathMetadata {
                                    path_type,
                                    hash: None,
                                    last_change: SystemTime::now(),
                                };
                                file_store.put(v_path, metadata);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub async fn create(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        event: Event,
    ) {
        for src_path in event.paths {
            let v_path = Utils::path_to_verbatim(&src_path);

            if is_in_excluded_paths(&v_path) {
                continue;
            }

            let path_str = v_path
                .strip_prefix(&Utils::args().source_dir)
                .unwrap()
                .to_str()
                .unwrap();

            if Utils::args().no_temporary_editor_files && path_str.ends_with('~') {
                continue;
            }

            let relative_path = v_path.strip_prefix(&Utils::args().source_dir).unwrap();
            let (dest_path, dirs) = Utils::get_destination_path_and_dirs(relative_path);

            if file_store.get(&v_path).is_some() {
                continue;
            }

            if v_path.is_file() && !dest_path.exists() {
                Self::create_depends_dirs(dirs, path_str, file_store, &emit_time).await;

                if Utils::copy_file(&v_path, &dest_path, path_str, emit_time)
                    .await
                    .is_ok()
                {
                    let current_hash = if let Ok(file_content) = fs::read(&v_path).await {
                        Some(hash(&file_content))
                    } else {
                        None
                    };

                    Self::write_in_file_store(file_store, v_path, PathType::File, current_hash)
                        .await;
                }
                continue;
            }

            if v_path.is_dir() && !dest_path.exists() {
                Self::create_depends_dirs(dirs, path_str, file_store, &emit_time).await;

                if Utils::create_dirs(&dest_path, path_str, &emit_time, false)
                    .await
                    .is_ok()
                {
                    Self::write_in_file_store(file_store, v_path, PathType::Dir, None).await;
                }
            }
        }
    }

    async fn write_in_file_store(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        path: PathBuf,
        path_type: PathType,
        current_hash_opt: Option<Hash>,
    ) {
        if file_store.get(&path).is_none() {
            file_store.put(
                path,
                PathMetadata {
                    path_type,
                    hash: current_hash_opt,
                    last_change: SystemTime::now(),
                },
            );
        } else {
            let path_metadata = file_store.get_mut(&path).unwrap();
            if path_type == PathType::File {
                path_metadata.hash = current_hash_opt;
            }
            path_metadata.last_change = SystemTime::now();
        }
    }

    async fn create_depends_dirs(
        dirs: PathBuf,
        path_str: &str,
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: &Instant,
    ) {
        if !dirs.exists()
            && Utils::create_dirs(&dirs, path_str, emit_time, true)
                .await
                .is_ok()
        {
            Self::write_in_file_store(file_store, dirs, PathType::Dir, None).await;
        }
    }
}

fn is_in_excluded_paths(path: &Path) -> bool {
    if Utils::excluded_paths().is_empty() {
        return false;
    }

    for excluded_path in Utils::excluded_paths() {
        if path.starts_with(excluded_path) {
            return true;
        }
    }

    false
}

fn handle_remove_err(err: std::io::Error, path_str: &str, entry_type: PathType) {
    let entry_type_str = match entry_type {
        PathType::File => "file",
        PathType::Dir => "dir",
    };

    if let Some(os_error_code) = err.raw_os_error() {
        // Mute errors 2 & 3 which means that the path does not exists
        if os_error_code != 2 && os_error_code != 3 {
            err!(
                "failed to remove {} '{}', error: {}",
                entry_type_str,
                path_str,
                err.to_string()
            );
        };
    } else {
        err!(
            "failed to remove {} '{}', error: {}",
            entry_type_str,
            path_str,
            err.to_string()
        );
    }
}
