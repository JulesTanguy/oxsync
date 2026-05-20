use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use blake3::Hash;
use lru::LruCache;
use notify::Event;
use notify::EventKind::Modify;
use notify::event::{ModifyKind, RenameMode};
use tokio::fs;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::utils::{PathType, Utils};
use crate::{PathMetadata, err, info, warn};

pub(crate) struct FileOperationsManager;

pub(crate) struct RenameFrom {
    source_path: PathBuf,
    dest_path: PathBuf,
}

#[derive(Clone)]
struct CopyPlan {
    source_path: PathBuf,
    dest_path: PathBuf,
    relative_path: String,
    source_type: PathType,
}

struct CopyOutcome {
    source_path: PathBuf,
    source_type: PathType,
    current_hash: Option<Hash>,
}

impl FileOperationsManager {
    pub async fn copy(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        event: Event,
    ) {
        let mut copy_plans = Vec::new();

        // "paths" length is always 1 on Windows
        for src_path in event.paths {
            let Some((v_path, relative_path, path_str)) = map_source_event_path(&src_path) else {
                continue;
            };
            let (dest_path, _) = Utils::get_destination_path_and_dirs(&relative_path);

            if path_type(&v_path).await == Some(PathType::Dir) {
                Self::sync_directory(file_store, emit_time, &v_path, &path_str).await;
                continue;
            }

            if let Some(path_metadata) = file_store.get(&v_path) {
                match path_metadata.path_type {
                    PathType::Dir => {
                        if path_type(&dest_path).await != Some(PathType::Dir)
                            && Utils::create_dirs(&dest_path, &path_str, &emit_time, false)
                                .await
                                .is_ok()
                        {
                            Self::write_in_file_store(file_store, v_path, PathType::Dir, None)
                                .await;
                        }
                    }
                    PathType::File => {
                        let current_hash = Utils::hash_file(&v_path).await.ok();

                        if current_hash.is_none() {
                            copy_plans.push(CopyPlan {
                                source_path: v_path,
                                dest_path,
                                relative_path: path_str.clone(),
                                source_type: PathType::File,
                            });
                            continue;
                        }

                        let file_is_identical = current_hash == path_metadata.hash;
                        let last_change_superior_to_one_sec = SystemTime::now()
                            .duration_since(path_metadata.last_change)
                            .unwrap_or_default()
                            .as_millis()
                            > 1000;

                        if file_is_identical && last_change_superior_to_one_sec {
                            info!("file '{}' not copied : content is identical", path_str);
                        } else if !file_is_identical {
                            copy_plans.push(CopyPlan {
                                source_path: v_path,
                                dest_path,
                                relative_path: path_str.clone(),
                                source_type: PathType::File,
                            });
                        }
                    }
                }
                continue;
            }

            if path_type(&v_path).await == Some(PathType::File) {
                copy_plans.push(CopyPlan {
                    source_path: v_path,
                    dest_path,
                    relative_path: path_str.clone(),
                    source_type: PathType::File,
                });
                continue;
            }

            if path_type(&v_path).await == Some(PathType::Dir)
                && path_type(&dest_path).await != Some(PathType::Dir)
                && Utils::create_dirs(&dest_path, &path_str, &emit_time, false)
                    .await
                    .is_ok()
            {
                Self::write_in_file_store(file_store, v_path, PathType::Dir, None).await;
            }
        }

        let copy_outcomes = Self::execute_copy_plans(copy_plans, emit_time).await;

        for outcome in copy_outcomes {
            Self::write_in_file_store(
                file_store,
                outcome.source_path,
                outcome.source_type,
                outcome.current_hash,
            )
            .await;
        }
    }

    pub async fn remove(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        event: Event,
    ) {
        // "paths" length is always 1 on Windows
        for src_path in event.paths {
            let Some((v_path, relative_path, path_str)) = map_source_event_path(&src_path) else {
                continue;
            };
            let dest_path = Utils::get_destination_path(&relative_path);

            match path_type(&dest_path).await {
                None => {
                    file_store.pop(&v_path);
                    continue;
                }
                Some(PathType::File) => {
                    if let Err(err) = fs::remove_file(&dest_path).await {
                        handle_remove_err(err, &path_str, PathType::File);
                    } else {
                        Utils::print_action("deleted", "file", &path_str, &emit_time);
                    };
                    file_store.pop(&v_path);
                }
                Some(PathType::Dir) => {
                    if let Err(err) = fs::remove_dir_all(&dest_path).await {
                        handle_remove_err(err, &path_str, PathType::Dir);
                    } else {
                        Utils::print_action("deleted", "dir", &path_str, &emit_time);
                    };
                    file_store.pop(&v_path);
                }
            }
        }
    }

    pub async fn rename(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        event: Event,
        rename_from: &mut Option<RenameFrom>,
    ) {
        // "paths" length is always 1 on Windows
        for src_path in event.paths {
            let Some((v_path, relative_path, path_str)) = map_source_event_path(&src_path) else {
                continue;
            };
            let (dest_path, _) = Utils::get_destination_path_and_dirs(&relative_path);

            match event.kind {
                Modify(ModifyKind::Name(RenameMode::From)) => {
                    *rename_from = Some(RenameFrom {
                        source_path: v_path,
                        dest_path,
                    });
                }
                Modify(ModifyKind::Name(RenameMode::To)) => {
                    if let Some(old_path) = rename_from.take() {
                        Self::finish_rename(
                            file_store, emit_time, old_path, v_path, dest_path, path_str,
                        )
                        .await;
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
        let mut copy_plans = Vec::new();

        for src_path in event.paths {
            let Some((v_path, relative_path, path_str)) = map_source_event_path(&src_path) else {
                continue;
            };
            let (dest_path, dirs) = Utils::get_destination_path_and_dirs(&relative_path);

            if file_store.get(&v_path).is_some() {
                continue;
            }

            if path_type(&v_path).await == Some(PathType::Dir) {
                Self::sync_directory(file_store, emit_time, &v_path, &path_str).await;
                continue;
            }

            if path_type(&v_path).await == Some(PathType::File) {
                if let Some(plan) =
                    Self::build_copy_plan_for_file(file_store, v_path, dest_path, path_str).await
                {
                    copy_plans.push(plan);
                }
                continue;
            }

            if path_type(&v_path).await == Some(PathType::Dir)
                && path_type(&dest_path).await.is_none()
            {
                Self::create_depends_dirs(dirs, &path_str, file_store, &emit_time).await;

                if Utils::create_dirs(&dest_path, &path_str, &emit_time, false)
                    .await
                    .is_ok()
                {
                    Self::write_in_file_store(file_store, v_path, PathType::Dir, None).await;
                }
            }
        }

        let copy_outcomes = Self::execute_copy_plans(copy_plans, emit_time).await;

        for outcome in copy_outcomes {
            Self::write_in_file_store(
                file_store,
                outcome.source_path,
                outcome.source_type,
                outcome.current_hash,
            )
            .await;
        }
    }

    async fn execute_copy_plans(copy_plans: Vec<CopyPlan>, emit_time: Instant) -> Vec<CopyOutcome> {
        if copy_plans.is_empty() {
            return Vec::new();
        }

        let max_parallelism = Utils::args().copy_parallelism.max(1);
        let mut next_index = 0;
        let mut in_flight = JoinSet::new();
        let mut outcomes = Vec::with_capacity(copy_plans.len());

        loop {
            while in_flight.len() < max_parallelism && next_index < copy_plans.len() {
                let plan = copy_plans[next_index].clone();
                next_index += 1;

                in_flight.spawn(async move { Self::run_copy_plan(plan, emit_time).await });
            }

            if in_flight.is_empty() {
                break;
            }

            if let Some(join_result) = in_flight.join_next().await {
                match join_result {
                    Ok(Some(outcome)) => outcomes.push(outcome),
                    Ok(None) => {}
                    Err(err) => err!("copy task failed: {}", err),
                }
            }
        }

        outcomes
    }

    async fn finish_rename(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        old_path: RenameFrom,
        new_source_path: PathBuf,
        new_dest_path: PathBuf,
        path_str: String,
    ) {
        let Some(new_type) = path_type(&new_source_path).await else {
            err!("'{}' is not a file or a directory", path_str);
            return;
        };

        match fs::rename(&old_path.dest_path, &new_dest_path).await {
            Ok(()) => {
                let path_type_str = match new_type {
                    PathType::File => "file",
                    PathType::Dir => "dir",
                };
                Utils::print_action("renamed", path_type_str, &path_str, &emit_time);
                Self::migrate_file_store_after_rename(
                    file_store,
                    &old_path.source_path,
                    &new_source_path,
                    new_type,
                );
            }
            Err(rename_err) => {
                err!(
                    "failed to rename '{}' to '{}', error: {}",
                    Utils::fmt_path(&old_path.dest_path),
                    Utils::fmt_path(&new_dest_path),
                    rename_err
                );
                Self::fallback_after_failed_rename(
                    file_store,
                    emit_time,
                    &old_path,
                    &new_source_path,
                    &new_dest_path,
                    &path_str,
                    new_type,
                )
                .await;
            }
        }
    }

    async fn fallback_after_failed_rename(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        old_path: &RenameFrom,
        new_source_path: &Path,
        new_dest_path: &Path,
        path_str: &str,
        new_type: PathType,
    ) {
        match new_type {
            PathType::File => {
                let plan = CopyPlan {
                    source_path: new_source_path.to_path_buf(),
                    dest_path: new_dest_path.to_path_buf(),
                    relative_path: path_str.to_string(),
                    source_type: PathType::File,
                };

                let outcomes = Self::execute_copy_plans(vec![plan], emit_time).await;
                if outcomes.is_empty() {
                    return;
                }

                for outcome in outcomes {
                    Self::write_in_file_store(
                        file_store,
                        outcome.source_path,
                        outcome.source_type,
                        outcome.current_hash,
                    )
                    .await;
                }
            }
            PathType::Dir => {
                Self::sync_directory(file_store, emit_time, new_source_path, path_str).await;
            }
        }

        remove_destination_path(&old_path.dest_path, path_str, &emit_time).await;
        Self::migrate_file_store_after_rename(
            file_store,
            &old_path.source_path,
            new_source_path,
            new_type,
        );
    }

    fn migrate_file_store_after_rename(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        old_source_path: &Path,
        new_source_path: &Path,
        new_type: PathType,
    ) {
        let now = SystemTime::now();
        let keys_to_move: Vec<PathBuf> = file_store
            .iter()
            .filter_map(|(path, _)| {
                if path.starts_with(old_source_path) {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect();

        if keys_to_move.is_empty() {
            file_store.put(
                new_source_path.to_path_buf(),
                PathMetadata {
                    path_type: new_type,
                    hash: None,
                    last_change: now,
                },
            );
            return;
        }

        for old_key in keys_to_move {
            let Some(mut metadata) = file_store.pop(&old_key) else {
                continue;
            };
            let new_key = match old_key.strip_prefix(old_source_path) {
                Ok(relative) => new_source_path.join(relative),
                Err(_) => new_source_path.to_path_buf(),
            };

            metadata.last_change = now;
            file_store.put(new_key, metadata);
        }
    }

    async fn run_copy_plan(plan: CopyPlan, emit_time: Instant) -> Option<CopyOutcome> {
        let path = Path::new(&plan.relative_path);
        let (_, dirs) = Utils::get_destination_path_and_dirs(path);

        if Utils::create_dirs(&dirs, &plan.relative_path, &emit_time, true)
            .await
            .is_err()
        {
            return None;
        }

        let current_hash = match Utils::copy_file(
            &plan.source_path,
            &plan.dest_path,
            &plan.relative_path,
            emit_time,
        )
        .await
        {
            Ok(hash) => Some(hash),
            Err(()) => return None,
        };

        Some(CopyOutcome {
            source_path: plan.source_path,
            source_type: plan.source_type,
            current_hash,
        })
    }

    async fn sync_directory(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        source_root: &Path,
        path_str: &str,
    ) {
        let relative_path = match source_relative_path(source_root) {
            Some(relative_path) => relative_path,
            None => {
                warn!(
                    "skipping directory outside source dir: '{}'",
                    Utils::fmt_path(source_root)
                );
                return;
            }
        };
        let dest_root = Utils::get_destination_path(&relative_path);

        if path_type(&dest_root).await.is_none()
            && Utils::create_dirs(&dest_root, path_str, &emit_time, false)
                .await
                .is_err()
        {
            return;
        }

        Self::write_in_file_store(file_store, source_root.to_path_buf(), PathType::Dir, None).await;

        let mut copy_plans = Vec::new();
        let mut seen_sources = HashSet::new();
        let mut dir_stack = vec![source_root.to_path_buf()];

        while let Some(current_dir) = dir_stack.pop() {
            seen_sources.insert(current_dir.clone());

            let mut entries = match fs::read_dir(&current_dir).await {
                Ok(entries) => entries,
                Err(err) => {
                    err!(
                        "failed to read dir '{}', error: {}",
                        Utils::fmt_path(&current_dir),
                        err
                    );
                    continue;
                }
            };

            loop {
                let entry = match entries.next_entry().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(err) => {
                        err!(
                            "failed to iterate dir '{}', error: {}",
                            Utils::fmt_path(&current_dir),
                            err
                        );
                        break;
                    }
                };

                let source_path = entry.path();

                if is_in_excluded_paths(&source_path) {
                    continue;
                }

                let child_relative = match source_relative_path(&source_path) {
                    Some(relative_path) => relative_path,
                    None => {
                        warn!(
                            "skipping path outside source dir: '{}'",
                            Utils::fmt_path(&source_path)
                        );
                        continue;
                    }
                };
                let child_path_str = child_relative.to_string_lossy().to_string();

                if Utils::args().no_temporary_editor_files && child_path_str.ends_with('~') {
                    continue;
                }

                let child_dest = Utils::get_destination_path(&child_relative);
                let file_type = match entry.file_type().await {
                    Ok(file_type) => file_type,
                    Err(err) => {
                        err!(
                            "failed to get file type for '{}', error: {}",
                            child_path_str,
                            err
                        );
                        continue;
                    }
                };

                seen_sources.insert(source_path.clone());

                if file_type.is_dir() {
                    if path_type(&child_dest).await.is_none()
                        && Utils::create_dirs(&child_dest, &child_path_str, &emit_time, false)
                            .await
                            .is_err()
                    {
                        continue;
                    }

                    Self::write_in_file_store(file_store, source_path.clone(), PathType::Dir, None)
                        .await;
                    dir_stack.push(source_path);
                    continue;
                }

                if !file_type.is_file() {
                    continue;
                }

                if let Some(plan) = Self::build_copy_plan_for_file(
                    file_store,
                    source_path,
                    child_dest,
                    child_path_str,
                )
                .await
                {
                    copy_plans.push(plan);
                }
            }
        }

        let copy_outcomes = Self::execute_copy_plans(copy_plans, emit_time).await;

        for outcome in copy_outcomes {
            Self::write_in_file_store(
                file_store,
                outcome.source_path,
                outcome.source_type,
                outcome.current_hash,
            )
            .await;
        }

        Self::remove_missing_entries(
            file_store,
            emit_time,
            source_root,
            &dest_root,
            &seen_sources,
        )
        .await;
    }

    async fn build_copy_plan_for_file(
        file_store: &LruCache<PathBuf, PathMetadata>,
        source_path: PathBuf,
        dest_path: PathBuf,
        relative_path: String,
    ) -> Option<CopyPlan> {
        let current_hash = Utils::hash_file(&source_path).await.ok();

        if let Some(path_metadata) = file_store.peek(&source_path)
            && path_metadata.path_type == PathType::File
            && current_hash.is_some()
            && current_hash == path_metadata.hash
            && path_type(&dest_path).await == Some(PathType::File)
        {
            return None;
        }

        if path_type(&dest_path).await == Some(PathType::File) {
            let dest_hash = Utils::hash_file(&dest_path).await.ok();
            if current_hash.is_some() && current_hash == dest_hash {
                return None;
            }
        }

        Some(CopyPlan {
            source_path,
            dest_path,
            relative_path,
            source_type: PathType::File,
        })
    }

    async fn remove_missing_entries(
        file_store: &mut LruCache<PathBuf, PathMetadata>,
        emit_time: Instant,
        source_root: &Path,
        dest_root: &Path,
        seen_sources: &HashSet<PathBuf>,
    ) {
        let mut dir_stack = vec![dest_root.to_path_buf()];

        while let Some(current_dest) = dir_stack.pop() {
            let mut entries = match fs::read_dir(&current_dest).await {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            loop {
                let entry = match entries.next_entry().await {
                    Ok(Some(entry)) => entry,
                    Ok(None) => break,
                    Err(_) => break,
                };

                let dest_path = entry.path();
                let relative_path = match dest_path.strip_prefix(&Utils::args().target_dir) {
                    Ok(relative_path) => relative_path,
                    Err(_) => continue,
                };

                let source_path = Utils::args().source_dir.join(relative_path);

                if is_in_excluded_paths(&source_path) {
                    continue;
                }

                if seen_sources.contains(&source_path) {
                    if entry
                        .file_type()
                        .await
                        .map(|ft| ft.is_dir())
                        .unwrap_or(false)
                    {
                        dir_stack.push(dest_path);
                    }
                    continue;
                }

                let entry_type = match entry.file_type().await {
                    Ok(file_type) => file_type,
                    Err(_) => continue,
                };

                let relative_str = relative_path.to_string_lossy().to_string();

                if entry_type.is_dir() {
                    if let Err(err) = fs::remove_dir_all(&dest_path).await {
                        handle_remove_err(err, &relative_str, PathType::Dir);
                    } else {
                        Utils::print_action("deleted", "dir", &relative_str, &emit_time);
                    }
                } else if entry_type.is_file() {
                    if let Err(err) = fs::remove_file(&dest_path).await {
                        handle_remove_err(err, &relative_str, PathType::File);
                    } else {
                        Utils::print_action("deleted", "file", &relative_str, &emit_time);
                    }
                }

                file_store.pop(&source_path);
            }
        }

        file_store.pop(source_root);
        Self::write_in_file_store(file_store, source_root.to_path_buf(), PathType::Dir, None).await;
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
        if path_type(&dirs).await.is_none()
            && Utils::create_dirs(&dirs, path_str, emit_time, true)
                .await
                .is_ok()
        {
            Self::write_in_file_store(file_store, dirs, PathType::Dir, None).await;
        }
    }
}

fn map_source_event_path(path: &Path) -> Option<(PathBuf, PathBuf, String)> {
    let source_path = path.to_path_buf();

    if is_in_excluded_paths(&source_path) {
        return None;
    }

    let relative_path = match source_relative_path(&source_path) {
        Some(relative_path) => relative_path,
        None => {
            warn!(
                "skipping event path outside source dir: '{}'",
                Utils::fmt_path(&source_path)
            );
            return None;
        }
    };

    let path_str = relative_path.to_string_lossy().to_string();
    if Utils::args().no_temporary_editor_files && path_str.ends_with('~') {
        return None;
    }

    Some((source_path, relative_path, path_str))
}

fn source_relative_path(path: &Path) -> Option<PathBuf> {
    if let Ok(relative_path) = path.strip_prefix(&Utils::args().source_dir) {
        return Some(relative_path.to_path_buf());
    }

    let verbatim_path = Utils::path_to_verbatim(path);
    let verbatim_source_dir = Utils::path_to_verbatim(&Utils::args().source_dir);

    verbatim_path
        .strip_prefix(verbatim_source_dir)
        .ok()
        .map(Path::to_path_buf)
}

async fn path_type(path: &Path) -> Option<PathType> {
    let metadata = fs::metadata(path).await.ok()?;

    if metadata.is_file() {
        Some(PathType::File)
    } else if metadata.is_dir() {
        Some(PathType::Dir)
    } else {
        None
    }
}

async fn remove_destination_path(dest_path: &Path, path_str: &str, emit_time: &Instant) {
    match path_type(dest_path).await {
        Some(PathType::File) => {
            if let Err(err) = fs::remove_file(dest_path).await {
                handle_remove_err(err, path_str, PathType::File);
            } else {
                Utils::print_action("deleted", "file", path_str, emit_time);
            }
        }
        Some(PathType::Dir) => {
            if let Err(err) = fs::remove_dir_all(dest_path).await {
                handle_remove_err(err, path_str, PathType::Dir);
            } else {
                Utils::print_action("deleted", "dir", path_str, emit_time);
            }
        }
        None => {}
    }
}

fn is_in_excluded_paths(path: &Path) -> bool {
    if Utils::excluded_paths().is_empty() {
        return false;
    }

    let verbatim_path = Utils::path_to_verbatim(path);

    for excluded_path in Utils::excluded_paths() {
        if path.starts_with(excluded_path)
            || verbatim_path.starts_with(Utils::path_to_verbatim(excluded_path).as_path())
        {
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

#[cfg(test)]
mod tests {
    use super::FileOperationsManager;
    use crate::utils::{PathMetadata, Utils};
    use crate::{Args, LOG_TRACE};
    use lru::LruCache;
    use notify::Event;
    use notify::EventKind;
    use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
    use std::fs;
    use std::num::NonZeroUsize;
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::time::Instant;

    struct TestRoots {
        source_root: PathBuf,
        target_root: PathBuf,
    }

    static TEST_ROOTS: OnceLock<TestRoots> = OnceLock::new();
    static CASE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn init_test_roots() -> &'static TestRoots {
        TEST_ROOTS.get_or_init(|| {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            let base = std::env::temp_dir().join(format!(
                "oxsync-tests-{}-{}",
                std::process::id(),
                unique
            ));
            let source_root = base.join("source");
            let target_root = base.join("target");

            fs::create_dir_all(&source_root).unwrap();
            fs::create_dir_all(&target_root).unwrap();

            LOG_TRACE.set(false).ok();
            Utils::set_excluded_paths(Vec::new());
            Utils::set_args(Args {
                source_dir: source_root.clone(),
                target_dir: target_root.clone(),
                exclude: Vec::new(),
                no_temporary_editor_files: false,
                no_creation_events: false,
                ide_mode: false,
                statistics: false,
                copy_parallelism: 4,
                trace: false,
            });

            TestRoots {
                source_root,
                target_root,
            }
        })
    }

    fn next_case_dirs(prefix: &str) -> (PathBuf, PathBuf) {
        let roots = init_test_roots();
        let case_id = CASE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let case_name = format!("{}-{}", prefix, case_id);
        let source_case = roots.source_root.join(&case_name);
        let target_case = roots.target_root.join(&case_name);

        if source_case.exists() {
            fs::remove_dir_all(&source_case).unwrap();
        }
        if target_case.exists() {
            fs::remove_dir_all(&target_case).unwrap();
        }

        fs::create_dir_all(&source_case).unwrap();

        (source_case, target_case)
    }

    fn new_store() -> LruCache<PathBuf, PathMetadata> {
        LruCache::new(NonZeroUsize::new(32_768).unwrap())
    }

    fn create_event(paths: impl IntoIterator<Item = PathBuf>) -> Event {
        let mut event = Event::new(EventKind::Create(CreateKind::Any));
        for path in paths {
            event = event.add_path(path);
        }
        event
    }

    fn modify_event(paths: impl IntoIterator<Item = PathBuf>) -> Event {
        let mut event = Event::new(EventKind::Modify(ModifyKind::Any));
        for path in paths {
            event = event.add_path(path);
        }
        event
    }

    fn remove_event(paths: impl IntoIterator<Item = PathBuf>) -> Event {
        let mut event = Event::new(EventKind::Remove(RemoveKind::Any));
        for path in paths {
            event = event.add_path(path);
        }
        event
    }

    fn rename_event(path: PathBuf, mode: RenameMode) -> Event {
        Event::new(EventKind::Modify(ModifyKind::Name(mode))).add_path(path)
    }

    fn write_small_file(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    #[tokio::test]
    async fn create_event_copies_ten_thousand_small_files_across_nested_directories() {
        let (source_case, target_case) = next_case_dirs("bulk-create");
        let mut store = new_store();

        let empty_dir = source_case.join("empty/a/b/c");
        fs::create_dir_all(&empty_dir).unwrap();

        let mut file_paths = Vec::with_capacity(10_000);
        for dir_index in 0..100 {
            for file_index in 0..100 {
                let path =
                    source_case.join(format!("tree/dir-{dir_index:03}/file-{file_index:03}.txt"));
                write_small_file(&path, &format!("{dir_index}:{file_index}"));
                file_paths.push(path);
            }
        }

        FileOperationsManager::create(
            &mut store,
            Instant::now(),
            create_event([empty_dir.clone()]),
        )
        .await;

        FileOperationsManager::create(&mut store, Instant::now(), create_event(file_paths.clone()))
            .await;

        assert!(target_case.join("empty/a/b/c").is_dir());
        assert_eq!(file_paths.len(), 10_000);
        for source_path in &file_paths {
            let relative = source_path.strip_prefix(&source_case).unwrap();
            let target_path = target_case.join(relative);
            assert!(
                target_path.is_file(),
                "missing target file {:?}",
                target_path
            );
            assert_eq!(
                fs::read(source_path).unwrap(),
                fs::read(target_path).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn modify_event_updates_nested_file_contents_without_touching_siblings() {
        let (source_case, target_case) = next_case_dirs("nested-modify");
        let mut store = new_store();

        let original = source_case.join("app/config/services/api.json");
        let sibling = source_case.join("app/config/services/web.json");
        write_small_file(&original, "{\"version\":1}");
        write_small_file(&sibling, "{\"stable\":true}");

        FileOperationsManager::create(
            &mut store,
            Instant::now(),
            create_event([original.clone(), sibling.clone()]),
        )
        .await;

        write_small_file(&original, "{\"version\":2,\"enabled\":true}");

        FileOperationsManager::copy(&mut store, Instant::now(), modify_event([original.clone()]))
            .await;

        let original_relative = original.strip_prefix(&source_case).unwrap();
        let sibling_relative = sibling.strip_prefix(&source_case).unwrap();

        assert_eq!(
            fs::read_to_string(target_case.join(original_relative)).unwrap(),
            "{\"version\":2,\"enabled\":true}"
        );
        assert_eq!(
            fs::read_to_string(target_case.join(sibling_relative)).unwrap(),
            "{\"stable\":true}"
        );
    }

    #[tokio::test]
    async fn directory_modify_event_syncs_new_files_added_under_existing_directory() {
        let (source_case, target_case) = next_case_dirs("dir-modify-create");
        let mut store = new_store();

        let existing = source_case.join("tools/parser/debug-template-parser.cpp");
        write_small_file(&existing, "int existing = 1;\n");

        FileOperationsManager::create(&mut store, Instant::now(), create_event([existing.clone()]))
            .await;

        let missing_before = target_case.join("tools/parser/CMakeLists.txt");
        assert!(!missing_before.exists());

        let added = source_case.join("tools/parser/CMakeLists.txt");
        write_small_file(&added, "add_library(parser)\n");

        let parent_dir = source_case.join("tools/parser");
        FileOperationsManager::copy(&mut store, Instant::now(), modify_event([parent_dir])).await;

        assert_eq!(
            fs::read_to_string(target_case.join("tools/parser/CMakeLists.txt")).unwrap(),
            "add_library(parser)\n"
        );
    }

    #[tokio::test]
    async fn remove_event_deletes_nested_directory_tree_from_target() {
        let (source_case, target_case) = next_case_dirs("nested-remove");
        let mut store = new_store();

        let nested_files = vec![
            source_case.join("packages/a/src/main.rs"),
            source_case.join("packages/a/src/lib.rs"),
            source_case.join("packages/a/tests/smoke.rs"),
        ];

        for (index, path) in nested_files.iter().enumerate() {
            write_small_file(path, &format!("fn f{}() {{}}\n", index));
        }

        FileOperationsManager::create(
            &mut store,
            Instant::now(),
            create_event(nested_files.clone()),
        )
        .await;

        let nested_dir = source_case.join("packages/a");
        fs::remove_dir_all(&nested_dir).unwrap();

        FileOperationsManager::remove(
            &mut store,
            Instant::now(),
            remove_event([nested_dir.clone()]),
        )
        .await;

        let target_dir = target_case.join("packages/a");
        assert!(!target_dir.exists(), "target directory should be removed");
    }

    #[tokio::test]
    async fn remove_event_continues_after_missing_target_path() {
        let (source_case, target_case) = next_case_dirs("remove-missing-continues");
        let mut store = new_store();

        let missing_source = source_case.join("already-gone.txt");
        let source_to_remove = source_case.join("remove-me.txt");
        let target_to_remove = target_case.join("remove-me.txt");
        write_small_file(&target_to_remove, "target content");

        FileOperationsManager::remove(
            &mut store,
            Instant::now(),
            remove_event([missing_source, source_to_remove]),
        )
        .await;

        assert!(!target_to_remove.exists());
    }

    #[tokio::test]
    async fn create_event_overwrites_stale_existing_target_file() {
        let (source_case, target_case) = next_case_dirs("create-overwrites-stale");
        let mut store = new_store();

        let source_file = source_case.join("settings.toml");
        let target_file = target_case.join("settings.toml");
        write_small_file(&source_file, "fresh = true\n");
        write_small_file(&target_file, "fresh = false\n");

        FileOperationsManager::create(
            &mut store,
            Instant::now(),
            create_event([source_file.clone()]),
        )
        .await;

        assert_eq!(fs::read_to_string(target_file).unwrap(), "fresh = true\n");
    }

    #[tokio::test]
    async fn rename_event_migrates_source_keyed_file_metadata() {
        let (source_case, target_case) = next_case_dirs("rename-file-metadata");
        let mut store = new_store();

        let old_source = source_case.join("old.txt");
        let new_source = source_case.join("new.txt");
        let old_target = target_case.join("old.txt");
        let new_target = target_case.join("new.txt");
        write_small_file(&old_source, "renamed");

        FileOperationsManager::create(
            &mut store,
            Instant::now(),
            create_event([old_source.clone()]),
        )
        .await;

        fs::rename(&old_source, &new_source).unwrap();
        let mut rename_from = None;
        FileOperationsManager::rename(
            &mut store,
            Instant::now(),
            rename_event(old_source.clone(), RenameMode::From),
            &mut rename_from,
        )
        .await;

        FileOperationsManager::rename(
            &mut store,
            Instant::now(),
            rename_event(new_source.clone(), RenameMode::To),
            &mut rename_from,
        )
        .await;

        assert!(new_target.is_file());
        assert!(!old_target.exists());
        assert!(store.peek(&new_source).is_some());
        assert!(store.peek(&old_source).is_none());
    }

    #[tokio::test]
    async fn rename_event_falls_back_to_copy_when_target_rename_fails() {
        let (source_case, target_case) = next_case_dirs("rename-fallback-copy");
        let mut store = new_store();

        let old_source = source_case.join("missing-target-old.txt");
        let new_source = source_case.join("new.txt");
        let new_target = target_case.join("new.txt");
        write_small_file(&new_source, "fallback content");

        let mut rename_from = None;
        FileOperationsManager::rename(
            &mut store,
            Instant::now(),
            rename_event(old_source, RenameMode::From),
            &mut rename_from,
        )
        .await;
        FileOperationsManager::rename(
            &mut store,
            Instant::now(),
            rename_event(new_source.clone(), RenameMode::To),
            &mut rename_from,
        )
        .await;

        assert_eq!(fs::read_to_string(new_target).unwrap(), "fallback content");
        assert!(store.peek(&new_source).is_some());
    }
}
