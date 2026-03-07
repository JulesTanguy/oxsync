use std::path::{Path, PathBuf};
use std::time::SystemTime;

use blake3::{hash, Hash};
use lru::LruCache;
use notify::event::{ModifyKind, RenameMode};
use notify::Event;
use notify::EventKind::Modify;
use tokio::fs;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::utils::{PathType, Utils};
use crate::{err, info, PathMetadata};

pub(crate) struct FileOperationsManager;

#[derive(Clone)]
struct CopyPlan {
    source_path: PathBuf,
    dest_path: PathBuf,
    relative_path: String,
    source_type: PathType,
    current_hash: Option<Hash>,
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
            let v_path = Utils::path_to_verbatim(&src_path);

            if is_in_excluded_paths(&v_path) {
                continue;
            }

            let path_str = v_path
                .strip_prefix(&Utils::args().source_dir)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            if Utils::args().no_temporary_editor_files && path_str.ends_with('~') {
                continue;
            }

            let relative_path = v_path.strip_prefix(&Utils::args().source_dir).unwrap();
            let (dest_path, _) = Utils::get_destination_path_and_dirs(relative_path);

            if let Some(path_metadata) = file_store.get(&v_path) {
                match path_metadata.path_type {
                    PathType::Dir => {
                        if !dest_path.is_dir()
                            && Utils::create_dirs(&dest_path, &path_str, &emit_time, false)
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
                            copy_plans.push(CopyPlan {
                                source_path: v_path,
                                dest_path,
                                relative_path: path_str.clone(),
                                source_type: PathType::File,
                                current_hash: None,
                            });
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
                        } else if !file_is_identical {
                            copy_plans.push(CopyPlan {
                                source_path: v_path,
                                dest_path,
                                relative_path: path_str.clone(),
                                source_type: PathType::File,
                                current_hash,
                            });
                        }
                    }
                }
                continue;
            }

            if v_path.is_file() {
                copy_plans.push(CopyPlan {
                    source_path: v_path,
                    dest_path,
                    relative_path: path_str.clone(),
                    source_type: PathType::File,
                    current_hash: None,
                });
                continue;
            }

            if v_path.is_dir()
                && !dest_path.is_dir()
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
            let v_path = Utils::path_to_verbatim(&src_path);

            if is_in_excluded_paths(&v_path) {
                continue;
            }
            let path_str = v_path
                .strip_prefix(&Utils::args().source_dir)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            if Utils::args().no_temporary_editor_files && path_str.ends_with('~') {
                continue;
            }

            let relative_path = v_path.strip_prefix(&Utils::args().source_dir).unwrap();
            let dest_path = Utils::get_destination_path(relative_path);

            if !dest_path.exists() {
                return;
            } else if dest_path.is_file() {
                if let Err(err) = fs::remove_file(dest_path).await {
                    handle_remove_err(err, &path_str, PathType::File);
                } else {
                    Utils::print_action("deleted", "file", &path_str, &emit_time);
                };
                file_store.pop(&v_path);
            } else if dest_path.is_dir() {
                if let Err(err) = fs::remove_dir_all(dest_path).await {
                    handle_remove_err(err, &path_str, PathType::Dir);
                } else {
                    Utils::print_action("deleted", "dir", &path_str, &emit_time);
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
                .unwrap()
                .to_string();

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

                            Utils::print_action("renamed", path_type_str, &path_str, &emit_time);

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
        let mut copy_plans = Vec::new();

        for src_path in event.paths {
            let v_path = Utils::path_to_verbatim(&src_path);

            if is_in_excluded_paths(&v_path) {
                continue;
            }

            let path_str = v_path
                .strip_prefix(&Utils::args().source_dir)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            if Utils::args().no_temporary_editor_files && path_str.ends_with('~') {
                continue;
            }

            let relative_path = v_path.strip_prefix(&Utils::args().source_dir).unwrap();
            let (dest_path, dirs) = Utils::get_destination_path_and_dirs(relative_path);

            if file_store.get(&v_path).is_some() {
                continue;
            }

            if v_path.is_file() && !dest_path.exists() {
                let current_hash = if let Ok(file_content) = fs::read(&v_path).await {
                    Some(hash(&file_content))
                } else {
                    None
                };

                copy_plans.push(CopyPlan {
                    source_path: v_path,
                    dest_path,
                    relative_path: path_str.to_string(),
                    source_type: PathType::File,
                    current_hash,
                });
                continue;
            }

            if v_path.is_dir() && !dest_path.exists() {
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

    async fn run_copy_plan(plan: CopyPlan, emit_time: Instant) -> Option<CopyOutcome> {
        let path = Path::new(&plan.relative_path);
        let (_, dirs) = Utils::get_destination_path_and_dirs(path);

        if Utils::create_dirs(&dirs, &plan.relative_path, &emit_time, true)
            .await
            .is_err()
        {
            return None;
        }

        if Utils::copy_file(
            &plan.source_path,
            &plan.dest_path,
            &plan.relative_path,
            emit_time,
        )
        .await
        .is_err()
        {
            return None;
        }

        Some(CopyOutcome {
            source_path: plan.source_path,
            source_type: plan.source_type,
            current_hash: plan.current_hash,
        })
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

#[cfg(test)]
mod tests {
    use super::FileOperationsManager;
    use crate::utils::{PathMetadata, Utils};
    use crate::{Args, LOG_TRACE};
    use lru::LruCache;
    use notify::event::{CreateKind, ModifyKind, RemoveKind};
    use notify::Event;
    use notify::EventKind;
    use std::fs;
    use std::num::NonZeroUsize;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::OnceLock;
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
}
