use std::path::Path;

use clap::Parser;
use notify::{Config, Event, RecommendedWatcher, Watcher};
use tokio::fs::canonicalize;
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::utils::Utils;
use crate::{Args, LOG_TRACE};

pub(crate) struct Start;

impl Start {
    pub async fn parse_args() -> Result<(), String> {
        let mut args = Args::parse();

        LOG_TRACE
            .set(args.trace)
            .map_err(|_| "trace logging was initialized more than once".to_string())?;

        if !Path::new(&args.source_dir).exists() {
            return Err(format!(
                "source dir '{}' does not exist",
                Utils::fmt_path(&args.source_dir)
            ));
        }

        if !Path::new(&args.target_dir).exists() {
            return Err(format!(
                "target dir '{}' does not exist",
                Utils::fmt_path(&args.target_dir)
            ));
        }

        if args.copy_parallelism == 0 {
            return Err("copy parallelism must be greater than 0".to_string());
        }

        args.source_dir = canonicalize(Path::new(&args.source_dir))
            .await
            .map_err(|_| {
                format!(
                    "unable to convert source dir '{}' to a valid path",
                    Utils::fmt_path(&args.source_dir)
                )
            })?;

        args.target_dir = canonicalize(Path::new(&args.target_dir))
            .await
            .map_err(|_| {
                format!(
                    "unable to convert target dir '{}' to a valid path",
                    Utils::fmt_path(&args.target_dir)
                )
            })?;

        let mut excluded_paths = Self::build_excluded_paths(&mut args);

        excluded_paths.shrink_to_fit();
        Utils::set_excluded_paths(excluded_paths);

        Utils::set_args(args);
        Ok(())
    }

    pub fn fs_watcher() -> notify::Result<(
        RecommendedWatcher,
        UnboundedReceiverStream<notify::Result<Event>>,
    )> {
        let (tx, rx) = unbounded_channel();

        // Automatically select the best implementation for your platform.
        // You can also access each implementation directly e.g. INotifyWatcher.
        let watcher = RecommendedWatcher::new(move |res| tx.send(res).unwrap(), Config::default())?;

        Ok((watcher, UnboundedReceiverStream::new(rx)))
    }

    fn build_excluded_paths(args: &mut Args) -> Vec<std::path::PathBuf> {
        let mut excluded_paths = Vec::new();

        for path in &args.exclude {
            let full_path = if !path.starts_with(&args.source_dir) {
                args.source_dir.as_path().join(path)
            } else {
                path.to_path_buf()
            };

            excluded_paths.push(Utils::path_to_verbatim(&full_path));
        }

        if args.ide_mode {
            excluded_paths.push(Utils::path_to_verbatim(&args.source_dir.join(".idea")));
            excluded_paths.push(Utils::path_to_verbatim(&args.source_dir.join(".git")));
            args.no_temporary_editor_files = true;
        }

        excluded_paths
    }
}

#[cfg(test)]
mod tests {
    use super::Start;
    use crate::Args;
    use std::path::PathBuf;

    #[test]
    fn ide_mode_keeps_creation_events_enabled() {
        let source_dir = PathBuf::from("/tmp/source");
        let mut args = Args {
            source_dir: source_dir.clone(),
            target_dir: PathBuf::from("/tmp/target"),
            exclude: Vec::new(),
            no_temporary_editor_files: false,
            no_creation_events: false,
            ide_mode: true,
            statistics: false,
            copy_parallelism: 1,
            trace: false,
        };

        let excluded_paths = Start::build_excluded_paths(&mut args);

        assert!(args.no_temporary_editor_files);
        assert!(!args.no_creation_events);
        assert!(excluded_paths.contains(&source_dir.join(".git")));
        assert!(excluded_paths.contains(&source_dir.join(".idea")));
    }
}
