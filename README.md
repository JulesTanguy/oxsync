# oxsync
Sync changes from a directory to another

[![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/JulesTanguy/oxsync/rust.yml?logo=github)](https://github.com/JulesTanguy/oxsync/actions/workflows/rust.yml)
[![Crates.io Version](https://img.shields.io/crates/v/oxsync)](https://crates.io/crates/oxsync)
[![GitHub License](https://img.shields.io/github/license/JulesTanguy/oxsync)](https://github.com/JulesTanguy/oxsync/blob/main/LICENSE)
```
Sync changes from a directory to another

Usage: oxsync [OPTIONS] <SOURCE_DIR> <TARGET_DIR>

Arguments:
  <SOURCE_DIR>  Path of the directory to watch changes from
  <TARGET_DIR>  Path of the directory to write changes to

Options:
  -e, --exclude <EXCLUDE>          Exclude file or dir from the <SOURCE_DIR>, can be used multiple times
      --no-temporary-editor-files  Exclude files with names ending by a tilde `~` [aliases: no-tmp]
      --no-creation-events         Ignore creation events [aliases: no-create]
      --ide-mode                   Exclude `.git`, `.idea` dirs + enables `no-temporary-editor-files` [aliases: ide]
      --statistics                 Display the time spent copying the file [aliases: stats]
      --copy-parallelism <COPY_PARALLELISM>
                                   Maximum number of files to copy concurrently [default: 1]
      --trace                      Set the log level to trace
  -h, --help                       Print help
  -V, --version                    Print version
```

## Purpose
oxsync is geared towards enabling fast, local reads with a remote filesystem.
The conventional setup advices to initiate with a copy of local files and directories on the remote machine,
while the tool monitors and synchronizes the modifications performed when the program is running.

## Features
- Real-time "watch for changes" functionality for near immediate synchronization.
- CLI interface with intuitive commands.
- Local copy of remote directories for quick reads.
- Handle big and small files
- An "exclude" argument
- Validated on Linux and intended to work across platforms supported by `notify`

## Installation
```sh
# Beforehand make sure to have a functional Rust compiler and 
# the Cargo package manager installed
cargo install oxsync
```

## Acknowledgements
As always, feel free to look at the `dependencies` of the `Cargo.toml` file at the root of the repository. It provides a comprehensive list 
of all the libraries that play a crucial part in the development of this project.
