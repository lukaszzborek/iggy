use bytes::{Bytes, BytesMut};
use error_set::ErrContext;
use iggy::{
    error::IggyError,
    models::messaging::{IggyIndexes, IggyMessagesBatch},
    prelude::IggyTimestamp,
};
use std::{
    fs::{File, OpenOptions},
    os::{fd::AsRawFd, unix::prelude::FileExt},
    time::Duration,
};
use std::{
    io::ErrorKind,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    task::{spawn_blocking, JoinHandle},
    time::interval,
};
use tracing::{debug, error, trace};

/// A dedicated struct for reading from the log file.
#[derive(Debug)]
pub struct MessagesReader {
    file_path: String,
    file: Arc<File>,
    log_size_bytes: Arc<AtomicU64>,
    last_access: Arc<AtomicU64>,
    cache_advisor_task: Option<JoinHandle<()>>,
}

impl MessagesReader {
    /// Opens the log file in read mode.
    pub async fn new(file_path: &str, log_size_bytes: Arc<AtomicU64>) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .read(true)
            .open(file_path)
            .with_error_context(|error| format!("Failed to open log file: {file_path}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        let actual_log_size = file
            .metadata()
            .with_error_context(|error| {
                format!("Failed to get metadata of log file: {file_path}. {error}")
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        // posix_fadvise() doesn't exist on MacOS
        #[cfg(not(target_os = "macos"))]
        {
            use std::os::unix::io::AsRawFd;
            let fd = file.as_raw_fd();
            let _ = nix::fcntl::posix_fadvise(
                fd,
                0,
                actual_log_size as i64,
                nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL,
            )
            .with_info_context(|error| {
                format!("Failed to set sequential access pattern on log file: {file_path}. {error}")
            });
        }

        log_size_bytes.store(actual_log_size, Ordering::Release);

        trace!("Opened messages file for reading: {file_path}, size: {actual_log_size}");

        let mut reader = Self {
            file_path: file_path.to_string(),
            file: Arc::new(file),
            log_size_bytes,
            last_access: Arc::new(AtomicU64::new(IggyTimestamp::now().as_micros())),
            cache_advisor_task: None,
        };

        reader.start_cache_advisor_task();
        Ok(reader)
    }

    /// Loads and returns all message IDs from the log file.
    pub async fn load_all_message_ids_from_disk(
        &self,
        indexes: IggyIndexes,
        messages_count: u32,
    ) -> Result<Vec<u128>, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            trace!("Messages file {} is empty.", self.file_path);
            return Ok(vec![]);
        }

        let messages_bytes = match self.read_bytes_at(0, file_size).await {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => return Ok(vec![]),
            Err(error) => {
                error!(
                    "Error reading {messages_count} messages at position 0 in file {} of size {}: {error}",
                    self.file_path, file_size
                );
                return Err(IggyError::CannotReadMessage);
            }
        };

        let messages = IggyMessagesBatch::new(indexes, messages_bytes, messages_count);
        let mut ids = Vec::with_capacity(messages_count as usize);

        for message in messages.iter() {
            ids.push(message.header().id());
        }

        Ok(ids)
    }

    /// Loads and returns a batch of messages from the log file.
    pub async fn load_messages_from_disk(
        &self,
        indexes: IggyIndexes,
    ) -> Result<IggyMessagesBatch, IggyError> {
        let file_size = self.file_size();
        if file_size == 0 {
            trace!("Messages file {} is empty.", self.file_path);
            return Ok(IggyMessagesBatch::empty());
        }

        let start_pos = indexes.base_position();
        let count_bytes = indexes.messages_size();
        let messages_count = indexes.count();

        let messages_bytes = match self.read_bytes_at(start_pos as u64, count_bytes).await {
            Ok(buf) => buf,
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                return Ok(IggyMessagesBatch::empty())
            }
            Err(error) => {
                error!(
                    "Error reading {messages_count} messages at position {start_pos} in file {} of size {}: {error}",
                    self.file_path, file_size
                );
                return Err(IggyError::CannotReadMessage);
            }
        };

        Ok(IggyMessagesBatch::new(
            indexes,
            messages_bytes,
            messages_count,
        ))
    }

    /// Returns the size of the log file in bytes.
    pub fn file_size(&self) -> u32 {
        self.log_size_bytes.load(Ordering::Acquire) as u32
    }

    /// Reads `len` bytes from the log file at the specified `offset`.
    async fn read_bytes_at(&self, offset: u64, len: u32) -> Result<Bytes, std::io::Error> {
        self.last_access
            .store(IggyTimestamp::now().as_micros(), Ordering::Release);
        let file = self.file.clone();
        spawn_blocking(move || {
            let mut buf = BytesMut::with_capacity(len as usize);
            unsafe { buf.set_len(len as usize) };
            file.read_exact_at(&mut buf, offset)?;
            Ok(buf.freeze())
        })
        .await?
    }

    /// Starts a background task that manages file caching based on access patterns
    fn start_cache_advisor_task(&mut self) {
        let file = self.file.clone();
        let file_path = self.file_path.clone();
        let last_access = self.last_access.clone();
        let log_size_bytes = self.log_size_bytes.clone();

        const SEQUENTIAL_ACCESS_THRESHOLD_SECS: u64 = 1;
        const DONTNEED_THRESHOLD_SECS: u64 = 5;
        const CHECK_INTERVAL_SECS: u64 = 1;

        let task = tokio::spawn(async move {
            let mut last_advice: Option<nix::fcntl::PosixFadviseAdvice> = None;
            let mut interval = interval(Duration::from_secs(CHECK_INTERVAL_SECS));

            #[cfg(not(target_os = "macos"))]
            loop {
                interval.tick().await;

                let now = IggyTimestamp::now().as_micros();
                let last_access_time = last_access.load(Ordering::Acquire);
                let elapsed_secs = (now - last_access_time) / 1_000_000;

                let fd = file.as_raw_fd();
                let file_size = log_size_bytes.load(Ordering::Acquire);

                let advice = if elapsed_secs < SEQUENTIAL_ACCESS_THRESHOLD_SECS {
                    Some(nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL)
                } else if elapsed_secs > DONTNEED_THRESHOLD_SECS {
                    Some(nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED)
                } else {
                    None
                };

                if let Some(adv) = advice {
                    if last_advice != Some(adv) {
                        match nix::fcntl::posix_fadvise(fd, 0, file_size as i64, adv) {
                            Ok(_) => {
                                if adv == nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED {
                                    trace!(
                                        "Set DONTNEED advice on file: {}, no access for {}s",
                                        file_path,
                                        elapsed_secs
                                    );
                                } else if adv
                                    == nix::fcntl::PosixFadviseAdvice::POSIX_FADV_SEQUENTIAL
                                {
                                    trace!(
                                        "Set SEQUENTIAL advice on file: {}, recent access {}s ago",
                                        file_path,
                                        elapsed_secs
                                    );
                                }
                                last_advice = Some(adv);
                            }
                            Err(e) => {
                                debug!("Failed to set posix_fadvise on file {}: {}", file_path, e);
                            }
                        }
                    }
                }
            }

            #[cfg(target_os = "macos")]
            loop {
                interval.tick().await;
            }
        });

        self.cache_advisor_task = Some(task);
    }
}
