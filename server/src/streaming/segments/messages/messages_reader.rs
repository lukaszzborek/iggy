use bytes::{Bytes, BytesMut};
use error_set::ErrContext;
use iggy::{
    error::IggyError,
    models::messaging::{IggyIndexes, IggyMessagesBatch},
};
use std::{
    fs::{File, OpenOptions},
    os::unix::prelude::FileExt,
};
use std::{
    io::ErrorKind,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::task::spawn_blocking;
use tracing::{error, trace};

/// A dedicated struct for reading from the log file.
#[derive(Debug)]
pub struct MessagesReader {
    file_path: String,
    file: Arc<File>,
    log_size_bytes: Arc<AtomicU64>,
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

        Ok(Self {
            file_path: file_path.to_string(),
            file: Arc::new(file),
            log_size_bytes,
        })
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

        let messages_bytes = match self
            .read_bytes_at(start_pos as u64, count_bytes as u64)
            .await
        {
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
    fn file_size(&self) -> u64 {
        self.log_size_bytes.load(Ordering::Acquire)
    }

    /// Reads `len` bytes from the log file at the specified `offset`.
    async fn read_bytes_at(&self, offset: u64, len: u64) -> Result<Bytes, std::io::Error> {
        let file = self.file.clone();
        spawn_blocking(move || {
            let mut buf = BytesMut::with_capacity(len as usize);
            unsafe { buf.set_len(len as usize) };
            file.read_exact_at(&mut buf, offset)?;
            Ok(buf.freeze())
        })
        .await?
    }
}
