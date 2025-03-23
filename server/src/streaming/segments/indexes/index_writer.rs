use error_set::ErrContext;
use iggy::error::IggyError;
use iggy::models::messaging::INDEX_SIZE;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};
use tracing::trace;

/// A dedicated struct for writing to the index file.
#[derive(Debug)]
pub struct IndexWriter {
    file_path: String,
    file: File,
    index_size_bytes: Arc<AtomicU64>,
    fsync: bool,
}

impl IndexWriter {
    /// Opens the index file in write mode.
    pub async fn new(
        file_path: &str,
        index_size_bytes: Arc<AtomicU64>,
        fsync: bool,
    ) -> Result<Self, IggyError> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)
            .await
            .with_error_context(|error| format!("Failed to open index file: {file_path}. {error}"))
            .map_err(|_| IggyError::CannotReadFile)?;

        let _ = file.sync_all().await.with_error_context(|error| {
            format!("Failed to fsync index file after creation: {file_path}. {error}",)
        });

        let actual_index_size = file
            .metadata()
            .await
            .with_error_context(|error| {
                format!("Failed to get metadata of index file: {file_path}. {error}")
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len();

        index_size_bytes.store(actual_index_size, Ordering::Release);

        trace!("Opened index file for writing: {file_path}, size: {actual_index_size}");

        Ok(Self {
            file_path: file_path.to_string(),
            file,
            index_size_bytes,
            fsync,
        })
    }

    /// Appends multiple index buffer to the index file in a single operation.
    pub async fn save_indexes(&mut self, indexes: &[u8]) -> Result<(), IggyError> {
        if indexes.is_empty() {
            return Ok(());
        }

        let count = indexes.len() / INDEX_SIZE;

        self.file
            .write_all(indexes)
            .await
            .with_error_context(|error| {
                format!(
                    "Failed to write {} indexes to file: {}. {error}",
                    count, self.file_path
                )
            })
            .map_err(|_| IggyError::CannotSaveIndexToSegment)?;

        if self.fsync {
            let _ = self.fsync().await;
        }

        self.index_size_bytes
            .fetch_add(indexes.len() as u64, Ordering::Release);

        trace!("XXXX Saved {count} indexes to file: {}", self.file_path);

        Ok(())
    }

    pub async fn fsync(&self) -> Result<(), IggyError> {
        self.file
            .sync_all()
            .await
            .with_error_context(|error| {
                format!("Failed to fsync index file: {}. {error}", self.file_path)
            })
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }
}
