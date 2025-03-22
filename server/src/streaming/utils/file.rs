use std::path::Path;
use tokio::fs::{remove_file, File, OpenOptions};

pub async fn open(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).open(path).await
}

pub async fn append(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().read(true).append(true).open(path).await
}

pub async fn overwrite(path: &str) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
        .await
}
pub async fn remove(path: &str) -> Result<(), std::io::Error> {
    remove_file(path).await
}

pub async fn rename(old_path: &str, new_path: &str) -> Result<(), std::io::Error> {
    tokio::fs::rename(Path::new(old_path), Path::new(new_path)).await
}

pub async fn exists(path: &str) -> Result<bool, std::io::Error> {
    tokio::fs::try_exists(path).await
}
