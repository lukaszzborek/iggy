mod index;
mod index_reader;
mod index_writer;

/// offset: 4 bytes, position: 4 bytes, timestamp: 8 bytes
pub const INDEX_SIZE: u32 = 16;

pub use index::Index;
pub use index_reader::IndexReader;
pub use index_writer::IndexWriter;
