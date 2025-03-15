pub mod index;
pub mod index_reader;
pub mod index_view;
pub mod index_writer;
pub mod indexes_mut;
pub mod read_boundary;

/// offset: 4 bytes, position: 4 bytes, timestamp: 8 bytes
pub const INDEX_SIZE: u32 = 16;

pub use index::IggyIndex;
pub use index_reader::IndexReader;
pub use index_view::IggyIndexView;
pub use index_writer::IndexWriter;
pub use indexes_mut::IggyIndexesMut;
pub use read_boundary::ReadBoundary;
