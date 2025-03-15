use std::fmt;

pub struct ReadBoundary {
    pub start_position: u32,
    pub bytes: u32,
    pub messages_count: u32,
}

impl ReadBoundary {
    pub fn new(start_position: u32, bytes: u32, messages_count: u32) -> Self {
        Self {
            start_position,
            bytes,
            messages_count,
        }
    }
}

impl fmt::Debug for ReadBoundary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReadBoundary(pos={}, bytes={}, count={})",
            self.start_position, self.bytes, self.messages_count
        )
    }
}
