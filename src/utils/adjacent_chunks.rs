/// Iterate over adjacent (consecutive) runs, further limited by `max_len`.
pub struct AdjacentChunks<'a> {
    v: &'a [u64],
    max_len: usize,
    start: usize,
}

impl<'a> AdjacentChunks<'a> {
    pub fn new(v: &'a [u64], max_len: usize) -> Self {
        Self {
            v,
            max_len,
            start: 0,
        }
    }
}

impl<'a> Iterator for AdjacentChunks<'a> {
    type Item = &'a [u64];

    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.v.len() {
            return None;
        }

        let mut end = self.start + 1;

        // Extend the slice while numbers stay consecutive and we don't exceed max_len
        while end < self.v.len()
            && (end - self.start) < self.max_len
            && self.v[end] == self.v[end - 1].saturating_add(1)
        {
            end += 1;
        }

        let chunk = &self.v[self.start..end];
        self.start = end;
        Some(chunk)
    }
}

/// Convenience helper
pub fn adjacent_chunks<'a>(v: &'a [u64], max_len: usize) -> AdjacentChunks<'a> {
    AdjacentChunks::new(v, max_len)
}
