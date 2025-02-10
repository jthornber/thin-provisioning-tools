use std::alloc::{alloc, dealloc, Layout};
use std::vec::Vec;

//--------------------------------

#[derive(Clone)]
pub struct IOBlock {
    pub loc: u64,
    pub data: *mut u8,
}

// SAFETY: Block contains a raw pointer, but we ensure it's used safely.
unsafe impl Send for IOBlock {}

//--------------------------------

pub struct BufferPool {
    nr_blocks: usize,
    block_size: usize,
    buffer: *mut u8,
    blocks: Vec<IOBlock>,
}

impl BufferPool {
    fn layout(nr_blocks: usize, block_size: usize) -> Layout {
        Layout::from_size_align(block_size * nr_blocks, block_size).unwrap()
    }

    pub fn new(nr_blocks: usize, block_size: usize) -> Self {
        let layout = Self::layout(nr_blocks, block_size);
        let buffer = unsafe { alloc(layout) };

        if buffer.is_null() {
            panic!("Failed to allocate memory for BufferPool");
        }

        let mut blocks = Vec::with_capacity(nr_blocks);
        for i in 0..nr_blocks {
            blocks.push(IOBlock {
                loc: 0,
                data: unsafe { buffer.add(i * block_size) },
            });
        }

        BufferPool {
            nr_blocks,
            block_size,
            buffer,
            blocks,
        }
    }

    pub fn get(&mut self, loc: u64) -> Option<IOBlock> {
        let b = self.blocks.pop();
        b.map(|b| IOBlock { loc, data: b.data })
    }

    pub fn put(&mut self, block: IOBlock) {
        self.blocks.push(block);
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let layout = Self::layout(self.nr_blocks, self.block_size);
        unsafe { dealloc(self.buffer, layout) };
    }
}
