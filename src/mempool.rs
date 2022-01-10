use std::alloc::{alloc, dealloc, Layout};
use std::cell::Cell;
use std::io;
use std::ptr;

//-----------------------------------------

pub struct AllocBlock {
    pub data: *mut u8,
    pub len: usize,
}

pub struct MemPool {
    free_list: Cell<*mut u8>,
    block_size: usize, // bytes
    alignment: usize,  // bytes
}

impl MemPool {
    pub fn new(block_size: usize, nr_blocks: usize, alignment: usize) -> io::Result<Self> {
        let mut head = ptr::null_mut();

        for _i in 0..nr_blocks {
            let layout = Layout::from_size_align(block_size, alignment).unwrap();
            let ptr = unsafe { alloc(layout) };
            if ptr.is_null() {
                Self::dealloc_free_list(head, block_size, alignment);
                return Err(io::Error::new(io::ErrorKind::OutOfMemory, "out of memory"));
            }

            unsafe {
                *(ptr as *mut *mut u8) = head;
            }
            head = ptr;
        }

        Ok(MemPool {
            free_list: Cell::new(head),
            block_size,
            alignment,
        })
    }

    fn dealloc_free_list(mut head: *mut u8, block_size: usize, alignment: usize) {
        while !head.is_null() {
            let layout = Layout::from_size_align(block_size, alignment).unwrap();
            unsafe {
                let next = *(head as *mut *mut u8);
                dealloc(head, layout);
                head = next;
            }
        }
    }

    pub fn alloc(&mut self) -> Option<AllocBlock> {
        let head = self.free_list.get();
        if head.is_null() {
            return None;
        }
        unsafe {
            let next = *(head as *mut *mut u8);
            self.free_list.set(next);
        }
        Some(AllocBlock {
            data: head,
            len: self.block_size,
        })
    }

    pub fn free(&mut self, b: AllocBlock) {
        if b.data.is_null() {
            return;
        }
        unsafe {
            *(b.data as *mut *mut u8) = self.free_list.get();
        }
        self.free_list.set(b.data);
    }
}

impl Drop for MemPool {
    fn drop(&mut self) {
        Self::dealloc_free_list(self.free_list.get(), self.block_size, self.alignment);
    }
}

//-----------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_destroy() {
        let pool_size = 524_288;
        for bs in [64, 128, 256, 512] {
            let nr_blocks = pool_size / bs;
            let pool = MemPool::new(bs, nr_blocks, bs);
            assert!(pool.is_ok())
        }
    }

    #[test]
    fn alloc_free_cycle() {
        let mut pool = MemPool::new(512, 1024, 512).expect("out of memory");
        for _ in 0..10000 {
            let b = pool.alloc();
            assert!(b.is_some());
            pool.free(b.unwrap());
        }
    }

    #[test]
    fn alloc_after_free() {
        let bs = 512;
        let nr_blocks = 100;
        let mut pool = MemPool::new(bs, nr_blocks, bs).expect("out of memory");

        let mut blocks = Vec::new();
        for _ in 0..nr_blocks {
            let b = pool.alloc();
            assert!(b.is_some());
            blocks.push(b.unwrap());
        }

        for b in blocks {
            pool.free(b);
        }

        for _ in 0..nr_blocks {
            let b = pool.alloc();
            assert!(b.is_some());
        }
    }

    #[test]
    fn exhaust_pool() {
        let bs = 512;
        let nr_blocks = 100;
        let mut pool = MemPool::new(bs, nr_blocks, bs).expect("out of memory");

        for _ in 0..nr_blocks {
            let b = pool.alloc();
            assert!(b.is_some());
        }

        let b = pool.alloc();
        assert!(b.is_none());
    }

    #[test]
    fn data_can_be_written() {
        let bs = 512;
        let nr_blocks = 100;
        let mut pool = MemPool::new(bs, nr_blocks, bs).expect("out of memory");

        for _ in 0..nr_blocks {
            let b = pool.alloc();
            assert!(b.is_some());
            unsafe {
                ptr::write_bytes(b.unwrap().data, 0u8, bs);
            }
        }

        let b = pool.alloc();
        assert!(b.is_none());
    }
}

//-----------------------------------------
