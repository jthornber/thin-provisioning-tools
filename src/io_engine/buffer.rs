use std::alloc::{alloc, dealloc, Layout};

// Because we use O_DIRECT we need to use page aligned blocks.  Buffer
// manages allocation of this aligned memory.
pub struct Buffer {
    size: usize,
    align: usize,
    data: *mut u8,
}

impl Buffer {
    pub fn new(size: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(size, align).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "out of memory");

        Self {
            size,
            align,
            data: ptr,
        }
    }

    pub fn get_data<'a>(&self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut::<'a>(self.data, self.size) }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.size, self.align).unwrap();
        unsafe {
            dealloc(self.data, layout);
        }
    }
}

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

//------------------------------------------
