use fixedbitset::FixedBitSet;
use std::alloc::{alloc, dealloc, Layout};
use std::io;

//-----------------------------------------

pub struct SendPtr(pub *mut u8);

unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}

//-----------------------------------------

const ALIGN: usize = 4096;

pub struct Buffer {
    pub data: SendPtr,
    pub len: usize,
}

impl Buffer {
    pub fn new(len: usize) -> io::Result<Buffer> {
        let layout = Layout::from_size_align(len, ALIGN).unwrap();
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "out of memory"));
        }
        Ok(Buffer {
            data: SendPtr(ptr),
            len,
        })
    }

    pub fn get_data<'a>(&self) -> &'a mut [u8] {
        unsafe { std::slice::from_raw_parts_mut::<'a>(self.data.0, self.len) }
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.len, ALIGN).unwrap();
        unsafe {
            dealloc(self.data.0, layout);
        }
    }
}

//-----------------------------------------

#[inline]
fn div_rem(x: usize, d: usize) -> (usize, usize) {
    (x / d, x % d)
}

fn set_first_bit(bits: &mut [u32], len: usize) -> Option<usize> {
    let mut pos = 0;
    for block in bits {
        if *block != u32::MAX {
            let off = block.trailing_ones();
            pos += off as usize;
            if pos >= len {
                return None; // out of bounds
            }
            *block |= 1 << off;
            return Some(pos);
        }
        pos += 32;
    }
    None
}

// returns the old value
fn unset_bit(bits: &mut [u32], bit: usize) -> io::Result<bool> {
    let (i, rem) = div_rem(bit, 32);
    if i >= bits.len() {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    }
    let mask = 1 << rem;
    let old = (bits[i] & mask) != 0;
    bits[i] &= !mask;
    Ok(old)
}

#[cfg(test)]
mod bitset_tests {
    use super::*;
    use crate::math::*;

    #[test]
    fn test_exhaust_bits() {
        let nr_bits: usize = 50;
        let mut bits = vec![0u32; div_up(nr_bits, 32)];
        for i in 0..nr_bits {
            assert_eq!(set_first_bit(&mut bits, nr_bits), Some(i));
        }
        assert!(set_first_bit(&mut bits, nr_bits).is_none());
    }

    #[test]
    fn test_unset_bits() {
        let nr_bits: usize = 50;
        let mut bits = vec![0u32; div_up(nr_bits, 32)];
        for _i in 0..nr_bits {
            assert!(set_first_bit(&mut bits, nr_bits).is_some());
        }
        assert!(matches!(unset_bit(&mut bits, 0), Ok(true)));
        assert!(matches!(unset_bit(&mut bits, 31), Ok(true)));
        assert!(matches!(unset_bit(&mut bits, 32), Ok(true)));
        assert!(matches!(unset_bit(&mut bits, 49), Ok(true)));
    }

    #[test]
    fn test_unset_out_of_bounds() {
        let nr_bits: usize = 50;
        let mut bits = vec![0u32; div_up(nr_bits, 32)];
        assert!(matches!(unset_bit(&mut bits, 49), Ok(false)));

        // allowed since it's not exceeding the array size
        assert!(matches!(unset_bit(&mut bits, 50), Ok(false)));

        // out-of-bounds access is disallowed
        assert!(matches!(unset_bit(&mut bits, 64), Err(_)));
    }

    #[test]
    fn test_set_after_unset() {
        let nr_bits: usize = 50;
        let mut bits = vec![0u32; div_up(nr_bits, 32)];
        for _i in 0..nr_bits {
            assert!(set_first_bit(&mut bits, nr_bits).is_some());
        }
        assert!(matches!(unset_bit(&mut bits, 0), Ok(true)));
        assert!(matches!(unset_bit(&mut bits, 31), Ok(true)));
        assert!(matches!(unset_bit(&mut bits, 32), Ok(true)));
        assert_eq!(set_first_bit(&mut bits, nr_bits), Some(0));
        assert_eq!(set_first_bit(&mut bits, nr_bits), Some(31));
        assert_eq!(set_first_bit(&mut bits, nr_bits), Some(32));
    }

    #[test]
    fn test_unset_multiple_times() {
        let nr_bits: usize = 50;
        let mut bits = vec![0u32; div_up(nr_bits, 32)];
        assert!(set_first_bit(&mut bits, nr_bits).is_some());
        assert!(matches!(unset_bit(&mut bits, 0), Ok(true)));
        assert!(matches!(unset_bit(&mut bits, 0), Ok(false)));
    }
}

//-----------------------------------------

pub struct AllocBlock {
    pub data: *mut u8,
    index: usize,
}

pub struct MemPool {
    mem: Vec<Buffer>,
    alloc_bits: FixedBitSet,
}

impl MemPool {
    pub fn new(block_size: usize, nr_blocks: usize) -> io::Result<Self> {
        let mut mem = Vec::new();
        for _i in 0..nr_blocks {
            mem.push(Buffer::new(block_size)?);
        }
        let alloc_bits = FixedBitSet::with_capacity(nr_blocks);

        Ok(MemPool { mem, alloc_bits })
    }

    pub fn alloc(&mut self) -> Option<AllocBlock> {
        let len = self.alloc_bits.len();
        if let Some(pos) = set_first_bit(self.alloc_bits.as_mut_slice(), len) {
            Some(AllocBlock {
                data: self.mem[pos].data.0,
                index: pos,
            })
        } else {
            None
        }
    }

    pub fn free(&mut self, b: AllocBlock) -> io::Result<()> {
        if b.index >= self.mem.len() || b.data != self.mem[b.index].data.0 {
            return Err(io::Error::new(io::ErrorKind::Other, "invalid address"));
        }
        unset_bit(self.alloc_bits.as_mut_slice(), b.index).and_then(|old| {
            if !old {
                return Err(io::Error::new(io::ErrorKind::Other, "double free"));
            }
            Ok(())
        })
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
            let pool = MemPool::new(bs, nr_blocks);
            assert!(pool.is_ok())
        }
    }

    #[test]
    fn alloc_free_cycle() {
        let mut pool = MemPool::new(512, 1024).expect("out of memory");
        for _ in 0..10000 {
            let b = pool.alloc();
            assert!(b.is_some());
            pool.free(b.unwrap()).expect("pool free");
        }
    }

    #[test]
    fn alloc_after_free() {
        let bs = 512;
        let nr_blocks = 100;
        let mut pool = MemPool::new(bs, nr_blocks).expect("out of memory");

        let mut blocks = Vec::new();
        for _ in 0..nr_blocks {
            let b = pool.alloc();
            assert!(b.is_some());
            blocks.push(b.unwrap());
        }

        for b in blocks {
            pool.free(b).expect("pool free");
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
        let mut pool = MemPool::new(bs, nr_blocks).expect("out of memory");

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
        let mut pool = MemPool::new(bs, nr_blocks).expect("out of memory");

        for _ in 0..nr_blocks {
            let b = pool.alloc();
            assert!(b.is_some());
            unsafe {
                std::ptr::write_bytes(b.unwrap().data, 0u8, bs);
            }
        }

        let b = pool.alloc();
        assert!(b.is_none());
    }
}

//-----------------------------------------
