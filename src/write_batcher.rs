use anyhow::{anyhow, Result};
use rangemap::RangeSet;
use std::sync::{Arc, Mutex};

use crate::checksum;
use crate::io_engine::*;
use crate::pdata::space_map::*;

//------------------------------------------

#[derive(Clone)]
pub struct WriteBatcher {
    pub engine: Arc<dyn IoEngine + Send + Sync>,

    // FIXME: this doesn't need to be in a mutex
    pub sm: Arc<Mutex<dyn SpaceMap>>,

    batch_size: usize,
    queue: Vec<Block>,

    // The allocations could be a hint of potentially modified blocks
    allocations: RangeSet<u64>,
}

impl WriteBatcher {
    pub fn new(
        engine: Arc<dyn IoEngine + Send + Sync>,
        sm: Arc<Mutex<dyn SpaceMap>>,
        batch_size: usize,
    ) -> WriteBatcher {
        WriteBatcher {
            engine,
            sm,
            batch_size,
            queue: Vec::with_capacity(batch_size),
            allocations: RangeSet::<u64>::new(),
        }
    }

    pub fn alloc(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = sm.alloc()?;
        if b.is_none() {
            return Err(anyhow!("out of metadata space"));
        }

        let loc = b.unwrap();
        self.allocations.insert(std::ops::Range {
            start: loc,
            end: loc + 1,
        });

        Ok(Block::new(loc))
    }

    pub fn alloc_zeroed(&mut self) -> Result<Block> {
        let mut sm = self.sm.lock().unwrap();
        let b = sm.alloc()?;
        if b.is_none() {
            return Err(anyhow!("out of metadata space"));
        }

        let loc = b.unwrap();
        self.allocations.insert(std::ops::Range {
            start: loc,
            end: loc + 1,
        });

        Ok(Block::zeroed(loc))
    }

    pub fn clear_allocations(&mut self) -> RangeSet<u64> {
        let mut tmp = RangeSet::<u64>::new();
        std::mem::swap(&mut tmp, &mut self.allocations);
        tmp
    }

    pub fn write(&mut self, b: Block, kind: checksum::BT) -> Result<()> {
        checksum::write_checksum(b.get_data(), kind)?;

        for blk in self.queue.iter().rev() {
            if blk.loc == b.loc {
                // write hit
                blk.get_data().copy_from_slice(b.get_data());
                return Ok(());
            }
        }

        if self.queue.len() == self.batch_size {
            let mut tmp = Vec::new();
            std::mem::swap(&mut tmp, &mut self.queue);
            self.flush_(tmp)?;
        }

        self.queue.push(b);
        Ok(())
    }

    pub fn read(&mut self, blocknr: u64) -> Result<Block> {
        for b in self.queue.iter().rev() {
            if b.loc == blocknr {
                let r = Block::new(b.loc);
                r.get_data().copy_from_slice(b.get_data());
                return Ok(r);
            }
        }

        self.engine
            .read(blocknr)
            .map_err(|_| anyhow!("read block error"))
    }

    fn flush_(&mut self, queue: Vec<Block>) -> Result<()> {
        self.engine.write_many(&queue)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.queue.is_empty() {
            return Ok(());
        }
        let mut tmp = Vec::new();
        std::mem::swap(&mut tmp, &mut self.queue);
        self.flush_(tmp)?;
        Ok(())
    }
}

impl Drop for WriteBatcher {
    fn drop(&mut self) {
        assert!(self.flush().is_ok());
    }
}

//------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use mockall::*;
    //use rand::seq::SliceRandom;
    use rand::prelude::*;
    use std::io;

    use checksum::BT;

    const NR_BLOCKS: u64 = 65536;

    mock! {
        Engine {}
        impl IoEngine for Engine {
            fn get_nr_blocks(&self) -> u64;
            fn get_batch_size(&self) -> usize;
            fn read(&self, b: u64) -> io::Result<Block>;
            fn read_many(&self, blocks: &[u64]) -> io::Result<Vec<io::Result<Block>>>;
            fn write(&self, block: &Block) -> io::Result<()>;
            fn write_many(&self, blocks: &[Block]) -> io::Result<Vec<io::Result<()>>>;
        }
    }

    mock! {
        TestSpaceMap {}
        impl SpaceMap for TestSpaceMap {
            fn get_nr_blocks(&self) -> Result<u64>;
            fn get_nr_allocated(&self) -> Result<u64>;
            fn get(&self, b: u64) -> Result<u32>;
            fn set(&mut self, b: u64, v: u32) -> Result<u32>;
            fn inc(&mut self, begin: u64, len: u64) -> Result<()>;
            fn alloc(&mut self) -> Result<Option<u64>>;
            fn find_free(&mut self, begin: u64, end: u64) -> Result<Option<u64>>;
            fn get_alloc_begin(&self) -> Result<u64>;
        }
    }

    #[test]
    fn runs_out_of_space_should_fail() {
        let engine = Arc::new(MockEngine::new());
        let sm = Arc::new(Mutex::new(CoreSpaceMap::<u8>::new(NR_BLOCKS)));
        let mut w = WriteBatcher::new(engine, sm, 16);
        for _i in 0..NR_BLOCKS {
            assert!(w.alloc().is_ok());
        }
        assert!(w.alloc().is_err());
    }

    #[test]
    fn allocated_ranges_should_be_coalesced() {
        let mut sm = MockTestSpaceMap::new();
        let mut blocks: Vec<u64> = (0..65536).collect();
        blocks.shuffle(&mut rand::thread_rng());
        let mut i = 0;
        sm.expect_alloc().times(65536).returning(move || {
            let b = blocks[i];
            i += 1;
            Ok(Some(b))
        });

        let engine = Arc::new(MockEngine::new());
        let mut w = WriteBatcher::new(engine, Arc::new(Mutex::new(sm)), 16);
        for _ in 0..65536 {
            assert!(w.alloc().is_ok());
        }

        let allocations = w.clear_allocations();
        let mut iter = allocations.iter();
        let range = iter.next().unwrap();
        assert_eq!(*range, (0..65536));
        assert!(iter.next().is_none());
    }

    #[test]
    fn writes_should_be_performed_in_batch() {
        let mut engine = MockEngine::new();
        engine
            .expect_write_many()
            .withf(|blocks: &[Block]| blocks.len() == 16)
            .times(3)
            .returning(|_| {
                let mut ret = Vec::new();
                ret.resize_with(16, || Ok(()));
                Ok(ret)
            });

        let sm = Arc::new(Mutex::new(MockTestSpaceMap::new()));

        let mut w = WriteBatcher::new(Arc::new(engine), sm, 16);
        for i in 0..48 {
            let b = Block::zeroed(i);
            assert!(w.write(b, BT::NODE).is_ok());
        }
    }

    #[test]
    fn write_hit() {
        let mut src = vec![0u8; 4096];
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        rng.fill_bytes(&mut src[4..]);
        assert!(checksum::write_checksum(&mut src[..], BT::NODE).is_ok());

        let expected = src.clone();
        let mut engine = MockEngine::new();
        engine
            .expect_write_many()
            .withf(move |blocks: &[Block]| {
                if blocks.len() != 1 {
                    return false;
                }

                let b = blocks.iter().last().unwrap();
                b.get_data()[..] == expected[..]
            })
            .times(1)
            .returning(|_| Ok(vec![Ok(())]));

        let sm = Arc::new(Mutex::new(MockTestSpaceMap::new()));
        let mut w = WriteBatcher::new(Arc::new(engine), sm, 16);

        let loc = 123;
        let b = Block::zeroed(loc);
        assert!(w.write(b, BT::NODE).is_ok());

        let b = Block::zeroed(loc);
        b.get_data()[4..].copy_from_slice(&src[4..]);
        assert!(w.write(b, BT::NODE).is_ok());

        assert!(w.flush().is_ok());
    }

    #[test]
    fn read_hit() {
        let mut src = vec![0u8; 4096];
        let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);
        rng.fill_bytes(&mut src[4..]);
        assert!(checksum::write_checksum(&mut src[..], BT::NODE).is_ok());

        let expected = src.clone();
        let mut engine = MockEngine::new();
        engine
            .expect_write_many()
            .withf(move |blocks: &[Block]| {
                if blocks.len() != 1 {
                    return false;
                }

                let b = blocks.iter().last().unwrap();
                b.get_data()[..] == expected[..]
            })
            .times(1)
            .returning(|_| Ok(vec![Ok(())]));

        let sm = Arc::new(Mutex::new(MockTestSpaceMap::new()));
        let mut w = WriteBatcher::new(Arc::new(engine), sm, 16);

        let loc = 123;
        let b = Block::zeroed(loc);
        b.get_data()[4..].copy_from_slice(&mut src[4..]);
        assert!(w.write(b, BT::NODE).is_ok());

        let actual = w.read(loc).unwrap();
        assert_eq!(actual.get_data()[..], src[..]);
    }
}

//------------------------------------------
