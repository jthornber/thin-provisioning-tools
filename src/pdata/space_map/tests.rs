use super::*;
use rand::Rng;

//------------------------------------------

fn test_get_nr_blocks(sm: &dyn SpaceMap, expected: u64) {
    assert_eq!(sm.get_nr_blocks().unwrap(), expected);
}

fn test_get_nr_allocated(sm: &mut dyn SpaceMap) {
    assert_eq!(sm.get_nr_allocated().unwrap(), 0);

    let nr_blocks = sm.get_nr_blocks().unwrap();
    for i in 0..nr_blocks {
        assert!(matches!(sm.alloc(), Ok(Some(_))));
        assert_eq!(sm.get_nr_allocated().unwrap(), i + 1);
    }

    for i in (0..nr_blocks).rev() {
        assert_eq!(sm.set(i, 0).unwrap(), 1);
        assert_eq!(sm.get_nr_allocated().unwrap(), i);
    }
}

fn test_runs_out_of_space(sm: &mut dyn SpaceMap) {
    let nr_blocks = sm.get_nr_blocks().unwrap();
    for _i in 0..nr_blocks {
        assert!(matches!(sm.alloc(), Ok(Some(_))));
    }
    assert!(matches!(sm.alloc(), Ok(None)));
}

fn test_inc_and_dec(sm: &mut dyn SpaceMap) {
    let nr_blocks = sm.get_nr_blocks().unwrap();
    let b = rand::thread_rng().gen_range(0..nr_blocks);

    for i in 0..255 {
        assert_eq!(sm.get(b).unwrap(), i);
        assert!(sm.inc(b, 1).is_ok());
    }

    for i in (2..=255).rev() {
        assert_eq!(sm.get(b).unwrap(), i);
        assert!(!sm.dec(b).unwrap());
    }

    assert_eq!(sm.get(b).unwrap(), 1);
    assert!(sm.dec(b).unwrap());
    assert_eq!(sm.get(b).unwrap(), 0);
}

fn test_not_allocated_twice(sm: &mut dyn SpaceMap) {
    let mb = sm.alloc().unwrap();
    assert!(mb.is_some());
    let first = mb.unwrap();

    loop {
        match sm.alloc() {
            Ok(Some(b)) => {
                assert_ne!(b, first);
            }
            Ok(None) => break,
            Err(e) => panic!("{}", e),
        }
    }
}

fn test_set_affects_nr_allocated(sm: &mut dyn SpaceMap) {
    let nr_blocks = sm.get_nr_blocks().unwrap();

    for i in 0..nr_blocks {
        assert_eq!(sm.set(i, 1).unwrap(), 0);
        assert_eq!(sm.get_nr_allocated().unwrap(), i + 1);
    }

    for i in 0..nr_blocks {
        assert_eq!(sm.set(i, 0).unwrap(), 1);
        assert_eq!(sm.get_nr_allocated().unwrap(), nr_blocks - i - 1);
    }
}

// 1. Verify the space map allocates blocks in round-robin fashion
// 2. Verify that freeing a block doesn't change the allocation address
fn test_wraparound_allocation(sm: &mut dyn SpaceMap) {
    let nr_blocks = sm.get_nr_blocks().unwrap();
    const BATCH_SIZE: u64 = 1000;
    assert!(nr_blocks > BATCH_SIZE);

    let loop_count = crate::math::div_up(nr_blocks, BATCH_SIZE);
    let mut batch_begin = 0;

    for _ in 0..loop_count {
        for i in batch_begin..(batch_begin + BATCH_SIZE) {
            let expected = i % nr_blocks;
            let actual = sm.alloc().unwrap();
            assert_eq!(expected, actual.unwrap());
        }

        for i in batch_begin..(batch_begin + BATCH_SIZE) {
            assert_eq!(sm.set(i % nr_blocks, 0).unwrap(), 1);
        }

        batch_begin += BATCH_SIZE;
    }
}

mod core_sm_u8 {
    use super::*;
    const NR_BLOCKS: u64 = 65536;

    #[test]
    fn get_nr_blocks() {
        let sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
        tests::test_get_nr_blocks(&sm, NR_BLOCKS);
    }

    #[test]
    fn get_nr_allocated() {
        let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
        tests::test_get_nr_allocated(&mut sm);
    }

    #[test]
    fn runs_out_of_space() {
        let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
        tests::test_runs_out_of_space(&mut sm);
    }

    #[test]
    fn inc_and_dec() {
        let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
        tests::test_inc_and_dec(&mut sm);
    }

    #[test]
    fn not_allocated_twice() {
        let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
        tests::test_not_allocated_twice(&mut sm);
    }

    #[test]
    fn set_affects_nr_allocated() {
        let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
        tests::test_set_affects_nr_allocated(&mut sm);
    }

    #[test]
    fn wraparound_allocation() {
        let mut sm = CoreSpaceMap::<u8>::new(NR_BLOCKS);
        tests::test_wraparound_allocation(&mut sm);
    }
}

//------------------------------------------

mod metadata_sm {
    use anyhow::{ensure, Result};
    use std::sync::Arc;

    use crate::io_engine::core::CoreIoEngine;
    use crate::io_engine::*;
    use crate::math::div_up;
    use crate::pdata::space_map::common::ENTRIES_PER_BITMAP;
    use crate::pdata::space_map::metadata::*;
    use crate::write_batcher::WriteBatcher;

    fn check_index_entries(nr_blocks: u64) -> Result<()> {
        let engine = Arc::new(CoreIoEngine::new(nr_blocks));
        let meta_sm = core_metadata_sm(engine.get_nr_blocks(), u32::MAX);

        let mut w = WriteBatcher::new(engine.clone(), meta_sm.clone(), engine.get_batch_size());
        w.alloc()?; // reserved for the superblock
        let root = write_metadata_sm(&mut w)?;

        let b = engine.read(root.bitmap_root)?;
        let entries = check_and_unpack_metadata_index(&b)?.indexes;
        ensure!(entries.len() as u64 == div_up(nr_blocks, ENTRIES_PER_BITMAP as u64));

        // the number of blocks observed by index_entries must be multiple of ENTRIES_PER_BITMAP
        let nr_allocated = meta_sm.lock().unwrap().get_nr_allocated()?;
        let nr_free: u64 = entries.iter().map(|ie| ie.nr_free as u64).sum();
        ensure!(nr_allocated + nr_free == (entries.len() * ENTRIES_PER_BITMAP) as u64);

        Ok(())
    }

    #[test]
    fn check_single_index_entry() -> Result<()> {
        check_index_entries(1000)
    }

    #[test]
    fn check_multiple_index_entries() -> Result<()> {
        check_index_entries(ENTRIES_PER_BITMAP as u64 * 16 + 1000)
    }
}

//------------------------------------------

mod disk_sm {
    use anyhow::{ensure, Result};
    use std::ops::Deref;
    use std::sync::Arc;

    use crate::io_engine::core::CoreIoEngine;
    use crate::io_engine::*;
    use crate::math::div_up;
    use crate::pdata::btree_walker::btree_to_value_vec;
    use crate::pdata::space_map::common::{IndexEntry, ENTRIES_PER_BITMAP};
    use crate::pdata::space_map::disk::*;
    use crate::pdata::space_map::metadata::*;
    use crate::pdata::space_map::*;
    use crate::write_batcher::WriteBatcher;

    fn check_index_entries(nr_blocks: u64) -> Result<()> {
        let engine = Arc::new(CoreIoEngine::new(1024));
        let meta_sm = core_metadata_sm(engine.get_nr_blocks(), u32::MAX);

        let mut w = WriteBatcher::new(engine.clone(), meta_sm.clone(), engine.get_batch_size());
        w.alloc()?; // reserved for the superblock

        let data_sm = core_sm(nr_blocks, u32::MAX);
        data_sm.lock().unwrap().inc(0, 100)?;

        let root = write_disk_sm(&mut w, data_sm.lock().unwrap().deref())?;

        let entries =
            btree_to_value_vec::<IndexEntry>(&mut Vec::new(), engine, false, root.bitmap_root)?;
        ensure!(entries.len() as u64 == div_up(nr_blocks, ENTRIES_PER_BITMAP as u64));

        // the number of blocks observed by index_entries must be a multiple of ENTRIES_PER_BITMAP
        let nr_allocated = data_sm.lock().unwrap().get_nr_allocated()?;
        let nr_free: u64 = entries.iter().map(|ie| ie.nr_free as u64).sum();
        ensure!(nr_allocated + nr_free == (entries.len() * ENTRIES_PER_BITMAP) as u64);

        Ok(())
    }

    #[test]
    fn check_single_index_entry() -> Result<()> {
        check_index_entries(1000)
    }

    #[test]
    fn check_multiple_index_entries() -> Result<()> {
        check_index_entries(ENTRIES_PER_BITMAP as u64 * 16 + 1000)
    }
}

//------------------------------------------
