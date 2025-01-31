use super::*;

use mockall::*;
use rand::prelude::*;
use std::io;

use checksum::BT;

//-----------------------------------------

const NR_BLOCKS: u64 = 65536;

mock! {
    Engine {}
    impl IoEngine for Engine {
        fn get_nr_blocks(&self) -> u64;
        fn get_batch_size(&self) -> usize;
        fn suggest_nr_threads(&self) -> usize;
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
    let mut b_iter = blocks.into_iter();
    sm.expect_alloc().times(65536).returning(move || {
        let b = b_iter.next().unwrap();
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
    type SeedType = <SmallRng as rand::SeedableRng>::Seed;

    let mut src = vec![0u8; 4096];
    let mut rng = SmallRng::from_seed(SeedType::default());
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
            b.get_data() == expected
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
    type SeedType = <SmallRng as rand::SeedableRng>::Seed;

    let mut src = vec![0u8; 4096];
    let mut rng = SmallRng::from_seed(SeedType::default());
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
    b.get_data()[4..].copy_from_slice(&src[4..]);
    assert!(w.write(b, BT::NODE).is_ok());

    let actual = w.read(loc).unwrap();
    assert_eq!(actual.get_data(), src);
}

//-----------------------------------------
