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
