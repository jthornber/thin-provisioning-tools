use super::*;

use mockall::mock;
use std::sync::atomic::Ordering;

use crate::checksum;

//------------------------------------------

mock! {
    Vio {}
    impl VectoredIo for Vio {
        fn read_vectored_at(&self, bufs: &mut [libc::iovec], pos: u64) -> io::Result<usize>;
        fn write_vectored_at(&self, bufs: &[libc::iovec], pos: u64) -> io::Result<usize>;
    }
}

fn allocate_test_blocks(blocknr: &[u64]) -> Vec<Block> {
    blocknr
        .iter()
        .map(|&bn| {
            let b = Block::zeroed(bn);
            stamp(b.get_data(), bn);
            b
        })
        .collect()
}

fn stamp(buf: &mut [u8], blocknr: u64) {
    buf[8..16].copy_from_slice(&blocknr.to_le_bytes());
    assert!(checksum::write_checksum(buf, checksum::BT::NODE).is_ok());
}

fn verify(buf: &[u8], blocknr: u64) {
    assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), blocknr);
    assert_eq!(checksum::metadata_block_type(buf), checksum::BT::NODE);
}

// Test the write_many_() helper sends iovec properly
fn test_write_many(blocks: &[Block]) -> io::Result<()> {
    let mut v = MockVio::new();

    let runs = find_runs_nogap(blocks, libc::UIO_MAXIOV as usize);
    let batch = std::sync::atomic::AtomicUsize::default();
    let blocks_issued = std::sync::atomic::AtomicUsize::default();
    let data: Vec<&mut [u8]> = blocks.iter().map(|b| b.get_data()).collect();

    v.expect_write_vectored_at()
        .times(runs.len())
        .withf(move |bufs, pos| {
            let batch = batch.fetch_add(1, Ordering::SeqCst);
            let (blocknr, batch_len) = runs[batch];
            let blocks_issued = blocks_issued.fetch_add(batch_len, Ordering::SeqCst);

            assert_eq!(*pos, blocknr * BLOCK_SIZE as u64);
            assert_eq!(bufs.len(), batch_len);

            for ((buf, addr), bn) in bufs
                .iter()
                .zip(&data[blocks_issued..])
                .zip(blocknr..blocknr + batch_len as u64)
            {
                assert_eq!(buf.iov_base, addr.as_ptr() as *mut _);
                assert_eq!(buf.iov_len, BLOCK_SIZE);
                let slice =
                    unsafe { std::slice::from_raw_parts(buf.iov_base as *const u8, BLOCK_SIZE) };
                verify(slice, bn);
            }

            true
        })
        .returning(|bufs, _| Ok(bufs.iter().map(|buf| buf.iov_len).sum()));

    let results = SyncIoEngine::write_many_(v.into(), blocks)?;
    assert_eq!(results.len(), blocks.len());

    Ok(())
}

//------------------------------------------

#[test]
fn test_write_many_empty() -> Result<()> {
    test_write_many(&[])?;
    Ok(())
}

#[test]
fn test_write_many_contiguous() -> Result<()> {
    let blocks = allocate_test_blocks(&[10, 11, 12, 13, 14]);
    test_write_many(&blocks)?;
    Ok(())
}

#[test]
fn test_write_many_with_gaps() -> Result<()> {
    let blocks = allocate_test_blocks(&[10, 11, 22, 23, 24, 35, 36, 37, 38]);
    test_write_many(&blocks)?;
    Ok(())
}

#[test]
fn test_long_run() -> Result<()> {
    let mut blocknr: Vec<u64> = vec![1, 2, 3];
    (100..(100 + libc::UIO_MAXIOV as u64)).for_each(|bn| blocknr.push(bn));
    let blocks = allocate_test_blocks(&blocknr);
    test_write_many(&blocks)?;
    Ok(())
}

#[test]
fn test_split_long_run() -> Result<()> {
    let mut blocknr: Vec<u64> = vec![1, 2, 3];
    (100..(100 + libc::UIO_MAXIOV as u64 * 2)).for_each(|bn| blocknr.push(bn));
    let blocks = allocate_test_blocks(&blocknr);
    test_write_many(&blocks)?;
    Ok(())
}

//------------------------------------------
