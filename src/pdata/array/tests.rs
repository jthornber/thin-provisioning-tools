use super::*;

//------------------------------------------

fn mk_random_block(nr_entries: usize) -> ArrayBlock<u64> {
    use rand::{thread_rng, Rng};

    let mut values = vec![0u64; nr_entries];
    thread_rng().fill(&mut values[..]);

    ArrayBlock {
        header: ArrayBlockHeader {
            max_entries: calc_max_entries::<u64>() as u32,
            nr_entries: nr_entries as u32,
            value_size: u64::disk_size(),
            blocknr: 0,
        },
        values,
    }
}

#[test]
fn pack_unpack_empty_block() {
    let origin = mk_random_block(0);

    let mut buffer: Vec<u8> = Vec::with_capacity(BLOCK_SIZE);
    let mut cursor = std::io::Cursor::new(&mut buffer);
    assert!(pack_array_block(&origin, &mut cursor).is_ok());

    let path = vec![0];
    let unpacked = unpack_array_block::<u64>(&path, buffer.as_slice()).unwrap();
    assert_eq!(unpacked.header, origin.header);
    assert!(unpacked.values.is_empty());
}

#[test]
fn pack_unpack_fully_populated_block() {
    let origin = mk_random_block(calc_max_entries::<u64>());

    let mut buffer: Vec<u8> = Vec::with_capacity(BLOCK_SIZE);
    let mut cursor = std::io::Cursor::new(&mut buffer);
    assert!(pack_array_block(&origin, &mut cursor).is_ok());

    let path = vec![0];
    let unpacked = unpack_array_block::<u64>(&path, buffer.as_slice()).unwrap();
    assert_eq!(unpacked.header, origin.header);
    assert_eq!(unpacked.values, origin.values);
}

//------------------------------------------
