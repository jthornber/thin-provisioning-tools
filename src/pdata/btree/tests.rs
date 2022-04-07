use super::*;

//------------------------------------------

fn mk_random_leaf(nr_entries: usize) -> Node<u64> {
    use rand::{thread_rng, Rng};

    let mut keys = vec![0u64; nr_entries];
    let mut values = vec![0u64; nr_entries];
    thread_rng().fill(&mut keys[..]);
    thread_rng().fill(&mut values[..]);
    keys.sort_unstable();

    Node::Leaf {
        header: NodeHeader {
            block: 0,
            is_leaf: true,
            nr_entries: nr_entries as u32,
            max_entries: calc_max_entries::<u64>() as u32,
            value_size: u64::disk_size(),
        },
        keys,
        values,
    }
}

#[test]
fn pack_unpack_empty_node() {
    let node = mk_random_leaf(0);

    let mut buffer: Vec<u8> = Vec::with_capacity(BLOCK_SIZE);
    let mut cursor = std::io::Cursor::new(&mut buffer);
    assert!(pack_node(&node, &mut cursor).is_ok());

    let path = vec![0];
    let unpacked = unpack_node::<u64>(&path, buffer.as_slice(), false, true).unwrap();
    assert!(
        matches!(unpacked, Node::Leaf {header, keys, values} if header.eq(node.get_header()) && keys.is_empty() && values.is_empty())
    );
}

#[test]
fn pack_unpack_fully_populated_node() {
    let node = mk_random_leaf(calc_max_entries::<u64>());
    let v = if let Node::Leaf { ref values, .. } = node {
        values.clone()
    } else {
        Vec::new()
    };

    let mut buffer: Vec<u8> = Vec::with_capacity(BLOCK_SIZE);
    let mut cursor = std::io::Cursor::new(&mut buffer);
    assert!(pack_node(&node, &mut cursor).is_ok());

    let path = vec![0];
    let unpacked = unpack_node::<u64>(&path, buffer.as_slice(), false, true).unwrap();
    assert!(
        matches!(unpacked, Node::Leaf {header, keys, values} if header.eq(node.get_header()) && keys.eq(node.get_keys()) && values.eq(&v))
    );
}

//------------------------------------------
