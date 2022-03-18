use super::*;

//------------------------------------------

pub fn push_values<V: Pack + Unpack + Clone>(
    builder: &mut NodeBuilder<V>,
    w: &mut WriteBatcher,
    mappings: &[(u64, V)],
) {
    for (k, v) in mappings {
        assert!(builder.push_value(w, *k, v.clone()).is_ok());
    }
}

pub fn build_leaves<V: Pack + Unpack + Clone>(
    w: &mut WriteBatcher,
    mappings: &[(u64, V)],
    shared: bool,
) -> Vec<NodeSummary> {
    let mut builder = NodeBuilder::<V>::new(Box::new(LeafIO {}), Box::new(NoopRC {}), shared);
    push_values(&mut builder, w, mappings);
    let leaves = builder.complete(w).unwrap();
    assert!(w.flush().is_ok());
    leaves
}

//------------------------------------------
