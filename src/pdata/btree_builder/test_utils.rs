use super::*;

//------------------------------------------

#[derive(Clone)]
pub struct NodeInfo {
    pub block: u64,
    pub key_range: KeyRange,
    pub entries_begin: u64,
    pub nr_entries: usize,
    pub height: usize,
}

pub struct BTreeLayout {
    height: usize,
    nodes: Vec<NodeInfo>,
    nr_entries: Vec<u64>, // numbers of children at different heights
}

impl BTreeLayout {
    fn new() -> BTreeLayout {
        BTreeLayout {
            height: 0,
            nodes: Vec::new(),
            nr_entries: vec![0],
        }
    }

    fn push_node(&mut self, n: &NodeSummary) {
        if let Some(last) = self.nodes.last_mut() {
            if last.height == self.height {
                last.key_range.end = Some(n.key);
            }
        }

        let nr_entries = self.nr_entries.last_mut().unwrap();
        self.nodes.push(NodeInfo {
            block: n.block,
            key_range: KeyRange {
                start: Some(n.key),
                end: None,
            },
            entries_begin: *nr_entries,
            nr_entries: n.nr_entries,
            height: self.height,
        });

        *nr_entries += n.nr_entries as u64;
    }

    fn increase_height(&mut self) {
        self.height += 1;
        self.nr_entries.push(0);
    }

    fn complete(&mut self) {
        // there's no parent key for a root node
        self.nodes.last_mut().unwrap().key_range.start = None;
    }

    pub fn height(&self) -> usize {
        self.height
    }

    pub fn nr_nodes(&self) -> u64 {
        self.nodes.len() as u64
    }

    pub fn nr_leaves(&self) -> u64 {
        if self.height == 0 {
            1
        } else {
            self.nr_entries[1]
        }
    }

    pub fn root(&self) -> NodeInfo {
        assert!(!self.nodes.is_empty());
        self.nodes.last().unwrap().clone()
    }

    pub fn nodes(&self, height: usize) -> &[NodeInfo] {
        assert!(height <= self.height);
        if height < self.height {
            let begin = self.nr_entries[1..height + 1]
                .iter()
                .map(|&v| v as usize)
                .sum();
            let end = begin + self.nr_entries[height + 1] as usize;
            &self.nodes[begin..end]
        } else {
            // returns the root node
            let len = self.nodes.len();
            &self.nodes[len - 1..len]
        }
    }

    pub fn leaves(&self) -> &[NodeInfo] {
        self.nodes(0)
    }

    // Returns the index of the first leave under a specific node
    pub fn first_leaf(&self, height: usize, index: u64) -> u64 {
        assert!(height <= self.height);
        let nodes = &self.nodes(height);
        assert!(index <= nodes.len() as u64);
        if index == nodes.len() as u64 {
            return self.nr_leaves();
        }

        let mut h = height;
        let mut idx = index;
        let mut node = &nodes[idx as usize];
        while h > 0 {
            h -= 1;
            idx = node.entries_begin;
            node = &self.nodes(h)[idx as usize];
        }
        idx
    }
}

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

pub fn build_btree_from_mappings<V: Pack + Unpack + Clone>(
    w: &mut WriteBatcher,
    mappings: &[(u64, V)],
) -> BTreeLayout {
    let leaves = build_leaves(w, mappings, false);
    let mut nodes = leaves;
    let mut layout = BTreeLayout::new();

    while nodes.len() > 1 {
        let mut builder = NodeBuilder::new(Box::new(InternalIO {}), Box::new(NoopRC {}), false);

        for n in nodes {
            assert!(builder.push_value(w, n.key, n.block).is_ok());
            layout.push_node(&n);
        }

        nodes = builder.complete(w).unwrap();
        layout.increase_height();
    }

    assert!(nodes.len() == 1);
    assert!(w.flush().is_ok());

    layout.push_node(&nodes[0]);
    layout.complete();
    layout
}

//------------------------------------------
