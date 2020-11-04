use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::mem;

//------------------------------------------

#[derive(Clone, Debug)]
struct Entry {
    neighbours: BTreeSet<u64>,
}

impl Entry {
    fn first_neighbour(&self) -> Option<u64> {
        self.neighbours.iter().cloned().next()
    }
}

pub struct Gatherer {
    prev: Option<u64>,
    heads: BTreeSet<u64>,
    tails: BTreeSet<u64>,
    entries: BTreeMap<u64, Entry>,
}

impl Gatherer {
    pub fn new() -> Gatherer {
        Gatherer {
            prev: None,
            heads: BTreeSet::new(),
            tails: BTreeSet::new(),
            entries: BTreeMap::new(),
        }
    }

    fn is_head(&self, b: u64) -> bool {
        self.heads.contains(&b)
    }

    fn mark_head(&mut self, b: u64) {
        self.heads.insert(b);
    }

    fn is_tail(&self, b: u64) -> bool {
        self.tails.contains(&b)
    }

    fn mark_tail(&mut self, b: u64) {
        self.tails.insert(b);
    }

    pub fn new_seq(&mut self) {
        if let Some(b) = self.prev {
            self.mark_tail(b);
        }

        self.prev = None;
    }

    pub fn next(&mut self, b: u64) {
        if let Some(prev) = self.prev {
            let e = self.entries.get_mut(&prev).unwrap();
            e.neighbours.insert(b);
        } else {
            self.mark_head(b);
        }

        if self.entries.get(&b).is_none() {
            let e = Entry {
                neighbours: BTreeSet::new(),
            };
            self.entries.insert(b, e);
        }

        self.prev = Some(b);
    }

    fn extract_seq(&self, mut b: u64) -> Vec<u64> {
        let mut r = Vec::new();

        // FIXME: remove
        assert!(self.is_head(b));

        loop {
            r.push(b);

            if self.is_tail(b) {
                return r;
            }

            let e = self.entries.get(&b).unwrap();

            b = e.first_neighbour().unwrap();
        }
    }

    fn complete_heads_and_tails(&mut self) {
        let mut tails = BTreeSet::new();

        // add extra tails
        for (b, e) in self.entries.iter() {
            if e.neighbours.len() != 1 {
                tails.insert(*b);
            }

            if let Some(n) = e.first_neighbour() {
                if self.is_head(n) {
                    tails.insert(*b);
                }
            }
        }

        for t in tails {
            self.mark_tail(t);
        }

        // Now we need to mark entries that follow a tail as heads.
        let mut heads = mem::take(&mut self.heads);
        for t in &self.tails {
            if let Some(e) = self.entries.get(&t) {
                for n in &e.neighbours {
                    heads.insert(*n);
                }
            }
        }
        mem::swap(&mut heads, &mut self.heads);
    }

    // Returns atomic subsequences.
    pub fn gather(&mut self) -> Vec<Vec<u64>> {
        // close the last sequence.
        self.new_seq();
        self.complete_heads_and_tails();

        // FIXME: there must be a 'map'
        let mut seqs = Vec::new();
        for b in &self.heads {
            seqs.push(self.extract_seq(*b));
        }
        seqs
    }
}

//------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gather() {
        struct Test(Vec<Vec<u64>>, Vec<Vec<u64>>);

        let tests = vec![
            Test(vec![], vec![]),
            Test(vec![vec![1]], vec![vec![1]]),
            Test(vec![vec![1, 2, 3]], vec![vec![1, 2, 3]]),
            Test(vec![vec![1, 2], vec![1, 2, 3]], vec![vec![1, 2], vec![3]]),
            Test(
                vec![vec![1, 2, 3, 4], vec![2, 3, 4, 5]],
                vec![vec![1], vec![2, 3, 4], vec![5]],
            ),
            Test(
                vec![vec![2, 3, 4, 5], vec![1, 2, 3, 4]],
                vec![vec![1], vec![2, 3, 4], vec![5]],
            ),
            Test(
                vec![
                    vec![1, 2, 3, 4],
                    vec![2, 3, 4, 5, 6],
                    vec![3, 4],
                    vec![5, 6],
                ],
                vec![vec![1], vec![2], vec![3, 4], vec![5, 6]],
            ),
        ];

        for t in tests {
            eprintln!("new test case");
            let mut g = Gatherer::new();
            for s in t.0 {
                g.new_seq();
                for b in s {
                    g.next(b);
                }
            }

            let seqs = g.gather();

            assert_eq!(seqs, t.1);
        }
    }
}

//------------------------------------------
