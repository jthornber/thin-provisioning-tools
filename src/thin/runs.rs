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
    heads: BTreeMap<u64, bool>,
    tails: BTreeSet<u64>,
    entries: BTreeMap<u64, Entry>,
    back: BTreeMap<u64, u64>,
}

impl Gatherer {
    pub fn new() -> Gatherer {
        Gatherer {
            prev: None,
            heads: BTreeMap::new(),
            tails: BTreeSet::new(),
            entries: BTreeMap::new(),
            back: BTreeMap::new(),
        }
    }

    fn is_head(&self, b: u64) -> bool {
        self.heads.contains_key(&b)
    }

    fn mark_head(&mut self, b: u64) {
        self.heads
            .entry(b)
            .and_modify(|shared| *shared = true)
            .or_insert(false);
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

            if let Some(back) = self.back.get(&b) {
                if *back != prev {
                    self.mark_head(b);
                }
            } else {
                self.back.insert(b, prev);
            }
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
            if let Some(e) = self.entries.get(t) {
                for n in &e.neighbours {
                    heads
                        .entry(*n)
                        .and_modify(|shared| *shared = true)
                        .or_insert(false);
                }
            }
        }
        mem::swap(&mut heads, &mut self.heads);
    }

    // Returns atomic subsequences and their sharing states.
    pub fn gather(&mut self) -> Vec<(Vec<u64>, bool)> {
        // close the last sequence.
        self.new_seq();
        self.complete_heads_and_tails();

        // FIXME: there must be a 'map'
        let mut seqs: Vec<(Vec<u64>, bool)> = Vec::new();
        for (b, shared) in &self.heads {
            seqs.push((self.extract_seq(*b), *shared));
        }
        seqs
    }
}

impl Default for Gatherer {
    fn default() -> Self {
        Self::new()
    }
}

//------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gather() {
        // the first value defines the input runs,
        // and the second defines expected sub sequences and their sharing states
        struct Test(Vec<Vec<u64>>, Vec<(Vec<u64>, bool)>);

        let tests = vec![
            Test(vec![], vec![]),
            Test(vec![vec![1]], vec![(vec![1], false)]),
            Test(vec![vec![1, 2, 3]], vec![(vec![1, 2, 3], false)]),
            Test(
                vec![vec![1, 2], vec![1, 2, 3]],
                vec![(vec![1, 2], true), (vec![3], false)],
            ),
            Test(
                vec![vec![2, 3], vec![1, 2, 3]],
                vec![(vec![1], false), (vec![2, 3], true)],
            ),
            Test(
                vec![vec![1, 2, 3, 4], vec![2, 3, 4, 5]],
                vec![(vec![1], false), (vec![2, 3, 4], true), (vec![5], false)],
            ),
            Test(
                vec![vec![2, 3, 4, 5], vec![1, 2, 3, 4]],
                vec![(vec![1], false), (vec![2, 3, 4], true), (vec![5], false)],
            ),
            Test(
                vec![
                    vec![1, 2, 3, 4],
                    vec![2, 3, 4, 5, 6],
                    vec![3, 4],
                    vec![5, 6],
                ],
                vec![
                    (vec![1], false),
                    (vec![2], true),
                    (vec![3, 4], true),
                    (vec![5, 6], true),
                ],
            ),
            Test(
                vec![vec![2, 3, 4, 5], vec![2, 3, 4, 6], vec![1, 3, 4, 5]],
                vec![
                    (vec![1], false),
                    (vec![2], true),
                    (vec![3, 4], true),
                    (vec![5], false),
                    (vec![6], false),
                ],
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
