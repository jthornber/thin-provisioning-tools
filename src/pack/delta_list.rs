//-------------------------------------------------

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum Delta {
    Base { n: u64 },
    Const { count: u64 },
    Pos { delta: u64, count: u64 },
    Neg { delta: u64, count: u64 },
}

use Delta::*;

pub fn to_delta(ns: &[u64]) -> Vec<Delta> {
    use std::cmp::Ordering::*;

    let mut ds = Vec::with_capacity(ns.len());

    if !ns.is_empty() {
        let mut base = ns[0];
        ds.push(Base { n: base });

        let mut i = 1;
        while i < ns.len() {
            let n = ns[i];
            match n.cmp(&base) {
                Less => {
                    let delta = base - n;
                    let mut count = 1;
                    while i < ns.len() && (ns[i] + (count * delta) == base) {
                        i += 1;
                        count += 1;
                    }
                    count -= 1;
                    ds.push(Neg { delta, count });
                    base -= delta * count;
                }
                Equal => {
                    let mut count = 1;
                    while i < ns.len() && ns[i] == base {
                        i += 1;
                        count += 1;
                    }
                    count -= 1;
                    ds.push(Const { count });
                }
                Greater => {
                    let delta = n - base;
                    let mut count = 1;
                    while i < ns.len() && (ns[i] == (base + (count * delta))) {
                        i += 1;
                        count += 1;
                    }
                    count -= 1;
                    ds.push(Pos { delta, count });
                    base += delta * count;
                }
            }
        }
    }

    ds
}

#[cfg(test)]
mod tests {
    use super::*;

    fn from_delta(ds: &[Delta]) -> Vec<u64> {
        let mut ns: Vec<u64> = Vec::new();
        let mut base = 0u64;

        for d in ds {
            match d {
                Base { n } => {
                    ns.push(*n);
                    base = *n;
                }
                Const { count } => {
                    for _ in 0..*count {
                        ns.push(base);
                    }
                }
                Pos { delta, count } => {
                    for _ in 0..*count {
                        base += delta;
                        ns.push(base);
                    }
                }
                Neg { delta, count } => {
                    for _ in 0..*count {
                        assert!(base >= *delta);
                        base -= delta;
                        ns.push(base);
                    }
                }
            }
        }

        ns
    }

    #[test]
    fn test_to_delta() {
        struct TestCase(Vec<u64>, Vec<Delta>);

        let cases = [
            TestCase(vec![], vec![]),
            TestCase(vec![1], vec![Base { n: 1 }]),
            TestCase(vec![1, 2], vec![Base { n: 1 }, Pos { delta: 1, count: 1 }]),
            TestCase(
                vec![1, 2, 3, 4],
                vec![Base { n: 1 }, Pos { delta: 1, count: 3 }],
            ),
            TestCase(
                vec![2, 4, 6, 8],
                vec![Base { n: 2 }, Pos { delta: 2, count: 3 }],
            ),
            TestCase(
                vec![7, 14, 21, 28],
                vec![Base { n: 7 }, Pos { delta: 7, count: 3 }],
            ),
            TestCase(
                vec![10, 9],
                vec![Base { n: 10 }, Neg { delta: 1, count: 1 }],
            ),
            TestCase(
                vec![10, 9, 8, 7],
                vec![Base { n: 10 }, Neg { delta: 1, count: 3 }],
            ),
            TestCase(
                vec![10, 8, 6, 4],
                vec![Base { n: 10 }, Neg { delta: 2, count: 3 }],
            ),
            TestCase(
                vec![28, 21, 14, 7],
                vec![Base { n: 28 }, Neg { delta: 7, count: 3 }],
            ),
            TestCase(
                vec![42, 42, 42, 42],
                vec![Base { n: 42 }, Const { count: 3 }],
            ),
            TestCase(
                vec![1, 2, 3, 10, 20, 30, 40, 38, 36, 34, 0, 0, 0, 0],
                vec![
                    Base { n: 1 },
                    Pos { delta: 1, count: 2 },
                    Pos { delta: 7, count: 1 },
                    Pos {
                        delta: 10,
                        count: 3,
                    },
                    Neg { delta: 2, count: 3 },
                    Neg {
                        delta: 34,
                        count: 1,
                    },
                    Const { count: 3 },
                ],
            ),
        ];

        for t in &cases {
            assert_eq!(to_delta(&t.0), t.1);
            assert_eq!(from_delta(&t.1), t.0);
        }
    }
}

//-------------------------------------------------
