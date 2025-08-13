use anyhow::{anyhow, Result};

//---------------------------------------

pub type BlockRange = std::ops::Range<u64>;

pub fn range_len(r: &BlockRange) -> u64 {
    r.end - r.start
}

//---------------------------------------

pub fn build_remaps<T1, T2>(ranges: T1, free: T2) -> Result<Vec<(BlockRange, u64)>>
where
    T1: IntoIterator<Item = BlockRange>,
    T2: IntoIterator<Item = BlockRange>,
{
    use std::cmp::Ordering;

    let mut remaps = Vec::new();
    let mut range_iter = ranges.into_iter();
    let mut free_iter = free.into_iter();

    let mut r = range_iter.next().unwrap_or(u64::MAX..u64::MAX);
    let mut f = free_iter.next().unwrap_or(u64::MAX..u64::MAX);

    while !r.is_empty() && !f.is_empty() {
        let rlen = range_len(&r);
        let flen = range_len(&f);

        match rlen.cmp(&flen) {
            Ordering::Less => {
                // range fits into the free chunk
                remaps.push((r, f.start));
                f.start += rlen;
                r = range_iter.next().unwrap_or(u64::MAX..u64::MAX);
            }
            Ordering::Equal => {
                remaps.push((r, f.start));
                r = range_iter.next().unwrap_or(u64::MAX..u64::MAX);
                f = free_iter.next().unwrap_or(u64::MAX..u64::MAX);
            }
            Ordering::Greater => {
                remaps.push((r.start..(r.start + flen), f.start));
                r.start += flen;
                f = free_iter.next().unwrap_or(u64::MAX..u64::MAX);
            }
        }
    }

    if r.is_empty() {
        Ok(remaps)
    } else {
        Err(anyhow!("Insufficient free space"))
    }
}

#[cfg(test)]
mod remap_tests {
    use super::*;

    struct Test {
        ranges: Vec<BlockRange>,
        free: Vec<BlockRange>,
        result: Result<Vec<(BlockRange, u64)>>,
    }

    fn run_build_remaps_test(test: &Test) {
        let remaps = build_remaps(test.ranges.iter().cloned(), test.free.iter().cloned());
        match (&remaps, &test.result) {
            (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            (Ok(actual), Err(expected)) => {
                panic!("got {:?}, while expected {:?}", actual, expected)
            }
            (Err(actual), Ok(expected)) => {
                panic!("got {:?}, while expected {:?}", actual, expected)
            }
        }
    }

    #[test]
    fn test_build_remaps_one_to_one() {
        #[allow(clippy::single_range_in_vec_init)]
        let tests = vec![
            // basic fitting
            Test {
                ranges: vec![1000..1002],
                free: vec![0..100],
                result: Ok(vec![(1000..1002, 0)]),
            },
            // exact fitting
            Test {
                ranges: vec![1000..1002],
                free: vec![0..2],
                result: Ok(vec![(1000..1002, 0)]),
            },
            // error: insufficient space
            Test {
                ranges: vec![1000..1002],
                free: vec![0..1],
                result: Err(anyhow!("Insufficient free space")),
            },
            // error: no free space
            Test {
                ranges: vec![0..10],
                free: vec![],
                result: Err(anyhow!("Insufficient free space")),
            },
        ];
        for test in &tests {
            run_build_remaps_test(test);
        }
    }

    #[test]
    fn test_build_remaps_one_to_many() {
        #[allow(clippy::single_range_in_vec_init)]
        let tests = vec![
            // split an input range
            Test {
                ranges: vec![100..120],
                free: vec![0..5, 20..23, 30..50],
                result: Ok(vec![(100..105, 0), (105..108, 20), (108..120, 30)]),
            },
            // error: insufficient total space
            Test {
                ranges: vec![100..120],
                free: vec![0..5, 20..30],
                result: Err(anyhow!("Insufficient free space")),
            },
        ];
        for test in &tests {
            run_build_remaps_test(test);
        }
    }

    #[test]
    fn test_build_remaps_many_to_one() {
        #[allow(clippy::single_range_in_vec_init)]
        let tests = vec![
            // fit multiple ranges
            Test {
                ranges: vec![1000..1002, 1100..1110],
                free: vec![0..100],
                result: Ok(vec![(1000..1002, 0), (1100..1110, 2)]),
            },
            // error: insufficient space for all ranges
            Test {
                ranges: vec![1000..1002, 1100..1110],
                free: vec![0..10],
                result: Err(anyhow!("Insufficient free space")),
            },
        ];
        for test in &tests {
            run_build_remaps_test(test);
        }
    }

    #[test]
    fn test_build_remaps_many_to_many() {
        //#[allow(clippy::single_range_in_vec_init)]
        let tests = vec![
            // exact fitting (one-to-one)
            Test {
                ranges: vec![100..103, 200..205, 300..304],
                free: vec![0..3, 10..15, 20..24],
                result: Ok(vec![(100..103, 0), (200..205, 10), (300..304, 20)]),
            },
            // interleaved pattern
            Test {
                ranges: vec![100..102, 200..203, 300..305],
                free: vec![0..10, 20..30],
                result: Ok(vec![(100..102, 0), (200..203, 2), (300..305, 5)]),
            },
            // error: complex insufficient space
            Test {
                ranges: vec![100..105, 200..210, 300..315],
                free: vec![0..3, 10..12, 20..25],
                result: Err(anyhow!("Insufficient free space")),
            },
        ];
        for test in &tests {
            run_build_remaps_test(test);
        }
    }

    #[test]
    fn test_build_remaps_edge_cases() {
        #[allow(clippy::single_range_in_vec_init)]
        let tests = vec![
            // noop
            Test {
                ranges: vec![],
                free: vec![],
                result: Ok(vec![]),
            },
            // no inputs
            Test {
                ranges: vec![],
                free: vec![0..100],
                result: Ok(vec![]),
            },
            // boundary values
            Test {
                ranges: vec![(u64::MAX - 5)..(u64::MAX)],
                free: vec![(u64::MAX - 10)..(u64::MAX - 5)],
                result: Ok(vec![((u64::MAX - 5)..(u64::MAX), u64::MAX - 10)]),
            },
        ];
        for test in &tests {
            run_build_remaps_test(test);
        }
    }
}

//---------------------------------------

fn overlaps(r1: &BlockRange, r2: &BlockRange, index: usize) -> Option<usize> {
    if r1.start >= r2.end {
        return None;
    }

    if r2.start >= r1.end {
        return None;
    }

    Some(index)
}

// Finds the index of the first entry that overlaps r.
// TODO: return the found remapping to save further accesses.
fn find_first(r: &BlockRange, remaps: &[(BlockRange, u64)]) -> Option<usize> {
    if remaps.is_empty() {
        return None;
    }

    match remaps.binary_search_by_key(&r.start, |(from, _)| from.start) {
        Ok(n) => Some(n),
        Err(n) => {
            if n == 0 {
                let (from, _) = &remaps[n];
                overlaps(r, from, n)
            } else if n == remaps.len() {
                let (from, _) = &remaps[n - 1];
                overlaps(r, from, n - 1)
            } else {
                // Need to check the previous entry
                let (from, _) = &remaps[n - 1];
                overlaps(r, from, n - 1).or_else(|| {
                    let (from, _) = &remaps[n];
                    overlaps(r, from, n)
                })
            }
        }
    }
}

// remaps must be in sorted order by from.start.
pub fn remap(r: &BlockRange, remaps: &[(BlockRange, u64)]) -> Vec<BlockRange> {
    let mut mapped = Vec::new();
    let mut r = r.start..r.end;

    if let Some(index) = find_first(&r, remaps) {
        let mut index = index;
        loop {
            let (from, to) = &remaps[index];

            // There may be a prefix that doesn't overlap with 'from'
            if r.start < from.start {
                let len = u64::min(range_len(&r), from.start - r.start);
                mapped.push(r.start..(r.start + len));
                r = (r.start + len)..r.end;

                if r.is_empty() {
                    break;
                }
            }

            let remap_start = to + (r.start - from.start);
            let from = r.start..from.end;
            let rlen = range_len(&r);
            let flen = range_len(&from);

            let len = u64::min(rlen, flen);
            mapped.push(remap_start..(remap_start + len));

            r = (r.start + len)..r.end;
            if r.is_empty() {
                break;
            }

            if len == flen {
                index += 1;
            }

            if index == remaps.len() {
                mapped.push(r.start..r.end);
                break;
            }
        }
    } else {
        mapped.push(r.start..r.end);
    }

    mapped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remap_test() {
        struct Test {
            remaps: Vec<(BlockRange, u64)>,
            input: BlockRange,
            output: Vec<BlockRange>,
        }

        #[allow(clippy::single_range_in_vec_init)]
        let tests = [
            // no remaps
            Test {
                remaps: vec![],
                input: 0..1,
                output: vec![0..1],
            },
            // no remaps
            Test {
                remaps: vec![],
                input: 100..1000,
                output: vec![100..1000],
            },
            // preceding to remaps
            Test {
                remaps: vec![(10..20, 110)],
                input: 0..5,
                output: vec![0..5],
            },
            // fully overlapped to a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 10..20,
                output: vec![110..120],
            },
            // tail overlapped with a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 5..15,
                output: vec![5..10, 110..115],
            },
            // stride across a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 5..25,
                output: vec![5..10, 110..120, 20..25],
            },
            // head overlapped with a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 15..25,
                output: vec![115..120, 20..25],
            },
            // succeeding to a remap
            Test {
                remaps: vec![(10..20, 110)],
                input: 25..35,
                output: vec![25..35],
            },
            // stride across multiple remaps
            Test {
                remaps: vec![(10..20, 110), (30..40, 230)],
                input: 0..50,
                output: vec![0..10, 110..120, 20..30, 230..240, 40..50],
            },
        ];

        for t in &tests {
            let rs = remap(&t.input, &t.remaps);
            assert_eq!(rs, t.output);
        }
    }
}

//---------------------------------------
