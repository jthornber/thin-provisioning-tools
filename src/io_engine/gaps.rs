/*
fn check_runs(blocks: &[u64], runs: &[(u64, u64)], gaps: &RoaringBitmap) {
    let mut b_index = 0;

    for (b, e) in runs {
        for b in *b..*e {
            if blocks[b_index] == b {
                b_index += 1;
            } else {
                assert!(gaps.contains(b as u32));
            }
        }
    }

    assert_eq!(b_index, blocks.len());
}
*/

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunOp {
    Run(u64, u64),
    Gap(u64, u64),
}

fn find_runs(blocks: &[u64], gap_threshold: u64) -> Vec<RunOp> {
    use RunOp::*;

    let mut runs: Vec<RunOp> = Vec::with_capacity(16);
    let mut last: Option<(u64, u64)> = None;

    for b in blocks {
        if let Some((begin, end)) = last {
            if *b > end {
                let len = b - end;
                if len > gap_threshold {
                    runs.push(Run(begin, end));
                    last = Some((*b, *b + 1));
                } else {
                    runs.push(Run(begin, end));
                    runs.push(Gap(end, *b));
                    last = Some((*b, *b + 1));
                }
            } else if *b == end {
                last = Some((begin, b + 1));
            } else {
                runs.push(Run(begin, end));
                last = Some((*b, b + 1));
            }
        } else {
            last = Some((*b, b + 1));
        }
    }

    if let Some((begin, end)) = last {
        runs.push(Run(begin, end));
    }

    runs
}

#[cfg(test)]
mod find_runs_tests {
    use super::*;

    use RunOp::*;

    #[test]
    fn single_run() {
        let bs = vec![1, 2, 3, 4, 5, 6, 7];

        let runs = find_runs(&bs, 0);
        assert_eq!(runs, vec![Run(1, 8)]);
    }

    #[test]
    fn two_runs() {
        let bs = vec![1, 2, 3, 5, 6, 7];

        let runs = find_runs(&bs, 0);
        assert_eq!(runs, vec![Run(1, 4), Run(5, 8)]);
    }

    #[test]
    fn three_runs() {
        let bs = vec![1, 2, 3, 5, 6, 7, 100, 101, 102];

        let runs = find_runs(&bs, 0);
        assert_eq!(runs, &[Run(1, 4), Run(5, 8), Run(100, 103)]);
    }

    #[test]
    fn large_gap() {
        let bs = vec![1, 2, 3, 5, 6, 7, 100, 101, 102];

        let runs = find_runs(&bs, 100);
        assert_eq!(
            runs,
            vec![Run(1, 4), Gap(4, 5), Run(5, 8), Gap(8, 100), Run(100, 103)]
        );
    }

    #[test]
    fn small_gap() {
        let bs = vec![1, 2, 3, 5, 6, 7, 10, 11, 12, 20, 21, 22, 23];
        let runs = find_runs(&bs, 4);
        assert_eq!(
            runs,
            vec![
                Run(1, 4),
                Gap(4, 5),
                Run(5, 8),
                Gap(8, 10),
                Run(10, 13),
                Run(20, 24)
            ]
        );
    }

    #[test]
    fn unordered() {
        let bs = vec![5, 6, 7, 1, 2, 3, 10, 11, 12, 20, 21, 22, 23];
        let runs = find_runs(&bs, 10);
        assert_eq!(
            runs,
            vec![
                Run(5, 8),
                Run(1, 4),
                Gap(4, 10),
                Run(10, 13),
                Gap(13, 20),
                Run(20, 24)
            ]
        );
    }

    #[test]
    fn singletons() {
        let bs = vec![50, 70, 10, 30, 100, 120, 210, 230];
        let runs = find_runs(&bs, 2);
        assert_eq!(
            runs,
            vec![
                Run(50, 51),
                Run(70, 71),
                Run(10, 11),
                Run(30, 31),
                Run(100, 101),
                Run(120, 121),
                Run(210, 211),
                Run(230, 231),
            ]
        );
    }
}

//-----------------------------------------

fn batch_adjacent(runs: &[RunOp]) -> Batches {
    use RunOp::*;

    let mut result: Vec<Vec<RunOp>> = Vec::new();
    let mut batch: Vec<RunOp> = Vec::new();
    let mut last: Option<u64> = None;

    for r in runs {
        match (last, r) {
            (None, Run(b, e)) => {
                batch.push(Run(*b, *e));
                last = Some(*e);
            }
            (None, Gap(b, e)) => {
                batch.push(Gap(*b, *e));
                last = Some(*e);
            }
            (Some(l), Run(b, e)) => {
                if *b == l {
                    batch.push(Run(*b, *e));
                } else {
                    let mut tmp = Vec::new();
                    std::mem::swap(&mut tmp, &mut batch);
                    result.push(tmp);
                    batch.push(Run(*b, *e));
                }
                last = Some(*e);
            }
            (Some(l), Gap(b, e)) => {
                if *b == l {
                    batch.push(Gap(*b, *e));
                } else {
                    let mut tmp = Vec::new();
                    std::mem::swap(&mut tmp, &mut batch);
                    result.push(tmp);
                    batch.push(Gap(*b, *e));
                }
                last = Some(*e);
            }
        }
    }

    if !batch.is_empty() {
        result.push(batch);
    }

    result
}

#[cfg(test)]
mod batch_adjacent_tests {
    use super::*;

    use RunOp::*;

    #[test]
    fn batch_zero() {
        let runs = vec![];
        let batches = batch_adjacent(&runs);
        assert!(batches.is_empty());
    }

    #[test]
    fn batch_one() {
        let runs = vec![Run(1, 5), Gap(5, 8), Run(8, 100)];
        let batches = batch_adjacent(&runs);
        assert_eq!(batches, vec![vec![Run(1, 5), Gap(5, 8), Run(8, 100)]]);
    }

    #[test]
    fn batch_two() {
        let runs = vec![
            Run(1, 5),
            Gap(5, 8),
            Run(8, 100),
            Run(500, 501),
            Gap(501, 513),
            Run(513, 600),
        ];
        let batches = batch_adjacent(&runs);
        assert_eq!(
            batches,
            vec![
                vec![Run(1, 5), Gap(5, 8), Run(8, 100)],
                vec![Run(500, 501), Gap(501, 513), Run(513, 600)]
            ]
        );
    }

    #[test]
    fn singletons() {
        let runs = vec![
            Run(50, 51),
            Run(70, 71),
            Run(10, 11),
            Run(30, 31),
            Run(100, 101),
            Run(120, 121),
            Run(210, 211),
            Run(230, 231),
        ];
        let batches = batch_adjacent(&runs);
        assert_eq!(
            batches,
            vec![
                vec![Run(50, 51)],
                vec![Run(70, 71)],
                vec![Run(10, 11)],
                vec![Run(30, 31)],
                vec![Run(100, 101)],
                vec![Run(120, 121)],
                vec![Run(210, 211)],
                vec![Run(230, 231)],
            ]
        );
    }
}

//-----------------------------------------

// Returns the remainder
fn split_op(op: &RunOp, mut remaining: u64, max: u64, result: &mut Vec<RunOp>) -> u64 {
    use RunOp::*;

    let mut op = op.clone();

    loop {
        match op {
            Run(b, e) => {
                let len = std::cmp::min(e - b, remaining);
                result.push(Run(b, b + len));
                op = Run(b + len, e);
                remaining -= len;
                if e == b + len {
                    break;
                }
            }
            Gap(b, e) => {
                let len = std::cmp::min(e - b, remaining);
                result.push(Gap(b, b + len));
                op = Gap(b + len, e);
                remaining -= len;
                if e == b + len {
                    break;
                }
            }
        }
        if remaining == 0 {
            remaining = max
        }
    }

    remaining
}

fn split_contiguous(runs: &[RunOp], max: u64) -> Vec<RunOp> {
    let mut remaining = max;
    let mut result: Vec<RunOp> = Vec::with_capacity(runs.len());

    for op in runs {
        remaining = split_op(op, remaining, max, &mut result);
    }

    result
}

type Batches = Vec<Vec<RunOp>>;

fn split_batches(batches: &Batches, max: u64) -> Batches {
    let mut result = Vec::new();

    for b in batches {
        result.push(split_contiguous(b, max));
    }

    result
}

#[cfg(test)]
mod split_tests {
    use super::*;
    use RunOp::*;

    #[test]
    fn single() {
        let runs = vec![vec![Run(1, 100)]];
        let batches = split_batches(&runs, 21);
        assert_eq!(
            batches,
            [[
                Run(1, 22),
                Run(22, 43),
                Run(43, 64),
                Run(64, 85),
                Run(85, 100)
            ]]
        );
    }

    #[test]
    fn singletons() {
        let runs = vec![
            vec![Run(50, 51)],
            vec![Run(70, 71)],
            vec![Run(10, 11)],
            vec![Run(30, 31)],
            vec![Run(100, 101)],
            vec![Run(120, 121)],
            vec![Run(210, 211)],
            vec![Run(230, 231)],
        ];
        let batches = split_batches(&runs, 100);
        assert_eq!(batches, runs);
    }
}

//-----------------------------------------

pub fn generate_runs(blocks: &[u64], gap_threshold: u64, max: u64) -> Batches {
    split_batches(&batch_adjacent(&find_runs(blocks, gap_threshold)), max)
}

//-----------------------------------------
