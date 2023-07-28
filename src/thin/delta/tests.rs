use super::*;

use crate::thin::ir::{self, Visit};

//------------------------------------------

struct DeltaCollector {
    deltas: Vec<Delta>,
}

impl DeltaCollector {
    fn new() -> Self {
        Self { deltas: Vec::new() }
    }
}

impl DeltaVisitor for DeltaCollector {
    fn superblock_b(&mut self, _sb: &ir::Superblock) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn diff_b(&mut self, _snap1: Snap, _snap2: Snap) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn diff_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn delta(&mut self, d: &Delta) -> Result<Visit> {
        self.deltas.push(d.clone());
        Ok(Visit::Continue)
    }
}

//------------------------------------------

fn test_build_runs_(keys: &[u64], values: &[u64], expected: &[(u64, u64, u64)]) -> Result<()> {
    let mut builder = RunBuilder::new();
    let exp: Vec<_> = expected
        .iter()
        .map(|(t, d, l)| DataMapping {
            thin_begin: *t,
            data_begin: *d,
            len: *l,
        })
        .collect();

    let mut actual = Vec::new();
    for (k, v) in keys.iter().zip(values) {
        if let Some(r) = builder.next(*k, *v) {
            actual.push(r);
        }
    }
    if let Some(r) = builder.complete() {
        actual.push(r);
    }

    assert_eq!(actual, exp);
    Ok(())
}

#[test]
fn test_build_runs() -> Result<()> {
    let keys = [1, 2, 3, 4, 5];
    let values = [100, 101, 104, 105, 110];
    let expected = [(1, 100, 2), (3, 104, 2), (5, 110, 1)];
    test_build_runs_(&keys, &values, &expected)
}

//------------------------------------------

fn test_delta(
    left: &[(u64, u64, u64)],
    right: &[(u64, u64, u64)],
    expected: &[(u8, u64, u64, u64, u64)],
) -> Result<()> {
    let lm: Vec<_> = left
        .iter()
        .map(|(t, d, l)| DataMapping {
            thin_begin: *t,
            data_begin: *d,
            len: *l,
        })
        .collect();

    let rm: Vec<_> = right
        .iter()
        .map(|(t, d, l)| DataMapping {
            thin_begin: *t,
            data_begin: *d,
            len: *l,
        })
        .collect();

    let expected: Vec<_> = expected
        .iter()
        .map(|(typ, arg1, arg2, arg3, arg4)| match typ {
            0 => Delta::Same(DataMapping {
                thin_begin: *arg1,
                data_begin: *arg2,
                len: *arg3,
            }),
            1 => Delta::LeftOnly(DataMapping {
                thin_begin: *arg1,
                data_begin: *arg2,
                len: *arg3,
            }),
            2 => Delta::RightOnly(DataMapping {
                thin_begin: *arg1,
                data_begin: *arg2,
                len: *arg3,
            }),
            _ => Delta::Differ(DiffMapping {
                thin_begin: *arg1,
                left_data_begin: *arg2,
                right_data_begin: *arg3,
                len: *arg4,
            }),
        })
        .collect();

    let mut visitor = DeltaCollector::new();
    dump_delta_mappings(&lm, &rm, &mut visitor)?;

    let actual = visitor.deltas;
    assert_eq!(actual, expected);

    Ok(())
}

#[test]
fn test_delta_ends_by_right() -> Result<()> {
    let left = [(10, 1234, 30), (40, 2345, 10)];
    let right = [(20, 1244, 50)];
    let expected = [
        (1, 10, 1234, 10, 0),
        (0, 20, 1244, 20, 0),
        (3, 40, 2345, 1264, 10),
        (2, 50, 1274, 20, 0),
    ];
    test_delta(&left, &right, &expected)
}

#[test]
fn test_delta_ends_by_left() -> Result<()> {
    let left = [(10, 1234, 30), (50, 2345, 20)];
    let right = [(10, 1234, 50)];
    let expected = [
        (0, 10, 1234, 30, 0),
        (2, 40, 1264, 10, 0),
        (3, 50, 2345, 1274, 10),
        (1, 60, 2355, 10, 0),
    ];
    test_delta(&left, &right, &expected)
}

//------------------------------------------
