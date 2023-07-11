use super::*;

//------------------------------------------

#[test]
fn test_with_empty_lhs() {
    let lhs = BTreeMap::new();
    let rhs = BTreeMap::from([(0, 10)]);
    let ordering = compare_time_counts(&lhs, &rhs);
    assert_eq!(ordering, Ordering::Greater);
}

#[test]
fn test_with_empty_rhs() {
    let lhs = BTreeMap::from([(0, 10)]);
    let rhs = BTreeMap::new();
    let ordering = compare_time_counts(&lhs, &rhs);
    assert_eq!(ordering, Ordering::Less);
}

#[test]
fn test_with_two_empty_sets() {
    let lhs = BTreeMap::new();
    let rhs = BTreeMap::new();
    let ordering = compare_time_counts(&lhs, &rhs);
    assert_eq!(ordering, Ordering::Equal);
}

#[test]
fn test_with_greater_time_at_lhs() {
    let lhs = BTreeMap::from([(1, 1), (0, 10)]);
    let rhs = BTreeMap::from([(0, 10)]);
    let ordering = compare_time_counts(&lhs, &rhs);
    assert_eq!(ordering, Ordering::Less);
}

#[test]
fn test_with_greater_time_at_rhs() {
    let lhs = BTreeMap::from([(0, 10)]);
    let rhs = BTreeMap::from([(1, 1), (0, 10)]);
    let ordering = compare_time_counts(&lhs, &rhs);
    assert_eq!(ordering, Ordering::Greater);
}

#[test]
fn test_with_greater_counts_at_lhs() {
    let lhs = BTreeMap::from([(1, 10), (0, 10)]);
    let rhs = BTreeMap::from([(1, 5), (0, 10)]);
    let ordering = compare_time_counts(&lhs, &rhs);
    assert_eq!(ordering, Ordering::Less);
}

#[test]
fn test_with_greater_counts_at_rhs() {
    let lhs = BTreeMap::from([(1, 5), (0, 10)]);
    let rhs = BTreeMap::from([(1, 10), (0, 10)]);
    let ordering = compare_time_counts(&lhs, &rhs);
    assert_eq!(ordering, Ordering::Greater);
}

//------------------------------------------
