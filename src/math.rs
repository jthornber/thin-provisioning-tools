use std::cmp::PartialEq;
use std::ops::{Add, Div, Rem};

//-----------------------------------------

pub trait Integer:
    Sized + Copy + Add<Output = Self> + Div<Output = Self> + Rem<Output = Self> + PartialEq
{
    fn zero() -> Self;
    fn one() -> Self;
}

pub fn div_up<T: Integer>(v: T, divisor: T) -> T {
    if v % divisor != Integer::zero() {
        v / divisor + Integer::one()
    } else {
        v / divisor
    }
}

#[test]
fn test_div_up() {
    assert_eq!(div_up(4usize, 4usize), 1);
    assert_eq!(div_up(5usize, 4usize), 2);
    assert_eq!(div_up(34usize, 8usize), 5);
}

pub fn div_down<T: Integer>(v: T, divisor: T) -> T {
    v / divisor
}

//-----------------------------------------

impl Integer for usize {
    fn zero() -> Self {
        0
    }

    fn one() -> Self {
        1
    }
}

impl Integer for u64 {
    fn zero() -> Self {
        0
    }

    fn one() -> Self {
        1
    }
}

//-----------------------------------------
