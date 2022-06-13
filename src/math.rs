use std::ops::{Add, Div, Sub};

//-----------------------------------------

pub fn div_up<T>(v: T, divisor: T) -> T
where
    T: Copy + Add<Output = T> + Div<Output = T> + Sub<Output = T> + From<u8>,
{
    (v + (divisor - T::from(1u8))) / divisor
}

#[test]
fn test_div_up() {
    assert_eq!(div_up(4usize, 4usize), 1);
    assert_eq!(div_up(5usize, 4usize), 2);
    assert_eq!(div_up(34usize, 8usize), 5);
}

//-----------------------------------------
