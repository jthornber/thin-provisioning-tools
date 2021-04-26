pub fn div_up(v: usize, divisor: usize) -> usize {
    v / divisor + (v % divisor != 0) as usize
}

pub fn div_down(v: usize, divisor: usize) -> usize {
    v / divisor
}
