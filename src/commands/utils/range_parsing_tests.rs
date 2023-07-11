use super::*;

//------------------------------------------

#[test]
fn test_normal() -> anyhow::Result<()> {
    let range = "0..18446744073709551615".parse::<RangeU64>()?;
    assert_eq!(range.start, 0);
    assert_eq!(range.end, u64::MAX);
    Ok(())
}

#[test]
fn test_no_dots() -> anyhow::Result<()> {
    assert!("0".parse::<RangeU64>().is_err());
    assert!("0.10".parse::<RangeU64>().is_err());
    Ok(())
}

#[test]
fn test_dots_only() -> anyhow::Result<()> {
    assert!("..".parse::<RangeU64>().is_err());
    Ok(())
}

#[test]
fn test_extra_characters() -> anyhow::Result<()> {
    assert!("0..10,".parse::<RangeU64>().is_err());
    assert!("0..10..".parse::<RangeU64>().is_err());
    Ok(())
}

#[test]
fn test_invalid_begin() -> anyhow::Result<()> {
    assert!("foo".parse::<RangeU64>().is_err());
    assert!("foo..10".parse::<RangeU64>().is_err());
    Ok(())
}

#[test]
fn test_invalid_end() -> anyhow::Result<()> {
    assert!("0..bar".parse::<RangeU64>().is_err());
    Ok(())
}

#[test]
fn test_negative_begin() -> anyhow::Result<()> {
    assert!("-1..0".parse::<RangeU64>().is_err());
    Ok(())
}

#[test]
fn test_negative_end() -> anyhow::Result<()> {
    assert!("0..-1".parse::<RangeU64>().is_err());
    Ok(())
}

#[test]
fn test_end_not_greater_than_begin() -> anyhow::Result<()> {
    assert!("0..0".parse::<RangeU64>().is_err());
    assert!("18446744073709551615..0".parse::<RangeU64>().is_err());
    Ok(())
}

//------------------------------------------
