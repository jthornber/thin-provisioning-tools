use anyhow::anyhow;
use quick_xml::events::attributes::Attribute;
use std::borrow::Cow;
use std::fmt::Display;

//------------------------------------------

pub fn bytes_val<'a>(kv: &'a Attribute) -> Cow<'a, [u8]> {
    kv.unescaped_value().unwrap()
}

// FIXME: nasty unwraps
pub fn string_val(kv: &Attribute) -> String {
    let v = kv.unescaped_value().unwrap();
    let bytes = v.to_vec();
    String::from_utf8(bytes).unwrap()
}

// FIXME: there's got to be a way of doing this without copying the string
pub fn u64_val(kv: &Attribute) -> anyhow::Result<u64> {
    let n = string_val(kv).parse::<u64>()?;
    Ok(n)
}

pub fn u32_val(kv: &Attribute) -> anyhow::Result<u32> {
    let n = string_val(kv).parse::<u32>()?;
    Ok(n)
}

pub fn bool_val(kv: &Attribute) -> anyhow::Result<bool> {
    let n = string_val(kv).parse::<bool>()?;
    Ok(n)
}

pub fn bad_attr<T>(tag: &str, attr: &[u8]) -> anyhow::Result<T> {
    Err(anyhow!(
        "unknown attribute {}in tag '{}'",
        std::str::from_utf8(attr)
            .map(|s| format!("'{}' ", s))
            .unwrap_or_default(),
        tag
    ))
}

pub fn check_attr<T>(tag: &str, name: &str, maybe_v: Option<T>) -> anyhow::Result<T> {
    match maybe_v {
        None => missing_attr(tag, name),
        Some(v) => Ok(v),
    }
}

fn missing_attr<T>(tag: &str, attr: &str) -> anyhow::Result<T> {
    let msg = format!("missing attribute '{}' for tag '{}", attr, tag);
    Err(anyhow!(msg))
}

pub fn mk_attr<T: Display>(key: &[u8], value: T) -> Attribute {
    Attribute {
        key,
        value: mk_attr_(value),
    }
}

fn mk_attr_<'a, T: Display>(n: T) -> Cow<'a, [u8]> {
    let str = format!("{}", n);
    Cow::Owned(str.into_bytes())
}

//------------------------------------------
