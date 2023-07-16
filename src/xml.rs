use anyhow::anyhow;
use quick_xml::events::attributes::Attribute;
use quick_xml::name::QName;
use std::borrow::Cow;
use std::fmt::Display;

//------------------------------------------

// FIXME: nasty unwraps
pub fn string_val(kv: &Attribute) -> anyhow::Result<String> {
    kv.unescape_value()
        .map_or_else(|e| Err(e.into()), |s| Ok(s.as_ref().to_string()))
}

fn parse_val<T>(kv: &Attribute) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::error::Error + Send + Sync + 'static,
{
    std::str::from_utf8(kv.value.as_ref())
        .map_or_else(|e| Err(e.into()), |s| s.parse::<T>().map_err(|e| e.into()))
}

pub fn u64_val(kv: &Attribute) -> anyhow::Result<u64> {
    parse_val::<u64>(kv)
}

pub fn u32_val(kv: &Attribute) -> anyhow::Result<u32> {
    parse_val::<u32>(kv)
}

pub fn bool_val(kv: &Attribute) -> anyhow::Result<bool> {
    parse_val::<bool>(kv)
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
        key: QName(key),
        value: mk_attr_(value),
    }
}

fn mk_attr_<'a, T: Display>(n: T) -> Cow<'a, [u8]> {
    let str = format!("{}", n);
    Cow::Owned(str.into_bytes())
}

//------------------------------------------
