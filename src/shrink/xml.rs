use anyhow::Result;
use std::{
    borrow::{Cow},
    fmt::Display,
    io::prelude::*,
    io::BufReader,
    io::Write,
};

use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::{Reader, Writer};

//---------------------------------------

pub struct Superblock {
    uuid: String,
    time: u64,
    transaction: u64,
    flags: Option<u32>,
    version: Option<u32>,
    data_block_size: u32,
    nr_data_blocks: u64,
    metadata_snap: Option<u64>,
}

pub struct Device {
    dev_id: u32,
    mapped_blocks: u64,
    transaction: u64,
    creation_time: u64,
    snap_time: u64,
}

pub struct Map {
    thin_begin: u64,
    data_begin: u64,
    time: u32,
    len: u64,
}

pub trait MetadataVisitor {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<()>;
    fn superblock_e(&mut self) -> Result<()>;

    fn device_b(&mut self, d: &Device) -> Result<()>;
    fn device_e(&mut self) -> Result<()>;

    fn map(&mut self, m: Map) -> Result<()>;

    fn eof(&mut self) -> Result<()>;
}

pub struct NoopVisitor {
}

impl NoopVisitor {
    pub fn new() -> NoopVisitor { NoopVisitor {} }
}

impl MetadataVisitor for NoopVisitor {
    fn superblock_b(&mut self, _sb: &Superblock) -> Result<()> {Ok(())}
    fn superblock_e(&mut self) -> Result<()> {Ok(())}

    fn device_b(&mut self, _d: &Device) -> Result<()> {Ok(())}
    fn device_e(&mut self) -> Result<()> {Ok(())}

    fn map(&mut self, _m: Map) -> Result<()> {Ok(())}

    fn eof(&mut self) -> Result<()> {Ok(())}
}

pub struct XmlWriter<W: Write> {
    w: Writer<W>,
}

impl<W: Write> XmlWriter<W> {
    pub fn new(w: W) -> XmlWriter<W> {
        XmlWriter { w: Writer::new_with_indent(w, 0x20, 2) }
    }
}

fn mk_attr_<'a, T: Display>(n: T) -> Cow<'a, [u8]> {
    let str = format!("{}", n);
    Cow::Owned(str.into_bytes())
}

fn mk_attr<'a, T: Display>(key: &[u8], value: T) -> Attribute {
    Attribute {
        key,
        value: mk_attr_(value),
    }
}

const XML_VERSION: u32 = 2;

impl<W: Write> MetadataVisitor for XmlWriter<W> {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<()> {
        let tag = b"superblock";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"uuid", sb.uuid.clone()));
        elem.push_attribute(mk_attr(b"time", sb.time));
        elem.push_attribute(mk_attr(b"transaction", sb.transaction));
        if let Some(flags) = sb.flags {
            // FIXME: is this really a nr?
            elem.push_attribute(mk_attr(b"flags", flags));
        }

        elem.push_attribute(mk_attr(b"version", XML_VERSION));
        elem.push_attribute(mk_attr(b"data_block_size", sb.data_block_size));
        elem.push_attribute(mk_attr(b"nr_data_blocks", sb.nr_data_blocks));

        if let Some(snap) = sb.metadata_snap {
            elem.push_attribute(mk_attr(b"metadata_snap", snap));
        }

        self.w.write_event(Event::Start(elem))?;
        Ok(())
    }

    fn superblock_e(&mut self) -> Result<()> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"superblock")))?;
        Ok(())
    }

    fn device_b(&mut self, d: &Device) -> Result<()> {
        let tag = b"device";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"dev_id", d.dev_id));
        elem.push_attribute(mk_attr(b"mapped_blocks", d.mapped_blocks));
        elem.push_attribute(mk_attr(b"transaction", d.transaction));
        elem.push_attribute(mk_attr(b"creation_time", d.creation_time));
        elem.push_attribute(mk_attr(b"snap_time", d.snap_time));
        self.w.write_event(Event::Start(elem))?;
        Ok(())
    }

    fn device_e(&mut self) -> Result<()> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"device")))?;
        Ok(())
    }

    fn map(&mut self, m: Map) -> Result<()> {
        match m.len {
            1 => {
                let tag = b"single_mapping";
                let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
                elem.push_attribute(mk_attr(b"origin_block", m.thin_begin));
                elem.push_attribute(mk_attr(b"data_block", m.data_begin));
                elem.push_attribute(mk_attr(b"time", m.time));
                self.w.write_event(Event::Empty(elem))?;
            }
            _ => {
                let tag = b"range_mapping";
                let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
                elem.push_attribute(mk_attr(b"origin_begin", m.thin_begin));
                elem.push_attribute(mk_attr(b"data_begin", m.data_begin));
                elem.push_attribute(mk_attr(b"length", m.len));
                elem.push_attribute(mk_attr(b"time", m.time));
                self.w.write_event(Event::Empty(elem))?;
            }
        }
        Ok(())
    }

    fn eof(&mut self) -> Result<()> {
        let w = self.w.inner();
        w.flush()?;
        Ok(())
    }
}

//---------------------------------------

// FIXME: nasty unwraps
fn string_val(kv: &Attribute) -> String {
    let v = kv.unescaped_value().unwrap();
    let bytes = v.to_vec();
    String::from_utf8(bytes).unwrap()
}

// FIXME: there's got to be a way of doing this without copying the string
fn u64_val(kv: &Attribute) -> Result<u64> {
    let n = string_val(kv).parse::<u64>()?;
    Ok(n)
}

fn u32_val(kv: &Attribute) -> Result<u32> {
    let n = string_val(kv).parse::<u32>()?;
    Ok(n)
}

fn bad_attr<T>(_tag: &str, _attr: &[u8]) -> Result<T> {
    todo!();
}

fn missing_attr<T>(_tag: &str, _attr: &str) -> Result<T> {
    todo!();
}

fn check_attr<T>(tag: &str, name: &str, maybe_v: Option<T>) -> Result<T> {
    match maybe_v {
        None => missing_attr(tag, name),
        Some(v) => Ok(v)
    }
}

fn parse_superblock(e: &BytesStart) -> Result<Superblock> {
    let mut uuid: Option<String> = None;
    let mut time: Option<u64> = None;
    let mut transaction: Option<u64> = None;
    let mut flags: Option<u32> = None;
    let mut version: Option<u32> = None;
    let mut data_block_size: Option<u32> = None;
    let mut nr_data_blocks: Option<u64> = None;
    let mut metadata_snap: Option<u64> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"uuid" => uuid = Some(string_val(&kv)),
            b"time" => time = Some(u64_val(&kv)?),
            b"transaction" => transaction = Some(u64_val(&kv)?),
            b"flags" => flags = Some(u32_val(&kv)?),
            b"version" => version = Some(u32_val(&kv)?),
            b"data_block_size" => data_block_size = Some(u32_val(&kv)?),
            b"nr_data_blocks" => nr_data_blocks = Some(u64_val(&kv)?),
            b"metadata_snap" => metadata_snap = Some(u64_val(&kv)?),
            _ => return bad_attr("superblock", kv.key),
        }
    }

    let tag = "superblock";

    Ok(Superblock {
        uuid: check_attr(tag, "uuid", uuid)?,
        time: check_attr(tag, "time", time)?,
        transaction: check_attr(tag, "transaction", transaction)?,
        flags: flags,
        version: version,
        data_block_size: check_attr(tag, "data_block_size", data_block_size)?,
        nr_data_blocks: check_attr(tag, "nr_data_blocks", nr_data_blocks)?,
        metadata_snap: metadata_snap,
    })
}

fn parse_device(e: &BytesStart) -> Result<Device> {
    let mut dev_id: Option<u32> = None;
    let mut mapped_blocks: Option<u64> = None;
    let mut transaction: Option<u64> = None;
    let mut creation_time: Option<u64> = None;
    let mut snap_time: Option<u64> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"dev_id" => dev_id = Some(u32_val(&kv)?),
            b"mapped_blocks" => mapped_blocks = Some(u64_val(&kv)?),
            b"transaction" => transaction = Some(u64_val(&kv)?),
            b"creation_time" => creation_time = Some(u64_val(&kv)?),
            b"snap_time" => snap_time = Some(u64_val(&kv)?),
            _ => return bad_attr("device", kv.key),
        }
    }

    let tag = "device";

    Ok(Device {
        dev_id: check_attr(tag, "dev_id", dev_id)?,
        mapped_blocks: check_attr(tag, "mapped_blocks", mapped_blocks)?,
        transaction: check_attr(tag, "transaction", transaction)?,
        creation_time: check_attr(tag, "creation_time", creation_time)?,
        snap_time: check_attr(tag, "snap_time", snap_time)?,
    })
}

fn parse_single_map(e: &BytesStart) -> Result<Map> {
    let mut thin_begin: Option<u64> = None;
    let mut data_begin: Option<u64> = None;
    let mut time: Option<u32> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"origin_block" => thin_begin = Some(u64_val(&kv)?),
            b"data_block" => data_begin = Some(u64_val(&kv)?),
            b"time" => time = Some(u32_val(&kv)?),
            _ => return bad_attr("single_mapping", kv.key),
        }
    }

    let tag = "single_mapping";

    Ok(Map {
        thin_begin: check_attr(tag, "origin_block", thin_begin)?,
        data_begin: check_attr(tag, "data_block", data_begin)?,
        time: check_attr(tag, "time", time)?,
        len: 1
    })
}

fn parse_range_map(e: &BytesStart) -> Result<Map> {
    let mut thin_begin: Option<u64> = None;
    let mut data_begin: Option<u64> = None;
    let mut time: Option<u32> = None;
    let mut length: Option<u64> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"origin_begin" => thin_begin = Some(u64_val(&kv)?),
            b"data_begin" => data_begin = Some(u64_val(&kv)?),
            b"time" => time = Some(u32_val(&kv)?),
            b"length" => length = Some(u64_val(&kv)?),
            _ => return bad_attr("range_mapping", kv.key),
        }
    }

    let tag = "range_mapping";

    Ok(Map {
        thin_begin: check_attr(tag, "origin_begin", thin_begin)?,
        data_begin: check_attr(tag, "data_begin", data_begin)?,
        time: check_attr(tag, "time", time)?,
        len: check_attr(tag, "length", length)?,
    })
}

pub fn read<R, M>(input: R, visitor: &mut M) -> Result<()>
where
    R: Read,
    M: MetadataVisitor,
{
    let input = BufReader::new(input);
    let mut reader = Reader::from_reader(input);

    reader.trim_text(true);
    let mut buf = Vec::new();

    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name() {
                b"superblock" => visitor.superblock_b(&parse_superblock(e)?)?,
                b"device" => visitor.device_b(&parse_device(e)?)?,
                _ => todo!(),
            },
            Ok(Event::End(ref e)) => match e.name() {
                b"superblock" => visitor.superblock_e()?,
                b"device" => visitor.device_e()?,
                _ => todo!(),
            },
            Ok(Event::Empty(ref e)) => match e.name() {
                b"single_mapping" => visitor.map(parse_single_map(e)?)?,
                b"range_mapping" => visitor.map(parse_range_map(e)?)?,
                _ => todo!(),
            },
            Ok(Event::Text(_)) => {}
            Ok(Event::Comment(_)) => {}
            Ok(Event::Eof) => break,
            Ok(_) => todo!(),

            // FIXME: don't panic!
            Err(e) => panic!("error parsing xml {:?}", e),
        }
    }

    Ok(())
}

//---------------------------------------
