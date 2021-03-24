use anyhow::{anyhow, Result};
use std::{borrow::Cow, fmt::Display, io::prelude::*, io::BufReader, io::Write};

use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::{Reader, Writer};

//---------------------------------------

#[derive(Clone)]
pub struct Superblock {
    pub uuid: String,
    pub time: u64,
    pub transaction: u64,
    pub flags: Option<u32>,
    pub version: Option<u32>,
    pub data_block_size: u32,
    pub nr_data_blocks: u64,
    pub metadata_snap: Option<u64>,
}

#[derive(Clone)]
pub struct Device {
    pub dev_id: u32,
    pub mapped_blocks: u64,
    pub transaction: u64,
    pub creation_time: u64,
    pub snap_time: u64,
}

#[derive(Clone)]
pub struct Map {
    pub thin_begin: u64,
    pub data_begin: u64,
    pub time: u32,
    pub len: u64,
}

#[derive(Clone)]
pub enum Visit {
    Continue,
    Stop,
}

pub trait MetadataVisitor {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<Visit>;
    fn superblock_e(&mut self) -> Result<Visit>;

    // Defines a shared sub tree.  May only contain a 'map' (no 'ref' allowed).
    fn def_shared_b(&mut self, name: &str) -> Result<Visit>;
    fn def_shared_e(&mut self) -> Result<Visit>;

    // A device contains a number of 'map' or 'ref' items.
    fn device_b(&mut self, d: &Device) -> Result<Visit>;
    fn device_e(&mut self) -> Result<Visit>;

    fn map(&mut self, m: &Map) -> Result<Visit>;
    fn ref_shared(&mut self, name: &str) -> Result<Visit>;

    fn eof(&mut self) -> Result<Visit>;
}

pub struct XmlWriter<W: Write> {
    w: Writer<W>,
}

impl<W: Write> XmlWriter<W> {
    pub fn new(w: W) -> XmlWriter<W> {
        XmlWriter {
            w: Writer::new_with_indent(w, 0x20, 2),
        }
    }
}

fn mk_attr_<'a, T: Display>(n: T) -> Cow<'a, [u8]> {
    let str = format!("{}", n);
    Cow::Owned(str.into_bytes())
}

fn mk_attr<T: Display>(key: &[u8], value: T) -> Attribute {
    Attribute {
        key,
        value: mk_attr_(value),
    }
}

const XML_VERSION: u32 = 2;

impl<W: Write> MetadataVisitor for XmlWriter<W> {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<Visit> {
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
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"superblock")))?;
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, name: &str) -> Result<Visit> {
        let tag = b"def";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"name", name));
        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        self.w.write_event(Event::End(BytesEnd::borrowed(b"def")))?;
        Ok(Visit::Continue)
    }

    fn device_b(&mut self, d: &Device) -> Result<Visit> {
        let tag = b"device";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"dev_id", d.dev_id));
        elem.push_attribute(mk_attr(b"mapped_blocks", d.mapped_blocks));
        elem.push_attribute(mk_attr(b"transaction", d.transaction));
        elem.push_attribute(mk_attr(b"creation_time", d.creation_time));
        elem.push_attribute(mk_attr(b"snap_time", d.snap_time));
        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn device_e(&mut self) -> Result<Visit> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"device")))?;
        Ok(Visit::Continue)
    }

    fn map(&mut self, m: &Map) -> Result<Visit> {
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
        Ok(Visit::Continue)
    }

    fn ref_shared(&mut self, name: &str) -> Result<Visit> {
        let tag = b"ref";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"name", name));
        self.w.write_event(Event::Empty(elem))?;
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        let w = self.w.inner();
        w.flush()?;
        Ok(Visit::Continue)
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

fn missing_attr<T>(tag: &str, attr: &str) -> Result<T> {
    let msg = format!("missing attribute '{}' for tag '{}", attr, tag);
    Err(anyhow!(msg))
}

fn check_attr<T>(tag: &str, name: &str, maybe_v: Option<T>) -> Result<T> {
    match maybe_v {
        None => missing_attr(tag, name),
        Some(v) => Ok(v),
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
        flags,
        version,
        data_block_size: check_attr(tag, "data_block_size", data_block_size)?,
        nr_data_blocks: check_attr(tag, "nr_data_blocks", nr_data_blocks)?,
        metadata_snap,
    })
}

fn parse_def(e: &BytesStart, tag: &str) -> Result<String> {
    let mut name: Option<String> = None;
    
    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"name" => {
                name = Some(string_val(&kv));
            },
            _ => {
                return bad_attr(tag, kv.key)
            }
        }
    }

    Ok(name.unwrap())
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
        len: 1,
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

fn handle_event<R, M>(reader: &mut Reader<R>, buf: &mut Vec<u8>, visitor: &mut M) -> Result<Visit>
where
    R: Read + BufRead,
    M: MetadataVisitor,
{
    match reader.read_event(buf) {
        Ok(Event::Start(ref e)) => match e.name() {
            b"superblock" => visitor.superblock_b(&parse_superblock(e)?),
            b"device" => visitor.device_b(&parse_device(e)?),
            b"def" => visitor.def_shared_b(&parse_def(e, "def")?),
            _ => todo!(),
        },
        Ok(Event::End(ref e)) => match e.name() {
            b"superblock" => visitor.superblock_e(),
            b"device" => visitor.device_e(),
            b"def" => visitor.def_shared_e(),
            _ => todo!(),
        },
        Ok(Event::Empty(ref e)) => match e.name() {
            b"single_mapping" => visitor.map(&parse_single_map(e)?),
            b"range_mapping" => visitor.map(&parse_range_map(e)?),
            b"ref" => visitor.ref_shared(&parse_def(e, "ref")?),
            _ => todo!(),
        },
        Ok(Event::Text(_)) => Ok(Visit::Continue),
        Ok(Event::Comment(_)) => Ok(Visit::Continue),
        Ok(Event::Eof) => {
            visitor.eof()?;
            Ok(Visit::Stop)
        }
        Ok(_) => todo!(),

        // FIXME: don't panic!
        Err(e) => panic!("error parsing xml {:?}", e),
    }
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

    while let Visit::Continue = handle_event(&mut reader, &mut buf, visitor)? {}
    Ok(())
}

//---------------------------------------

struct SBVisitor {
    superblock: Option<Superblock>,
}

impl MetadataVisitor for SBVisitor {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<Visit> {
        self.superblock = Some(sb.clone());
        Ok(Visit::Stop)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, _name: &str) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn device_b(&mut self, _d: &Device) -> Result<Visit> {
        Ok(Visit::Continue)
    }
    fn device_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn map(&mut self, _m: &Map) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn ref_shared(&mut self, _name: &str) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        Ok(Visit::Stop)
    }
}

pub fn read_superblock<R>(input: R) -> Result<Superblock>
where
    R: Read,
{
    let mut v = SBVisitor { superblock: None };
    read(input, &mut v)?;
    Ok(v.superblock.unwrap())
}

//---------------------------------------
