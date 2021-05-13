use anyhow::Result;
use base64::encode;
use std::{borrow::Cow, fmt::Display, io::Write};

use quick_xml::events::attributes::Attribute;
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::Writer;

//---------------------------------------

#[derive(Clone)]
pub struct Superblock {
    pub uuid: String,
    pub block_size: u32,
    pub nr_cache_blocks: u32,
    pub policy: String,
    pub hint_width: u32,
}

#[derive(Clone)]
pub struct Map {
    pub cblock: u32,
    pub oblock: u64,
    pub dirty: bool,
}

#[derive(Clone)]
pub struct Hint {
    pub cblock: u32,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct Discard {
    pub begin: u64,
    pub end: u64,
}

#[derive(Clone)]
pub enum Visit {
    Continue,
    Stop,
}

pub trait MetadataVisitor {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<Visit>;
    fn superblock_e(&mut self) -> Result<Visit>;

    fn mappings_b(&mut self) -> Result<Visit>;
    fn mappings_e(&mut self) -> Result<Visit>;
    fn mapping(&mut self, m: &Map) -> Result<Visit>;

    fn hints_b(&mut self) -> Result<Visit>;
    fn hints_e(&mut self) -> Result<Visit>;
    fn hint(&mut self, h: &Hint) -> Result<Visit>;

    fn discards_b(&mut self) -> Result<Visit>;
    fn discards_e(&mut self) -> Result<Visit>;
    fn discard(&mut self, d: &Discard) -> Result<Visit>;

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

impl<W: Write> MetadataVisitor for XmlWriter<W> {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<Visit> {
        let tag = b"superblock";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"uuid", sb.uuid.clone()));
        elem.push_attribute(mk_attr(b"block_size", sb.block_size));
        elem.push_attribute(mk_attr(b"nr_cache_blocks", sb.nr_cache_blocks));
        elem.push_attribute(mk_attr(b"policy", sb.policy.clone()));
        elem.push_attribute(mk_attr(b"hint_width", sb.hint_width));

        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"superblock")))?;
        Ok(Visit::Continue)
    }

    fn mappings_b(&mut self) -> Result<Visit> {
        let tag = b"mappings";
        let elem = BytesStart::owned(tag.to_vec(), tag.len());
        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn mappings_e(&mut self) -> Result<Visit> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"mappings")))?;
        Ok(Visit::Continue)
    }

    fn mapping(&mut self, m: &Map) -> Result<Visit> {
        let tag = b"mapping";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"cache_block", m.cblock));
        elem.push_attribute(mk_attr(b"origin_block", m.oblock));
        elem.push_attribute(mk_attr(b"dirty", m.dirty));
        self.w.write_event(Event::Empty(elem))?;
        Ok(Visit::Continue)
    }

    fn hints_b(&mut self) -> Result<Visit> {
        let tag = b"hints";
        let elem = BytesStart::owned(tag.to_vec(), tag.len());
        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn hints_e(&mut self) -> Result<Visit> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"hints")))?;
        Ok(Visit::Continue)
    }

    fn hint(&mut self, h: &Hint) -> Result<Visit> {
        let tag = b"hint";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"cache_block", h.cblock));
        elem.push_attribute(mk_attr(b"data", encode(&h.data[0..])));
        self.w.write_event(Event::Empty(elem))?;
        Ok(Visit::Continue)
    }

    fn discards_b(&mut self) -> Result<Visit> {
        let tag = b"discards";
        let elem = BytesStart::owned(tag.to_vec(), tag.len());
        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn discards_e(&mut self) -> Result<Visit> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"discards")))?;
        Ok(Visit::Continue)
    }

    fn discard(&mut self, d: &Discard) -> Result<Visit> {
        let tag = b"discard";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"dbegin", d.begin));
        elem.push_attribute(mk_attr(b"dend", d.end));
        self.w.write_event(Event::Empty(elem))?;
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }
}
