use anyhow::{anyhow, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use std::io::{BufRead, BufReader};
use std::io::{Read, Write};

use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::{Reader, Writer};

use crate::cache::ir::*;
use crate::xml::*;

//---------------------------------------

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

impl<W: Write> MetadataVisitor for XmlWriter<W> {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<Visit> {
        let mut elem = BytesStart::new("superblock");
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
            .write_event(Event::End(BytesEnd::new("superblock")))?;
        Ok(Visit::Continue)
    }

    fn mappings_b(&mut self) -> Result<Visit> {
        let elem = BytesStart::new("mappings");
        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn mappings_e(&mut self) -> Result<Visit> {
        self.w.write_event(Event::End(BytesEnd::new("mappings")))?;
        Ok(Visit::Continue)
    }

    fn mapping(&mut self, m: &Map) -> Result<Visit> {
        let mut elem = BytesStart::new("mapping");
        elem.push_attribute(mk_attr(b"cache_block", m.cblock));
        elem.push_attribute(mk_attr(b"origin_block", m.oblock));
        elem.push_attribute(mk_attr(b"dirty", m.dirty));
        self.w.write_event(Event::Empty(elem))?;
        Ok(Visit::Continue)
    }

    fn hints_b(&mut self) -> Result<Visit> {
        let elem = BytesStart::new("hints");
        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn hints_e(&mut self) -> Result<Visit> {
        self.w.write_event(Event::End(BytesEnd::new("hints")))?;
        Ok(Visit::Continue)
    }

    fn hint(&mut self, h: &Hint) -> Result<Visit> {
        let mut elem = BytesStart::new("hint");
        elem.push_attribute(mk_attr(b"cache_block", h.cblock));
        elem.push_attribute(mk_attr(b"data", STANDARD.encode(&h.data[0..])));
        self.w.write_event(Event::Empty(elem))?;
        Ok(Visit::Continue)
    }

    fn discards_b(&mut self) -> Result<Visit> {
        let elem = BytesStart::new("discards");
        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn discards_e(&mut self) -> Result<Visit> {
        self.w.write_event(Event::End(BytesEnd::new("discards")))?;
        Ok(Visit::Continue)
    }

    fn discard(&mut self, d: &Discard) -> Result<Visit> {
        let mut elem = BytesStart::new("discard");
        elem.push_attribute(mk_attr(b"dbegin", d.begin));
        elem.push_attribute(mk_attr(b"dend", d.end));
        self.w.write_event(Event::Empty(elem))?;
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        let w = self.w.get_mut();
        w.flush()?;
        Ok(Visit::Continue)
    }
}

//------------------------------------------

fn parse_superblock(e: &BytesStart) -> Result<Superblock> {
    let mut uuid: Option<String> = None;
    let mut block_size: Option<u32> = None;
    let mut nr_cache_blocks: Option<u32> = None;
    let mut policy: Option<String> = None;
    let mut hint_width: Option<u32> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key.0 {
            b"uuid" => uuid = Some(string_val(&kv)?),
            b"block_size" => block_size = Some(u32_val(&kv)?),
            b"nr_cache_blocks" => nr_cache_blocks = Some(u32_val(&kv)?),
            b"policy" => policy = Some(string_val(&kv)?),
            b"hint_width" => hint_width = Some(u32_val(&kv)?),
            _ => return bad_attr("superblock", kv.key.0),
        }
    }

    let tag = "cache";

    Ok(Superblock {
        uuid: check_attr(tag, "uuid", uuid)?,
        block_size: check_attr(tag, "block_size", block_size)?,
        nr_cache_blocks: check_attr(tag, "nr_cache_blocks", nr_cache_blocks)?,
        policy: check_attr(tag, "policy", policy)?,
        hint_width: check_attr(tag, "hint_width", hint_width)?,
    })
}

fn parse_mapping(e: &BytesStart) -> Result<Map> {
    let mut cblock: Option<u32> = None;
    let mut oblock: Option<u64> = None;
    let mut dirty: Option<bool> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key.0 {
            b"cache_block" => cblock = Some(u32_val(&kv)?),
            b"origin_block" => oblock = Some(u64_val(&kv)?),
            b"dirty" => dirty = Some(bool_val(&kv)?),
            _ => return bad_attr("mapping", kv.key.0),
        }
    }

    let tag = "mapping";

    Ok(Map {
        cblock: check_attr(tag, "cache_block", cblock)?,
        oblock: check_attr(tag, "origin_block", oblock)?,
        dirty: check_attr(tag, "dirty", dirty)?,
    })
}

fn parse_hint(e: &BytesStart) -> Result<Hint> {
    let mut cblock: Option<u32> = None;
    let mut data: Option<Vec<u8>> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key.0 {
            b"cache_block" => cblock = Some(u32_val(&kv)?),
            b"data" => data = Some(STANDARD.decode(kv.value.as_ref())?),
            _ => return bad_attr("mapping", kv.key.0),
        }
    }

    let tag = "hint";

    Ok(Hint {
        cblock: check_attr(tag, "cache_block", cblock)?,
        data: check_attr(tag, "data", data)?,
    })
}

fn handle_event<R, M>(reader: &mut Reader<R>, buf: &mut Vec<u8>, visitor: &mut M) -> Result<Visit>
where
    R: Read + BufRead,
    M: MetadataVisitor,
{
    match reader.read_event_into(buf) {
        Ok(Event::Start(ref e)) => match e.name().0 {
            b"superblock" => visitor.superblock_b(&parse_superblock(e)?),
            b"mappings" => visitor.mappings_b(),
            b"hints" => visitor.hints_b(),
            _ => Err(anyhow!(
                "unknown start tag at byte {}",
                reader.buffer_position()
            )),
        },
        Ok(Event::End(ref e)) => match e.name().0 {
            b"superblock" => visitor.superblock_e(),
            b"mappings" => visitor.mappings_e(),
            b"hints" => visitor.hints_e(),
            _ => Err(anyhow!(
                "unknown end tag at byte {}",
                reader.buffer_position()
            )),
        },
        Ok(Event::Empty(ref e)) => match e.name().0 {
            b"mapping" => visitor.mapping(&parse_mapping(e)?),
            b"hint" => visitor.hint(&parse_hint(e)?),
            _ => Err(anyhow!(
                "unknown empty element at byte {}",
                reader.buffer_position()
            )),
        },
        Ok(Event::Text(_)) => Ok(Visit::Continue),
        Ok(Event::Comment(_)) => Ok(Visit::Continue),
        Ok(Event::Eof) => {
            visitor.eof()?;
            Ok(Visit::Stop)
        }
        Ok(_) => Err(anyhow!(
            "unsupported element at byte {}",
            reader.buffer_position()
        )),
        Err(e) => Err(anyhow!(
            "parse error at byte {}: {:?}",
            reader.buffer_position(),
            e
        )),
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
