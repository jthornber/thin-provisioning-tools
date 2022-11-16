use anyhow::{anyhow, Result};
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::{Reader, Writer};
use std::io::{BufRead, BufReader};
use std::io::{Read, Write};

use crate::era::ir::*;
use crate::xml::*;

//---------------------------------------

pub struct XmlWriter<W: Write> {
    w: Writer<W>,
    compact: bool,
    nr_blocks: u32,
    emitted_blocks: u32,
}

impl<W: Write> XmlWriter<W> {
    pub fn new(w: W, compact: bool) -> XmlWriter<W> {
        XmlWriter {
            w: Writer::new_with_indent(w, 0x20, 2),
            compact,
            nr_blocks: 0,
            emitted_blocks: 0,
        }
    }
}

impl<W: Write> MetadataVisitor for XmlWriter<W> {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<Visit> {
        let tag = b"superblock";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"uuid", sb.uuid.clone()));
        elem.push_attribute(mk_attr(b"block_size", sb.block_size));
        elem.push_attribute(mk_attr(b"nr_blocks", sb.nr_blocks));
        elem.push_attribute(mk_attr(b"current_era", sb.current_era));
        self.w.write_event(Event::Start(elem))?;

        self.nr_blocks = sb.nr_blocks;

        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"superblock")))?;
        Ok(Visit::Continue)
    }

    fn writeset_b(&mut self, ws: &Writeset) -> Result<Visit> {
        let tag = b"writeset";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"era", ws.era));
        elem.push_attribute(mk_attr(b"nr_bits", ws.nr_bits));
        self.w.write_event(Event::Start(elem))?;

        self.emitted_blocks = 0;

        Ok(Visit::Continue)
    }

    fn writeset_e(&mut self) -> Result<Visit> {
        if !self.compact {
            for b in self.emitted_blocks..self.nr_blocks {
                let tag = b"bit";
                let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
                elem.push_attribute(mk_attr(b"block", b));
                elem.push_attribute(mk_attr(b"value", "false"));
                self.w.write_event(Event::Empty(elem))?;
            }
        }
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"writeset")))?;
        Ok(Visit::Continue)
    }

    fn writeset_blocks(&mut self, blocks: &MarkedBlocks) -> Result<Visit> {
        if self.compact {
            let tag = b"marked";
            let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
            elem.push_attribute(mk_attr(b"block_begin", blocks.begin));
            elem.push_attribute(mk_attr(b"len", blocks.len));
            self.w.write_event(Event::Empty(elem))?;
        } else {
            for b in self.emitted_blocks..blocks.begin {
                let tag = b"bit";
                let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
                elem.push_attribute(mk_attr(b"block", b));
                elem.push_attribute(mk_attr(b"value", "false"));
                self.w.write_event(Event::Empty(elem))?;
            }

            let end = blocks.begin + blocks.len;
            for b in blocks.begin..end {
                let tag = b"bit";
                let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
                elem.push_attribute(mk_attr(b"block", b));
                elem.push_attribute(mk_attr(b"value", "true"));
                self.w.write_event(Event::Empty(elem))?;
            }

            self.emitted_blocks = end;
        }

        Ok(Visit::Continue)
    }

    fn era_b(&mut self) -> Result<Visit> {
        let tag = b"era_array";
        let elem = BytesStart::owned(tag.to_vec(), tag.len());
        self.w.write_event(Event::Start(elem))?;
        Ok(Visit::Continue)
    }

    fn era_e(&mut self) -> Result<Visit> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"era_array")))?;
        Ok(Visit::Continue)
    }

    fn era(&mut self, era: &Era) -> Result<Visit> {
        let tag = b"era";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"block", era.block));
        elem.push_attribute(mk_attr(b"era", era.era));
        self.w.write_event(Event::Empty(elem))?;
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        let w = self.w.inner();
        w.flush()?;
        Ok(Visit::Continue)
    }
}

//------------------------------------------

fn parse_superblock(e: &BytesStart) -> Result<Superblock> {
    let tag = "superblock";
    let mut uuid: Option<String> = None;
    let mut block_size: Option<u32> = None;
    let mut nr_blocks: Option<u32> = None;
    let mut current_era: Option<u32> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"uuid" => uuid = Some(string_val(&kv)),
            b"block_size" => block_size = Some(u32_val(&kv)?),
            b"nr_blocks" => nr_blocks = Some(u32_val(&kv)?),
            b"current_era" => current_era = Some(u32_val(&kv)?),
            _ => return bad_attr(tag, kv.key),
        }
    }

    Ok(Superblock {
        uuid: check_attr(tag, "uuid", uuid)?,
        block_size: check_attr(tag, "block_size", block_size)?,
        nr_blocks: check_attr(tag, "nr_cache_blocks", nr_blocks)?,
        current_era: check_attr(tag, "current_era", current_era)?,
    })
}

fn parse_writeset(e: &BytesStart) -> Result<Writeset> {
    let tag = "writeset";
    let mut era: Option<u32> = None;
    let mut nr_bits: Option<u32> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"era" => era = Some(u32_val(&kv)?),
            b"nr_bits" => nr_bits = Some(u32_val(&kv)?),
            _ => return bad_attr(tag, kv.key),
        }
    }

    Ok(Writeset {
        era: check_attr(tag, "era", era)?,
        nr_bits: check_attr(tag, "nr_bits", nr_bits)?,
    })
}

fn parse_writeset_bit(e: &BytesStart) -> Result<Option<MarkedBlocks>> {
    let tag = "bit";
    let mut block: Option<u32> = None;
    let mut value: Option<bool> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"block" => block = Some(u32_val(&kv)?),
            b"value" => value = Some(bool_val(&kv)?),
            _ => return bad_attr(tag, kv.key),
        }
    }

    check_attr(tag, "block", block)?;
    check_attr(tag, "value", value)?;

    if let Some(true) = value {
        Ok(Some(MarkedBlocks {
            begin: block.unwrap(),
            len: 1,
        }))
    } else {
        Ok(None)
    }
}

fn parse_writeset_blocks(e: &BytesStart) -> Result<MarkedBlocks> {
    let tag = "marked";
    let mut begin: Option<u32> = None;
    let mut len: Option<u32> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"block_begin" => begin = Some(u32_val(&kv)?),
            b"len" => len = Some(u32_val(&kv)?),
            _ => return bad_attr(tag, kv.key),
        }
    }

    Ok(MarkedBlocks {
        begin: check_attr(tag, "block_begin", begin)?,
        len: check_attr(tag, "len", len)?,
    })
}

fn parse_era(e: &BytesStart) -> Result<Era> {
    let tag = "era";
    let mut block: Option<u32> = None;
    let mut era: Option<u32> = None;

    for a in e.attributes() {
        let kv = a.unwrap();
        match kv.key {
            b"block" => block = Some(u32_val(&kv)?),
            b"era" => era = Some(u32_val(&kv)?),
            _ => return bad_attr(tag, kv.key),
        }
    }

    Ok(Era {
        block: check_attr(tag, "block", block)?,
        era: check_attr(tag, "era", era)?,
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
            b"writeset" => visitor.writeset_b(&parse_writeset(e)?),
            b"era_array" => visitor.era_b(),
            _ => Err(anyhow!("Parse error at byte {}", reader.buffer_position())),
        },
        Ok(Event::End(ref e)) => match e.name() {
            b"superblock" => visitor.superblock_e(),
            b"writeset" => visitor.writeset_e(),
            b"era_array" => visitor.era_e(),
            _ => Err(anyhow!("Parse error at byte {}", reader.buffer_position())),
        },
        Ok(Event::Empty(ref e)) => match e.name() {
            b"bit" => {
                if let Some(b) = parse_writeset_bit(e)? {
                    visitor.writeset_blocks(&b)
                } else {
                    Ok(Visit::Continue)
                }
            }
            b"marked" => visitor.writeset_blocks(&parse_writeset_blocks(e)?),
            b"era" => visitor.era(&parse_era(e)?),
            _ => Err(anyhow!("Parse error at byte {}", reader.buffer_position())),
        },
        Ok(Event::Text(_)) => Ok(Visit::Continue),
        Ok(Event::Comment(_)) => Ok(Visit::Continue),
        Ok(Event::Eof) => {
            visitor.eof()?;
            Ok(Visit::Stop)
        }
        Ok(_) => Err(anyhow!("Parse error at byte {}", reader.buffer_position())),
        Err(e) => Err(anyhow!(
            "Parse error at byte {}: {:?}",
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

//------------------------------------------
