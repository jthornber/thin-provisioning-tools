use anyhow::Result;
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::Writer;
use std::io::Write;

use crate::era::ir::*;
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
        let tag = b"superblock";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"uuid", sb.uuid.clone()));
        elem.push_attribute(mk_attr(b"block_size", sb.block_size));
        elem.push_attribute(mk_attr(b"nr_blocks", sb.nr_blocks));
        elem.push_attribute(mk_attr(b"current_era", sb.current_era));

        self.w.write_event(Event::Start(elem))?;
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
        Ok(Visit::Continue)
    }

    fn writeset_e(&mut self) -> Result<Visit> {
        self.w
            .write_event(Event::End(BytesEnd::borrowed(b"writeset")))?;
        Ok(Visit::Continue)
    }

    fn writeset_bit(&mut self, wbit: &WritesetBit) -> Result<Visit> {
        let tag = b"bit";
        let mut elem = BytesStart::owned(tag.to_vec(), tag.len());
        elem.push_attribute(mk_attr(b"block", wbit.block));
        elem.push_attribute(mk_attr(b"value", wbit.value));
        self.w.write_event(Event::Empty(elem))?;
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
        Ok(Visit::Continue)
    }
}

//------------------------------------------
