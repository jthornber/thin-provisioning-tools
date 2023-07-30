use anyhow::Result;
use std::io::Write;

use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::Writer;

use crate::thin::ir::{self, Visit};
use crate::xml::mk_attr;

//------------------------------------------

// The `time` field is ignored since people are more interest in block address.
#[derive(Clone)]
#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct DataMapping {
    pub thin_begin: u64,
    pub data_begin: u64,
    pub len: u64,
}

#[derive(Clone)]
#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct DiffMapping {
    pub thin_begin: u64,
    pub left_data_begin: u64,
    pub right_data_begin: u64,
    pub len: u64,
}

#[derive(Clone)]
#[cfg_attr(test, derive(Debug, PartialEq))]
pub enum Delta {
    LeftOnly(DataMapping),
    RightOnly(DataMapping),
    Differ(DiffMapping),
    Same(DataMapping),
}

pub enum Snap {
    DeviceId(u64),
    RootBlock(u64),
}

pub trait DeltaVisitor {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit>;
    fn superblock_e(&mut self) -> Result<Visit>;
    fn diff_b(&mut self, snap1: Snap, snap2: Snap) -> Result<Visit>;
    fn diff_e(&mut self) -> Result<Visit>;
    fn delta(&mut self, d: &Delta) -> Result<Visit>;
}

//------------------------------------------

struct DeltaRunBuilder {
    run: Option<Delta>,
}

impl DeltaRunBuilder {
    fn new() -> DeltaRunBuilder {
        DeltaRunBuilder { run: None }
    }

    fn next(&mut self, d: &Delta) -> Option<Delta> {
        match d {
            Delta::LeftOnly(r) => {
                if let Some(Delta::LeftOnly(ref mut cur)) = self.run {
                    if r.thin_begin == cur.thin_begin + cur.len {
                        cur.len += r.len;
                        return None;
                    }
                }
                self.run.replace(d.clone())
            }
            Delta::RightOnly(r) => {
                if let Some(Delta::RightOnly(ref mut cur)) = self.run {
                    if r.thin_begin == cur.thin_begin + cur.len {
                        cur.len += r.len;
                        return None;
                    }
                }
                self.run.replace(d.clone())
            }
            Delta::Differ(r) => {
                if let Some(Delta::Differ(ref mut cur)) = self.run {
                    if r.thin_begin == cur.thin_begin + cur.len {
                        cur.len += r.len;
                        return None;
                    }
                }
                self.run.replace(d.clone())
            }
            Delta::Same(r) => {
                if let Some(Delta::Same(ref mut cur)) = self.run {
                    if r.thin_begin == cur.thin_begin + cur.len {
                        cur.len += r.len;
                        return None;
                    }
                }
                self.run.replace(d.clone())
            }
        }
    }

    fn complete(&mut self) -> Option<Delta> {
        self.run.take()
    }
}

//------------------------------------------

pub struct SimpleXmlWriter<W: Write> {
    w: Writer<W>,
    builder: DeltaRunBuilder,
}

impl<W: Write> SimpleXmlWriter<W> {
    pub fn new(w: W) -> SimpleXmlWriter<W> {
        SimpleXmlWriter {
            w: Writer::new_with_indent(w, 0x20, 2),
            builder: DeltaRunBuilder::new(),
        }
    }

    fn write_delta(&mut self, d: &Delta) -> Result<()> {
        match d {
            Delta::LeftOnly(r) => {
                let mut elem = BytesStart::new("left_only");
                elem.push_attribute(mk_attr(b"begin", r.thin_begin));
                elem.push_attribute(mk_attr(b"length", r.len));
                self.w.write_event(Event::Empty(elem))?;
            }
            Delta::RightOnly(r) => {
                let mut elem = BytesStart::new("right_only");
                elem.push_attribute(mk_attr(b"begin", r.thin_begin));
                elem.push_attribute(mk_attr(b"length", r.len));
                self.w.write_event(Event::Empty(elem))?;
            }
            Delta::Differ(r) => {
                let mut elem = BytesStart::new("different");
                elem.push_attribute(mk_attr(b"begin", r.thin_begin));
                elem.push_attribute(mk_attr(b"length", r.len));
                self.w.write_event(Event::Empty(elem))?;
            }
            Delta::Same(r) => {
                let mut elem = BytesStart::new("same");
                elem.push_attribute(mk_attr(b"begin", r.thin_begin));
                elem.push_attribute(mk_attr(b"length", r.len));
                self.w.write_event(Event::Empty(elem))?;
            }
        }
        Ok(())
    }
}

impl<W: Write> DeltaVisitor for SimpleXmlWriter<W> {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit> {
        write_superblock_b(&mut self.w, sb)?;
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        write_superblock_e(&mut self.w)?;
        Ok(Visit::Continue)
    }

    fn diff_b(&mut self, snap1: Snap, snap2: Snap) -> Result<Visit> {
        write_diff_b(&mut self.w, snap1, snap2)?;
        Ok(Visit::Continue)
    }

    fn diff_e(&mut self) -> Result<Visit> {
        if let Some(r) = self.builder.complete() {
            self.write_delta(&r)?;
        }
        write_diff_e(&mut self.w)?;
        Ok(Visit::Continue)
    }

    fn delta(&mut self, d: &Delta) -> Result<Visit> {
        if let Some(run) = self.builder.next(d) {
            self.write_delta(&run)?;
        }
        Ok(Visit::Continue)
    }
}

//------------------------------------------

#[derive(PartialEq, Clone, Copy)]
enum DeltaType {
    LeftOnly,
    RightOnly,
    Differ,
    Same,
}

pub struct VerboseXmlWriter<W: Write> {
    w: Writer<W>,
    builder: DeltaRunBuilder,
    current_type: Option<DeltaType>,
}

impl<W: Write> VerboseXmlWriter<W> {
    pub fn new(w: W) -> VerboseXmlWriter<W> {
        VerboseXmlWriter {
            w: Writer::new_with_indent(w, 0x20, 2),
            builder: DeltaRunBuilder::new(),
            current_type: None,
        }
    }

    fn open_type_tag(&mut self, typ: DeltaType) -> Result<()> {
        let tag: &str = match typ {
            DeltaType::LeftOnly => "left_only",
            DeltaType::RightOnly => "right_only",
            DeltaType::Differ => "different",
            DeltaType::Same => "same",
        };
        self.w.write_event(Event::Start(BytesStart::new(tag)))?;
        Ok(())
    }

    fn close_type_tag(&mut self, typ: DeltaType) -> Result<()> {
        let tag: &str = match typ {
            DeltaType::LeftOnly => "left_only",
            DeltaType::RightOnly => "right_only",
            DeltaType::Differ => "different",
            DeltaType::Same => "same",
        };
        self.w.write_event(Event::End(BytesEnd::new(tag)))?;
        Ok(())
    }

    fn update_type_tag(&mut self, new_type: DeltaType) -> Result<()> {
        if let Some(current_type) = self.current_type {
            if new_type != current_type {
                self.close_type_tag(current_type)?;
                self.current_type = Some(new_type);
                self.open_type_tag(new_type)?;
            }
        } else {
            self.current_type = Some(new_type);
            self.open_type_tag(new_type)?;
        }
        Ok(())
    }

    fn write_delta_range(&mut self, m: &DataMapping) -> Result<()> {
        let mut elem = BytesStart::new("range");
        elem.push_attribute(mk_attr(b"begin", m.thin_begin));
        elem.push_attribute(mk_attr(b"data_begin", m.data_begin));
        elem.push_attribute(mk_attr(b"length", m.len));
        self.w.write_event(Event::Empty(elem))?;
        Ok(())
    }

    fn write_diff_range(&mut self, m: &DiffMapping) -> Result<()> {
        let mut elem = BytesStart::new("range");
        elem.push_attribute(mk_attr(b"begin", m.thin_begin));
        elem.push_attribute(mk_attr(b"left_data_begin", m.left_data_begin));
        elem.push_attribute(mk_attr(b"right_data_begin", m.right_data_begin));
        elem.push_attribute(mk_attr(b"length", m.len));
        self.w.write_event(Event::Empty(elem))?;
        Ok(())
    }

    fn write_delta(&mut self, d: &Delta) -> Result<()> {
        match d {
            Delta::LeftOnly(m) => {
                self.update_type_tag(DeltaType::LeftOnly)?;
                self.write_delta_range(m)
            }
            Delta::RightOnly(m) => {
                self.update_type_tag(DeltaType::RightOnly)?;
                self.write_delta_range(m)
            }
            Delta::Differ(m) => {
                self.update_type_tag(DeltaType::Differ)?;
                self.write_diff_range(m)
            }
            Delta::Same(m) => {
                self.update_type_tag(DeltaType::Same)?;
                self.write_delta_range(m)
            }
        }
    }
}

impl<W: Write> DeltaVisitor for VerboseXmlWriter<W> {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit> {
        write_superblock_b(&mut self.w, sb)?;
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        write_superblock_e(&mut self.w)?;
        Ok(Visit::Continue)
    }

    fn diff_b(&mut self, snap1: Snap, snap2: Snap) -> Result<Visit> {
        write_diff_b(&mut self.w, snap1, snap2)?;
        Ok(Visit::Continue)
    }

    fn diff_e(&mut self) -> Result<Visit> {
        if let Some(r) = self.builder.complete() {
            self.write_delta(&r)?;
        }
        if let Some(current_type) = self.current_type.take() {
            self.close_type_tag(current_type)?;
        }
        write_diff_e(&mut self.w)?;
        Ok(Visit::Continue)
    }

    fn delta(&mut self, d: &Delta) -> Result<Visit> {
        if let Some(run) = self.builder.next(d) {
            self.write_delta(&run)?;
        }
        Ok(Visit::Continue)
    }
}

//------------------------------------------

// TODO: move these common functions into an abstract class
fn write_superblock_b<W: Write>(w: &mut Writer<W>, sb: &ir::Superblock) -> Result<()> {
    let mut elem = BytesStart::new("superblock");
    elem.push_attribute(mk_attr(b"uuid", sb.uuid.clone()));
    elem.push_attribute(mk_attr(b"time", sb.time));
    elem.push_attribute(mk_attr(b"transaction", sb.transaction));
    if let Some(flags) = sb.flags {
        // FIXME: is this really a nr?
        elem.push_attribute(mk_attr(b"flags", flags));
    }

    elem.push_attribute(mk_attr(b"data_block_size", sb.data_block_size));
    elem.push_attribute(mk_attr(b"nr_data_blocks", sb.nr_data_blocks));

    if let Some(snap) = sb.metadata_snap {
        elem.push_attribute(mk_attr(b"metadata_snap", snap));
    }

    w.write_event(Event::Start(elem))?;
    Ok(())
}

fn write_superblock_e<W: Write>(w: &mut Writer<W>) -> Result<()> {
    w.write_event(Event::End(BytesEnd::new("superblock")))?;
    Ok(())
}

fn write_diff_b<W: Write>(w: &mut Writer<W>, snap1: Snap, snap2: Snap) -> Result<()> {
    let mut elem = BytesStart::new("diff");
    match snap1 {
        Snap::DeviceId(dev_id) => elem.push_attribute(mk_attr(b"left", dev_id)),
        Snap::RootBlock(blocknr) => elem.push_attribute(mk_attr(b"left_root", blocknr)),
    }
    match snap2 {
        Snap::DeviceId(dev_id) => elem.push_attribute(mk_attr(b"right", dev_id)),
        Snap::RootBlock(blocknr) => elem.push_attribute(mk_attr(b"right_root", blocknr)),
    }
    w.write_event(Event::Start(elem))?;
    Ok(())
}

fn write_diff_e<W: Write>(w: &mut Writer<W>) -> Result<()> {
    w.write_event(Event::End(BytesEnd::new("diff")))?;
    Ok(())
}

//------------------------------------------
