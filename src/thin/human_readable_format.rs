use anyhow::Result;
use std::io::Write;

use crate::thin::ir::*;

//---------------------------------------

pub struct HumanReadableWriter<W: Write> {
    w: W,
}

impl<W: Write> HumanReadableWriter<W> {
    pub fn new(w: W) -> HumanReadableWriter<W> {
        HumanReadableWriter { w }
    }
}

const METADATA_VERSION: u32 = 2;

impl<W: Write> MetadataVisitor for HumanReadableWriter<W> {
    fn superblock_b(&mut self, sb: &Superblock) -> Result<Visit> {
        write!(
            self.w,
            "begin superblock: \"{}\", {}, {}, {}, {}, {}, {}",
            sb.uuid,
            sb.time,
            sb.transaction,
            sb.flags.unwrap_or(0),
            sb.version.unwrap_or(METADATA_VERSION),
            sb.data_block_size,
            sb.nr_data_blocks
        )?;

        if let Some(b) = sb.metadata_snap {
            writeln!(self.w, ", {}", b)?;
        } else {
            self.w.write_all(b"\n")?;
        }

        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.w.write_all(b"end superblock\n")?;
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, name: &str) -> Result<Visit> {
        writeln!(self.w, "def: {}", name)?;
        Ok(Visit::Continue)
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        self.w.write_all(b"\n")?;
        Ok(Visit::Continue)
    }

    fn device_b(&mut self, d: &Device) -> Result<Visit> {
        // The words "mapped_blocks" is connected by an underscore for backward compatibility.
        writeln!(
            self.w,
            "device: {}\nmapped_blocks: {}\ntransaction: {}\ncreation time: {}\nsnap time: {}",
            d.dev_id, d.mapped_blocks, d.transaction, d.creation_time, d.snap_time
        )?;
        Ok(Visit::Continue)
    }

    fn device_e(&mut self) -> Result<Visit> {
        self.w.write_all(b"\n")?;
        Ok(Visit::Continue)
    }

    fn map(&mut self, m: &Map) -> Result<Visit> {
        writeln!(
            self.w,
            "    ({}..{}) -> ({}..{}), {}",
            m.thin_begin,
            m.thin_begin + m.len - 1,
            m.data_begin,
            m.data_begin + m.len - 1,
            m.time
        )?;
        Ok(Visit::Continue)
    }

    fn ref_shared(&mut self, name: &str) -> Result<Visit> {
        writeln!(self.w, "    ref: {}", name)?;
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        self.w.flush()?;
        Ok(Visit::Continue)
    }
}

//---------------------------------------
