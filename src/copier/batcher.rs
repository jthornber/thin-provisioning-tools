use anyhow::anyhow;
use std::sync::mpsc::SyncSender;
use std::vec::Vec;

use crate::copier::CopyOp;

//------------------------------------------

// This builds vectors of copy operations and posts them into a channel.
// The larger the batch size the more chance we have of finding and
// aggregating adjacent copies.
pub struct CopyOpBatcher {
    batch_size: usize,
    ops: Vec<CopyOp>,
    tx: SyncSender<Vec<CopyOp>>,
}

impl CopyOpBatcher {
    pub fn new(batch_size: usize, tx: SyncSender<Vec<CopyOp>>) -> Self {
        Self {
            batch_size,
            ops: Vec::with_capacity(batch_size),
            tx,
        }
    }

    /// Append a CopyOp to the current batch.
    pub fn push(&mut self, op: CopyOp) -> anyhow::Result<()> {
        self.ops.push(op);
        if self.ops.len() >= self.batch_size {
            self.send_ops()?;
        }

        Ok(())
    }

    /// Send the current batch and return the dirty ablocks bitmap.
    pub fn complete(mut self) -> anyhow::Result<()> {
        self.send_ops()?;
        Ok(())
    }

    /// Send the current batch.
    fn send_ops(&mut self) -> anyhow::Result<()> {
        let mut ops = std::mem::take(&mut self.ops);

        // We sort by the destination since this is likely to be a spindle
        // and keeping the io in sequential order will help performance.
        ops.sort_by(|lhs, rhs| lhs.dst.cmp(&rhs.dst));
        self.tx
            .send(ops)
            .map_err(|_| anyhow!("unable to send copy op array"))
    }
}

//------------------------------------------
