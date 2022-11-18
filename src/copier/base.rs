use anyhow::Result;
use std::sync::Arc;

//-------------------------------------

pub type Block = u64;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CopyOp {
    pub src: Block,
    pub dst: Block,
}

#[derive(Debug)]
pub struct CopyStats {
    pub nr_blocks: Block,
    pub nr_copied: Block,
    pub read_errors: Vec<CopyOp>,
    pub write_errors: Vec<CopyOp>,
}

impl CopyStats {
    pub fn new(nr_blocks: u64) -> Self {
        Self {
            nr_blocks,
            nr_copied: 0,
            read_errors: Vec::new(),
            write_errors: Vec::new(),
        }
    }
}

pub trait CopyProgress {
    fn update(&self, stats: &CopyStats);
}

// The constructor for the instance should be passed the src and dst
// paths and the block size.
pub trait Copier {
    /// This copies the blocks in roughly the order given, so sort ops before
    /// submitting. eg, cache writeback would sort by dst since that's
    /// likely a spindle device where ordering really matters.
    fn copy(
        &mut self,
        ops: &[CopyOp],
        progress: Arc<dyn CopyProgress + Sync + Send>,
    ) -> Result<CopyStats>;
}

//-------------------------------------

#[derive(Default)]
pub struct IgnoreProgress {}

impl CopyProgress for IgnoreProgress {
    fn update(&self, _: &CopyStats) {}
}

//-------------------------------------
