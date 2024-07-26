use anyhow::{anyhow, Result};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::copier::*;

//---------------------------------------

/*
 * A simple wrapper that runs Copier in a worker thread.
 * The copy process terminates on any read/write error.
 */
pub struct ThreadedCopier<T> {
    copier: T,
}

impl<T: Copier + Send + 'static> ThreadedCopier<T> {
    pub fn new(copier: T) -> ThreadedCopier<T> {
        ThreadedCopier { copier }
    }

    pub fn run(
        self,
        rx: mpsc::Receiver<Vec<CopyOp>>,
        progress: Arc<dyn CopyProgress + Send + Sync>,
    ) -> JoinHandle<Result<()>> {
        thread::spawn(move || Self::run_(rx, self.copier, progress))
    }

    fn run_(
        rx: mpsc::Receiver<Vec<CopyOp>>,
        mut copier: T,
        progress: Arc<dyn CopyProgress + Send + Sync>,
    ) -> Result<()> {
        while let Ok(ops) = rx.recv() {
            let stats = copier
                .copy(&ops, progress.clone())
                .map_err(|e| anyhow!("copy failed: {}", e))?;

            if !stats.read_errors.is_empty() {
                return Err(anyhow!("read error in block {}", stats.read_errors[0].src));
            }

            if !stats.write_errors.is_empty() {
                return Err(anyhow!(
                    "write error in block {}",
                    stats.write_errors[0].dst
                ));
            }
        }

        Ok(())
    }
}

//---------------------------------------
