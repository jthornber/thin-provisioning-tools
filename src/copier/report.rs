use std::sync::{Arc, Mutex};

use crate::copier::{CopyProgress, CopyStats};
use crate::report::Report;

//-------------------------------------

#[derive(Default)]
pub struct IgnoreProgress {}

impl CopyProgress for IgnoreProgress {
    fn update(&self, _: &CopyStats) {}
    fn inc_stats(&self, _: &CopyStats) {}
}

//-----------------------------------------

struct AccumulatedStats {
    /// Number of blocks that need to be copied
    nr_blocks: u64,

    /// Number of blocks that were successfully copied
    nr_copied: u64,

    /// Number of read errors
    nr_read_errors: u64,

    /// Number of write errors
    nr_write_errors: u64,
}

/// Struct that updates the progress bar in the reporter as the stats are
/// updated by the copier threads.
pub struct ProgressReporter {
    report: Arc<Report>,
    inner: Mutex<AccumulatedStats>,
}

impl ProgressReporter {
    /// nr_blocks: total number of blocks to be copied
    pub fn new(report: Arc<Report>, nr_blocks: u64) -> Self {
        Self {
            report,
            inner: Mutex::new(AccumulatedStats {
                nr_blocks,
                nr_copied: 0,
                nr_read_errors: 0,
                nr_write_errors: 0,
            }),
        }
    }

    fn update_error_logs(&self, nr_read_errors: u64, nr_write_errors: u64) {
        if nr_read_errors > 0 || nr_write_errors > 0 {
            self.report.set_sub_title(&format!(
                "read errors {}, write errors {}",
                nr_read_errors, nr_write_errors
            ));
        }
    }

    fn update_progress(&self, nr_copied: u64, nr_blocks: u64) {
        let percent = (nr_copied * 100).checked_div(nr_blocks).unwrap_or(100);
        self.report.progress(percent as u8);
    }
}

impl CopyProgress for ProgressReporter {
    // This doesn't update the internal stats in struct, that is left until the copy
    // batch is complete.
    fn update(&self, stats: &CopyStats) {
        let inner = self.inner.lock().unwrap();

        self.update_error_logs(
            inner.nr_read_errors + stats.read_errors.len() as u64,
            inner.nr_write_errors + stats.write_errors.len() as u64,
        );
        self.update_progress(inner.nr_copied + stats.nr_copied, inner.nr_blocks);
    }

    // Adds the resulting stats of a copy batch into the internal base,
    // then updates the displayed progress.
    fn inc_stats(&self, stats: &CopyStats) {
        let mut inner = self.inner.lock().unwrap();
        inner.nr_copied += stats.nr_copied;
        inner.nr_read_errors += stats.read_errors.len() as u64;
        inner.nr_write_errors += stats.write_errors.len() as u64;

        self.update_error_logs(inner.nr_read_errors, inner.nr_write_errors);
        self.update_progress(inner.nr_copied, inner.nr_blocks);
    }
}

//-----------------------------------------
