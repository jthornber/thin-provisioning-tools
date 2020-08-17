use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Mutex;

//------------------------------------------

#[derive(Clone)]
pub enum ReportOutcome {
    Success,
    NonFatal,
    Fatal,
}

use ReportOutcome::*;

impl ReportOutcome {
    pub fn combine(lhs: &ReportOutcome, rhs: &ReportOutcome) -> ReportOutcome {
        match (lhs, rhs) {
            (Success, rhs) => rhs.clone(),
            (lhs, Success) => lhs.clone(),
            (Fatal, _) => Fatal,
            (_, Fatal) => Fatal,
            (_, _) => NonFatal,
        }
    }
}

pub struct Report {
    outcome: Mutex<ReportOutcome>,
    inner: Mutex<Box<dyn ReportInner + Send>>,
}

trait ReportInner {
    fn set_title(&mut self, txt: &str) -> Result<()>;
    fn progress(&mut self, percent: u8) -> Result<()>;
    fn log(&mut self, txt: &str) -> Result<()>;
    fn complete(&mut self) -> Result<()>;
}

impl Report {
    fn new(inner: Box<dyn ReportInner + Send>) -> Report {
        Report {
            outcome: Mutex::new(Success),
            inner: Mutex::new(inner),
        }
    }

    fn update_outcome(&self, rhs: ReportOutcome) {
        let mut lhs = self.outcome.lock().unwrap();
        *lhs = ReportOutcome::combine(&lhs, &rhs);
    }

    pub fn set_title(&self, txt: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.set_title(txt)
    }

    pub fn progress(&self, percent: u8) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.progress(percent)
    }

    pub fn info(&self, txt: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt)
    }

    pub fn non_fatal(&self, txt: &str) -> Result<()> {
        self.update_outcome(NonFatal);
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt)
    }

    pub fn fatal(&self, txt: &str) -> Result<()> {
        self.update_outcome(Fatal);
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt)
    }

    pub fn complete(&mut self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.complete()?;
        Ok(())
    }
}

//------------------------------------------

struct PBInner {
    bar: ProgressBar,
}

impl ReportInner for PBInner {
    fn set_title(&mut self, txt: &str) -> Result<()> {
        let mut fmt = "Checking thin metadata [{bar:40}] Remaining {eta}, ".to_string();
        fmt.push_str(&txt);
        self.bar.set_style(
            ProgressStyle::default_bar()
                .template(&fmt)
                .progress_chars("=> "),
        );
        Ok(())
    }

    fn progress(&mut self, percent: u8) -> Result<()> {
        self.bar.set_position(percent as u64);
        self.bar.tick();
        Ok(())
    }

    fn log(&mut self, txt: &str) -> Result<()> {
        self.bar.println(txt);
        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        self.bar.finish();
        Ok(())
    }
}

pub fn mk_progress_bar_report() -> Report {
    Report::new(Box::new(PBInner {
        bar: ProgressBar::new(100),
    }))
}

//------------------------------------------

struct SimpleInner {
    last_progress: std::time::SystemTime,
}

impl SimpleInner {
    fn new() -> SimpleInner {
        SimpleInner {
            last_progress: std::time::SystemTime::now(),
        }
    }
}

impl ReportInner for SimpleInner {
    fn set_title(&mut self, txt: &str) -> Result<()> {
        println!("{}", txt);
        Ok(())
    }

    fn progress(&mut self, percent: u8) -> Result<()> {
        let elapsed = self.last_progress.elapsed().unwrap();
        if elapsed > std::time::Duration::from_secs(5) {
            println!("Progress: {}%", percent);
            self.last_progress = std::time::SystemTime::now();
        }
        Ok(())
    }

    fn log(&mut self, txt: &str) -> Result<()> {
        eprintln!("{}", txt);
        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn mk_simple_report() -> Report {
    Report::new(Box::new(SimpleInner::new()))
}

//------------------------------------------

struct QuietInner {
}

impl ReportInner for QuietInner {
    fn set_title(&mut self, _txt: &str) -> Result<()> {
        Ok(())
    }

    fn progress(&mut self, _percent: u8) -> Result<()> {
        Ok(())
    }

    fn log(&mut self, _txt: &str) -> Result<()> {
        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        Ok(())
    }
}

pub fn mk_quiet_report() -> Report {
    Report::new(Box::new(QuietInner {}))
}

//------------------------------------------
