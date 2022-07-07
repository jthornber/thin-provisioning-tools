use indicatif::{ProgressBar, ProgressStyle};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

//------------------------------------------

#[derive(Clone, PartialEq)]
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

pub trait ReportInner {
    fn set_title(&mut self, txt: &str);
    fn set_sub_title(&mut self, txt: &str);
    fn progress(&mut self, percent: u8);
    fn log(&mut self, txt: &str);
    fn to_stdout(&mut self, txt: &str);
    fn complete(&mut self);
}

impl Report {
    pub fn new(inner: Box<dyn ReportInner + Send>) -> Report {
        Report {
            outcome: Mutex::new(Success),
            inner: Mutex::new(inner),
        }
    }

    fn update_outcome(&self, rhs: ReportOutcome) {
        let mut lhs = self.outcome.lock().unwrap();
        *lhs = ReportOutcome::combine(&lhs, &rhs);
    }

    pub fn set_title(&self, txt: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.set_title(txt)
    }

    pub fn set_sub_title(&self, txt: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.set_sub_title(txt)
    }

    pub fn progress(&self, percent: u8) {
        let mut inner = self.inner.lock().unwrap();
        inner.progress(percent)
    }

    pub fn info(&self, txt: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt)
    }

    pub fn non_fatal(&self, txt: &str) {
        self.update_outcome(NonFatal);
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt)
    }

    pub fn fatal(&self, txt: &str) {
        self.update_outcome(Fatal);
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt)
    }

    pub fn complete(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.complete();
    }

    pub fn get_outcome(&self) -> ReportOutcome {
        let outcome = self.outcome.lock().unwrap();
        outcome.clone()
    }

    // Force a message to be printed to stdout.  eg,
    // TRANSACTION_ID = <blah>
    pub fn to_stdout(&self, txt: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.to_stdout(txt)
    }
}

//------------------------------------------

#[allow(dead_code)]
struct PBInner {
    title: String,
    bar: ProgressBar,
}

impl PBInner {
    fn new(title: String) -> Self {
        let fmt = "[{bar:40}] {eta} remaining".to_string();
        let bar = ProgressBar::new(100);
        bar.set_style(
            ProgressStyle::default_bar()
                .template(&fmt)
                .progress_chars("=> "),
        );
        Self { title, bar }
    }
}

impl ReportInner for PBInner {
    fn set_title(&mut self, txt: &str) {
        self.title = txt.to_string();
    }

    fn set_sub_title(&mut self, txt: &str) {
        //let mut fmt = "".to_string(); //Checking thin metadata".to_string(); //self.title.clone();
        let mut fmt = "Checking thin metadata [{bar:40}] Remaining {eta}, ".to_string();
        fmt.push_str(txt);
        self.bar.set_style(
            ProgressStyle::default_bar()
                .template(&fmt)
                .progress_chars("=> "),
        );
    }

    fn progress(&mut self, percent: u8) {
        self.bar.set_position(percent as u64);
        self.bar.tick();
    }

    fn log(&mut self, txt: &str) {
        self.bar.println(txt);
    }

    fn to_stdout(&mut self, txt: &str) {
        println!("{}", txt);
    }

    fn complete(&mut self) {
        self.bar.finish();
    }
}

pub fn mk_progress_bar_report() -> Report {
    Report::new(Box::new(PBInner::new("".to_string())))
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
    fn set_title(&mut self, txt: &str) {
        eprintln!("{}", txt);
    }

    fn set_sub_title(&mut self, txt: &str) {
        eprintln!("{}", txt);
    }

    fn progress(&mut self, percent: u8) {
        let elapsed = self.last_progress.elapsed().unwrap();
        if elapsed > std::time::Duration::from_secs(5) {
            eprintln!("Progress: {}%", percent);
            self.last_progress = std::time::SystemTime::now();
        }
    }

    fn log(&mut self, txt: &str) {
        eprintln!("{}", txt);
    }

    fn to_stdout(&mut self, txt: &str) {
        println!("{}", txt);
    }

    fn complete(&mut self) {}
}

pub fn mk_simple_report() -> Report {
    Report::new(Box::new(SimpleInner::new()))
}

//------------------------------------------

struct QuietInner {}

impl ReportInner for QuietInner {
    fn set_title(&mut self, _txt: &str) {}

    fn set_sub_title(&mut self, _txt: &str) {}

    fn progress(&mut self, _percent: u8) {}

    fn log(&mut self, _txt: &str) {}
    fn to_stdout(&mut self, _txt: &str) {}

    fn complete(&mut self) {}
}

pub fn mk_quiet_report() -> Report {
    Report::new(Box::new(QuietInner {}))
}

//------------------------------------------

pub struct ProgressMonitor {
    tid: JoinHandle<()>,
    stop_flag: Arc<AtomicBool>,
}

impl ProgressMonitor {
    pub fn new<F>(report: Arc<Report>, total: u64, processed: F) -> Self
    where
        F: Fn() -> u64 + Send + 'static,
    {
        let stop_flag = Arc::new(AtomicBool::new(false));

        let stopped = stop_flag.clone();
        let tid = thread::spawn(move || {
            let interval = std::time::Duration::from_millis(250);
            loop {
                if stopped.load(Ordering::Relaxed) {
                    break;
                }

                let n = processed() * 100 / total;

                let _r = report.progress(n as u8);
                thread::sleep(interval);
            }
        });

        ProgressMonitor { tid, stop_flag }
    }

    // Only the owner could stop the Monitor
    pub fn stop(self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        let _ = self.tid.join();
    }
}

//------------------------------------------
