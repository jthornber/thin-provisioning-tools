use indicatif::{ProgressBar, ProgressStyle};

use std::io::{self, Write};
use std::ops::Add;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

//------------------------------------------

#[derive(Copy, Clone, Eq, PartialEq, PartialOrd)]
pub enum LogLevel {
    Fatal = 1,
    Error,
    Warning,
    Info,
    Debug,
}

impl TryFrom<u8> for LogLevel {
    type Error = String;

    fn try_from(level: u8) -> Result<LogLevel, <Self as TryFrom<u8>>::Error> {
        match level {
            0 => Err(String::from("invalid index")),
            1 => Ok(LogLevel::Fatal),
            2 => Ok(LogLevel::Error),
            3 => Ok(LogLevel::Warning),
            4 => Ok(LogLevel::Info),
            5..=u8::MAX => Ok(LogLevel::Debug),
        }
    }
}

pub fn verbose_args(cmd: clap::Command) -> clap::Command {
    use clap::Arg;

    cmd.arg(
        Arg::new("VERBOSE")
            .help("Increase log verbosity")
            .short('v')
            .multiple_occurrences(true)
            .hide(true),
    )
}

pub fn parse_log_level(matches: &clap::ArgMatches) -> Result<LogLevel, String> {
    let cnt = matches.occurrences_of("VERBOSE");
    if cnt > 0 {
        let v: u8 = LogLevel::Warning as u8;
        (v + cnt as u8).try_into()
    } else {
        Ok(LogLevel::Warning)
    }
}

//------------------------------------------

#[derive(Clone, PartialEq, Eq)]
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
    fn set_level(&mut self, level: LogLevel);
    fn progress(&mut self, percent: u8);
    fn log(&mut self, txt: &str, level: LogLevel);
    fn to_stdout(&mut self, txt: &str);
    fn complete(&mut self);
    fn get_prompt_input(&mut self, prompt: &str) -> io::Result<String>;
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

    pub fn set_level(&self, level: LogLevel) {
        let mut inner = self.inner.lock().unwrap();
        inner.set_level(level)
    }

    pub fn progress(&self, percent: u8) {
        let mut inner = self.inner.lock().unwrap();
        inner.progress(percent)
    }

    pub fn info(&self, txt: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt, LogLevel::Info)
    }

    pub fn debug(&self, txt: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt, LogLevel::Debug)
    }

    pub fn warning(&self, txt: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt, LogLevel::Warning)
    }

    pub fn non_fatal(&self, txt: &str) {
        self.update_outcome(NonFatal);
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt, LogLevel::Error)
    }

    pub fn fatal(&self, txt: &str) {
        self.update_outcome(Fatal);
        let mut inner = self.inner.lock().unwrap();
        inner.log(txt, LogLevel::Fatal)
    }

    pub fn complete(&self) {
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

    pub fn get_prompt_input(&self, prompt: &str) -> io::Result<String> {
        let mut inner = self.inner.lock().unwrap();
        inner.get_prompt_input(prompt)
    }
}

fn get_prompt_input_(prompt: &str) -> io::Result<String> {
    let mut stderr = io::stderr().lock();
    stderr.write_all(prompt.as_bytes())?;
    stderr.flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim_end_matches('\n').to_string())
}

//------------------------------------------

#[allow(dead_code)]
struct PBInner {
    bar: ProgressBar,
    level: LogLevel,
}

impl PBInner {
    fn new() -> Self {
        let fmt = "{prefix}[{bar:40}] Remaining {eta}{msg}".to_string();
        let bar = ProgressBar::new(100);
        bar.set_style(
            ProgressStyle::default_bar()
                .template(&fmt)
                .expect("invalid template for the progress bar")
                .progress_chars("=> "),
        );
        Self {
            bar,
            level: LogLevel::Warning,
        }
    }
}

impl ReportInner for PBInner {
    // Setting title clears subtitle
    fn set_title(&mut self, txt: &str) {
        let prefix = if !txt.is_empty() {
            String::from(txt).add(" ")
        } else {
            String::new()
        };
        self.bar.set_prefix(prefix);
        self.bar.set_message("");
    }

    fn set_sub_title(&mut self, txt: &str) {
        let msg = if !txt.is_empty() {
            String::from(", ").add(txt)
        } else {
            String::new()
        };
        self.bar.set_message(msg);
    }

    fn set_level(&mut self, level: LogLevel) {
        self.level = level;
    }

    fn progress(&mut self, percent: u8) {
        self.bar.set_position(percent as u64);
        self.bar.tick();
    }

    fn log(&mut self, txt: &str, level: LogLevel) {
        if level <= self.level {
            self.bar.println(txt);
        }
    }

    fn to_stdout(&mut self, txt: &str) {
        println!("{}", txt);
    }

    fn complete(&mut self) {
        self.bar.finish_and_clear();
    }

    fn get_prompt_input(&mut self, prompt: &str) -> io::Result<String> {
        self.bar.suspend(|| get_prompt_input_(prompt))
    }
}

pub fn mk_progress_bar_report() -> Report {
    Report::new(Box::new(PBInner::new()))
}

//------------------------------------------

struct SimpleInner {
    last_progress: std::time::SystemTime,
    level: LogLevel,
}

impl SimpleInner {
    fn new() -> SimpleInner {
        SimpleInner {
            last_progress: std::time::SystemTime::now(),
            level: LogLevel::Warning,
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

    fn set_level(&mut self, level: LogLevel) {
        self.level = level;
    }

    fn progress(&mut self, percent: u8) {
        let elapsed = self.last_progress.elapsed().unwrap();
        if elapsed > std::time::Duration::from_secs(5) {
            eprintln!("Progress: {}%", percent);
            self.last_progress = std::time::SystemTime::now();
        }
    }

    fn log(&mut self, txt: &str, level: LogLevel) {
        if level <= self.level {
            eprintln!("{}", txt);
        }
    }

    fn to_stdout(&mut self, txt: &str) {
        println!("{}", txt);
    }

    fn complete(&mut self) {}

    fn get_prompt_input(&mut self, prompt: &str) -> io::Result<String> {
        get_prompt_input_(prompt)
    }
}

pub fn mk_simple_report() -> Report {
    Report::new(Box::new(SimpleInner::new()))
}

//------------------------------------------

struct QuietInner {}

impl ReportInner for QuietInner {
    fn set_title(&mut self, _txt: &str) {}

    fn set_sub_title(&mut self, _txt: &str) {}

    fn set_level(&mut self, _level: LogLevel) {}

    fn progress(&mut self, _percent: u8) {}

    fn log(&mut self, _txt: &str, _level: LogLevel) {}

    fn to_stdout(&mut self, _txt: &str) {}

    fn complete(&mut self) {}

    fn get_prompt_input(&mut self, _prompt: &str) -> io::Result<String> {
        Ok(String::new()) // the quiet report doesn't accept inputs
    }
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
            let interval = std::time::Duration::from_millis(500);
            loop {
                if stopped.load(Ordering::Relaxed) {
                    break;
                }

                let n = processed() * 100 / total;

                report.progress(n as u8);
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
