use anyhow::{anyhow, Result};
use rand::prelude::*;
use std::fs;
use std::path::PathBuf;

//---------------------------------------

pub struct TestDir {
    dir: PathBuf,
    files: Vec<PathBuf>,
    clean_up: bool,
    file_count: usize,
}

fn mk_dir(prefix: &str) -> Result<PathBuf> {
    for _n in 0..100 {
        let mut p = PathBuf::new();
        let nr = rand::thread_rng().gen_range(1000000..9999999);
        p.push(format!("./{}_{}", prefix, nr));
        if let Ok(()) = fs::create_dir(&p) {
            return Ok(p);
        }
    }

    Err(anyhow!("Couldn't create test directory"))
}

impl TestDir {
    pub fn new() -> Result<TestDir> {
        let dir = mk_dir("test_fixture")?;
        Ok(TestDir {
            dir,
            files: Vec::new(),
            clean_up: true,
            file_count: 0,
        })
    }

    pub fn dont_clean_up(&mut self) {
        self.clean_up = false;
    }

    pub fn mk_path(&mut self, file: &str) -> PathBuf {
        let mut p = PathBuf::new();
        p.push(&self.dir);
        p.push(PathBuf::from(format!("{:02}_{}", self.file_count, file)));
        self.files.push(p.clone());
        self.file_count += 1;
        p
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if !std::thread::panicking() && self.clean_up {
            while let Some(f) = self.files.pop() {
                // It's not guaranteed that the path generated was actually created.
                let _ignore = fs::remove_file(f);
            }
            fs::remove_dir(&self.dir).expect("couldn't remove test directory");
        } else {
            eprintln!("leaving test directory: {:?}", self.dir);
        }
    }
}

//---------------------------------------
