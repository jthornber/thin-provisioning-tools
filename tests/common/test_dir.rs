use anyhow::Result;
use std::path::{PathBuf};
use tempfile::{tempdir, TempDir};

//---------------------------------------

pub struct TestDir {
    dir: TempDir,
    file_count: usize,
}

impl TestDir {
    pub fn new() -> Result<TestDir> {
        let dir = tempdir()?;
        Ok(TestDir { dir, file_count: 0 })
    }

    pub fn mk_path(&mut self, file: &str) -> PathBuf {
        let mut p = PathBuf::new();
        p.push(&self.dir);
        p.push(PathBuf::from(format!("{:02}_{}", self.file_count, file)));
        self.file_count += 1;
        p
    }
}

//---------------------------------------
