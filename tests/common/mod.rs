use std::path::{Path, PathBuf};

pub mod xml_generator;

pub fn mk_path(dir: &Path, file: &str) -> PathBuf {
    let mut p = PathBuf::new();
    p.push(dir);
    p.push(PathBuf::from(file));
    p
}

