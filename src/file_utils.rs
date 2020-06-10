use std::fs;

pub fn file_exists(path: &str) -> bool {
    return match fs::metadata(path) {
        Ok(_) => {true}
        Err(_) => {false}
    }
}
