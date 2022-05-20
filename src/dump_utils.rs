//------------------------------------------

// A wrapper for callers to identify the error type
#[derive(Debug)]
pub struct OutputError;

impl std::fmt::Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "output error")
    }
}

//------------------------------------------
