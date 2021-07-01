use crate::common::*;

//-----------------------------------------
// test invalid arguments

pub fn test_missing_output_option<F>(program: &str, mk_input: F) -> Result<()>
where
    F: Fn(&mut TestDir) -> Result<PathBuf>,
{
    let mut td = TestDir::new()?;
    let input = mk_input(&mut td)?;
    let stderr = run_fail(program, &["-i", input.to_str().unwrap()])?;
    assert!(stderr.contains(msg::MISSING_OUTPUT_ARG));
    Ok(())
}

#[macro_export]
macro_rules! test_missing_output_option {
    ($program: ident, $mk_input: ident) => {
        #[test]
        fn missing_output_option() -> Result<()> {
            test_missing_output_option($program, $mk_input)
        }
    };
}

pub fn test_output_file_not_found<F>(program: &str, mk_input: F) -> Result<()>
where
    F: Fn(&mut TestDir) -> Result<PathBuf>,
{
    let mut td = TestDir::new()?;
    let input = mk_input(&mut td)?;
    let stderr = run_fail(
        program,
        &["-i", input.to_str().unwrap(), "-o", "no-such-file"],
    )?;
    assert!(stderr.contains(msg::FILE_NOT_FOUND));
    Ok(())
}

#[macro_export]
macro_rules! test_output_file_not_found {
    ($program: ident, $mk_input: ident) => {
        #[test]
        fn output_file_not_found() -> Result<()> {
            test_output_file_not_found($program, $mk_input)
        }
    };
}

pub fn test_output_cannot_be_a_directory<F>(program: &str, mk_input: F) -> Result<()>
where
    F: Fn(&mut TestDir) -> Result<PathBuf>,
{
    let mut td = TestDir::new()?;
    let input = mk_input(&mut td)?;
    let stderr = run_fail(program, &["-i", input.to_str().unwrap(), "-o", "/tmp"])?;
    assert!(stderr.contains(msg::FILE_NOT_FOUND));
    Ok(())
}

#[macro_export]
macro_rules! test_output_cannot_be_a_directory {
    ($program: ident, $mk_input: ident) => {
        #[test]
        fn output_cannot_be_a_directory() -> Result<()> {
            test_output_cannot_be_a_directory($program, $mk_input)
        }
    };
}

pub fn test_unwritable_output_file<F>(program: &str, mk_input: F) -> Result<()>
where
    F: Fn(&mut TestDir) -> Result<PathBuf>,
{
    let mut td = TestDir::new()?;
    let input = mk_input(&mut td)?;

    let output = td.mk_path("meta.bin");
    let _file = file_utils::create_sized_file(&output, 4096);
    duct::cmd!("chmod", "-w", &output).run()?;

    let stderr = run_fail(
        program,
        &[
            "-i",
            input.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
        ],
    )?;
    assert!(stderr.contains("Permission denied"));
    Ok(())
}

#[macro_export]
macro_rules! test_unwritable_output_file {
    ($program: ident, $mk_input: ident) => {
        #[test]
        fn unwritable_output_file() -> Result<()> {
            test_unwritable_output_file($program, $mk_input)
        }
    };
}

//----------------------------------------
// test invalid content

// currently thin/cache_restore only
pub fn test_tiny_output_file<F>(program: &str, mk_input: F) -> Result<()>
where
    F: Fn(&mut TestDir) -> Result<PathBuf>,
{
    let mut td = TestDir::new()?;
    let input = mk_input(&mut td)?;

    let output = td.mk_path("meta.bin");
    let _file = file_utils::create_sized_file(&output, 4096);

    let stderr = run_fail(
        program,
        &[
            "-i",
            input.to_str().unwrap(),
            "-o",
            output.to_str().unwrap(),
        ],
    )?;
    assert!(stderr.contains("Output file too small"));
    Ok(())
}

#[macro_export]
macro_rules! test_tiny_output_file {
    ($program: ident, $mk_input: ident) => {
        #[test]
        fn tiny_output_file() -> Result<()> {
            test_tiny_output_file($program, $mk_input)
        }
    };
}

//-----------------------------------------
