use crate::common::thin_xml_generator::{write_xml, FragmentedS};
use crate::common::*;

//------------------------------------------
// wrappers

pub fn with_output_md_untouched(
    td: &mut TestDir,
    input: &str,
    thunk: &dyn Fn(&[&str]) -> Result<()>,
) -> Result<()> {
    let output = mk_zeroed_md(td)?;
    ensure_untouched(&output, || {
        let args = ["-i", input, "-o", output.to_str().unwrap()];
        thunk(&args)
    })
}

pub fn input_arg_only(
    _td: &mut TestDir,
    input: &str,
    thunk: &dyn Fn(&[&str]) -> Result<()>,
) -> Result<()> {
    let args = [input];
    thunk(&args)
}

//------------------------------------------
// test invalid arguments

pub fn test_missing_input_arg(program: &str) -> Result<()> {
    let stderr = run_fail(program, &[])?;
    assert!(stderr.contains(msg::MISSING_INPUT_ARG));
    Ok(())
}

#[macro_export]
macro_rules! test_missing_input_arg {
    ($program: ident) => {
        #[test]
        fn missing_input_arg() -> Result<()> {
            test_missing_input_arg($program)
        }
    };
}

pub fn test_missing_input_option(program: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let output = mk_zeroed_md(&mut td)?;
    ensure_untouched(&output, || {
        let args = ["-o", output.to_str().unwrap()];
        let stderr = run_fail(program, &args)?;
        assert!(stderr.contains(msg::MISSING_INPUT_ARG));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_missing_input_option {
    ($program: ident) => {
        #[test]
        fn missing_input_option() -> Result<()> {
            test_missing_input_option($program)
        }
    };
}

pub fn test_input_file_not_found<F>(program: &str, wrapper: F) -> Result<()>
where
    F: Fn(&mut TestDir, &str, &dyn Fn(&[&str]) -> Result<()>) -> Result<()>,
{
    let mut td = TestDir::new()?;

    wrapper(&mut td, "no-such-file", &|args: &[&str]| {
        let stderr = run_fail(program, args)?;
        assert!(stderr.contains(msg::FILE_NOT_FOUND));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_input_file_not_found {
    ($program: ident, ARG) => {
        #[test]
        fn input_file_not_found() -> Result<()> {
            test_input_file_not_found($program, input_arg_only)
        }
    };
    ($program: ident, OPTION) => {
        #[test]
        fn input_file_not_found() -> Result<()> {
            test_input_file_not_found($program, with_output_md_untouched)
        }
    };
}

pub fn test_input_cannot_be_a_directory<F>(program: &str, wrapper: F) -> Result<()>
where
    F: Fn(&mut TestDir, &str, &dyn Fn(&[&str]) -> Result<()>) -> Result<()>,
{
    let mut td = TestDir::new()?;

    wrapper(&mut td, "/tmp", &|args: &[&str]| {
        let stderr = run_fail(program, args)?;
        assert!(stderr.contains("Not a block device or regular file"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_input_cannot_be_a_directory {
    ($program: ident, ARG) => {
        #[test]
        fn input_cannot_be_a_directory() -> Result<()> {
            test_input_cannot_be_a_directory($program, input_arg_only)
        }
    };
    ($program: ident, OPTION) => {
        #[test]
        fn input_cannot_be_a_directory() -> Result<()> {
            test_input_cannot_be_a_directory($program, with_output_md_untouched)
        }
    };
}

pub fn test_unreadable_input_file<F>(program: &str, wrapper: F) -> Result<()>
where
    F: Fn(&mut TestDir, &str, &dyn Fn(&[&str]) -> Result<()>) -> Result<()>,
{
    let mut td = TestDir::new()?;

    // input an unreadable file
    let input = mk_valid_md(&mut td)?;
    duct::cmd!("chmod", "-r", &input).run()?;

    wrapper(&mut td, input.to_str().unwrap(), &|args: &[&str]| {
        let stderr = run_fail(program, args)?;
        assert!(stderr.contains("Permission denied"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_unreadable_input_file {
    ($program: ident, ARG) => {
        #[test]
        fn unreadable_input_file() -> Result<()> {
            test_unreadable_input_file($program, input_arg_only)
        }
    };
    ($program: ident, OPTION) => {
        #[test]
        fn unreadable_input_file() -> Result<()> {
            test_unreadable_input_file($program, with_output_md_untouched)
        }
    };
}

//------------------------------------------
// test invalid content

pub fn test_help_message_for_tiny_input_file<F>(program: &str, wrapper: F) -> Result<()>
where
    F: Fn(&mut TestDir, &str, &dyn Fn(&[&str]) -> Result<()>) -> Result<()>,
{
    let mut td = TestDir::new()?;

    let input = td.mk_path("meta.bin");
    file_utils::create_sized_file(&input, 1024)?;

    wrapper(&mut td, input.to_str().unwrap(), &|args: &[&str]| {
        let stderr = run_fail(program, args)?;
        assert!(stderr.contains("Metadata device/file too small.  Is this binary metadata?"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_help_message_for_tiny_input_file {
    ($program: ident, ARG) => {
        #[test]
        fn prints_help_message_for_tiny_input_file() -> Result<()> {
            test_help_message_for_tiny_input_file($program, input_arg_only)
        }
    };
    ($program: ident, OPTION) => {
        #[test]
        fn prints_help_message_for_tiny_input_file() -> Result<()> {
            test_help_message_for_tiny_input_file($program, with_output_md_untouched)
        }
    };
}

pub fn test_spot_xml_data<F>(program: &str, name: &str, wrapper: F) -> Result<()>
where
    F: Fn(&mut TestDir, &str, &dyn Fn(&[&str]) -> Result<()>) -> Result<()>,
{
    let mut td = TestDir::new()?;

    // input a large xml file
    let input = td.mk_path("meta.xml");
    let mut gen = FragmentedS::new(4, 10240);
    write_xml(&input, &mut gen)?;

    wrapper(&mut td, input.to_str().unwrap(), &|args: &[&str]| {
        let stderr = run_fail(program, args)?;
        eprintln!("{}", stderr);
        let msg = format!(
            "This looks like XML.  {} only checks the binary metadata format.",
            name
        );
        assert!(stderr.contains(&msg));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_spot_xml_data {
    ($program: ident, $name: expr, ARG) => {
        #[test]
        fn spot_xml_data() -> Result<()> {
            test_spot_xml_data($program, $name, input_arg_only)
        }
    };
    ($program: ident, $name: expr, OPTION) => {
        #[test]
        fn spot_xml_data() -> Result<()> {
            test_spot_xml_data($program, $name, with_output_md_untouched)
        }
    };
}

pub fn test_corrupted_input_data<F>(program: &str, wrapper: F) -> Result<()>
where
    F: Fn(&mut TestDir, &str, &dyn Fn(&[&str]) -> Result<()>) -> Result<()>,
{
    let mut td = TestDir::new()?;
    let input = mk_zeroed_md(&mut td)?;

    wrapper(&mut td, input.to_str().unwrap(), &|args: &[&str]| {
        let stderr = run_fail(program, args)?;
        assert!(stderr.contains("bad checksum in superblock"));
        Ok(())
    })
}

#[macro_export]
macro_rules! test_corrupted_input_data {
    ($program: ident, ARG) => {
        #[test]
        fn corrupted_input_data() -> Result<()> {
            test_corrupted_input_data($program, input_arg_only)
        }
    };
    ($program: ident, OPTION) => {
        #[test]
        fn corrupted_input_data() -> Result<()> {
            test_corrupted_input_data($program, with_output_md_untouched)
        }
    };
}

//------------------------------------------
