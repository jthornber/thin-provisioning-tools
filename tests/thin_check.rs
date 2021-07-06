use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::test_dir::*;
use common::*;

//------------------------------------------

const USAGE: &str = "Usage: thin_check [options] {device|file}\n\
                     Options:\n  \
                       {-q|--quiet}\n  \
                       {-h|--help}\n  \
                       {-V|--version}\n  \
                       {-m|--metadata-snap}\n  \
                       {--auto-repair}\n  \
                       {--override-mapping-root}\n  \
                       {--clear-needs-check-flag}\n  \
                       {--ignore-non-fatal-errors}\n  \
                       {--skip-mappings}\n  \
                       {--super-block-only}";

//-----------------------------------------

struct ThinCheck;

impl<'a> Program<'a> for ThinCheck {
    fn name() -> &'a str {
        "thin_check"
    }

    fn path() -> &'a str {
        THIN_CHECK
    }

    fn usage() -> &'a str {
        USAGE
    }

    fn arg_type() -> ArgType {
        ArgType::InputArg
    }

    fn bad_option_hint(option: &str) -> String {
        msg::bad_option_hint(option)
    }
}

impl<'a> InputProgram<'a> for ThinCheck {
    fn mk_valid_input(td: &mut TestDir) -> Result<std::path::PathBuf> {
        mk_valid_md(td)
    }

    fn file_not_found() -> &'a str {
        msg::FILE_NOT_FOUND
    }

    fn missing_input_arg() -> &'a str {
        msg::MISSING_INPUT_ARG
    }

    fn corrupted_input() -> &'a str {
        msg::BAD_SUPERBLOCK
    }
}

impl<'a> BinaryInputProgram<'_> for ThinCheck {}

//------------------------------------------

test_accepts_help!(ThinCheck);
test_accepts_version!(ThinCheck);
test_rejects_bad_option!(ThinCheck);

test_missing_input_arg!(ThinCheck);
test_input_file_not_found!(ThinCheck);
test_input_cannot_be_a_directory!(ThinCheck);
test_unreadable_input_file!(ThinCheck);

test_help_message_for_tiny_input_file!(ThinCheck);
test_spot_xml_data!(ThinCheck);
test_corrupted_input_data!(ThinCheck);

//------------------------------------------
// test exclusive flags

fn accepts_flag(flag: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let md_path = md.to_str().unwrap();
    run_ok(THIN_CHECK, &[flag, md_path])?;
    Ok(())
}

#[test]
fn accepts_superblock_only() -> Result<()> {
    accepts_flag("--super-block-only")
}

#[test]
fn accepts_skip_mappings() -> Result<()> {
    accepts_flag("--skip-mappings")
}

#[test]
fn accepts_ignore_non_fatal_errors() -> Result<()> {
    accepts_flag("--ignore-non-fatal-errors")
}

#[test]
fn accepts_clear_needs_check_flag() -> Result<()> {
    accepts_flag("--clear-needs-check-flag")
}

#[test]
fn accepts_auto_repair() -> Result<()> {
    accepts_flag("--auto-repair")
}

//------------------------------------------
// test the --quiet flag

#[test]
fn accepts_quiet() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let md_path = md.to_str().unwrap();

    let output = run_ok_raw(THIN_CHECK, &["--quiet", md_path])?;
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

//------------------------------------------
// test superblock-block-only

#[test]
fn detects_corrupt_superblock_with_superblock_only() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let md_path = md.to_str().unwrap();
    let _stderr = run_fail(THIN_CHECK, &["--super-block-only", md_path])?;
    Ok(())
}

//------------------------------------------
// test info outputs

#[test]
fn prints_info_fields() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let md_path = md.to_str().unwrap();
    let stdout = run_ok(THIN_CHECK, &[md_path])?;
    assert!(stdout.contains("TRANSACTION_ID="));
    assert!(stdout.contains("METADATA_FREE_BLOCKS="));
    Ok(())
}

//------------------------------------------
// test compatibility between options

#[test]
fn auto_repair_incompatible_opts() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let md_path = md.to_str().unwrap();
    run_fail(THIN_CHECK, &["--auto-repair", "-m", md_path])?;
    run_fail(
        THIN_CHECK,
        &["--auto-repair", "--override-mapping-root", "123", md_path],
    )?;
    run_fail(
        THIN_CHECK,
        &["--auto-repair", "--super-block-only", md_path],
    )?;
    run_fail(THIN_CHECK, &["--auto-repair", "--skip-mappings", md_path])?;
    run_fail(
        THIN_CHECK,
        &["--auto-repair", "--ignore-non-fatal-errors", md_path],
    )?;
    Ok(())
}

#[test]
fn clear_needs_check_incompatible_opts() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let md_path = md.to_str().unwrap();
    run_fail(THIN_CHECK, &["--clear-needs-check-flag", "-m", md_path])?;
    run_fail(
        THIN_CHECK,
        &[
            "--clear-needs-check-flag",
            "--override-mapping-root",
            "123",
            md_path,
        ],
    )?;
    run_fail(
        THIN_CHECK,
        &["--clear-needs-check-flag", "--super-block-only", md_path],
    )?;
    run_fail(
        THIN_CHECK,
        &[
            "--clear-needs-check-flag",
            "--ignore-non-fatal-errors",
            md_path,
        ],
    )?;
    Ok(())
}

//------------------------------------------
// test clear-needs-check

#[test]
fn clear_needs_check() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    let md_path = md.to_str().unwrap();

    set_needs_check(&md)?;

    assert!(get_needs_check(&md)?);
    run_ok(THIN_CHECK, &["--clear-needs-check-flag", md_path])?;
    assert!(!get_needs_check(&md)?);
    Ok(())
}

#[test]
fn no_clear_needs_check_if_error() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    let md_path = md.to_str().unwrap();

    set_needs_check(&md)?;
    generate_metadata_leaks(&md, 1, 0, 1)?;
    run_fail(THIN_CHECK, &["--clear-needs-check-flag", md_path])?;
    assert!(get_needs_check(&md)?);
    Ok(())
}

#[test]
fn clear_needs_check_if_skip_mappings() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    let md_path = md.to_str().unwrap();

    set_needs_check(&md)?;
    generate_metadata_leaks(&md, 1, 0, 1)?;

    assert!(get_needs_check(&md)?);
    run_ok(
        THIN_CHECK,
        &["--clear-needs-check-flag", "--skip-mappings", md_path],
    )?;
    assert!(!get_needs_check(&md)?);
    Ok(())
}

//------------------------------------------
// test ignore-non-fatal-errors

#[test]
fn metadata_leaks_are_non_fatal() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    let md_path = md.to_str().unwrap();
    generate_metadata_leaks(&md, 1, 0, 1)?;
    run_fail(THIN_CHECK, &[md_path])?;
    run_ok(THIN_CHECK, &["--ignore-non-fatal-errors", md_path])?;
    Ok(())
}

#[test]
fn fatal_errors_cant_be_ignored() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    let md_path = md.to_str().unwrap();

    generate_metadata_leaks(&md, 1, 1, 0)?;
    ensure_untouched(&md, || {
        run_fail(THIN_CHECK, &["--ignore-non-fatal-errors", md_path])?;
        Ok(())
    })
}

//------------------------------------------
// test auto-repair

#[test]
fn auto_repair() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    let md_path = md.to_str().unwrap();

    // auto-repair should have no effect on good metadata.
    ensure_untouched(&md, || {
        run_ok(THIN_CHECK, &["--auto-repair", md_path])?;
        Ok(())
    })?;

    generate_metadata_leaks(&md, 16, 0, 1)?;
    run_fail(THIN_CHECK, &[md_path])?;
    run_ok(THIN_CHECK, &["--auto-repair", md_path])?;
    run_ok(THIN_CHECK, &[md_path])?;
    Ok(())
}

#[test]
fn auto_repair_has_limits() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    let md_path = md.to_str().unwrap();

    generate_metadata_leaks(&md, 16, 1, 0)?;
    ensure_untouched(&md, || {
        run_fail(THIN_CHECK, &["--auto-repair", md_path])?;
        Ok(())
    })?;
    Ok(())
}

#[test]
fn auto_repair_clears_needs_check() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    let md_path = md.to_str().unwrap();

    set_needs_check(&md)?;
    run_ok(THIN_CHECK, &["--auto-repair", md_path])?;
    assert!(!get_needs_check(&md)?);
    Ok(())
}

//------------------------------------------
