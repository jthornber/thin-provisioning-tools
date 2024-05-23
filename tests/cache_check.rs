use anyhow::Result;

mod common;

use common::cache::*;
use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::process::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;

//------------------------------------------

const USAGE: &str = "Validates cache metadata on a device or file.

Usage: cache_check [OPTIONS] <INPUT>

Arguments:
  <INPUT>  Specify the input device to check

Options:
      --auto-repair              Auto repair trivial issues
      --clear-needs-check-flag   Clears the 'needs_check' flag in the superblock
  -h, --help                     Print help
      --ignore-non-fatal-errors  Only return a non-zero exit code if a fatal error is found.
  -q, --quiet                    Suppress output messages, return only exit code.
      --skip-discards            Don't check the discard bitset
      --skip-hints               Don't check the hint array
      --skip-mappings            Don't check the mapping array
      --super-block-only         Only check the superblock
  -V, --version                  Print version";

//------------------------------------------

struct CacheCheck;

impl<'a> Program<'a> for CacheCheck {
    fn name() -> &'a str {
        "cache_check"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        cache_check_cmd(args)
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

impl<'a> InputProgram<'a> for CacheCheck {
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

impl<'a> MetadataReader<'a> for CacheCheck {}

//------------------------------------------

test_accepts_help!(CacheCheck);
test_accepts_version!(CacheCheck);
test_rejects_bad_option!(CacheCheck);

test_missing_input_arg!(CacheCheck);
test_input_file_not_found!(CacheCheck);
test_input_cannot_be_a_directory!(CacheCheck);
test_unreadable_input_file!(CacheCheck);

test_help_message_for_tiny_input_file!(CacheCheck);
test_spot_xml_data!(CacheCheck);
test_corrupted_input_data!(CacheCheck);

test_readonly_input_file!(CacheCheck);

//------------------------------------------
// test exclusive flags

fn accepts_flag(flag: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_ok(cache_check_cmd(args![flag, &md]))?;
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
fn accepts_skip_hints() -> Result<()> {
    accepts_flag("--skip-hints")
}

#[test]
fn accepts_skip_discards() -> Result<()> {
    accepts_flag("--skip-discards")
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

#[test]
fn failing_q() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = run_fail_raw(cache_check_cmd(args!["-q", &md]))?;
    assert_eq!(output.stdout.len(), 0);
    eprintln!(
        "stderr = '{}'",
        std::str::from_utf8(&output.stderr).unwrap()
    );
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn failing_quiet() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = run_fail_raw(cache_check_cmd(args!["--quiet", &md]))?;
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn valid_metadata_passes() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_ok(cache_check_cmd(args![&md]))?;
    Ok(())
}

#[test]
fn bad_metadata_version() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(cache_restore_cmd(args!["-i", &xml, "-o", &md]))?;
    run_ok(cache_generate_metadata_cmd(args![
        "-o",
        &md,
        "--set-superblock-version",
        "3"
    ]))?;
    run_fail(cache_check_cmd(args![&md]))?;
    Ok(())
}

#[test]
fn incompat_metadata_version_v1() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(cache_restore_cmd(args!["-i", &xml, "-o", &md]))?;
    run_ok(cache_generate_metadata_cmd(args![
        "-o",
        &md,
        "--set-superblock-version",
        "1"
    ]))?;
    run_fail(cache_check_cmd(args![&md]))?;
    Ok(())
}

#[test]
fn incompat_metadata_version_v2() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = mk_valid_xml(&mut td)?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(cache_restore_cmd(args![
        "-i",
        &xml,
        "-o",
        &md,
        "--metadata-version",
        "1"
    ]))?;
    run_ok(cache_generate_metadata_cmd(args![
        "-o",
        &md,
        "--set-superblock-version",
        "2"
    ]))?;
    run_fail(cache_check_cmd(args![&md]))?;
    Ok(())
}

#[test]
fn dirty_bitset_not_found_should_fail() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    run_ok(cache_generate_metadata_cmd(args![
        "-o",
        &md,
        "--format",
        "--metadata-version",
        "1",
        "--percent-dirty",
        "0"
    ]))?;
    run_ok(cache_generate_metadata_cmd(args![
        "-o",
        &md,
        "--set-superblock-version",
        "2"
    ]))?;
    run_fail(cache_check_cmd(args![&md]))?;
    Ok(())
}

//------------------------------------------
// test clear-needs-check

#[test]
fn clear_needs_check() -> Result<()> {
    test_option_clears_needs_check("--clear-needs-check-flag")
}

#[test]
fn clear_needs_check_without_clean_shutdown() -> Result<()> {
    test_option_clears_needs_check_without_clean_shutdown("--clear-needs-check-flag")
}

#[test]
fn clear_needs_check_opt_fixes_metadata_leaks() -> Result<()> {
    test_option_fixes_metadata_leaks("--clear-needs-check-flag")
}

#[test]
fn clear_needs_check_opt_fixes_metadata_leaks_and_clears_flag() -> Result<()> {
    test_option_fixes_metadata_leaks_and_clears_flag("--clear-needs-check-flag")
}

#[test]
fn clear_needs_check_opt_keeps_health_metadata_untouched() -> Result<()> {
    test_option_keeps_health_metadata_untouched("--clear-needs-check-flag")
}

#[test]
fn clear_needs_check_opt_cannot_fix_unexpected_ref_counts() -> Result<()> {
    test_option_cannot_fix_unexpected_ref_counts("--clear-needs-check-flag")
}

//------------------------------------------
// test auto-repair

fn test_option_fixes_metadata_leaks(option: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;

    generate_metadata_leaks(&md, 16, 0, 1)?; // non-fatal errors

    run_fail(cache_check_cmd(args![&md]))?;
    run_ok(cache_check_cmd(args![option, &md]))?;
    run_ok(cache_check_cmd(args![&md]))?; // ensure metadata is repaired

    Ok(())
}

fn test_option_fixes_metadata_leaks_and_clears_flag(option: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;

    set_needs_check(&md)?;
    generate_metadata_leaks(&md, 16, 0, 1)?; // non-fatal errors

    run_fail(cache_check_cmd(args![&md]))?;
    run_ok(cache_check_cmd(args![option, &md]))?;
    run_ok(cache_check_cmd(args![&md]))?; // ensure metadata is repaired
    assert!(!get_needs_check(&md)?);

    Ok(())
}

fn test_option_keeps_health_metadata_untouched(option: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;

    ensure_untouched(&md, || {
        run_ok(cache_check_cmd(args![option, &md]))?;
        Ok(())
    })?;
    Ok(())
}

fn test_option_cannot_fix_unexpected_ref_counts(option: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;

    generate_metadata_leaks(&md, 16, 1, 0)?;
    ensure_untouched(&md, || {
        run_fail(cache_check_cmd(args![option, &md]))?;
        Ok(())
    })?;
    Ok(())
}

fn test_option_clears_needs_check(option: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;

    set_needs_check(&md)?;
    assert!(get_needs_check(&md)?);
    assert!(get_clean_shutdown(&md)?);
    run_ok(cache_check_cmd(args![option, &md]))?;
    assert!(!get_needs_check(&md)?);
    assert!(get_clean_shutdown(&md)?); // ensure the flag is not touched
    Ok(())
}

fn test_option_clears_needs_check_without_clean_shutdown(option: &str) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;

    set_needs_check(&md)?;
    unset_clean_shutdown(&md)?;
    assert!(get_needs_check(&md)?);
    assert!(!get_clean_shutdown(&md)?);
    run_ok(cache_check_cmd(args![option, &md]))?;
    assert!(!get_needs_check(&md)?);
    assert!(!get_clean_shutdown(&md)?); // ensure the flag is not touched
    Ok(())
}

#[test]
fn auto_repair_fixes_metadata_leaks() -> Result<()> {
    test_option_fixes_metadata_leaks("--auto-repair")
}

#[test]
fn auto_repair_fixes_metadata_leaks_and_clears_flag() -> Result<()> {
    test_option_fixes_metadata_leaks_and_clears_flag("--auto-repair")
}

#[test]
fn auto_repair_keeps_health_metadata_untouched() -> Result<()> {
    test_option_keeps_health_metadata_untouched("--auto-repair")
}

#[test]
fn auto_repair_cannot_fix_unexpected_ref_counts() -> Result<()> {
    test_option_cannot_fix_unexpected_ref_counts("--auto-repair")
}

#[test]
fn auto_repair_clears_needs_check() -> Result<()> {
    test_option_clears_needs_check("--auto-repair")
}

#[test]
fn auto_repair_clears_needs_check_without_clean_shutdown() -> Result<()> {
    test_option_clears_needs_check_without_clean_shutdown("--auto-repair")
}

//------------------------------------------

fn metadata_without_slow_dev_size_info(use_v1: bool) -> Result<()> {
    let mut td = TestDir::new()?;

    // The input metadata has a cached oblock with address equals to the default bitset size
    // boundary (DEFAULT_OBLOCKS = 16777216), triggering bitset resize.
    let xml = td.mk_path("meta.xml");
    let content = b"<superblock uuid=\"\" block_size=\"128\" nr_cache_blocks=\"1024\" policy=\"smq\" hint_width=\"4\">
  <mappings>
    <mapping cache_block=\"0\" origin_block=\"16777216\" dirty=\"false\"/>
  </mappings>
  <hints>
    <hint cache_block=\"0\" data=\"AAAAAA==\"/>
  </hints>
</superblock>";
    write_file(&xml, content)?;

    let md = td.mk_path("meta.bin");
    thinp::file_utils::create_sized_file(&md, 4096 * 4096)?;

    let cache_restore_args = if use_v1 {
        args!["-i", &xml, "-o", &md, "--metadata-version=1"]
    } else {
        args!["-i", &xml, "-o", &md, "--metadata-version=2"]
    };

    run_ok(cache_restore_cmd(cache_restore_args))?;
    run_ok(cache_check_cmd(args![&md]))?;

    Ok(())
}

#[test]
fn metadata_v1_without_slow_dev_size_info() -> Result<()> {
    metadata_without_slow_dev_size_info(true)
}

#[test]
fn metadata_v2_without_slow_dev_size_info() -> Result<()> {
    metadata_without_slow_dev_size_info(false)
}

//------------------------------------------
