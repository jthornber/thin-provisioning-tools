use anyhow::Result;
use thinp::file_utils;
use thinp::version::TOOLS_VERSION;

mod common;

use common::test_dir::*;
use common::thin_xml_generator::{write_xml, FragmentedS};
use common::*;

//------------------------------------------

#[test]
fn accepts_v() -> Result<()> {
    let stdout = thin_check!("-V").read()?;
    assert_eq!(stdout, TOOLS_VERSION);
    Ok(())
}

#[test]
fn accepts_version() -> Result<()> {
    let stdout = thin_check!("--version").read()?;
    assert_eq!(stdout, TOOLS_VERSION);
    Ok(())
}

const USAGE: &str = "Usage: thin_check [options] {device|file}\nOptions:\n  {-q|--quiet}\n  {-h|--help}\n  {-V|--version}\n  {-m|--metadata-snap}\n  {--auto-repair}\n  {--override-mapping-root}\n  {--clear-needs-check-flag}\n  {--ignore-non-fatal-errors}\n  {--skip-mappings}\n  {--super-block-only}";

#[test]
fn accepts_h() -> Result<()> {
    let stdout = thin_check!("-h").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn accepts_help() -> Result<()> {
    let stdout = thin_check!("--help").read()?;
    assert_eq!(stdout, USAGE);
    Ok(())
}

#[test]
fn rejects_bad_option() -> Result<()> {
    let stderr = run_fail(thin_check!("--hedgehogs-only"))?;
    assert!(stderr.contains("unrecognized option \'--hedgehogs-only\'"));
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
fn accepts_quiet() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;

    let output = thin_check!("--quiet", &md).run()?;
    assert!(output.status.success());
    assert_eq!(output.stdout.len(), 0);
    assert_eq!(output.stderr.len(), 0);
    Ok(())
}

#[test]
fn accepts_auto_repair() -> Result<()> {
    accepts_flag("--auto-repair")
}

#[test]
fn detects_corrupt_superblock_with_superblock_only() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let output = thin_check!("--super-block-only", &md).unchecked().run()?;
    assert!(!output.status.success());
    Ok(())
}

#[test]
fn prints_help_message_for_tiny_metadata() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = td.mk_path("meta.bin");
    let _file = file_utils::create_sized_file(&md, 1024);
    let stderr = run_fail(thin_check!(&md))?;
    assert!(stderr.contains("Metadata device/file too small.  Is this binary metadata?"));
    Ok(())
}

#[test]
fn spot_xml_data() -> Result<()> {
    let mut td = TestDir::new()?;
    let xml = td.mk_path("meta.xml");

    let mut gen = FragmentedS::new(4, 10240);
    write_xml(&xml, &mut gen)?;

    let stderr = run_fail(thin_check!(&xml))?;
    eprintln!("{}", stderr);
    assert!(
        stderr.contains("This looks like XML.  thin_check only checks the binary metadata format.")
    );
    Ok(())
}

#[test]
fn prints_info_fields() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    let stdout = thin_check!(&md).read()?;
    assert!(stdout.contains("TRANSACTION_ID="));
    assert!(stdout.contains("METADATA_FREE_BLOCKS="));
    Ok(())
}

#[test]
fn auto_repair_incompatible_opts() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_fail(thin_check!("--auto-repair", "-m", &md))?;
    run_fail(thin_check!(
        "--auto-repair",
        "--override-mapping-root",
        "123",
        &md
    ))?;
    run_fail(thin_check!("--auto-repair", "--super-block-only", &md))?;
    run_fail(thin_check!("--auto-repair", "--skip-mappings", &md))?;
    run_fail(thin_check!(
        "--auto-repair",
        "--ignore-non-fatal-errors",
        &md
    ))?;
    Ok(())
}

#[test]
fn clear_needs_check_incompatible_opts() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_valid_md(&mut td)?;
    run_fail(thin_check!("--clear-needs-check-flag", "-m", &md))?;
    run_fail(thin_check!(
        "--clear-needs-check-flag",
        "--override-mapping-root",
        "123",
        &md
    ))?;
    run_fail(thin_check!(
        "--clear-needs-check-flag",
        "--super-block-only",
        &md
    ))?;
    run_fail(thin_check!(
        "--clear-needs-check-flag",
        "--ignore-non-fatal-errors",
        &md
    ))?;
    Ok(())
}

//------------------------------------------

#[test]
fn clear_needs_check() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    set_needs_check(&md)?;

    assert!(get_needs_check(&md)?);
    thin_check!("--clear-needs-check-flag", &md).run()?;
    assert!(!get_needs_check(&md)?);
    Ok(())
}

#[test]
fn no_clear_needs_check_if_error() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    set_needs_check(&md)?;
    generate_metadata_leaks(&md, 1, 0, 1)?;
    run_fail(thin_check!("--clear-needs-check-flag", &md))?;
    assert!(get_needs_check(&md)?);
    Ok(())
}

#[test]
fn clear_needs_check_if_skip_mappings() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    set_needs_check(&md)?;
    generate_metadata_leaks(&md, 1, 0, 1)?;

    assert!(get_needs_check(&md)?);
    thin_check!("--clear-needs-check-flag", "--skip-mappings", &md).run()?;
    assert!(!get_needs_check(&md)?);
    Ok(())
}

#[test]
fn metadata_leaks_are_non_fatal() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    generate_metadata_leaks(&md, 1, 0, 1)?;
    run_fail(thin_check!(&md))?;
    thin_check!("--ignore-non-fatal-errors", &md).run()?;
    Ok(())
}

#[test]
fn fatal_errors_cant_be_ignored() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    generate_metadata_leaks(&md, 1, 1, 0)?;
    ensure_untouched(&md, || {
        run_fail(thin_check!("--ignore-non-fatal-errors", &md))?;
        Ok(())
    })
}

#[test]
fn auto_repair() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;

    // auto-repair should have no effect on good metadata.
    ensure_untouched(&md, || {
        thin_check!("--auto-repair", &md).run()?;
        Ok(())
    })?;

    generate_metadata_leaks(&md, 16, 0, 1)?;
    run_fail(thin_check!(&md))?;
    thin_check!("--auto-repair", &md).run()?;
    thin_check!(&md).run()?;
    Ok(())
}

#[test]
fn auto_repair_has_limits() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    generate_metadata_leaks(&md, 16, 1, 0)?;

    ensure_untouched(&md, || {
        run_fail(thin_check!("--auto-repair", &md))?;
        Ok(())
    })?;
    Ok(())
}

#[test]
fn auto_repair_clears_needs_check() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata(&mut td)?;
    set_needs_check(&md)?;
    thin_check!("--auto-repair", &md).run()?;
    assert!(!get_needs_check(&md)?);
    Ok(())
}

//------------------------------------------
