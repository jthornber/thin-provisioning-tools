use anyhow::Result;

mod common;

use common::common_args::*;
use common::fixture::*;
use common::input_arg::*;
use common::program::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;

//------------------------------------------

const USAGE: &str = "List thin volumes within a pool

Usage: thin_ls [OPTIONS] <INPUT>

Arguments:
  <INPUT>  Specify the input device

Options:
  -h, --help             Print help
  -m, --metadata-snap    Use metadata snapshot
      --no-headers       Don't output headers
  -o, --format <FIELDS>  Give a comma separated list of fields to be output
  -V, --version          Print version";

//-----------------------------------------

struct ThinLs;

impl<'a> Program<'a> for ThinLs {
    fn name() -> &'a str {
        "thin_ls"
    }

    fn cmd<I>(args: I) -> Command
    where
        I: IntoIterator,
        I::Item: Into<std::ffi::OsString>,
    {
        thin_ls_cmd(args)
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

impl<'a> InputProgram<'a> for ThinLs {
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

//------------------------------------------

test_accepts_help!(ThinLs);
test_accepts_version!(ThinLs);
test_rejects_bad_option!(ThinLs);

test_missing_input_arg!(ThinLs);
test_input_file_not_found!(ThinLs);
test_input_cannot_be_a_directory!(ThinLs);
test_unreadable_input_file!(ThinLs);

test_readonly_input_file!(ThinLs);

//------------------------------------------
// test reading metadata snapshot from a live metadata.
// here we use a corrupted metadata to ensure that "thin_ls -m" reads the
// metadata snapshot only.

#[test]
fn read_metadata_snapshot() -> Result<()> {
    let mut td = TestDir::new()?;
    let md = prep_metadata_from_file(&mut td, "corrupted_tmeta_with_metadata_snap.pack")?;
    let _ = run_ok(thin_ls_cmd(args![&md, "-m"]))?;
    Ok(())
}

//------------------------------------------

fn test_list_blocks(metadata_dump: &[u8], expected: &[[u32; 5]]) -> Result<()> {
    let mut td = TestDir::new()?;
    let md = mk_zeroed_md(&mut td)?;
    let xml = td.mk_path("meta.xml");

    write_file(&xml, metadata_dump)?;
    run_ok(thin_restore_cmd(args!["-i", &xml, "-o", &md]))?;

    let stdout = run_ok(thin_ls_cmd(args![
        &md,
        "--no-headers",
        "-o",
        "DEV,MAPPED_BLOCKS,EXCLUSIVE_BLOCKS,SHARED_BLOCKS,HIGHEST_BLOCK"
    ]))?;

    if expected.is_empty() {
        assert!(stdout.is_empty());
        return Ok(());
    }

    for (i, line) in stdout.lines().enumerate() {
        let values = line
            .split_whitespace()
            .map(str::parse::<u32>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(&values[..], &expected[i][..]);
    }

    Ok(())
}

#[test]
fn list_empty_pool() -> Result<()> {
    let metadata_dump = r#"<superblock uuid="" time="2" transaction="3" version="2" data_block_size="128" nr_data_blocks="16384">
</superblock>"#.as_bytes();
    let expected = [];
    test_list_blocks(metadata_dump, &expected)
}

#[test]
fn list_exclusive_mappings() -> Result<()> {
    let metadata_dump = r#"<superblock uuid="" time="2" transaction="3" version="2" data_block_size="128" nr_data_blocks="16384">
  <device dev_id="1" mapped_blocks="8192" transaction="0" creation_time="0" snap_time="1">
    <range_mapping origin_begin="200" data_begin="0" length="8192" time="0"/>
  </device>
  <device dev_id="2" mapped_blocks="4096" transaction="0" creation_time="0" snap_time="1">
    <range_mapping origin_begin="0" data_begin="8192" length="4096" time="0"/>
  </device>
</superblock>"#.as_bytes();

    // DEV,MAPPED_BLOCKS,EXCLUSIVE_BLOCKS,SHARED_BLOCKS,HIGHEST_BLOCK
    let expected = [[1, 8192, 8192, 0, 8391], [2, 4096, 4096, 0, 4095]];

    test_list_blocks(metadata_dump, &expected)
}

#[test]
fn list_mappings_under_shared_leaves() -> Result<()> {
    let metadata_dump = r#"<superblock uuid="" time="15" transaction="21" version="2" data_block_size="128" nr_data_blocks="16384">
  <def name="1">
    <range_mapping origin_begin="0" data_begin="750" length="1000" time="0"/>
  </def>
  <device dev_id="1" mapped_blocks="1126" transaction="0" creation_time="0" snap_time="15">
    <ref name="1"/>
    <range_mapping origin_begin="1650" data_begin="5637" length="126" time="0"/>
  </device>
  <device dev_id="2" mapped_blocks="1250" transaction="0" creation_time="0" snap_time="14">
    <ref name="1"/>
    <range_mapping origin_begin="10600" data_begin="2048" length="250" time="0"/>
  </device>
</superblock>"#.as_bytes();

    // DEV,MAPPED_BLOCKS,EXCLUSIVE_BLOCKS,SHARED_BLOCKS,HIGHEST_BLOCK
    let expected = [[1, 1126, 126, 1000, 1775], [2, 1250, 250, 1000, 10849]];

    test_list_blocks(metadata_dump, &expected)
}

#[test]
fn list_mappings_under_shared_internals() -> Result<()> {
    let mut td = TestDir::new()?;

    // Use the metadata with snapshots over shared internal nodes
    let md = prep_metadata_from_file(&mut td, "tmeta_with_shared_internal_root.pack")?;

    let stdout = run_ok(thin_ls_cmd(args![
        &md,
        "--no-headers",
        "-o",
        "DEV,MAPPED_BLOCKS,EXCLUSIVE_BLOCKS,SHARED_BLOCKS,HIGHEST_BLOCK"
    ]))?;

    // DEV,MAPPED_BLOCKS,EXCLUSIVE_BLOCKS,SHARED_BLOCKS,HIGHEST_BLOCK
    let expected = [[1, 1600, 0, 1600, 1599], [2, 1600, 0, 1600, 1599]];

    for (i, line) in stdout.lines().enumerate() {
        let values = line
            .split_whitespace()
            .map(str::parse::<u32>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(&values[..], &expected[i][..]);
    }

    Ok(())
}

#[test]
fn list_mappings_under_shared_internals_and_leaves() -> Result<()> {
    let mut td = TestDir::new()?;

    // Use the metadata with snapshots over shared internal nodes
    let md = prep_metadata_from_file(&mut td, "tmeta_with_shared_internals_and_leaves.pack")?;

    let stdout = run_ok(thin_ls_cmd(args![
        &md,
        "--no-headers",
        "-o",
        "DEV,MAPPED_BLOCKS,EXCLUSIVE_BLOCKS,SHARED_BLOCKS,HIGHEST_BLOCK"
    ]))?;

    // DEV,MAPPED_BLOCKS,EXCLUSIVE_BLOCKS,SHARED_BLOCKS,HIGHEST_BLOCK
    let expected = [[1, 65536, 0, 65536, 65535], [2, 65600, 64, 65536, 65599]];

    for (i, line) in stdout.lines().enumerate() {
        let values = line
            .split_whitespace()
            .map(str::parse::<u32>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(&values[..], &expected[i][..]);
    }

    Ok(())
}

//------------------------------------------
