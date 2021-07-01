use anyhow::Result;

mod common;

use common::common_args::*;
use common::input_arg::*;
use common::output_option::*;
use common::test_dir::*;
use common::*;

//------------------------------------------

const USAGE: &str = concat!(
    "thin_metadata_unpack ",
    include_str!("../VERSION"),
    "Unpack a compressed file of thin metadata.\n\
     \n\
     USAGE:\n    \
         thin_metadata_unpack -i <DEV> -o <FILE>\n\
     \n\
     FLAGS:\n    \
         -h, --help       Prints help information\n    \
         -V, --version    Prints version information\n\
     \n\
     OPTIONS:\n    \
         -i <DEV>         Specify thinp metadata binary device/file\n    \
         -o <FILE>        Specify packed output file"
);

//------------------------------------------

test_accepts_help!(THIN_METADATA_UNPACK, USAGE);
test_accepts_version!(THIN_METADATA_UNPACK);
test_rejects_bad_option!(THIN_METADATA_UNPACK);

test_missing_input_option!(THIN_METADATA_PACK);
test_input_file_not_found!(THIN_METADATA_UNPACK, OPTION);
test_corrupted_input_data!(THIN_METADATA_UNPACK, OPTION);

test_missing_output_option!(THIN_METADATA_UNPACK, mk_valid_md);

//------------------------------------------

// TODO: share with thin_restore/cache_restore/era_restore

#[test]
fn end_to_end() -> Result<()> {
    let mut td = TestDir::new()?;
    let md_in = mk_valid_md(&mut td)?;
    let md_out = mk_zeroed_md(&mut td)?;
    run_ok(
        THIN_METADATA_PACK,
        &["-i", md_in.to_str().unwrap(), "-o", "meta.pack"],
    )?;
    run_ok(
        THIN_METADATA_UNPACK,
        &["-i", "meta.pack", "-o", md_out.to_str().unwrap()],
    )?;

    let dump1 = run_ok(THIN_DUMP, &[md_in.to_str().unwrap()])?;
    let dump2 = run_ok(THIN_DUMP, &[md_out.to_str().unwrap()])?;
    assert_eq!(dump1, dump2);
    Ok(())
}

//------------------------------------------
