use anyhow::{anyhow, Result};
use fixedbitset::FixedBitSet;
use rand::prelude::*;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use thinp::file_utils;
use thinp::random::Generator;
use thinp::thin::ir::{self, MetadataVisitor, Visit};
use thinp::thin::xml;

mod common;

use common::process::*;
use common::target::*;
use common::test_dir::*;
use common::thin::*;
use common::thin_xml_generator::{write_xml, EmptyPoolS, FragmentedS, SingleThinS, SnapS, XmlGen};

//------------------------------------

#[derive(Debug)]
struct ThinBlock {
    thin_id: u64,
    thin_block: u64,
    data_block: u64,
    time: u32,
}

struct ThinReadRef {
    pub data: Vec<u8>,
}

struct ThinWriteRef<'a, W: Write + Seek> {
    file: &'a mut W,
    block_byte: u64,
    pub data: Vec<u8>,
}

impl ThinBlock {
    fn read_ref<R: Read + Seek>(r: &mut R, b: u64, block_size: usize) -> Result<ThinReadRef> {
        let mut rr = ThinReadRef {
            data: vec![0; block_size * 512],
        };
        let byte = b * (block_size as u64) * 512;
        r.seek(SeekFrom::Start(byte))?;
        r.read_exact(&mut rr.data)?;
        Ok(rr)
    }

    fn zero_ref<W: Write + Seek>(w: &mut W, b: u64, block_size: usize) -> ThinWriteRef<W> {
        ThinWriteRef {
            file: w,
            block_byte: b * (block_size as u64) * 512,
            data: vec![0; block_size * 512],
        }
    }

    //fn write_ref<'a, W>(&self, w: &'a mut W) -> Result<ThinWriteRef<'a, W>>
    //  where
    //  W: Read + Write + Seek,
    //{
    //  let mut data = vec![0; self.block_size];
    //  w.seek(SeekFrom::Start(self.data_block * (self.block_size as u64)))?;
    //  w.read_exact(&mut data[0..])?;
    //
    //  let wr = ThinWriteRef {
    //      file: w,
    //      block_byte: self.data_block * (self.block_size as u64),
    //      data: vec![0; self.block_size],
    //  };
    //
    //  Ok(wr)
    //}
}

impl<'a, W: Write + Seek> Drop for ThinWriteRef<'a, W> {
    fn drop(&mut self) {
        // FIXME: We shouldn't panic in a drop function, so any IO
        // errors will have to make their way back to the user
        // another way (eg, via a flush() method).
        self.file.seek(SeekFrom::Start(self.block_byte)).unwrap();
        self.file.write_all(&self.data).unwrap();
    }
}

//------------------------------------

trait ThinVisitor {
    fn init(&mut self, data_block_size: usize, nr_data_blocks: u64) -> Result<()>;
    fn thin_block(&mut self, tb: &ThinBlock) -> Result<()>;
    fn complete(&mut self) -> Result<()>;
}

struct ThinXmlVisitor<'a, V: ThinVisitor> {
    inner: &'a mut V,
    thin_id: Option<u64>,
}

impl<'a, V: ThinVisitor> MetadataVisitor for ThinXmlVisitor<'a, V> {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit> {
        self.inner
            .init(sb.data_block_size as usize, sb.nr_data_blocks)?;
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, name: &str) -> Result<Visit> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut s = DefaultHasher::new();
        name.hash(&mut s);
        self.thin_id = Some(s.finish());

        Ok(Visit::Continue)
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn device_b(&mut self, d: &ir::Device) -> Result<Visit> {
        self.thin_id = Some(d.dev_id as u64);
        Ok(Visit::Continue)
    }

    fn device_e(&mut self) -> Result<Visit> {
        self.thin_id = None;
        Ok(Visit::Continue)
    }

    fn map(&mut self, m: &ir::Map) -> Result<Visit> {
        for i in 0..m.len {
            let block = ThinBlock {
                thin_id: self.thin_id.unwrap(),
                thin_block: m.thin_begin + i,
                data_block: m.data_begin + i,
                time: m.time,
            };
            self.inner.thin_block(&block)?;
        }
        Ok(Visit::Continue)
    }

    // FIXME: Verify that references to defs are not changed after shrinking
    fn ref_shared(&mut self, _name: &str) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
        self.inner.complete()?;
        Ok(Visit::Stop)
    }
}

fn thin_visit<R, M>(input: R, visitor: &mut M) -> Result<()>
where
    R: Read,
    M: ThinVisitor,
{
    let mut xml_visitor = ThinXmlVisitor {
        inner: visitor,
        thin_id: None,
    };

    xml::read(input, &mut xml_visitor)
}

//------------------------------------

// To test thin_shrink we'd like to stamp a known pattern across the
// provisioned areas of the thins in the pool, do the shrink, verify
// the patterns.
struct Stamper<'a, W: Write + Seek> {
    data_file: &'a mut W,
    data_block_size: usize, // in sectors
    seed: u64,
    provisioned: FixedBitSet, // provisioned data blocks
    salts: Vec<u64>,          // salts for each data block
}

impl<'a, W: Write + Seek> Stamper<'a, W> {
    fn new(w: &'a mut W, seed: u64) -> Stamper<'a, W> {
        Stamper {
            data_file: w,
            data_block_size: 0,
            seed,
            provisioned: FixedBitSet::new(),
            salts: Vec::new(),
        }
    }
}

impl<'a, W: Write + Seek> ThinVisitor for Stamper<'a, W> {
    fn init(&mut self, data_block_size: usize, nr_data_blocks: u64) -> Result<()> {
        self.data_block_size = data_block_size;
        self.provisioned.grow(nr_data_blocks as usize);
        self.salts.resize(nr_data_blocks as usize, self.seed);
        Ok(())
    }

    fn thin_block(&mut self, b: &ThinBlock) -> Result<()> {
        self.provisioned.set(b.data_block as usize, true);
        self.salts[b.data_block as usize] ^= b.thin_id ^ b.thin_block ^ b.time as u64;
        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        for b in self.provisioned.ones() {
            let mut wr = ThinBlock::zero_ref(self.data_file, b as u64, self.data_block_size);
            let mut gen = Generator::new();
            gen.fill_buffer(self.salts[b], &mut wr.data)?;
        }
        Ok(())
    }
}

//------------------------------------

struct Verifier<'a, R: Read + Seek> {
    data_file: &'a mut R,
    data_block_size: usize, // in sectors
    seed: u64,
    provisioned: FixedBitSet, // provisioned data blocks
    salts: Vec<u64>,          // salts for each data block
}

impl<'a, R: Read + Seek> Verifier<'a, R> {
    fn new(r: &'a mut R, seed: u64) -> Verifier<'a, R> {
        Verifier {
            data_file: r,
            data_block_size: 0,
            seed,
            provisioned: FixedBitSet::new(),
            salts: Vec::new(),
        }
    }
}

impl<'a, R: Read + Seek> ThinVisitor for Verifier<'a, R> {
    fn init(&mut self, data_block_size: usize, nr_data_blocks: u64) -> Result<()> {
        self.data_block_size = data_block_size;
        self.provisioned.grow(nr_data_blocks as usize);
        self.salts.resize(nr_data_blocks as usize, self.seed);
        Ok(())
    }

    fn thin_block(&mut self, b: &ThinBlock) -> Result<()> {
        self.provisioned.set(b.data_block as usize, true);
        self.salts[b.data_block as usize] ^= b.thin_id ^ b.thin_block ^ b.time as u64;
        Ok(())
    }

    fn complete(&mut self) -> Result<()> {
        for b in self.provisioned.ones() {
            let rr = ThinBlock::read_ref(self.data_file, b as u64, self.data_block_size)?;
            let mut gen = Generator::new();
            if !gen.verify_buffer(self.salts[b], &rr.data)? {
                return Err(anyhow!("data verify failed for data block {}", b));
            }
        }
        Ok(())
    }
}

//------------------------------------

fn create_data_file(data_path: &Path, xml_path: &Path) -> Result<()> {
    let input = OpenOptions::new().read(true).write(false).open(xml_path)?;

    let sb = xml::read_superblock(input)?;
    let nr_blocks = sb.nr_data_blocks;
    let block_size = sb.data_block_size as u64 * 512;

    let _file = file_utils::create_sized_file(data_path, nr_blocks * block_size)?;
    Ok(())
}

fn stamp(xml_path: &Path, data_path: &Path, seed: u64) -> Result<()> {
    let mut data = OpenOptions::new().read(false).write(true).open(data_path)?;
    let xml = OpenOptions::new().read(true).write(false).open(xml_path)?;

    let mut stamper = Stamper::new(&mut data, seed);
    thin_visit(xml, &mut stamper)
}

fn verify(xml_path: &Path, data_path: &Path, seed: u64) -> Result<()> {
    let mut data = OpenOptions::new().read(true).write(false).open(data_path)?;
    let xml = OpenOptions::new().read(true).write(false).open(xml_path)?;
    let mut verifier = Verifier::new(&mut data, seed);
    thin_visit(xml, &mut verifier)
}

trait Scenario {
    fn get_new_nr_blocks(&self) -> u64;
}

fn test_shrink_<S>(scenario: &mut S, expect_ok: bool) -> Result<()>
where
    S: Scenario + XmlGen,
{
    let mut td = TestDir::new()?;
    let xml_before = td.mk_path("before.xml");
    let xml_after = td.mk_path("after.xml");
    let data_path = td.mk_path("data.bin");

    write_xml(&xml_before, scenario)?;
    create_data_file(&data_path, &xml_before)?;

    let mut rng = rand::thread_rng();
    let seed = rng.gen::<u64>();

    stamp(&xml_before, &data_path, seed)?;
    verify(&xml_before, &data_path, seed)?;

    let new_nr_blocks = scenario.get_new_nr_blocks().to_string();

    let cmd = thin_shrink_cmd(args![
        "-i",
        &xml_before,
        "-o",
        &xml_after,
        "--data",
        &data_path,
        "--nr-blocks",
        &new_nr_blocks
    ]);

    if expect_ok {
        run_ok(cmd)?;
        verify(&xml_after, &data_path, seed)?;
    } else {
        run_fail(cmd)?;
    }

    Ok(())
}

fn test_shrink<S>(scenario: &mut S) -> Result<()>
where
    S: Scenario + XmlGen,
{
    test_shrink_(scenario, true)
}

fn test_shrink_fail<S>(scenario: &mut S) -> Result<()>
where
    S: Scenario + XmlGen,
{
    test_shrink_(scenario, false)
}

fn test_shrink_in_binary_<S>(scenario: &mut S, expect_ok: bool) -> Result<()>
where
    S: Scenario + XmlGen,
{
    use common::fixture::mk_zeroed_md;

    let mut td = TestDir::new()?;
    let xml_before = td.mk_path("before.xml");
    let xml_after = td.mk_path("after.xml");
    let meta_before = mk_zeroed_md(&mut td)?;
    let meta_after = mk_zeroed_md(&mut td)?;
    let data_path = td.mk_path("data.bin");

    write_xml(&xml_before, scenario)?;
    run_ok(thin_restore_cmd(args![
        "-i",
        &xml_before,
        "-o",
        &meta_before
    ]))?;

    create_data_file(&data_path, &xml_before)?;

    let mut rng = rand::thread_rng();
    let seed = rng.gen::<u64>();

    stamp(&xml_before, &data_path, seed)?;
    verify(&xml_before, &data_path, seed)?;

    let new_nr_blocks = scenario.get_new_nr_blocks().to_string();

    let cmd = thin_shrink_cmd(args![
        "-i",
        &meta_before,
        "-o",
        &meta_after,
        "--data",
        &data_path,
        "--nr-blocks",
        &new_nr_blocks,
        "--binary"
    ]);

    if expect_ok {
        run_ok(cmd)?;
        run_ok(thin_dump_cmd(args![&meta_after, "-o", &xml_after]))?;
        verify(&xml_after, &data_path, seed)?;
    } else {
        run_fail(cmd)?;
    }

    Ok(())
}

fn test_shrink_in_binary<S>(scenario: &mut S) -> Result<()>
where
    S: Scenario + XmlGen,
{
    test_shrink_in_binary_(scenario, true)
}

fn test_shrink_in_binary_fail<S>(scenario: &mut S) -> Result<()>
where
    S: Scenario + XmlGen,
{
    test_shrink_in_binary_(scenario, false)
}

//------------------------------------

impl Scenario for EmptyPoolS {
    fn get_new_nr_blocks(&self) -> u64 {
        512
    }
}

#[test]
fn shrink_empty_pool() -> Result<()> {
    let mut s = EmptyPoolS {};
    test_shrink(&mut s)
}

//------------------------------------

impl Scenario for SingleThinS {
    fn get_new_nr_blocks(&self) -> u64 {
        self.new_nr_data_blocks
    }
}

#[test]
fn shrink_single_no_move_1() -> Result<()> {
    let mut s = SingleThinS::new(0, 1024, 2048, 1280);
    test_shrink(&mut s)
}

#[test]
fn shrink_single_no_move_2() -> Result<()> {
    let mut s = SingleThinS::new(100, 1024, 2048, 1280);
    test_shrink(&mut s)
}

#[test]
fn shrink_single_no_move_3() -> Result<()> {
    let mut s = SingleThinS::new(1024, 1024, 2048, 2048);
    test_shrink(&mut s)
}

#[test]
fn shrink_single_partial_move() -> Result<()> {
    let mut s = SingleThinS::new(1024, 1024, 2048, 1280);
    test_shrink(&mut s)
}

#[test]
fn shrink_single_total_move() -> Result<()> {
    let mut s = SingleThinS::new(2048, 1024, 1024 + 2048, 1280);
    test_shrink(&mut s)
}

#[test]
fn shrink_insufficient_space() -> Result<()> {
    let mut s = SingleThinS::new(0, 2048, 3000, 1280);
    test_shrink_fail(&mut s)
}

//------------------------------------

#[test]
fn shrink_single_no_move_in_binary_1() -> Result<()> {
    let mut s = SingleThinS::new(0, 1024, 2048, 1280);
    test_shrink_in_binary(&mut s)
}

#[test]
fn shrink_single_no_move_in_binary_2() -> Result<()> {
    let mut s = SingleThinS::new(100, 1024, 2048, 1280);
    test_shrink_in_binary(&mut s)
}

#[test]
fn shrink_single_no_move_in_binary_3() -> Result<()> {
    let mut s = SingleThinS::new(1024, 1024, 2048, 2048);
    test_shrink_in_binary(&mut s)
}

#[test]
fn shrink_single_partial_move_in_binary() -> Result<()> {
    let mut s = SingleThinS::new(1024, 1024, 2048, 1280);
    test_shrink_in_binary(&mut s)
}

#[test]
fn shrink_single_total_move_in_binary() -> Result<()> {
    let mut s = SingleThinS::new(2048, 1024, 1024 + 2048, 1280);
    test_shrink_in_binary(&mut s)
}

#[test]
fn shrink_insufficient_space_in_binary() -> Result<()> {
    let mut s = SingleThinS::new(0, 2048, 3000, 1280);
    test_shrink_in_binary_fail(&mut s)
}

//------------------------------------

impl Scenario for FragmentedS {
    fn get_new_nr_blocks(&self) -> u64 {
        self.new_nr_data_blocks
    }
}

#[test]
fn shrink_fragmented_thin_1() -> Result<()> {
    let mut s = FragmentedS::new(1, 2048);
    test_shrink(&mut s)
}

#[test]
fn shrink_fragmented_thin_2() -> Result<()> {
    let mut s = FragmentedS::new(2, 2048);
    test_shrink(&mut s)
}

#[test]
fn shrink_fragmented_thin_8() -> Result<()> {
    let mut s = FragmentedS::new(8, 2048);
    test_shrink(&mut s)
}

#[test]
fn shrink_fragmented_thin_64() -> Result<()> {
    let mut s = FragmentedS::new(64, 2048);
    test_shrink(&mut s)
}

//------------------------------------

#[test]
fn shrink_fragmented_thin_1_in_binary() -> Result<()> {
    let mut s = FragmentedS::new(1, 2048);
    test_shrink_in_binary(&mut s)
}

#[test]
fn shrink_fragmented_thin_2_in_binary() -> Result<()> {
    let mut s = FragmentedS::new(2, 2048);
    test_shrink_in_binary(&mut s)
}

#[test]
fn shrink_fragmented_thin_8_in_binary() -> Result<()> {
    let mut s = FragmentedS::new(8, 2048);
    test_shrink_in_binary(&mut s)
}

#[test]
fn shrink_fragmented_thin_64_in_binary() -> Result<()> {
    let mut s = FragmentedS::new(64, 2048);
    test_shrink_in_binary(&mut s)
}

//------------------------------------

impl Scenario for SnapS {
    fn get_new_nr_blocks(&self) -> u64 {
        self.new_nr_data_blocks
    }
}

#[test]
fn shrink_identical_snap() -> Result<()> {
    let mut s = SnapS::new(1024, 1, 0);
    test_shrink(&mut s)
}

// Using XML from the pre-generated packed metadata
#[test]
fn shrink_multiple_snaps() -> Result<()> {
    let mut td = TestDir::new()?;
    let meta_before = prep_rebuilt_metadata(&mut td)?;
    let xml_before = td.mk_path("before.xml");
    let data_path = td.mk_path("data.bin");

    run_ok(thin_dump_cmd(args![&meta_before, "-o", &xml_before]))?;
    create_data_file(&data_path, &xml_before)?;

    let mut rng = rand::thread_rng();
    let seed = rng.gen::<u64>();

    stamp(&xml_before, &data_path, seed)?;
    verify(&xml_before, &data_path, seed)?;

    let xml_after = td.mk_path("after.xml");
    let new_nr_blocks = get_data_usage(&meta_before)?.1.to_string();

    run_ok(thin_shrink_cmd(args![
        "-i",
        &xml_before,
        "-o",
        &xml_after,
        "--data",
        &data_path,
        "--nr-blocks",
        &new_nr_blocks
    ]))?;

    verify(&xml_after, &data_path, seed)?;
    Ok(())
}

//------------------------------------

// Using the pre-generated packed metadata
#[test]
fn shrink_multiple_snaps_in_binary() -> Result<()> {
    let mut td = TestDir::new()?;
    let meta_before = prep_rebuilt_metadata(&mut td)?;
    let xml_before = td.mk_path("before.xml");
    let data_path = td.mk_path("data.bin");

    run_ok(thin_dump_cmd(args![&meta_before, "-o", &xml_before]))?;
    create_data_file(&data_path, &xml_before)?;

    let mut rng = rand::thread_rng();
    let seed = rng.gen::<u64>();

    stamp(&xml_before, &data_path, seed)?;
    verify(&xml_before, &data_path, seed)?;

    let meta_after = td.mk_path("after.bin");
    let xml_after = td.mk_path("after.xml");
    let new_nr_blocks = get_data_usage(&meta_before)?.1.to_string();

    file_utils::create_sized_file(&meta_after, file_utils::file_size(&meta_before)?)?;

    run_ok(thin_shrink_cmd(args![
        "-i",
        &meta_before,
        "-o",
        &meta_after,
        "--data",
        &data_path,
        "--nr-blocks",
        &new_nr_blocks,
        "--binary"
    ]))?;

    run_ok(thin_dump_cmd(args![&meta_after, "-o", &xml_after]))?;
    verify(&xml_after, &data_path, seed)?;
    Ok(())
}

//------------------------------------
