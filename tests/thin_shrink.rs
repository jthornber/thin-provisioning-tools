use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use rand::Rng;
use std::fs::OpenOptions;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use thinp::file_utils;
use thinp::thin::xml::{self, Visit};

//------------------------------------

struct ThinBlock {
    thin_id: u32,
    thin_block: u64,
    data_block: u64,
    block_size: usize,
}

struct ThinReadRef {
    pub data: Vec<u8>,
}

struct ThinWriteRef<'a, W: Write + Seek> {
    file: &'a mut W,
    pub data: Vec<u8>,
}

impl ThinBlock {
    fn read_ref<R: Read + Seek>(&self, r: &mut R) -> Result<ThinReadRef> {
        let mut rr = ThinReadRef {
            data: vec![0; self.block_size],
        };
        r.seek(SeekFrom::Start(self.data_block * (self.block_size as u64)))?;
        r.read_exact(&mut rr.data[0..])?;
        Ok(rr)
    }

    fn zero_ref<'a, W: Write + Seek>(&self, w: &'a mut W) -> ThinWriteRef<'a, W> {
        ThinWriteRef {
            file: w,
            data: vec![0; self.block_size],
        }
    }

    fn write_ref<'a, W>(&self, w: &'a mut W) -> Result<ThinWriteRef<'a, W>>
    where
        W: Read + Write + Seek,
    {
        let mut data = vec![0; self.block_size];
        w.seek(SeekFrom::Start(self.data_block * (self.block_size as u64)))?;
        w.read_exact(&mut data[0..])?;

        let wr = ThinWriteRef {
            file: w,
            data: vec![0; self.block_size],
        };

        Ok(wr)
    }
}

impl<'a, W: Write + Seek> Drop for ThinWriteRef<'a, W> {
    fn drop(&mut self) {
        self.file.write_all(&self.data[0..]).unwrap();
    }
}

//------------------------------------

trait ThinVisitor {
    fn thin_block(&mut self, tb: &ThinBlock) -> Result<()>;
}

struct ThinXmlVisitor<'a, V: ThinVisitor> {
    inner: &'a mut V,
    block_size: Option<u32>,
    thin_id: Option<u32>,
}

impl<'a, V: ThinVisitor> xml::MetadataVisitor for ThinXmlVisitor<'a, V> {
    fn superblock_b(&mut self, sb: &xml::Superblock) -> Result<Visit> {
        self.block_size = Some(sb.data_block_size);
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn device_b(&mut self, d: &xml::Device) -> Result<Visit> {
        self.thin_id = Some(d.dev_id);
        Ok(Visit::Continue)
    }

    fn device_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn map(&mut self, m: &xml::Map) -> Result<Visit> {
        for i in 0..m.len {
            let block = ThinBlock {
                thin_id: self.thin_id.unwrap(),
                thin_block: m.thin_begin + i,
                data_block: m.data_begin + i,
                block_size: self.block_size.unwrap() as usize,
            };
            self.inner.thin_block(&block)?;
        }
        Ok(Visit::Continue)
    }

    fn eof(&mut self) -> Result<Visit> {
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
        block_size: None,
        thin_id: None,
    };

    xml::read(input, &mut xml_visitor)
}

//------------------------------------

// To test thin_shrink we'd like to stamp a known pattern across the
// provisioned areas of the thins in the pool, do the shrink, verify
// the patterns.

// A simple linear congruence generator used to create the data to
// go into the thin blocks.
struct Generator {
    x: u64,
    a: u64,
    c: u64,
}

impl Generator {
    fn new() -> Generator {
        Generator {
            x: 0,
            a: 6364136223846793005,
            c: 1442695040888963407,
        }
    }

    fn step(&mut self) {
        self.x = (self.a * self.x) + self.c
    }

    fn fill_buffer(&mut self, seed: u64, bytes: &mut [u8]) {
        self.x = seed;

        assert!(bytes.len() % 8 == 64);
        let nr_words = bytes.len() / 8;
        let mut out = Cursor::new(bytes);

        for _ in 0..nr_words {
            out.write_u64::<LittleEndian>(self.x).unwrap();
            self.step();
        }
    }

    fn verify_buffer(&mut self, seed: u64, bytes: &[u8]) {
        self.x = seed;

        assert!(bytes.len() % 8 == 64);
        let nr_words = bytes.len() / 8;
        let mut input = Cursor::new(bytes);

        for _ in 0..nr_words {
            let w = input.read_u64::<LittleEndian>().unwrap();
            assert_eq!(w, self.x);
            self.step();
        }
    }
}

//------------------------------------

struct Stamper<'a, W: Write + Seek> {
    data_file: &'a mut W,
    seed: u64,
}

impl<'a, W: Write + Seek> Stamper<'a, W> {
    fn new(w: &'a mut W, seed: u64) -> Stamper<'a, W> {
        Stamper { data_file: w, seed }
    }
}

impl<'a, W: Write + Seek> ThinVisitor for Stamper<'a, W> {
    fn thin_block(&mut self, b: &ThinBlock) -> Result<()> {
        let mut wr = b.zero_ref(self.data_file);
        let mut gen = Generator::new();
        gen.fill_buffer(
            self.seed ^ (b.thin_id as u64) ^ b.thin_block,
            &mut wr.data[0..],
        );
        Ok(())
    }
}

//------------------------------------

struct Verifier<'a, R: Read + Seek> {
    data_file: &'a mut R,
    seed: u64,
}

impl<'a, R: Read + Seek> Verifier<'a, R> {
    fn new(r: &'a mut R, seed: u64) -> Verifier<'a, R> {
        Verifier { data_file: r, seed }
    }
}

impl<'a, R: Read + Seek> ThinVisitor for Verifier<'a, R> {
    fn thin_block(&mut self, b: &ThinBlock) -> Result<()> {
        let rr = b.read_ref(self.data_file)?;
        let mut gen = Generator::new();
        gen.verify_buffer(self.seed ^ (b.thin_id as u64) ^ b.thin_block, &rr.data[0..]);
        Ok(())
    }
}

//------------------------------------

fn create_data_file(xml_path: &str) -> Result<std::fs::File> {
    let input = OpenOptions::new().read(true).write(false).open(xml_path)?;

    let sb = xml::read_superblock(input)?;
    let nr_blocks = sb.nr_data_blocks as u64;
    let block_size = sb.data_block_size as u64 * 512;

    let file = file_utils::temp_file_sized(nr_blocks * block_size)?;
    Ok(file)
}

fn main(xml_path: &str) -> Result<()> {
    let mut data_file = create_data_file(xml_path)?;
    let mut xml_in = OpenOptions::new().read(true).write(false).open(xml_path)?;

    let mut rng = rand::thread_rng();
    let seed = rng.gen::<u64>();

    let mut stamper = Stamper::new(&mut data_file, seed);
    thin_visit(&mut xml_in, &mut stamper)?;

    let mut verifier = Verifier::new(&mut data_file, seed);
    thin_visit(&mut xml_in, &mut verifier)?;
    Ok(())
}
