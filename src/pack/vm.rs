use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;
use std::io::{Cursor, Read, Write};

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::pack::delta_list::*;

//-------------------------------------------------

// Deltas are converted to instructions.  A delta may not fit
// into a single instruction.
#[derive(Debug, FromPrimitive)]
enum Tag {
    Set, // Operand width given in nibble

    Pos,  // Delta in nibble
    PosW, // Delta in operand, whose width is in nibble

    Neg,  // Delta in nibble
    NegW, // Delta in operand, whose width is in nibble

    Const,  // Count in nibble
    Const8, // count = (nibble << 8) | byte

    // Controls how many times the next instruction is applied.
    // Not applicable to Const instructions which hold their own count.
    Count,  // count stored in nibble
    Count8, // count = (nibble << 8) | byte

    Lit, // len in nibble
    LitW,

    ShiftedRun,
}

fn pack_tag<W: Write>(w: &mut W, t: Tag, nibble: u8) -> io::Result<()> {
    assert!(nibble < 16);
    let mut b: u8 = t as u8;
    assert!(b < 16);
    b = (b << 4) | nibble;
    w.write_u8(b)
}

fn pack_count<W>(w: &mut W, count: u64) -> io::Result<()>
where
    W: Write,
{
    if count == 1u64 {
        Ok(())
    } else if count < 16 {
        pack_tag(w, Tag::Count, count as u8)
    } else {
        assert!(count < 4096);
        let nibble = count >> 8;
        assert!(nibble < 16);
        let byte = count & 0xff;
        pack_tag(w, Tag::Count8, nibble as u8)?;
        w.write_u8(byte as u8)
    }
}

fn pack_delta<W: Write>(w: &mut W, d: &Delta) -> io::Result<()> {
    use Tag::*;

    match d {
        Delta::Base { n } => {
            if *n <= std::u8::MAX as u64 {
                pack_tag(w, Set, 1)?;
                w.write_u8(*n as u8)
            } else if *n <= std::u16::MAX as u64 {
                pack_tag(w, Set, 2)?;
                w.write_u16::<LittleEndian>(*n as u16)
            } else if *n <= u32::MAX as u64 {
                pack_tag(w, Set, 4)?;
                w.write_u32::<LittleEndian>(*n as u32)
            } else {
                pack_tag(w, Set, 8)?;
                w.write_u64::<LittleEndian>(*n)
            }
        }
        Delta::Pos { delta, count } => {
            pack_count(w, *count)?;
            if *delta < 16 {
                pack_tag(w, Tag::Pos, *delta as u8)
            } else if *delta <= u8::MAX as u64 {
                pack_tag(w, PosW, 1)?;
                w.write_u8(*delta as u8)
            } else if *delta <= u16::MAX as u64 {
                pack_tag(w, PosW, 2)?;
                w.write_u16::<LittleEndian>(*delta as u16)
            } else if *delta <= u32::MAX as u64 {
                pack_tag(w, PosW, 4)?;
                w.write_u32::<LittleEndian>(*delta as u32)
            } else {
                pack_tag(w, PosW, 8)?;
                w.write_u64::<LittleEndian>(*delta)
            }
        }
        Delta::Neg { delta, count } => {
            pack_count(w, *count)?;

            if *delta < 16 {
                pack_tag(w, Neg, *delta as u8)
            } else if *delta <= u8::MAX as u64 {
                pack_tag(w, NegW, 1)?;
                w.write_u8(*delta as u8)
            } else if *delta <= u16::MAX as u64 {
                pack_tag(w, NegW, 2)?;
                w.write_u16::<LittleEndian>(*delta as u16)
            } else if *delta <= u32::MAX as u64 {
                pack_tag(w, NegW, 4)?;
                w.write_u32::<LittleEndian>(*delta as u32)
            } else {
                pack_tag(w, NegW, 8)?;
                w.write_u64::<LittleEndian>(*delta)
            }
        }
        Delta::Const { count } => {
            if *count < 16 {
                pack_tag(w, Tag::Const, *count as u8)
            } else {
                assert!(*count < 4096);
                let nibble = *count >> 8;
                assert!(nibble < 16);
                pack_tag(w, Tag::Const8, nibble as u8)?;
                w.write_u8((*count & 0xff) as u8)
            }
        }
    }
}

fn pack_deltas<W: Write>(w: &mut W, ds: &[Delta]) -> io::Result<()> {
    for d in ds {
        pack_delta(w, d)?;
    }
    Ok(())
}

//-------------------------------------------------

pub fn pack_u64s<W: Write>(w: &mut W, ns: &[u64]) -> io::Result<()> {
    let ds = to_delta(ns);
    pack_deltas(w, &ds[0..])
}

fn unshift_nrs(shift: usize, ns: &[u64]) -> (Vec<u64>, Vec<u64>) {
    let mut values = Vec::with_capacity(ns.len());
    let mut shifts = Vec::with_capacity(ns.len());

    let mask = (1 << shift) - 1;
    for n in ns {
        values.push(n >> shift);
        shifts.push(n & mask);
    }

    (values, shifts)
}

pub fn pack_shifted_u64s<W: Write>(w: &mut W, ns: &[u64]) -> io::Result<()> {
    let len = ns.len();
    let nibble = len >> 8;
    assert!(nibble < 16);
    pack_tag(w, Tag::ShiftedRun, nibble as u8)?;
    w.write_u8((len & 0xff) as u8)?;
    let (high, low) = unshift_nrs(24, ns);
    pack_u64s(w, &high[0..])?;
    pack_u64s(w, &low[0..])
}

pub fn pack_literal<W: Write>(w: &mut W, bs: &[u8]) -> io::Result<()> {
    use Tag::LitW;

    let len = bs.len() as u64;
    if len < 16 {
        pack_tag(w, Tag::Lit, len as u8)?;
    } else if len <= u8::MAX as u64 {
        pack_tag(w, LitW, 1)?;
        w.write_u8(len as u8)?;
    } else if len <= u16::MAX as u64 {
        pack_tag(w, LitW, 2)?;
        w.write_u16::<LittleEndian>(len as u16)?;
    } else if len <= u32::MAX as u64 {
        pack_tag(w, LitW, 4)?;
        w.write_u32::<LittleEndian>(len as u32)?;
    } else {
        pack_tag(w, LitW, 8)?;
        w.write_u64::<LittleEndian>(len)?;
    }
    w.write_all(bs)
}

//-------------------------------------------------

fn unpack_with_width<R: Read>(r: &mut R, nibble: u8) -> io::Result<u64> {
    let v = match nibble {
        1 => r.read_u8()? as u64,
        2 => r.read_u16::<LittleEndian>()? as u64,
        4 => r.read_u32::<LittleEndian>()? as u64,
        8 => r.read_u64::<LittleEndian>()?,
        _ => {
            panic!("SET with bad width");
        }
    };
    Ok(v)
}

pub fn unpack_u64s<R: Read>(r: &mut R, count: usize) -> io::Result<Vec<u64>> {
    let mut v = Vec::with_capacity(count);
    for _ in 0..count {
        let n = r.read_u64::<LittleEndian>()?;
        v.push(n);
    }
    Ok(v)
}

pub struct VM {
    base: u64,
    bytes_written: usize,
}

impl VM {
    pub fn new() -> VM {
        VM {
            base: 0,
            bytes_written: 0,
        }
    }

    fn emit_u64<W: Write>(&mut self, w: &mut W, n: u64) -> io::Result<()> {
        w.write_u64::<LittleEndian>(n)?;
        self.bytes_written += 8;
        Ok(())
    }

    fn emit_base<W: Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.emit_u64(w, self.base)
    }

    fn emit_bytes<W: Write>(&mut self, w: &mut W, bytes: &[u8]) -> io::Result<()> {
        let len = bytes.len();
        w.write_all(bytes)?;
        self.bytes_written += len;
        Ok(())
    }

    fn unpack_instr<R: Read, W: Write>(
        &mut self,
        r: &mut R,
        w: &mut W,
        count: usize,
    ) -> io::Result<()> {
        use Tag::*;

        let b = r.read_u8()?;
        let kind: Tag = match Tag::from_u8(b >> 4) {
            Some(k) => k,
            None => {
                panic!("bad tag");
            }
        };
        let nibble = b & 0xf;

        match kind {
            Set => {
                self.base = unpack_with_width(r, nibble)?;
                for _ in 0..count {
                    self.emit_base(w)?;
                }
            }
            Pos => {
                for _ in 0..count {
                    self.base += nibble as u64;
                    self.emit_base(w)?;
                }
            }
            PosW => {
                let delta = unpack_with_width(r, nibble)?;
                for _ in 0..count {
                    self.base += delta;
                    self.emit_base(w)?;
                }
            }
            Neg => {
                for _ in 0..count {
                    self.base -= nibble as u64;
                    self.emit_base(w)?;
                }
            }
            NegW => {
                let delta = unpack_with_width(r, nibble)?;
                for _ in 0..count {
                    self.base -= delta;
                    self.emit_base(w)?;
                }
            }
            Const => {
                assert_eq!(count, 1);
                for _ in 0..nibble as usize {
                    self.emit_base(w)?;
                }
            }
            Const8 => {
                assert_eq!(count, 1);
                let count = ((nibble as usize) << 8) | (r.read_u8()? as usize);
                for _ in 0..count {
                    self.emit_base(w)?;
                }
            }
            Count => {
                self.unpack_instr(r, w, nibble as usize)?;
            }
            Count8 => {
                let count = ((nibble as usize) << 8) | (r.read_u8()? as usize);
                self.unpack_instr(r, w, count)?;
            }
            Lit => {
                assert_eq!(count, 1);
                let len = nibble as usize;
                let mut bytes = vec![0; len];
                r.read_exact(&mut bytes[0..])?;
                self.emit_bytes(w, &bytes)?;
            }
            LitW => {
                assert_eq!(count, 1);
                let len = unpack_with_width(r, nibble)? as usize;
                let mut bytes = vec![0; len];
                r.read_exact(&mut bytes[0..])?;
                self.emit_bytes(w, &bytes)?;
            }
            ShiftedRun => {
                // FIXME: repeated unpack, pack, unpack
                let len = ((nibble as usize) << 8) | (r.read_u8()? as usize);
                let nr_bytes = len * std::mem::size_of::<u64>();

                let mut high_bytes: Vec<u8> = Vec::with_capacity(nr_bytes);
                let written = self.exec(r, &mut high_bytes, nr_bytes)?;
                self.bytes_written -= written; // hack
                let mut high_r = Cursor::new(high_bytes);
                let high = unpack_u64s(&mut high_r, len)?;

                let mut low_bytes: Vec<u8> = Vec::with_capacity(nr_bytes);
                let written = self.exec(r, &mut low_bytes, nr_bytes)?;
                self.bytes_written -= written; // hack
                let mut low_r = Cursor::new(low_bytes);
                let low = unpack_u64s(&mut low_r, len)?;

                let mask = (1 << 24) - 1;
                for i in 0..len {
                    self.emit_u64(w, (high[i] << 24) | (low[i] & mask))?;
                }
            }
        }
        Ok(())
    }

    // Runs until at least a number of bytes have been emitted.  Returns nr emitted.
    pub fn exec<R: Read, W: Write>(
        &mut self,
        r: &mut R,
        w: &mut W,
        emit_bytes: usize,
    ) -> io::Result<usize> {
        let begin = self.bytes_written;
        while (self.bytes_written - begin) < emit_bytes {
            self.unpack_instr(r, w, 1)?;
        }
        Ok(self.bytes_written - begin)
    }
}

impl Default for VM {
    fn default() -> Self {
        Self::new()
    }
}

pub fn unpack<R: Read>(r: &mut R, count: usize) -> io::Result<Vec<u8>> {
    let mut w = Vec::with_capacity(4096);
    let mut cursor = Cursor::new(&mut w);

    let mut vm = VM::new();
    let written = vm.exec(r, &mut cursor, count)?;

    assert_eq!(w.len(), count);
    assert_eq!(written, count);
    Ok(w)
}

//-------------------------------------------------

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_pack_literals() {
        struct TestCase(Vec<u8>);

        let cases = [
            // This is a bad test case, because unpack will not exec
            // any instructions.
            TestCase(b"".to_vec()),
            TestCase(b"foo".to_vec()),
            TestCase(vec![42; 15]),
            TestCase(vec![42; 256]),
            TestCase(vec![42; 4096]),
        ];

        for t in &cases {
            let mut bs = Vec::with_capacity(4096);

            let mut w = Cursor::new(&mut bs);
            pack_literal(&mut w, &t.0[0..]).unwrap();

            let mut r = Cursor::new(&mut bs);
            let unpacked = unpack(&mut r, t.0.len()).unwrap();

            assert_eq!(&t.0[0..], &unpacked[0..]);
        }
    }

    fn check_u64s_match(ns: &[u64], bytes: &[u8]) -> bool {
        let mut packed = Vec::with_capacity(ns.len() * 8);
        let mut w = Cursor::new(&mut packed);
        for n in ns {
            w.write_u64::<LittleEndian>(*n).unwrap();
        }
        packed == bytes
    }

    fn check_pack_u64s(ns: &[u64]) -> bool {
        println!("packing {:?}", &ns);
        let mut bs = Vec::with_capacity(4096);

        let mut w = Cursor::new(&mut bs);
        pack_u64s(&mut w, &ns[0..]).unwrap();
        println!("unpacked len = {}, packed len = {}", ns.len() * 8, bs.len());

        let mut r = Cursor::new(&mut bs);
        let unpacked = unpack(&mut r, ns.len() * 8).unwrap();

        check_u64s_match(ns, &unpacked[0..])
    }

    #[test]
    fn test_pack_u64s() {
        let cases = [
            vec![0],
            vec![1, 5, 9, 10],
            b"the quick brown fox jumps over the lazy dog"
                .iter()
                .map(|b| *b as u64)
                .collect(),
        ];

        for t in &cases {
            assert!(check_pack_u64s(t));
        }
    }

    #[quickcheck]
    fn prop_pack_u64s(mut ns: Vec<u64>) -> bool {
        ns.push(42); // We don't handle empty vecs
        check_pack_u64s(&ns)
    }

    fn check_pack_shifted_u64s(ns: &[(u64, u64)]) -> bool {
        let shifted: Vec<u64> = ns
            .iter()
            .map(|(h, l)| (h << 24) | (l & ((1 << 24) - 1)))
            .collect();

        println!("packing {:?}", &ns);
        let mut bs = Vec::with_capacity(4096);

        let mut w = Cursor::new(&mut bs);
        pack_shifted_u64s(&mut w, &shifted[0..]).unwrap();
        println!("unpacked len = {}, packed len = {}", ns.len() * 8, bs.len());

        let mut r = Cursor::new(&mut bs);
        let unpacked = unpack(&mut r, ns.len() * 8).unwrap();

        check_u64s_match(&shifted, &unpacked[0..])
    }

    #[quickcheck]
    fn prop_pack_shifted_u64s(mut ns: Vec<(u64, u64)>) -> bool {
        ns.push((42, 42));
        check_pack_shifted_u64s(&ns)
    }
}

//-------------------------------------------------
