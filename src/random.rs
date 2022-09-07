use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

//------------------------------------

// A simple linear congruence generator used to create the data to
// go into the thin/cache blocks.
pub struct Generator {
    x: u64,
    a: u64,
    c: u64,
}

impl Generator {
    pub fn new() -> Generator {
        Generator {
            x: 0,
            a: 6364136223846793005,
            c: 1442695040888963407,
        }
    }

    fn step(&mut self) {
        self.x = self.a.wrapping_mul(self.x).wrapping_add(self.c)
    }

    pub fn fill_buffer(&mut self, seed: u64, bytes: &mut [u8]) -> Result<()> {
        self.x = seed;

        assert!(bytes.len() % 8 == 0);
        let nr_words = bytes.len() / 8;
        let mut out = Cursor::new(bytes);

        for _ in 0..nr_words {
            out.write_u64::<LittleEndian>(self.x)?;
            self.step();
        }

        Ok(())
    }

    pub fn verify_buffer(&mut self, seed: u64, bytes: &[u8]) -> Result<bool> {
        self.x = seed;

        assert!(bytes.len() % 8 == 0);
        let nr_words = bytes.len() / 8;
        let mut input = Cursor::new(bytes);

        for _ in 0..nr_words {
            let w = input.read_u64::<LittleEndian>()?;
            if w != self.x {
                eprintln!("{} != {}", w, self.x);
                return Ok(false);
            }
            self.step();
        }

        Ok(true)
    }
}

impl Default for Generator {
    fn default() -> Self {
        Self::new()
    }
}

//------------------------------------
