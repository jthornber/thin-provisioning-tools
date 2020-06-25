use anyhow::Result;

pub type Sector = u64;

pub struct Region {
    src: Sector,
    dest: Sector,
    len: Sector,
}

// FIXME: pass in
pub fn copy(path: &str, regions: &Vec<Region>) -> Result<()> {
    Ok(())
}
