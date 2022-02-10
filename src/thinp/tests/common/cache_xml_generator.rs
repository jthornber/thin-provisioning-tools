use anyhow::Result;
use std::fs::OpenOptions;
use std::path::Path;
use thinp::cache::xml;
use thinp_dev::cache::metadata_generator::MetadataGenerator;

//------------------------------------------

pub fn write_xml(path: &Path, g: &mut dyn MetadataGenerator) -> Result<()> {
    let xml_out = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    let mut w = xml::XmlWriter::new(xml_out);

    g.generate_metadata(&mut w)
}

//------------------------------------------
