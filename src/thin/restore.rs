use anyhow::{anyhow, Result};

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

use crate::io_engine::*;
use crate::pdata::btree_builder::*;
use crate::pdata::space_map::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::{self, *};
use crate::thin::xml::{self, *};
use crate::write_batcher::*;

//------------------------------------------

enum MappedSection {
    Def(String),
    Dev(u32),
}

impl std::fmt::Display for MappedSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MappedSection::Def(name) => write!(f, "Def {}", name),
            MappedSection::Dev(thin_id) => write!(f, "Device {}", thin_id),
        }
    }
}

struct Pass1Result {
    sb: Option<xml::Superblock>,
    devices: BTreeMap<u32, (DeviceDetail, Vec<NodeSummary>)>,
}

struct Pass1<'a> {
    w: &'a mut WriteBatcher,

    current_dev: Option<DeviceDetail>,
    sub_trees: BTreeMap<String, Vec<NodeSummary>>,

    // The builder for the current shared sub tree or device
    map: Option<(MappedSection, NodeBuilder<BlockTime>)>,

    result: Pass1Result,
}

impl<'a> Pass1<'a> {
    fn new(w: &'a mut WriteBatcher) -> Self {
        Pass1 {
            w,
            current_dev: None,
            sub_trees: BTreeMap::new(),
            map: None,
            result: Pass1Result {
                sb: None,
                devices: BTreeMap::new(),
            },
        }
    }

    fn get_result(self) -> Pass1Result {
        self.result
    }

    fn begin_section(&mut self, section: MappedSection) -> Result<Visit> {
        if let Some((outer, _)) = self.map.as_ref() {
            let msg = format!(
                "Nested subtrees are not allowed '{}' within '{}'",
                section, outer
            );
            return Err(anyhow!(msg));
        }

        let value_rc = Box::new(NoopRC {});
        let leaf_builder = NodeBuilder::new(Box::new(LeafIO {}), value_rc);

        self.map = Some((section, leaf_builder));
        Ok(Visit::Continue)
    }

    fn end_section(&mut self) -> Result<(MappedSection, Vec<NodeSummary>)> {
        let mut current = None;
        std::mem::swap(&mut self.map, &mut current);

        if let Some((name, nodes)) = current {
            Ok((name, nodes.complete(self.w)?))
        } else {
            let msg = format!("Unbalanced </def> tag");
            Err(anyhow!(msg))
        }
    }
}

impl<'a> MetadataVisitor for Pass1<'a> {
    fn superblock_b(&mut self, sb: &xml::Superblock) -> Result<Visit> {
        self.result.sb = Some(sb.clone());
        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, name: &str) -> Result<Visit> {
        self.begin_section(MappedSection::Def(name.to_string()))
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        if let (MappedSection::Def(name), nodes) = self.end_section()? {
            self.sub_trees.insert(name, nodes);
            Ok(Visit::Continue)
        } else {
            Err(anyhow!("unexpected </def>"))
        }
    }

    fn device_b(&mut self, d: &Device) -> Result<Visit> {
        self.current_dev = Some(DeviceDetail {
            mapped_blocks: d.mapped_blocks,
            transaction_id: d.transaction,
            creation_time: d.creation_time as u32,
            snapshotted_time: d.snap_time as u32,
        });
        self.begin_section(MappedSection::Dev(d.dev_id))
    }

    fn device_e(&mut self) -> Result<Visit> {
        if let Some(detail) = self.current_dev.take() {
            if let (MappedSection::Dev(thin_id), nodes) = self.end_section()? {
                self.result.devices.insert(thin_id, (detail, nodes));
                Ok(Visit::Continue)
            } else {
                Err(anyhow!("internal error, couldn't find device details"))
            }
        } else {
            Err(anyhow!("unexpected </device>"))
        }
    }

    fn map(&mut self, m: &Map) -> Result<Visit> {
        if let Some((_name, _builder)) = self.map.as_mut() {
            for i in 0..m.len {
                let bt = BlockTime {
                    block: m.data_begin + i,
                    time: m.time,
                };
                let (_, builder) = self.map.as_mut().unwrap();
                builder.push_value(self.w, m.thin_begin + i, bt)?;
            }
            Ok(Visit::Continue)
        } else {
            let msg = format!("Mapping tags must appear within a <def> or <device> tag.");
            Err(anyhow!(msg))
        }
    }

    fn ref_shared(&mut self, name: &str) -> Result<Visit> {
        if self.current_dev.is_none() {
            return Err(anyhow!(
                "<ref> tags may only occur within <device> sections."
            ));
        }

        if let Some(leaves) = self.sub_trees.get(name) {
            // We could be in a <def> or <device>
            if let Some((_name, builder)) = self.map.as_mut() {
                builder.push_nodes(self.w, leaves)?;
            } else {
                let msg = format!(
                    "<ref name=\"{}\"> tag must be within either a <def> or <device> section",
                    name
                );
                return Err(anyhow!(msg));
            }
            Ok(Visit::Continue)
        } else {
            let msg = format!("Couldn't find sub tree '{}'.", name);
            Err(anyhow!(msg))
        }
    }

    fn eof(&mut self) -> Result<Visit> {
        // FIXME: build the rest of the device trees
        Ok(Visit::Continue)
    }
}

//------------------------------------------
/*
/// Writes a data space map to disk.  Returns the space map root that needs
/// to be written to the superblock.
fn build_data_sm(batcher: WriteBatcher, sm: Box<dyn SpaceMap>) -> Result<Vec<u8>> {

}
*/

//------------------------------------------

pub struct ThinRestoreOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub async_io: bool,
    pub report: Arc<Report>,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

const MAX_CONCURRENT_IO: u32 = 1024;

fn new_context(opts: &ThinRestoreOptions) -> Result<Context> {
    let engine: Arc<dyn IoEngine + Send + Sync>;

    if opts.async_io {
        engine = Arc::new(AsyncIoEngine::new(opts.output, MAX_CONCURRENT_IO, true)?);
    } else {
        let nr_threads = std::cmp::max(8, num_cpus::get() * 2);
        engine = Arc::new(SyncIoEngine::new(opts.output, nr_threads, true)?);
    }

    Ok(Context {
        report: opts.report.clone(),
        engine,
    })
}

//------------------------------------------

pub fn restore(opts: ThinRestoreOptions) -> Result<()> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(opts.input)?;

    let ctx = new_context(&opts)?;
    let max_count = u32::MAX;

    let sm = core_sm(ctx.engine.get_nr_blocks(), max_count);
    let mut w = WriteBatcher::new(ctx.engine.clone(), sm.clone(), ctx.engine.get_batch_size());
    let mut pass = Pass1::new(&mut w);
    xml::read(input, &mut pass)?;
    let pass = pass.get_result();

    // Build the device details tree.
    let mut details_builder: Builder<DeviceDetail> = Builder::new(Box::new(NoopRC {}));
    for (thin_id, (detail, _)) in &pass.devices {
        details_builder.push_value(&mut w, *thin_id as u64, *detail)?;
    }
    let details_root = details_builder.complete(&mut w)?;

    // Build the individual mapping trees that make up the bottom layer.
    let mut devs: BTreeMap<u32, u64> = BTreeMap::new();
    for (thin_id, (_, nodes)) in &pass.devices {
        ctx.report
            .info(&format!("building btree for device {}", thin_id));
        let mut builder: Builder<BlockTime> = Builder::new(Box::new(NoopRC {}));
        builder.push_leaves(&mut w, nodes)?;
        let root = builder.complete(&mut w)?;
        devs.insert(*thin_id, root);
    }

    // Build the top level mapping tree
    let mut builder: Builder<u64> = Builder::new(Box::new(NoopRC {}));
    for (thin_id, root) in devs {
        builder.push_value(&mut w, thin_id as u64, root)?;
    }
    let mapping_root = builder.complete(&mut w)?;

    // Build data space map

    // FIXME: I think we need to decrement the shared leaves
    // Build metadata space map

    // Write the superblock
    if let Some(xml_sb) = pass.sb {
        let sb = superblock::Superblock {
            flags: SuperblockFlags { needs_check: false },
            block: SUPERBLOCK_LOCATION,
            version: 2,
            time: xml_sb.time as u32,
            transaction_id: xml_sb.transaction,
            metadata_snap: 0,
            data_sm_root: vec![0; SPACE_MAP_ROOT_SIZE],
            metadata_sm_root: vec![0; SPACE_MAP_ROOT_SIZE],
            mapping_root,
            details_root,
            data_block_size: xml_sb.data_block_size,
            nr_metadata_blocks: ctx.engine.get_nr_blocks(),
        };

        write_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION, &sb)?;
    } else {
        return Err(anyhow!("No superblock found in xml file"));
    }

    Ok(())
}

//------------------------------------------
