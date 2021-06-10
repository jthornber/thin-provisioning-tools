use anyhow::{anyhow, Result};

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Cursor;
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::io_engine::*;
use crate::pdata::btree_builder::*;
use crate::pdata::space_map::*;
use crate::pdata::space_map_disk::*;
use crate::pdata::space_map_metadata::*;
use crate::pdata::unpack::Pack;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::{self, *};
use crate::thin::xml::{self, *};
use crate::write_batcher::*;

//------------------------------------------

struct MappingRC {
    sm: Arc<Mutex<dyn SpaceMap>>,
}

impl RefCounter<BlockTime> for MappingRC {
    fn get(&self, v: &BlockTime) -> Result<u32> {
        return self.sm.lock().unwrap().get(v.block);
    }
    fn inc(&mut self, v: &BlockTime) -> Result<()> {
        self.sm.lock().unwrap().inc(v.block, 1)
    }
    fn dec(&mut self, v: &BlockTime) -> Result<()> {
        self.sm.lock().unwrap().dec(v.block)?;
        Ok(())
    }
}

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

//------------------------------------------

struct Pass1Result {
    sb: xml::Superblock,
    devices: BTreeMap<u32, (DeviceDetail, Vec<NodeSummary>)>,
    data_sm: Arc<Mutex<dyn SpaceMap>>,
}

struct Pass1<'a> {
    w: &'a mut WriteBatcher,

    current_dev: Option<DeviceDetail>,
    sub_trees: BTreeMap<String, Vec<NodeSummary>>,

    // The builder for the current shared sub tree or device
    map: Option<(MappedSection, NodeBuilder<BlockTime>)>,

    sb: Option<xml::Superblock>,
    devices: BTreeMap<u32, (DeviceDetail, Vec<NodeSummary>)>,
    data_sm: Option<Arc<Mutex<dyn SpaceMap>>>,
}

impl<'a> Pass1<'a> {
    fn new(w: &'a mut WriteBatcher) -> Self {
        Pass1 {
            w,
            current_dev: None,
            sub_trees: BTreeMap::new(),
            map: None,
            sb: None,
            devices: BTreeMap::new(),
            data_sm: None,
        }
    }

    fn get_result(self) -> Result<Pass1Result> {
        if self.sb.is_none() {
            return Err(anyhow!("No superblock found in xml file"));
        }
        Ok(Pass1Result {
            sb: self.sb.unwrap(),
            devices: self.devices,
            data_sm: self.data_sm.unwrap(),
        })
    }

    fn begin_section(&mut self, section: MappedSection) -> Result<Visit> {
        if let Some((outer, _)) = self.map.as_ref() {
            let msg = format!(
                "Nested subtrees are not allowed '{}' within '{}'",
                section, outer
            );
            return Err(anyhow!(msg));
        }

        let value_rc = Box::new(MappingRC {
            sm: self.data_sm.as_ref().unwrap().clone(),
        });
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
            let msg = "Unbalanced </def> tag".to_string();
            Err(anyhow!(msg))
        }
    }
}

impl<'a> MetadataVisitor for Pass1<'a> {
    fn superblock_b(&mut self, sb: &xml::Superblock) -> Result<Visit> {
        self.sb = Some(sb.clone());
        self.data_sm = Some(core_sm(sb.nr_data_blocks, u32::MAX));
        self.w.alloc()?;
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
                self.devices.insert(thin_id, (detail, nodes));
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
            let msg = "Mapping tags must appear within a <def> or <device> tag.".to_string();
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

/// Writes a data space map to disk.  Returns the space map root that needs
/// to be written to the superblock.
fn build_data_sm(w: &mut WriteBatcher, sm: &dyn SpaceMap) -> Result<Vec<u8>> {
    let mut sm_root = vec![0u8; SPACE_MAP_ROOT_SIZE];
    let mut cur = Cursor::new(&mut sm_root);
    let r = write_disk_sm(w, sm)?;
    r.pack(&mut cur)?;

    Ok(sm_root)
}

/// Writes the metadata space map to disk.  Returns the space map root that needs
/// to be written to the superblock.
fn build_metadata_sm(w: &mut WriteBatcher) -> Result<Vec<u8>> {
    let mut sm_root = vec![0u8; SPACE_MAP_ROOT_SIZE];
    let mut cur = Cursor::new(&mut sm_root);
    let sm_without_meta = clone_space_map(w.sm.lock().unwrap().deref())?;
    let r = write_metadata_sm(w, sm_without_meta.deref())?;
    r.pack(&mut cur)?;

    Ok(sm_root)
}

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
    let pass = pass.get_result()?;

    // Build the device details tree.
    let mut details_builder: BTreeBuilder<DeviceDetail> = BTreeBuilder::new(Box::new(NoopRC {}));
    for (thin_id, (detail, _)) in &pass.devices {
        details_builder.push_value(&mut w, *thin_id as u64, *detail)?;
    }
    let details_root = details_builder.complete(&mut w)?;

    // Build the individual mapping trees that make up the bottom layer.
    let mut devs: BTreeMap<u32, u64> = BTreeMap::new();
    for (thin_id, (_, nodes)) in &pass.devices {
        ctx.report
            .info(&format!("building btree for device {}", thin_id));
        let mut builder: BTreeBuilder<BlockTime> = BTreeBuilder::new(Box::new(NoopRC {}));
        builder.push_leaves(&mut w, nodes)?;
        let root = builder.complete(&mut w)?;
        devs.insert(*thin_id, root);
    }

    // Build the top level mapping tree
    let mut builder: BTreeBuilder<u64> = BTreeBuilder::new(Box::new(NoopRC {}));
    for (thin_id, root) in devs {
        builder.push_value(&mut w, thin_id as u64, root)?;
    }
    let mapping_root = builder.complete(&mut w)?;

    // Build data space map
    let data_sm_root = build_data_sm(&mut w, pass.data_sm.lock().unwrap().deref())?;

    // Build metadata space map
    let metadata_sm_root = build_metadata_sm(&mut w)?;

    // Write the superblock
    let sb = superblock::Superblock {
        flags: SuperblockFlags { needs_check: false },
        block: SUPERBLOCK_LOCATION,
        version: 2,
        time: pass.sb.time as u32,
        transaction_id: pass.sb.transaction,
        metadata_snap: 0,
        data_sm_root,
        metadata_sm_root,
        mapping_root,
        details_root,
        data_block_size: pass.sb.data_block_size,
        nr_metadata_blocks: ctx.engine.get_nr_blocks(),
    };
    write_superblock(ctx.engine.as_ref(), SUPERBLOCK_LOCATION, &sb)?;

    Ok(())
}

//------------------------------------------
