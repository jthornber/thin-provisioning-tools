use anyhow::{anyhow, Result};

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::commands::engine::*;
use crate::io_engine::*;
use crate::pdata::btree_builder::*;
use crate::pdata::space_map::common::pack_root;
use crate::pdata::space_map::disk::*;
use crate::pdata::space_map::metadata::*;
use crate::pdata::space_map::*;
use crate::report::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::ir::{self, MetadataVisitor, Visit};
use crate::thin::metadata_repair::{Override, SuperblockOverrides};
use crate::thin::superblock::{self, *};
use crate::thin::xml;
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

#[derive(PartialEq)]
enum Section {
    None,
    Superblock,
    Device,
    Def,
    Finalized,
}

pub struct Restorer<'a> {
    w: &'a mut WriteBatcher,
    report: Arc<Report>,

    // Shared leaves built from the <def> tags
    sub_trees: BTreeMap<String, Vec<NodeSummary>>,

    // The builder for the current shared sub tree or device
    current_map: Option<(MappedSection, NodeBuilder<BlockTime>)>,
    current_dev: Option<DeviceDetail>,

    sb: Option<ir::Superblock>,
    devices: BTreeMap<u32, (DeviceDetail, u64)>,
    data_sm: Option<Arc<Mutex<dyn SpaceMap>>>,
    in_section: Section,
    overrides: SuperblockOverrides,
}

impl<'a> Restorer<'a> {
    pub fn new(w: &'a mut WriteBatcher, report: Arc<Report>) -> Self {
        Restorer {
            w,
            report,
            sub_trees: BTreeMap::new(),
            current_map: None,
            current_dev: None,
            sb: None,
            devices: BTreeMap::new(),
            data_sm: None,
            in_section: Section::None,
            overrides: SuperblockOverrides::default(),
        }
    }

    pub fn new_with(
        w: &'a mut WriteBatcher,
        overrides: &'a SuperblockOverrides,
        report: Arc<Report>,
    ) -> Self {
        Restorer {
            w,
            report,
            sub_trees: BTreeMap::new(),
            current_map: None,
            current_dev: None,
            sb: None,
            devices: BTreeMap::new(),
            data_sm: None,
            in_section: Section::None,
            overrides: *overrides,
        }
    }

    fn begin_section(&mut self, section: MappedSection) -> Result<Visit> {
        if let Some((outer, _)) = self.current_map.as_ref() {
            let msg = format!(
                "Nested subtrees are not allowed '{}' within '{}'",
                section, outer
            );
            return Err(anyhow!(msg));
        }

        let value_rc = Box::new(MappingRC {
            sm: self.data_sm.as_ref().unwrap().clone(),
        });
        let shared = matches!(section, MappedSection::Def(_));
        let leaf_builder = NodeBuilder::new(Box::new(LeafIO {}), value_rc, shared);

        self.current_map = Some((section, leaf_builder));
        Ok(Visit::Continue)
    }

    fn end_section(&mut self) -> Result<(MappedSection, Vec<NodeSummary>)> {
        let mut current = None;
        std::mem::swap(&mut self.current_map, &mut current);

        if let Some((name, nodes)) = current {
            Ok((name, nodes.complete(self.w)?))
        } else {
            let msg = "Unbalanced </def> or </device> tag".to_string();
            Err(anyhow!(msg))
        }
    }

    // Build the device details and the top level mapping trees
    fn build_device_details(&mut self) -> Result<(u64, u64)> {
        let mut details_builder: BTreeBuilder<DeviceDetail> =
            BTreeBuilder::new(Box::new(NoopRC {}));
        let mut dev_builder: BTreeBuilder<u64> = BTreeBuilder::new(Box::new(NoopRC {}));
        for (thin_id, (detail, root)) in self.devices.iter() {
            details_builder.push_value(self.w, *thin_id as u64, *detail)?;
            dev_builder.push_value(self.w, *thin_id as u64, *root)?;
        }
        let details_root = details_builder.complete(self.w)?;
        let mapping_root = dev_builder.complete(self.w)?;

        Ok((details_root, mapping_root))
    }

    // Release the temporary references to the leaves of pre-built subtrees.
    // The contained child values will also be decreased if the leaf is
    // no longer referenced.
    fn release_subtrees(&mut self) -> Result<()> {
        let mut value_rc = MappingRC {
            sm: self.data_sm.as_ref().unwrap().clone(),
        };

        for (_, leaves) in self.sub_trees.iter() {
            release_leaves(self.w, leaves, &mut value_rc)?;
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<()> {
        let src_sb = if let Some(sb) = self.sb.take() {
            sb
        } else {
            return Err(anyhow!("missing superblock"));
        };

        let (details_root, mapping_root) = self.build_device_details()?;

        self.release_subtrees()?;

        // Build data space map
        let data_sm = self.data_sm.as_ref().unwrap();
        let data_sm_root = build_data_sm(self.w, data_sm.lock().unwrap().deref())?;

        // Build metadata space map
        let metadata_sm = write_metadata_sm(self.w)?;
        let metadata_sm_root = pack_root(&metadata_sm, SPACE_MAP_ROOT_SIZE)?;

        // Write the superblock
        let sb = superblock::Superblock {
            flags: SuperblockFlags { needs_check: false },
            block: SUPERBLOCK_LOCATION,
            version: 2,
            time: src_sb.time,
            transaction_id: src_sb.transaction,
            metadata_snap: 0,
            data_sm_root,
            metadata_sm_root,
            mapping_root,
            details_root,
            data_block_size: src_sb.data_block_size,
            nr_metadata_blocks: metadata_sm.nr_blocks,
        }
        .overrides(&self.overrides)?;
        write_superblock(self.w.engine.as_ref(), SUPERBLOCK_LOCATION, &sb)?;
        self.in_section = Section::Finalized;

        Ok(())
    }
}

impl<'a> MetadataVisitor for Restorer<'a> {
    fn superblock_b(&mut self, sb: &ir::Superblock) -> Result<Visit> {
        if self.in_section != Section::None {
            return Err(anyhow!("duplicated superblock"));
        }

        if !(128..=2097152).contains(&sb.data_block_size) || (sb.data_block_size & 0x7F != 0) {
            return Err(anyhow!("invalid data block size"));
        }

        self.sb = Some(sb.clone());
        self.data_sm = Some(core_sm(sb.nr_data_blocks, u32::MAX));
        let b = self.w.alloc()?;
        if b.loc != SUPERBLOCK_LOCATION {
            return Err(anyhow!("superblock was occupied"));
        }
        self.in_section = Section::Superblock;

        Ok(Visit::Continue)
    }

    fn superblock_e(&mut self) -> Result<Visit> {
        self.finalize()?;
        Ok(Visit::Continue)
    }

    fn def_shared_b(&mut self, name: &str) -> Result<Visit> {
        if self.in_section != Section::Superblock {
            return Err(anyhow!("missing superblock"));
        }
        self.in_section = Section::Def;
        self.begin_section(MappedSection::Def(name.to_string()))
    }

    fn def_shared_e(&mut self) -> Result<Visit> {
        if let (MappedSection::Def(name), nodes) = self.end_section()? {
            self.sub_trees.insert(name, nodes);
            self.in_section = Section::Superblock;
            Ok(Visit::Continue)
        } else {
            Err(anyhow!("unexpected </def>"))
        }
    }

    fn device_b(&mut self, d: &ir::Device) -> Result<Visit> {
        if self.in_section != Section::Superblock {
            return Err(anyhow!("missing superblock"));
        }
        self.report
            .info(&format!("building btree for device {}", d.dev_id));
        self.current_dev = Some(DeviceDetail {
            mapped_blocks: d.mapped_blocks,
            transaction_id: d.transaction,
            creation_time: d.creation_time,
            snapshotted_time: d.snap_time,
        });
        self.in_section = Section::Device;
        self.begin_section(MappedSection::Dev(d.dev_id))
    }

    fn device_e(&mut self) -> Result<Visit> {
        if let Some(detail) = self.current_dev.take() {
            if let (MappedSection::Dev(thin_id), nodes) = self.end_section()? {
                let root = build_btree(self.w, nodes)?;
                self.devices.insert(thin_id, (detail, root));
                self.in_section = Section::Superblock;
                Ok(Visit::Continue)
            } else {
                Err(anyhow!("internal error, couldn't find device details"))
            }
        } else {
            Err(anyhow!("unexpected </device>"))
        }
    }

    fn map(&mut self, m: &ir::Map) -> Result<Visit> {
        if let Some((_, builder)) = self.current_map.as_mut() {
            for i in 0..m.len {
                let bt = BlockTime {
                    block: m.data_begin + i,
                    time: m.time,
                };
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
            if let Some((_name, builder)) = self.current_map.as_mut() {
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
        if self.in_section != Section::Finalized {
            return Err(anyhow!("incomplete source metadata"));
        }
        Ok(Visit::Continue)
    }
}

//------------------------------------------

/// Writes a data space map to disk.  Returns the space map root that needs
/// to be written to the superblock.
fn build_data_sm(w: &mut WriteBatcher, sm: &dyn SpaceMap) -> Result<Vec<u8>> {
    let r = write_disk_sm(w, sm)?;
    let sm_root = pack_root(&r, SPACE_MAP_ROOT_SIZE)?;

    Ok(sm_root)
}

//------------------------------------------

pub struct ThinRestoreOptions<'a> {
    pub input: &'a Path,
    pub output: &'a Path,
    pub engine_opts: EngineOptions,
    pub report: Arc<Report>,
    pub overrides: SuperblockOverrides,
}

struct Context {
    report: Arc<Report>,
    engine: Arc<dyn IoEngine + Send + Sync>,
}

fn new_context(opts: &ThinRestoreOptions) -> Result<Context> {
    let engine = EngineBuilder::new(opts.output, &opts.engine_opts)
        .write(true)
        .build()?;

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

    let sm = core_metadata_sm(ctx.engine.get_nr_blocks(), max_count);
    let mut w = WriteBatcher::new(ctx.engine.clone(), sm.clone(), ctx.engine.get_batch_size());
    let mut restorer = Restorer::new_with(&mut w, &opts.overrides, ctx.report);
    xml::read(input, &mut restorer)?;

    Ok(())
}

//------------------------------------------
