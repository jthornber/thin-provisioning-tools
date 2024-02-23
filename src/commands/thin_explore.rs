extern crate clap;

use anyhow::{anyhow, Result};
use clap::Arg;
use std::fmt;
use std::io::{self, Write};
use std::path::Path;

use std::sync::mpsc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use termion::event::Key;
use termion::input::TermRead;
use termion::raw::IntoRawMode;

use tui::{
    backend::TermionBackend,
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    terminal::Frame,
    text::Span,
    widgets::{Block, Borders, List, ListItem, ListState, Row, StatefulWidget, Table, Widget},
    Terminal,
};

use crate::commands::utils::*;
use crate::commands::Command;
use crate::io_engine::*;
use crate::pdata::btree;
use crate::pdata::btree_error;
use crate::pdata::space_map::common::SMRoot;
use crate::pdata::unpack::*;
use crate::thin::block_time::*;
use crate::thin::device_detail::*;
use crate::thin::superblock::*;
use crate::version::*;

//------------------------------------

pub enum Event<I> {
    Input(I),
    Tick,
}

pub struct Events {
    rx: mpsc::Receiver<Event<Key>>,
    input_handle: thread::JoinHandle<()>,
    ignore_exit_key: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub exit_key: Key,
    pub tick_rate: Duration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            exit_key: Key::Char('q'),
            tick_rate: Duration::from_millis(250),
        }
    }
}

impl Events {
    pub fn new() -> Events {
        Events::with_config(Config::default())
    }

    pub fn with_config(config: Config) -> Events {
        let (tx, rx) = mpsc::channel();
        let ignore_exit_key = Arc::new(AtomicBool::new(false));
        let input_handle = {
            let ignore_exit_key = ignore_exit_key.clone();
            thread::spawn(move || {
                let stdin = io::stdin();
                for key in stdin.keys().flatten() {
                    if let Err(err) = tx.send(Event::Input(key)) {
                        eprintln!("{}", err);
                        return;
                    }
                    if !ignore_exit_key.load(Ordering::Relaxed) && key == config.exit_key {
                        return;
                    }
                }
            })
        };

        Events {
            rx,
            input_handle,
            ignore_exit_key,
        }
    }

    pub fn next(&self) -> Result<Event<Key>, mpsc::RecvError> {
        self.rx.recv()
    }

    pub fn disable_exit_key(&mut self) {
        self.ignore_exit_key.store(true, Ordering::Relaxed);
    }

    pub fn enable_exit_key(&mut self) {
        self.ignore_exit_key.store(false, Ordering::Relaxed);
    }
}

impl Default for Events {
    fn default() -> Self {
        Self::new()
    }
}

//------------------------------------

fn ls_next(ls: &mut ListState, max: usize) {
    let i = match ls.selected() {
        Some(i) => {
            if i >= max - 1 {
                max - 1
            } else {
                i + 1
            }
        }
        None => 0,
    };
    ls.select(Some(i));
}

fn ls_previous(ls: &mut ListState) {
    let i = match ls.selected() {
        Some(i) => {
            if i == 0 {
                0
            } else {
                i - 1
            }
        }
        None => 0,
    };
    ls.select(Some(i));
}

//------------------------------------

struct SBWidget<'a> {
    sb: &'a Superblock,
}

impl<'a> StatefulWidget for SBWidget<'a> {
    type State = ListState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut ListState) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(18), Constraint::Percentage(80)].as_ref())
            .split(area);

        let sb = self.sb;
        let flags = vec!["flags".to_string(), format!("{}", sb.flags)];
        let block = vec!["block".to_string(), format!("{}", sb.block)];
        let uuid = vec!["uuid".to_string(), "-".to_string()];
        let version = vec!["version".to_string(), format!("{}", sb.version)];
        let time = vec!["time".to_string(), format!("{}", sb.time)];
        let transaction_id = vec![
            "transaction_id".to_string(),
            format!("{}", sb.transaction_id),
        ];
        let metadata_snap = vec![
            "metadata_snap".to_string(),
            if sb.metadata_snap == 0 {
                "-".to_string()
            } else {
                format!("{}", sb.metadata_snap)
            },
        ];
        let metadata_root = unpack::<SMRoot>(&sb.metadata_sm_root[0..]).unwrap();
        let data_root = unpack::<SMRoot>(&sb.data_sm_root[0..]).unwrap();
        let metadata_allocated = vec![
            "metadata allocated".to_string(),
            format!("{}/{}", metadata_root.nr_allocated, metadata_root.nr_blocks),
        ];
        let meta_sm_roots = vec![
            "metadata roots".to_string(),
            format!(
                "[{}, {}]",
                metadata_root.bitmap_root, metadata_root.ref_count_root
            ),
        ];
        let data_allocated = vec![
            "data allocated".to_string(),
            format!("{}/{}", data_root.nr_allocated, data_root.nr_blocks),
        ];
        let data_sm_roots = vec![
            "data roots".to_string(),
            format!("[{}, {}]", data_root.bitmap_root, data_root.ref_count_root),
        ];
        let mapping_root = vec!["mapping root".to_string(), format!("{}", sb.mapping_root)];
        let details_root = vec!["details root".to_string(), format!("{}", sb.details_root)];
        let data_block_size = vec![
            "data block size".to_string(),
            format!("{}k", sb.data_block_size / 2),
        ];

        let table = Table::new(vec![
            Row::new(flags),
            Row::new(block),
            Row::new(uuid),
            Row::new(version),
            Row::new(time),
            Row::new(transaction_id),
            Row::new(metadata_snap),
            Row::new(metadata_allocated),
            Row::new(meta_sm_roots),
            Row::new(data_allocated),
            Row::new(data_sm_roots),
            Row::new(mapping_root),
            Row::new(details_root),
            Row::new(data_block_size),
        ])
        .header(Row::new(vec!["Field", "Value"]).style(Style::default().fg(Color::Yellow)))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Superblock".to_string()),
        )
        .widths(&[Constraint::Length(20), Constraint::Length(60)])
        .style(Style::default().fg(Color::White))
        .column_spacing(1);

        Widget::render(table, chunks[0], buf);

        let items = vec![
            ListItem::new(Span::raw("Device tree".to_string())),
            ListItem::new(Span::raw("Mapping tree".to_string())),
        ];

        let items = List::new(items)
            .block(Block::default().borders(Borders::ALL).title("Entries"))
            .highlight_style(
                Style::default()
                    .bg(Color::LightGreen)
                    .add_modifier(Modifier::BOLD),
            );

        StatefulWidget::render(items, chunks[1], buf, state);
    }
}

//------------------------------------

struct HeaderWidget<'a> {
    title: String,
    hdr: &'a btree::NodeHeader,
}

impl<'a> Widget for HeaderWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let hdr = &self.hdr;
        let block = vec!["block".to_string(), format!("{}", hdr.block)];
        let kind = vec![
            "type".to_string(),
            match hdr.is_leaf {
                true => "LEAF".to_string(),
                false => "INTERNAL".to_string(),
            },
        ];
        let nr_entries = vec!["nr_entries".to_string(), format!("{}", hdr.nr_entries)];
        let max_entries = vec!["max_entries".to_string(), format!("{}", hdr.max_entries)];
        let value_size = vec!["value size".to_string(), format!("{}", hdr.value_size)];

        let table = Table::new(vec![
            Row::new(block),
            Row::new(kind),
            Row::new(nr_entries),
            Row::new(max_entries),
            Row::new(value_size),
        ])
        .header(Row::new(vec!["Field", "Value"]).style(Style::default().fg(Color::Yellow)))
        .block(Block::default().borders(Borders::ALL).title(self.title))
        .widths(&[Constraint::Length(20), Constraint::Length(60)])
        .style(Style::default().fg(Color::White))
        .column_spacing(1);

        Widget::render(table, area, buf);
    }
}

fn read_node<V: Unpack>(engine: &dyn IoEngine, loc: u64) -> Result<btree::Node<V>> {
    let b = engine.read(loc)?;
    let path = Vec::new();
    btree::unpack_node(&path, b.get_data(), true, false)
        .map_err(|_| anyhow!("couldn't unpack btree node"))
}

//------------------------------------

// For types that have the concept of adjacency, but not of a distance
// between values.  For instance with a BlockTime there is no delta that
// will get between two values with different times.
trait Adjacent {
    fn adjacent(&self, rhs: &Self) -> bool;
}

impl Adjacent for u64 {
    fn adjacent(&self, rhs: &Self) -> bool {
        (*self + 1) == *rhs
    }
}

impl Adjacent for BlockTime {
    fn adjacent(&self, rhs: &Self) -> bool {
        if self.time != rhs.time {
            return false;
        }

        self.block + 1 == rhs.block
    }
}

impl Adjacent for DeviceDetail {
    fn adjacent(&self, _rhs: &Self) -> bool {
        false
    }
}

impl<X: Adjacent, Y: Adjacent> Adjacent for (X, Y) {
    fn adjacent(&self, rhs: &Self) -> bool {
        self.0.adjacent(&rhs.0) && self.1.adjacent(&rhs.1)
    }
}

fn adjacent_runs<V: Adjacent + Copy>(mut ns: Vec<V>) -> Vec<(V, usize)> {
    let mut result = Vec::new();

    if ns.is_empty() {
        return result;
    }

    // Reverse so we can pop without cloning the value.
    ns.reverse();

    let mut base = ns.pop().unwrap();
    let mut current = base;
    let mut len = 1;
    while let Some(v) = ns.pop() {
        if current.adjacent(&v) {
            current = v;
            len += 1;
        } else {
            result.push((base, len));
            base = v;
            current = v;
            len = 1;
        }
    }
    result.push((base, len));

    result
}

fn mk_runs<V: Adjacent + Sized + Copy>(keys: &[u64], values: &[V]) -> Vec<((u64, V), usize)> {
    let mut pairs = Vec::new();
    for (k, v) in keys.iter().zip(values.iter()) {
        pairs.push((*k, *v));
    }

    adjacent_runs(pairs)
}

//------------------------------------

struct NodeWidget<'a, V: Unpack + Adjacent + Clone> {
    title: String,
    node: &'a btree::Node<V>,
}

fn mk_item<'a, V: fmt::Display>(k: u64, v: &V, len: usize) -> ListItem<'a> {
    if len > 1 {
        ListItem::new(Span::raw(format!("{} x {} -> {}", k, len as u64, v)))
    } else {
        ListItem::new(Span::raw(format!("{} -> {}", k, v)))
    }
}

fn mk_items<'a, V>(keys: &[u64], values: &[V], selected: usize) -> (Vec<ListItem<'a>>, usize)
where
    V: Adjacent + Copy + fmt::Display,
{
    let mut items = Vec::new();

    if keys.is_empty() {
        return (items, 0);
    }

    let bkeys = &keys[0..selected];
    let key = keys[selected];
    let akeys = &keys[(selected + 1)..];

    let bvalues = &values[0..selected];
    let value = values[selected];
    let avalues = &values[(selected + 1)..];

    let bruns = mk_runs(bkeys, bvalues);
    let aruns = mk_runs(akeys, avalues);
    let i = bruns.len();

    for ((k, v), len) in bruns {
        items.push(mk_item(k, &v, len));
    }

    items.push(ListItem::new(Span::raw(format!("{} -> {}", key, value))));

    for ((k, v), len) in aruns {
        items.push(mk_item(k, &v, len));
    }

    (items, i)
}

impl<'a, V: Unpack + fmt::Display + Adjacent + Copy> StatefulWidget for NodeWidget<'a, V> {
    type State = ListState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut ListState) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(10), Constraint::Percentage(80)].as_ref())
            .split(area);

        let hdr = HeaderWidget {
            title: self.title,
            hdr: self.node.get_header(),
        };
        hdr.render(chunks[0], buf);

        let items: Vec<ListItem>;
        let i: usize;
        let selected = state.selected().unwrap();
        let mut state = ListState::default();

        match self.node {
            btree::Node::Internal { keys, values, .. } => {
                let (items_, i_) = mk_items(keys, values, selected);
                items = items_;
                i = i_;
            }
            btree::Node::Leaf { keys, values, .. } => {
                let (items_, i_) = mk_items(keys, values, selected);
                items = items_;
                i = i_;
            }
        }
        state.select(Some(i));

        let items = List::new(items)
            .block(Block::default().borders(Borders::ALL).title("Entries"))
            .highlight_style(
                Style::default()
                    .bg(Color::LightGreen)
                    .add_modifier(Modifier::BOLD),
            );

        StatefulWidget::render(items, chunks[1], buf, &mut state);
    }
}

//------------------------------------

enum Action {
    PushDeviceDetail(u64),
    PushTopLevel(u64),
    PushBottomLevel(u32, u64),
    PopPanel,
}

use Action::*;

type Frame_<'a, 'b> = Frame<'a, TermionBackend<termion::raw::RawTerminal<std::io::StdoutLock<'b>>>>;

trait Panel {
    fn render(&mut self, area: Rect, f: &mut Frame_);
    fn input(&mut self, k: Key) -> Option<Action>;
    fn path_action(&mut self, child: u64) -> Option<Action>;
}

//------------------------------------

struct SBPanel {
    sb: Superblock,
    state: ListState,
}

impl SBPanel {
    fn new(sb: Superblock) -> SBPanel {
        let mut state = ListState::default();
        state.select(Some(0));

        SBPanel { sb, state }
    }
}

impl Panel for SBPanel {
    fn render(&mut self, area: Rect, f: &mut Frame_) {
        let w = SBWidget { sb: &self.sb };
        f.render_stateful_widget(w, area, &mut self.state);
    }

    fn input(&mut self, k: Key) -> Option<Action> {
        match k {
            Key::Char('j') | Key::Down => {
                ls_next(&mut self.state, 2);
                None
            }
            Key::Char('k') | Key::Up => {
                ls_previous(&mut self.state);
                None
            }
            Key::Char('l') | Key::Right => {
                if self.state.selected().unwrap() == 0 {
                    Some(PushDeviceDetail(self.sb.details_root))
                } else {
                    Some(PushTopLevel(self.sb.mapping_root))
                }
            }
            Key::Char('h') | Key::Left => Some(PopPanel),
            _ => None,
        }
    }

    fn path_action(&mut self, child: u64) -> Option<Action> {
        if child == self.sb.mapping_root {
            Some(PushTopLevel(child))
        } else if child == self.sb.details_root {
            Some(PushDeviceDetail(child))
        } else {
            None
        }
    }
}

//------------------------------------

struct DeviceDetailPanel {
    node: btree::Node<DeviceDetail>,
    nr_entries: usize,
    state: ListState,
}

impl DeviceDetailPanel {
    fn new(node: btree::Node<DeviceDetail>) -> DeviceDetailPanel {
        let nr_entries = node.get_header().nr_entries as usize;
        let mut state = ListState::default();
        state.select(Some(0));

        DeviceDetailPanel {
            node,
            nr_entries,
            state,
        }
    }
}

impl Panel for DeviceDetailPanel {
    fn render(&mut self, area: Rect, f: &mut Frame_) {
        let w = NodeWidget {
            title: "Device Details".to_string(),
            node: &self.node,
        };

        f.render_stateful_widget(w, area, &mut self.state);
    }

    fn input(&mut self, k: Key) -> Option<Action> {
        match k {
            Key::Char('j') | Key::Down => {
                ls_next(&mut self.state, self.nr_entries);
                None
            }
            Key::Char('k') | Key::Up => {
                ls_previous(&mut self.state);
                None
            }
            Key::Char('l') | Key::Right => match &self.node {
                btree::Node::Internal { values, .. } => {
                    Some(PushDeviceDetail(values[self.state.selected().unwrap()]))
                }
                btree::Node::Leaf { .. } => None,
            },
            Key::Char('h') | Key::Left => Some(PopPanel),
            _ => None,
        }
    }

    fn path_action(&mut self, child: u64) -> Option<Action> {
        match &self.node {
            btree::Node::Internal { values, .. } => {
                for (i, v) in values.iter().enumerate() {
                    if *v == child {
                        self.state.select(Some(i));
                        return Some(PushDeviceDetail(child));
                    }
                }

                None
            }
            btree::Node::Leaf { .. } => None,
        }
    }
}

//------------------------------------

struct TopLevelPanel {
    node: btree::Node<u64>,
    nr_entries: usize,
    state: ListState,
}

impl TopLevelPanel {
    fn new(node: btree::Node<u64>) -> TopLevelPanel {
        let nr_entries = node.get_header().nr_entries as usize;
        let mut state = ListState::default();
        state.select(Some(0));

        TopLevelPanel {
            node,
            nr_entries,
            state,
        }
    }
}

impl Panel for TopLevelPanel {
    fn render(&mut self, area: Rect, f: &mut Frame_) {
        let w = NodeWidget {
            title: "Top Level".to_string(),
            node: &self.node,
        };

        f.render_stateful_widget(w, area, &mut self.state);
    }

    fn input(&mut self, k: Key) -> Option<Action> {
        match k {
            Key::Char('j') | Key::Down => {
                ls_next(&mut self.state, self.nr_entries);
                None
            }
            Key::Char('k') | Key::Up => {
                ls_previous(&mut self.state);
                None
            }
            Key::Char('l') | Key::Right => match &self.node {
                btree::Node::Internal { values, .. } => {
                    Some(PushTopLevel(values[self.state.selected().unwrap()]))
                }
                btree::Node::Leaf { values, keys, .. } => {
                    let index = self.state.selected().unwrap();

                    Some(PushBottomLevel(keys[index] as u32, values[index]))
                }
            },
            Key::Char('h') | Key::Left => Some(PopPanel),
            _ => None,
        }
    }

    fn path_action(&mut self, child: u64) -> Option<Action> {
        match &self.node {
            btree::Node::Internal { values, .. } => {
                for (i, v) in values.iter().enumerate() {
                    if *v == child {
                        self.state.select(Some(i));
                        return Some(PushTopLevel(child));
                    }
                }

                None
            }
            btree::Node::Leaf { keys, values, .. } => {
                for i in 0..values.len() {
                    if values[i] == child {
                        self.state.select(Some(i));
                        return Some(PushBottomLevel(keys[i] as u32, child));
                    }
                }

                None
            }
        }
    }
}

struct BottomLevelPanel {
    thin_id: u32,
    node: btree::Node<BlockTime>,
    nr_entries: usize,
    state: ListState,
}

impl BottomLevelPanel {
    fn new(thin_id: u32, node: btree::Node<BlockTime>) -> BottomLevelPanel {
        let nr_entries = node.get_header().nr_entries as usize;
        let mut state = ListState::default();
        state.select(Some(0));

        BottomLevelPanel {
            thin_id,
            node,
            nr_entries,
            state,
        }
    }
}

impl Panel for BottomLevelPanel {
    fn render(&mut self, area: Rect, f: &mut Frame_) {
        let w = NodeWidget {
            title: format!("Thin dev #{}", self.thin_id),
            node: &self.node,
        };

        f.render_stateful_widget(w, area, &mut self.state);
    }

    fn input(&mut self, k: Key) -> Option<Action> {
        match k {
            Key::Char('j') | Key::Down => {
                ls_next(&mut self.state, self.nr_entries);
                None
            }
            Key::Char('k') | Key::Up => {
                ls_previous(&mut self.state);
                None
            }
            Key::Char('l') | Key::Right => match &self.node {
                btree::Node::Internal { values, .. } => Some(PushBottomLevel(
                    self.thin_id,
                    values[self.state.selected().unwrap()],
                )),
                _ => None,
            },

            Key::Char('h') | Key::Left => Some(PopPanel),
            _ => None,
        }
    }

    fn path_action(&mut self, child: u64) -> Option<Action> {
        match &self.node {
            btree::Node::Internal { values, .. } => {
                for (i, v) in values.iter().enumerate() {
                    if *v == child {
                        self.state.select(Some(i));
                        return Some(PushBottomLevel(self.thin_id, child));
                    }
                }

                None
            }
            btree::Node::Leaf { .. } => None,
        }
    }
}

//------------------------------------

fn perform_action(
    panels: &mut Vec<Box<dyn Panel>>,
    engine: &dyn IoEngine,
    action: Action,
) -> Result<()> {
    match action {
        PushDeviceDetail(b) => {
            let node = read_node::<DeviceDetail>(engine, b)?;
            panels.push(Box::new(DeviceDetailPanel::new(node)));
        }
        PushTopLevel(b) => {
            let node = read_node::<u64>(engine, b)?;
            panels.push(Box::new(TopLevelPanel::new(node)));
        }
        PushBottomLevel(thin_id, b) => {
            let node = read_node::<BlockTime>(engine, b)?;
            panels.push(Box::new(BottomLevelPanel::new(thin_id, node)));
        }
        PopPanel => {
            if panels.len() > 1 {
                panels.pop();
            }
        }
    };
    Ok(())
}

fn explore(path: &Path, node_path: Option<Vec<u64>>) -> Result<()> {
    let engine = SyncIoEngine::new(path, false)?;

    let mut panels: Vec<Box<dyn Panel>> = Vec::new();

    if let Some(path) = node_path {
        eprintln!("using path: {:?}", path);
        assert_eq!(path[0], 0);
        let sb = read_superblock(&engine, path[0])?;
        panels.push(Box::new(SBPanel::new(sb)));
        for b in &path[1..] {
            let action = panels.last_mut().unwrap().path_action(*b);
            if let Some(action) = action {
                perform_action(&mut panels, &engine, action)?;
            } else {
                return Err(anyhow!("bad node path: couldn't find child node {}", b));
            }
        }
    } else {
        let sb = read_superblock(&engine, 0)?;
        panels.push(Box::new(SBPanel::new(sb)));
    }

    let events = Events::new();

    let stdout = io::stdout();
    let mut stdout = stdout.lock().into_raw_mode()?;
    write!(stdout, "{}", termion::clear::All)?;
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    'main: loop {
        let render_panels = |f: &mut Frame_| {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
                .split(f.size());

            let mut base = panels.len();
            if base >= 2 {
                base -= 2;
            } else {
                base = 0;
            }

            for i in base..panels.len() {
                panels[i].render(chunks[i - base], f);
            }
        };

        terminal.draw(render_panels)?;

        let last = panels.len() - 1;
        let active_panel = &mut panels[last];
        if let Event::Input(key) = events.next()? {
            match key {
                Key::Char('q') => break 'main,
                _ => {
                    if let Some(action) = active_panel.input(key) {
                        perform_action(&mut panels, &engine, action)?;
                    }
                }
            }
        }
    }

    events.input_handle.join().unwrap();

    Ok(())
}

//------------------------------------

pub struct ThinExploreCommand;

impl ThinExploreCommand {
    fn cli(&self) -> clap::Command {
        clap::Command::new(self.name())
            .next_display_order(None)
            .version(crate::tools_version!())
            .disable_version_flag(true)
            .about("A text user interface for examining thin metadata.")
            .arg(
                Arg::new("NODE_PATH")
                    .help("Pass in a node path as output by thin_check")
                    .short('p')
                    .long("node-path")
                    .value_name("NODE_PATH"),
            )
            .arg(
                Arg::new("INPUT")
                    .help("Specify the input device to check")
                    .required(true)
                    .index(1),
            )
    }
}

impl<'a> Command<'a> for ThinExploreCommand {
    fn name(&self) -> &'a str {
        "thin_explore"
    }

    fn run(&self, args: &mut dyn Iterator<Item = std::ffi::OsString>) -> exitcode::ExitCode {
        let matches = self.cli().get_matches_from(args);
        display_version(&matches);

        let node_path = matches
            .get_one::<String>("NODE_PATH")
            .map(|text| btree_error::decode_node_path(text).unwrap());
        let input_file = Path::new(matches.get_one::<String>("INPUT").unwrap());
        let report = mk_report(false);

        to_exit_code(&report, explore(input_file, node_path))
    }
}

//------------------------------------
