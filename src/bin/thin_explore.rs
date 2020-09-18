extern crate clap;

use anyhow::{anyhow, Result};
use clap::{App, Arg};
use std::fmt;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::mpsc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use termion::clear;
use termion::color;
use termion::event::Key;
use termion::input::TermRead;
use termion::raw::IntoRawMode;

use tui::{
    backend::{Backend, TermionBackend},
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    terminal::Frame,
    text::{Span, Spans},
    widgets::{Block, Borders, List, ListItem, ListState, Row, StatefulWidget, Table, Widget},
    Terminal,
};

use thinp::io_engine::*;
use thinp::pdata::btree;
use thinp::pdata::unpack::*;
use thinp::thin::block_time::*;
use thinp::thin::superblock::*;

//------------------------------------

pub enum Event<I> {
    Input(I),
    Tick,
}

pub struct Events {
    rx: mpsc::Receiver<Event<Key>>,
    input_handle: thread::JoinHandle<()>,
    ignore_exit_key: Arc<AtomicBool>,
    tick_handle: thread::JoinHandle<()>,
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
            let tx = tx.clone();
            let ignore_exit_key = ignore_exit_key.clone();
            thread::spawn(move || {
                let stdin = io::stdin();
                for evt in stdin.keys() {
                    if let Ok(key) = evt {
                        if let Err(err) = tx.send(Event::Input(key)) {
                            eprintln!("{}", err);
                            return;
                        }
                        if !ignore_exit_key.load(Ordering::Relaxed) && key == config.exit_key {
                            return;
                        }
                    }
                }
            })
        };

        let tick_handle = {
            thread::spawn(move || loop {
                if tx.send(Event::Tick).is_err() {
                    break;
                }
                thread::sleep(config.tick_rate);
            })
        };

        Events {
            rx,
            ignore_exit_key,
            input_handle,
            tick_handle,
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

fn ls_previous(ls: &mut ListState, max: usize) {
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

struct SBWidget {
    sb: Superblock,
}

impl Widget for SBWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let sb = &self.sb;
        let flags = ["flags".to_string(), format!("{}", sb.flags)];
        let block = ["block".to_string(), format!("{}", sb.block)];
        let uuid = ["uuid".to_string(), format!("-")];
        let version = ["version".to_string(), format!("{}", sb.version)];
        let time = ["time".to_string(), format!("{}", sb.time)];
        let transaction_id = [
            "transaction_id".to_string(),
            format!("{}", sb.transaction_id),
        ];
        let metadata_snap = [
            "metadata_snap".to_string(),
            if sb.metadata_snap == 0 {
                "-".to_string()
            } else {
                format!("{}", sb.metadata_snap)
            },
        ];
        let mapping_root = ["mapping root".to_string(), format!("{}", sb.mapping_root)];
        let details_root = ["details root".to_string(), format!("{}", sb.details_root)];
        let data_block_size = [
            "data block size".to_string(),
            format!("{}k", sb.data_block_size * 2),
        ];

        let row_style = Style::default().fg(Color::White);
        let table = Table::new(
            ["Field", "Value"].iter(),
            vec![
                Row::Data(flags.iter()),
                Row::Data(block.iter()),
                Row::Data(uuid.iter()),
                Row::Data(version.iter()),
                Row::Data(time.iter()),
                Row::Data(transaction_id.iter()),
                Row::Data(metadata_snap.iter()),
                Row::Data(mapping_root.iter()),
                Row::Data(details_root.iter()),
                Row::Data(data_block_size.iter()),
            ]
            .into_iter(),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Superblock".to_string()),
        )
        .header_style(Style::default().fg(Color::Yellow))
        .widths(&[Constraint::Length(20), Constraint::Length(60)])
        .style(Style::default().fg(Color::White))
        .column_spacing(1);

        Widget::render(table, area, buf);
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
        let kind = [
            "type".to_string(),
            match hdr.is_leaf {
                true => "LEAF".to_string(),
                false => "INTERNAL".to_string(),
            },
        ];
        let nr_entries = ["nr_entries".to_string(), format!("{}", hdr.nr_entries)];
        let max_entries = ["max_entries".to_string(), format!("{}", hdr.max_entries)];
        let value_size = ["value size".to_string(), format!("{}", hdr.value_size)];

        let row_style = Style::default().fg(Color::White);
        let table = Table::new(
            ["Field", "Value"].iter(),
            vec![
                Row::Data(kind.iter()),
                Row::Data(nr_entries.iter()),
                Row::Data(max_entries.iter()),
                Row::Data(value_size.iter()),
            ]
            .into_iter(),
        )
        .block(Block::default().borders(Borders::ALL).title(self.title))
        .header_style(Style::default().fg(Color::Yellow))
        .widths(&[Constraint::Length(20), Constraint::Length(60)])
        .style(Style::default().fg(Color::White))
        .column_spacing(1);

        Widget::render(table, area, buf);
    }
}

fn read_node_header(engine: &dyn IoEngine, loc: u64) -> Result<btree::NodeHeader> {
    let b = engine.read(loc)?;
    unpack(&b.get_data()).map_err(|_| anyhow!("couldn't unpack btree header"))
}

fn read_node<V: Unpack>(engine: &dyn IoEngine, loc: u64) -> Result<btree::Node<V>> {
    let b = engine.read(loc)?;
    btree::unpack_node(&b.get_data(), true, false)
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

impl<X: Adjacent, Y: Adjacent> Adjacent for (X, Y) {
    fn adjacent(&self, rhs: &Self) -> bool {
        self.0.adjacent(&rhs.0) && self.1.adjacent(&rhs.1)
    }
}

fn adjacent_runs<V: Adjacent + Copy>(mut ns: Vec<V>) -> Vec<(V, usize)> {
    let mut result = Vec::new();

    if ns.len() == 0 {
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
            result.push((base.clone(), len));
            base = v.clone();
            current = v.clone();
            len = 1;
        }
    }
    result.push((base.clone(), len));

    result
}

fn mk_runs<V: Adjacent + Sized + Copy>(keys: &[u64], values: &[V]) -> Vec<((u64, V), usize)> {
    let mut pairs = Vec::new();
    for (k, v) in keys.iter().zip(values.iter()) {
        pairs.push((k.clone(), v.clone()));
    }

    adjacent_runs(pairs)
}

struct NodeWidget<'a, V: Unpack + Adjacent + Clone> {
    title: String,
    node: &'a btree::Node<V>,
}

fn mk_item<'a, V: fmt::Display>(k: u64, v: &V, len: usize) -> ListItem<'a> {
    if len > 1 {
        ListItem::new(Span::raw(format!(
            "[{}..{}] -> {}",
            k, k + len as u64, v
        )))
    } else {
        ListItem::new(Span::raw(format!("{} -> {}", k, v)))
    }
}

fn mk_items<'a, V>(keys:  &[u64], values: &[V], selected: usize) -> (Vec<ListItem<'a>>, usize)
    where
    V: Adjacent + Copy + fmt::Display
    {
    let mut items = Vec::new();
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

        let mut items: Vec<ListItem>;
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
    PushTopLevel(u64),
    PushBottomLevel(u32, u64),
    PopPanel,
}

type Frame_<'a, 'b> = Frame<'a, TermionBackend<termion::raw::RawTerminal<std::io::StdoutLock<'b>>>>;

trait Panel {
    fn render(&mut self, area: Rect, f: &mut Frame_);
    fn input(&mut self, k: Key) -> Option<Action>;
}

struct SBPanel {
    sb: Superblock,
}

impl Panel for SBPanel {
    fn render(&mut self, area: Rect, f: &mut Frame_) {
        // FIXME: get rid of clone
        let w = SBWidget {
            sb: self.sb.clone(),
        };
        f.render_widget(w, area);
    }

    fn input(&mut self, k: Key) -> Option<Action> {
        None
    }
}

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
            node: &self.node, // FIXME: get rid of clone
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
                ls_previous(&mut self.state, self.nr_entries);
                None
            }
            Key::Char('l') | Key::Right => match &self.node {
                btree::Node::Internal { values, .. } => {
                    Some(Action::PushTopLevel(values[self.state.selected().unwrap()]))
                }
                btree::Node::Leaf { values, keys, .. } => {
                    let index = self.state.selected().unwrap();

                    Some(Action::PushBottomLevel(keys[index] as u32, values[index]))
                }
            },
            Key::Char('h') | Key::Left => Some(Action::PopPanel),
            _ => None,
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
                ls_previous(&mut self.state, self.nr_entries);
                None
            }
            Key::Char('l') | Key::Right => match &self.node {
                btree::Node::Internal { values, .. } => Some(Action::PushBottomLevel(
                    self.thin_id,
                    values[self.state.selected().unwrap()],
                )),
                _ => None,
            },

            Key::Char('h') | Key::Left => Some(Action::PopPanel),
            _ => None,
        }
    }
}

//------------------------------------

fn explore(path: &Path) -> Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock().into_raw_mode()?;
    write!(stdout, "{}", termion::clear::All);

    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let path = std::path::Path::new("bz1763895/meta.bin");
    let engine = SyncIoEngine::new(&path, 1, false)?;

    let mut panels: Vec<Box<dyn Panel>> = Vec::new();

    let sb = read_superblock(&engine, 0)?;
    panels.push(Box::new(SBPanel { sb: sb.clone() }));

    let node = read_node::<u64>(&engine, sb.mapping_root)?;
    panels.push(Box::new(TopLevelPanel::new(node)));

    let events = Events::new();

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
                _ => match active_panel.input(key) {
                    Some(Action::PushTopLevel(b)) => {
                        let node = read_node::<u64>(&engine, b)?;
                        panels.push(Box::new(TopLevelPanel::new(node)));
                    }
                    Some(Action::PushBottomLevel(thin_id, b)) => {
                        let node = read_node::<BlockTime>(&engine, b)?;
                        panels.push(Box::new(BottomLevelPanel::new(thin_id, node)));
                    }
                    Some(Action::PopPanel) => {
                        if panels.len() > 2 {
                            panels.pop();
                        }
                    }
                    _ => {}
                },
            }
        }
    }

    Ok(())
}

//------------------------------------

fn main() -> Result<()> {
    let parser = App::new("thin_explore")
        .version(thinp::version::TOOLS_VERSION)
        .about("A text user interface for examining thin metadata.")
        .arg(
            Arg::with_name("INPUT")
                .help("Specify the input device to check")
                .required(true)
                .index(1),
        );

    let matches = parser.get_matches();
    let input_file = Path::new(matches.value_of("INPUT").unwrap());

    explore(&input_file)
}

//------------------------------------
