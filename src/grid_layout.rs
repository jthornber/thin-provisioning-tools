use anyhow::Result;
use std::io::Write;
use std::string::String;
use std::vec::Vec;

pub struct GridLayout {
    grid: Vec<Vec<String>>,
    current: Vec<String>,
    max_columns: usize,
}

impl GridLayout {
    pub fn new() -> GridLayout {
        GridLayout {
            grid: Vec::new(),
            current: Vec::new(),
            max_columns: 0,
        }
    }

    pub fn new_with_size(rows: usize, columns: usize) -> GridLayout {
        GridLayout {
            grid: Vec::with_capacity(rows),
            current: Vec::with_capacity(columns),
            max_columns: columns,
        }
    }

    pub fn field(&mut self, s: String) {
        self.current.push(s)
    }

    pub fn new_row(&mut self) {
        if self.current.len() > self.max_columns {
            self.max_columns = self.current.len();
        }
        let mut last = Vec::with_capacity(self.max_columns);
        std::mem::swap(&mut self.current, &mut last);
        self.grid.push(last);
    }

    fn calc_field_widths(&self) -> Result<Vec<usize>> {
        let mut widths = vec![0; self.max_columns];
        for row in self.grid.iter() {
            for (col, width) in row.iter().zip(widths.iter_mut()) {
                *width = std::cmp::max(*width, col.len());
            }
        }
        Ok(widths)
    }

    // right align the string
    fn push_justified(buf: &mut String, s: &str, width: usize) {
        for _ in 0..width - s.len() {
            buf.push(' ');
        }
        buf.push_str(s);
        buf.push(' ');
    }

    pub fn render(&self, w: &mut dyn Write) -> Result<()> {
        let widths = self.calc_field_widths()?;

        for row in self.grid.iter() {
            let mut line = String::new();
            for (col, width) in row.iter().zip(widths.iter()) {
                Self::push_justified(&mut line, col.as_str(), *width);
            }
            line.push('\n');
            w.write_all(line.as_bytes())?;
        }

        Ok(())
    }
}

impl Default for GridLayout {
    fn default() -> Self {
        Self::new()
    }
}
