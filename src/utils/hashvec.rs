use std::collections::HashMap;
use std::vec::Vec;

//------------------------------------------

/// A HashVec is an associative array indexed by a HashMap.
#[derive(Debug)]
pub struct HashVec<T> {
    map: HashMap<u32, u32>, // TODO: parameterize the index type
    entries: Vec<T>,
}

impl<T: Clone> Default for HashVec<T> {
    fn default() -> HashVec<T> {
        HashVec::new()
    }
}

impl<T: Clone> HashVec<T> {
    pub fn new() -> HashVec<T> {
        HashVec::<T> {
            map: HashMap::new(),
            entries: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: u32) -> HashVec<T> {
        HashVec::<T> {
            map: HashMap::with_capacity(capacity as usize),
            entries: Vec::with_capacity(capacity as usize),
        }
    }

    pub fn insert(&mut self, index: u32, value: T) {
        self.map
            .entry(index)
            .and_modify(|e| {
                self.entries[*e as usize] = value.clone();
            })
            .or_insert_with(|| {
                self.entries.push(value);
                (self.entries.len() - 1) as u32
            });
    }

    pub fn get(&self, index: u32) -> Option<&T> {
        self.map.get(&index).map(|i| &self.entries[*i as usize])
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn reserve(&mut self, additional: u32) {
        self.map.reserve(additional as usize);
        self.entries.reserve(additional as usize);
    }

    pub fn values(&self) -> std::slice::Iter<T> {
        self.entries.iter()
    }
}

//------------------------------------------
