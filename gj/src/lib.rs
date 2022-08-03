pub mod join;
pub mod sql;
pub mod trie;
pub mod util;

use std::collections::HashMap;

use trie::Value;

// TODO refactor these types into different modules?
pub enum Col {
    IdCol(Vec<i32>), // join attributes
    DataCol(Vec<Value>),
}

impl Col {
    pub fn len(&self) -> usize {
        match self {
            Col::IdCol(v) => v.len(),
            Col::DataCol(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type Relation = HashMap<String, Col>;

pub type DB = HashMap<String, Relation>;
