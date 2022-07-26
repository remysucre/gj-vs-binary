pub mod join;
pub mod sql;
pub mod trie;
pub mod util;

use std::collections::HashMap;

// TODO refactor these types into different modules?
pub enum Col {
    IdCol(Vec<i32>),     // join attributes
    StrCol(Vec<String>), // text data
    NumCol(Vec<i32>),    // numeric data
}

impl Col {
    pub fn len(&self) -> usize {
        match self {
            Col::IdCol(v) => v.len(),
            Col::StrCol(v) => v.len(),
            Col::NumCol(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type Relation = HashMap<String, Col>;

pub type DB = HashMap<String, Relation>;
