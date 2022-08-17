pub mod join;
pub mod sql;
pub mod trie;
pub mod util;

use std::collections::HashMap;

use trie::Value;

pub type Col = Vec<Value>;
pub type Relation = HashMap<String, Col>;
pub type DB = HashMap<String, Relation>;
