pub mod join;
pub mod sql;
pub mod trie;
pub mod util;

use std::collections::HashMap;

use sql::{TreeOp, Attribute};
use trie::Value;

pub type Col = Vec<Value>;
pub type Relation = HashMap<String, Col>;
pub type DB = HashMap<String, Relation>;
pub type Views<'a> = HashMap<&'a TreeOp, HashMap<Attribute, Col>>;