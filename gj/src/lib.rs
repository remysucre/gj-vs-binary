pub mod join;
pub mod sql;
pub mod trie;
pub mod util;

use std::collections::HashMap;

use sql::Attribute;
use trie::{RawValue, Value};

pub type Col<'a> = Vec<Value<'a>>;
pub type Relation<'a> = HashMap<Attribute, Col<'a>>;
pub type DB<'a> = HashMap<String, Relation<'a>>;

pub type RawCol = Vec<RawValue>;
pub type RawRelation = HashMap<Attribute, RawCol>;
pub type RawDB = HashMap<String, RawRelation>;
