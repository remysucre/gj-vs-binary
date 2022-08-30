pub mod join;
pub mod sql;
pub mod trie;
pub mod util;

use std::{collections::HashMap, rc::Rc};

use sql::{Attribute, TreeOp};
use trie::Value;

pub type Col = Vec<Value>;
pub type Relation = HashMap<Attribute, Col>;
pub type DB = HashMap<String, Relation>;
pub type Views<'a> = HashMap<&'a TreeOp, HashMap<Attribute, Rc<Col>>>;
