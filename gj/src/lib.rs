pub mod join;
pub mod sql;
pub mod trie;
pub mod util;

pub type BuildHasher = std::hash::BuildHasherDefault<rustc_hash::FxHasher>;

pub type HashMap<K, V> = hashbrown::HashMap<K, V, BuildHasher>;
pub type HashSet<K> = hashbrown::HashSet<K, BuildHasher>;

pub type IndexMap<K, V> = indexmap::IndexMap<K, V, BuildHasher>;
pub type IndexSet<K> = indexmap::IndexSet<K, BuildHasher>;

use std::rc::Rc;

use sql::Attribute;
use trie::Value;

// pub type Col = Vec<Value<'a>>;
// pub type Relation = HashMap<Attribute, Col<'a>>;
// pub type DB = HashMap<String, Relation<'a>>;

pub type Col = Rc<ColInner>;

#[derive(Debug, Clone)]
pub enum ColInner {
    Int(Vec<i32>),
    Str(Vec<Rc<String>>),
}

impl Default for ColInner {
    fn default() -> Self {
        Self::Int(Vec::new())
    }
}

impl ColInner {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        match self {
            ColInner::Int(v) => v.len(),
            ColInner::Str(v) => v.len(),
        }
    }

    pub fn ints(&self) -> &[i32] {
        match self {
            ColInner::Int(v) => v,
            ColInner::Str(_) => panic!("not an int col"),
        }
    }

    pub fn strs(&self) -> &[Rc<String>] {
        match self {
            ColInner::Int(_) => panic!("not a str col"),
            ColInner::Str(v) => v,
        }
    }

    pub fn values(&self) -> impl Iterator<Item = Value> + '_ {
        match self {
            ColInner::Int(v) => either::Left(v.iter().map(|&i| Value::Num(i))),
            ColInner::Str(v) => either::Right(v.iter().map(|s| Value::Str(s.clone()))),
        }
    }

    pub fn push(&mut self, v: Value) {
        match (self, v) {
            (ColInner::Int(v), Value::Num(i)) => v.push(i),
            (ColInner::Str(v), Value::Str(s)) => v.push(s),
            (this, v) => {
                if this.len() == 0 {
                    *this = match v {
                        Value::Num(i) => ColInner::Int(vec![i]),
                        Value::Str(s) => ColInner::Str(vec![s]),
                    };
                } else {
                    panic!("cannot push {:?} to {:?}", v, this);
                }
            }
        }
    }

    fn get_value(&self, idx: usize) -> Value {
        match self {
            ColInner::Int(v) => Value::Num(v[idx]),
            ColInner::Str(v) => Value::Str(v[idx].clone()),
        }
    }
}

pub type Relation = HashMap<Attribute, Col>;
pub type DB = HashMap<String, Relation>;

use clap::Parser;

#[derive(Parser)]
pub struct Args {
    #[clap(short = 'O', long, default_value = "0")]
    pub optimize: usize,
    #[clap(long)]
    pub no_cache: bool,
    #[clap(long)]
    pub query: Option<String>,
}
