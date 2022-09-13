pub mod join;
pub mod sql;
pub mod trie;
pub mod util;

pub type BuildHasher = std::hash::BuildHasherDefault<rustc_hash::FxHasher>;

pub type HashMap<K, V> = hashbrown::HashMap<K, V, BuildHasher>;
pub type HashSet<K> = hashbrown::HashSet<K, BuildHasher>;

pub type IndexMap<K, V> = indexmap::IndexMap<K, V, BuildHasher>;
pub type IndexSet<K> = indexmap::IndexSet<K, BuildHasher>;

use std::sync::Arc;

use sql::Attribute;
use trie::{RawValue, Value};

pub type Col<'a> = Vec<Value<'a>>;
pub type Relation<'a> = HashMap<Attribute, Col<'a>>;
pub type DB<'a> = HashMap<String, Relation<'a>>;

pub type RawCol = Vec<RawValue>;
pub type RawRelation = Arc<HashMap<Attribute, RawCol>>;
pub type RawDB = HashMap<String, RawRelation>;

use clap::Parser;

#[derive(Parser)]
pub struct Args {
    #[clap(short = 'O', long, default_value = "0")]
    pub optimize: usize,
    #[clap(long)]
    pub cache: bool,
    #[clap(long)]
    pub query: Option<String>,
}
