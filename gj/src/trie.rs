use rustc_hash::FxHashMap as HashMap;
use indexmap::IndexMap;
use serde_json::map::IntoIter;

use std::fmt;
use std::fmt::{Debug, Display};
use std::cmp::Ordering;
use std::iter::{ExactSizeIterator, once};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum Value {
    Str(String),
    Num(i32),
}

// maps an attribute name to a column
pub type Relation = IndexMap<String, Vec<Value>>;
// maps a table name to a relation
pub type DB = HashMap<String, Relation>;

#[derive(Debug, Clone)]
pub enum Trie {
    Node(HashMap<Value, Self>),
    Leaf(Vec<Value>),
    Relation(Relation),
}

impl Trie {
    pub fn len(&self) -> usize {
        match self {
            Trie::Leaf(_) => 1,
            Trie::Node(map) => map.len(),
            Trie::Relation(rel) => rel.len(),
        }
    }
}

impl IntoIter

pub fn intersect_order(t1: &Trie, t2: &Trie) -> Ordering {
    match (t1, t2) {
        (Trie::Relation(_), Trie::Relation(_)) => panic!("intersecting two relations"),
        (Trie::Relation(_r1), _) => Ordering::Greater,
        _ => t1.len().cmp(&t2.len()),
    }
}

// #[derive(Debug, Clone)]
// pub enum Trie<T> {
//     Node(HashMap<Id, Self>),
//     Data(Vec<Vec<T>>),
// }

// pub enum Table<'a, T> {
//     Trie(Trie<T>),
//     Arr((Vec<&'a [i32]>, Vec<&'a [T]>)),
// }

// impl<'a, T> Table<'a, T> {
//     pub fn get_data(&self) -> Result<&[Vec<T>], NotAData> {
//         match self {
//             Table::Trie(Trie::Data(data)) => Ok(data),
//             // Table::Single((_, data)) => Ok(&data),
//             _ => Err(NotAData),
//         }
//     }

//     pub fn get_map(&self) -> Result<&HashMap<Id, Trie<T>>, NotANode> {
//         match self {
//             Table::Trie(Trie::Node(map)) => Ok(map),
//             _ => Err(NotANode),
//         }
//     }
// }

// #[derive(Debug, Clone, Copy)]
// pub struct NotAData;

// impl Display for NotAData {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "Not a data")
//     }
// }

// impl std::error::Error for NotAData {}

// #[derive(Debug, Clone, Copy)]
// pub struct NotANode;

// impl Display for NotANode {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "Not a node")
//     }
// }

// impl std::error::Error for NotANode {}

// impl<T> Default for Trie<T> {
//     fn default() -> Self {
//         Trie::Node(HashMap::default())
//     }
// }

// impl<T> Trie<T> {
//     pub fn get_map(&self) -> Result<&HashMap<Id, Self>, NotANode> {
//         if let Trie::Node(ref map) = *self {
//             Ok(map)
//         } else {
//             Err(NotANode)
//         }
//     }

//     pub fn get_map_mut(&mut self) -> Result<&mut HashMap<Id, Self>, NotANode> {
//         if let Trie::Node(ref mut map) = *self {
//             Ok(map)
//         } else {
//             Err(NotANode)
//         }
//     }

//     pub fn get_data(&self) -> Result<&[Vec<T>], NotAData> {
//         if let Trie::Data(ref data) = *self {
//             Ok(data)
//         } else {
//             Err(NotAData)
//         }
//     }

//     pub fn get_data_mut(&mut self) -> Result<&mut Vec<Vec<T>>, NotAData> {
//         if let Trie::Data(ref mut data) = *self {
//             Ok(data)
//         } else {
//             Err(NotAData)
//         }
//     }

//     pub fn insert(&mut self, ids: &[Id], data: Vec<T>) {
//         let mut trie = self;
//         for id in ids {
//             trie = trie.get_map_mut().unwrap().entry(*id).or_default();
//         }

//         if !data.is_empty() {
//             *trie = Trie::Data(vec![data]);
//         } else {
//             *trie = Trie::Data(vec![]);
//         }
//     }
// }
