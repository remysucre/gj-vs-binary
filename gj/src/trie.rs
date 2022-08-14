// use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHasher;
use hashbrown::{HashMap as HBMap, BumpWrapper};
use bumpalo::Bump;

use std::{fmt, default};
use std::fmt::{Debug, Display};
use std::hash::BuildHasherDefault;

type HashMap<'a, K, V>  = HBMap<K, V, BuildHasherDefault<FxHasher>, BumpWrapper<'a>>;
type Id = i32;

// static VEC_CAPACITY: usize = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum Value {
    Str(String),
    Num(i32),
}

#[derive(Debug, Clone)]
pub enum Trie<'a, T> {
    Node(HashMap<'a, Id, Self>),
    Data(Vec<Vec<T>>),
}

impl <'a, T> Trie<'a, T> {
    pub fn new_in(bump: &'a Bump) -> Self {
        Trie::Node(HashMap::with_hasher_in(BuildHasherDefault::<FxHasher>::default(), BumpWrapper(bump)))
    }
}

pub enum Table<'a, 'b, T> {
    Trie(Trie<'b, T>),
    Arr((Vec<&'a [i32]>, Vec<&'a [T]>)),
}

impl<'a, 'b, T> Table<'a, 'b, T> {
    pub fn get_data(&self) -> Result<&[Vec<T>], NotAData> {
        match self {
            Table::Trie(Trie::Data(data)) => Ok(data),
            // Table::Single((_, data)) => Ok(&data),
            _ => Err(NotAData),
        }
    }

    pub fn get_map(&self) -> Result<&HashMap<Id, Trie<T>>, NotANode> {
        match self {
            Table::Trie(Trie::Node(map)) => Ok(map),
            _ => Err(NotANode),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NotAData;

impl Display for NotAData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Not a data")
    }
}

impl std::error::Error for NotAData {}

#[derive(Debug, Clone, Copy)]
pub struct NotANode;

impl Display for NotANode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Not a node")
    }
}

impl std::error::Error for NotANode {}

// impl<T> Default for Trie<T> {
//     fn default() -> Self {
//         Trie::Node(HashMap::default())
//     }
// }

impl<'a, T> Trie<'a, T> {
    pub fn get_map(&self) -> Result<&HashMap<'a, Id, Self>, NotANode> {
        if let Trie::Node(ref map) = *self {
            Ok(map)
        } else {
            Err(NotANode)
        }
    }

    pub fn get_map_mut(&mut self) -> Result<&mut HashMap<'a, Id, Self>, NotANode> {
        if let Trie::Node(ref mut map) = *self {
            Ok(map)
        } else {
            Err(NotANode)
        }
    }

    pub fn get_data(&self) -> Result<&[Vec<T>], NotAData> {
        if let Trie::Data(ref data) = *self {
            Ok(data)
        } else {
            Err(NotAData)
        }
    }

    pub fn get_data_mut(&mut self) -> Result<&mut Vec<Vec<T>>, NotAData> {
        if let Trie::Data(ref mut data) = *self {
            Ok(data)
        } else {
            Err(NotAData)
        }
    }

    pub fn insert(&mut self, arena: &'a Bump, ids: &[Id], data: Vec<T>) {
        let mut trie = self;
        for id in ids {
            trie = trie.get_map_mut().unwrap().entry(*id).or_insert_with(|| Trie::new_in(arena));
        }

        if !data.is_empty() {
            *trie = Trie::Data(vec![data]);
        } else {
            *trie = Trie::Data(vec![]);
        }
    }
}
