use rustc_hash::FxHashMap as HashMap;
use std::fmt;
use std::fmt::{Debug, Display};

use crate::sql::Attribute;

type Id = i32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum Value {
    Str(String),
    Num(i32),
}

#[derive(Clone)]
pub enum ValRef<'a> {
    Id(i32),
    Val(&'a Value),
}

impl Value {
    pub fn as_num(&self) -> i32 {
        match self {
            Value::Num(n) => *n,
            Value::Str(_) => panic!("Value is not a number"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Trie<'a, T> {
    Node(HashMap<Id, Self>),
    Data(Vec<Vec<&'a T>>),
}

pub type Schema = Vec<Attribute>;

pub struct Table<'a, T> {
    pub schema: Schema,
    pub data: Tb<'a, T>,
}

pub enum Tb<'a, T> {
    Trie(Trie<'a, T>),
    Arr((Vec<&'a [Value]>, Vec<&'a [T]>)),
}

impl<'a, T> Table<'a, T> {
    pub fn get_data(&self) -> Result<&[Vec<&'a T>], NotAData> {
        match &self.data {
            Tb::Trie(Trie::Data(data)) => Ok(data),
            // Table::Single((_, data)) => Ok(&data),
            _ => Err(NotAData),
        }
    }

    pub fn get_map(&self) -> Result<&HashMap<Id, Trie<T>>, NotANode> {
        match &self.data {
            Tb::Trie(Trie::Node(map)) => Ok(map),
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

impl<'a, T> Default for Trie<'a, T> {
    fn default() -> Self {
        Trie::Node(HashMap::default())
    }
}

impl<'a, T> Trie<'a, T> {
    pub fn get_map(&self) -> Result<&HashMap<Id, Self>, NotANode> {
        if let Trie::Node(ref map) = *self {
            Ok(map)
        } else {
            Err(NotANode)
        }
    }

    pub fn get_map_mut(&mut self) -> Result<&mut HashMap<Id, Self>, NotANode> {
        if let Trie::Node(ref mut map) = *self {
            Ok(map)
        } else {
            Err(NotANode)
        }
    }

    pub fn get_data(&self) -> Result<&[Vec<&'a T>], NotAData> {
        if let Trie::Data(ref data) = *self {
            Ok(data)
        } else {
            Err(NotAData)
        }
    }

    pub fn get_data_mut(&mut self) -> Result<&mut Vec<Vec<&'a T>>, NotAData> {
        if let Trie::Data(ref mut data) = *self {
            Ok(data)
        } else {
            Err(NotAData)
        }
    }

    pub fn insert(&mut self, ids: &[Id], data: Vec<&'a T>) {
        let mut trie = self;
        for id in &ids[..ids.len() - 1] {
            trie = trie.get_map_mut().unwrap().entry(*id).or_default();
        }

        trie = trie
            .get_map_mut()
            .unwrap()
            .entry(ids[ids.len() - 1])
            .or_insert_with(|| Trie::Data(vec![]));

        if !data.is_empty() {
            trie.get_data_mut().unwrap().push(data);
        }
    }
}
