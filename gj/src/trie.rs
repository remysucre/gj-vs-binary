use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display};

type Id = i32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum Value {
    Str(String),
    Num(i32),
}

#[derive(Debug, Clone)]
pub enum Trie<T> {
    Node(HashMap<Id, Self>),
    Data(Vec<Vec<T>>),
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

impl<T> Default for Trie<T> {
    fn default() -> Self {
        Trie::Node(HashMap::new())
    }
}

impl<T> Trie<T> {
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

    pub fn insert(&mut self, ids: &[Id], data: Vec<T>) {
        let mut trie = self;
        for id in &ids[..ids.len() - 1] {
            trie = trie.get_map_mut().unwrap().entry(*id).or_default();
        }

        let d = trie
            .get_map_mut()
            .unwrap()
            .entry(*ids.last().unwrap())
            .or_insert_with(|| Trie::Data(vec![]));
        // I think this skips NULL...
        if !data.is_empty() {
            d.get_data_mut().unwrap().push(data);
        }
        if d.get_data().unwrap().len() == 0 {
        }
    }
}
