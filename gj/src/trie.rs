use std::fmt;
use std::fmt::{Debug, Display};
use std::rc::Rc;
use std::slice::Chunks;

use crate::sql::Attribute;
use crate::*;

type Id = i32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum Value {
    Str(Rc<String>),
    Num(i32),
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
pub enum Trie {
    Node(HashMap<Id, Self>),
    Data(usize, Vec<Value>),
}

pub type Schema = Vec<Attribute>;

pub enum Table {
    Trie(Trie),
    Arr {
        id_cols: Vec<Col>,
        data_cols: Vec<Col>,
    },
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

impl Default for Trie {
    fn default() -> Self {
        Trie::Node(HashMap::default())
    }
}

impl Trie {
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

    pub fn get_data(&self) -> Result<Chunks<Value>, NotAData> {
        if let Trie::Data(arity, data) = self {
            if *arity > 0 {
                Ok(data.chunks(*arity))
            } else {
                Ok(data.chunks(1))
            }
        } else {
            Err(NotAData)
        }
    }

    // pub fn get_data_mut(&mut self) -> Result<&mut Vec<T>, NotAData> {
    //     if let Trie::Data(ref mut data) = *self {
    //         Ok(data)
    //     } else {
    //         Err(NotAData)
    //     }
    // }

    pub fn insert(&mut self, ids: &[Id], data: &[Value]) {
        let mut trie = self;
        for id in &ids[..ids.len() - 1] {
            trie = trie.get_map_mut().unwrap().entry(*id).or_default();
        }

        trie = trie
            .get_map_mut()
            .unwrap()
            .entry(ids[ids.len() - 1])
            .or_insert_with(|| Trie::Data(data.len(), vec![]));

        if !data.is_empty() {
            if let Trie::Data(arity, ref mut vec) = trie {
                assert_eq!(*arity, data.len());
                vec.extend_from_slice(data);
            } else {
                unreachable!()
            }
        }
    }
}
