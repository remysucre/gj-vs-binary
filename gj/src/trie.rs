use std::fmt::Debug;
use std::rc::Rc;
use std::slice::Chunks;

use hashbrown::hash_map::Entry;
use smallvec::SmallVec;

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
    Data(usize, SmallVec<[Value; 2]>),
}

pub type Schema = Vec<Attribute>;

pub enum Table {
    Trie(Trie),
    Arr {
        id_cols: Vec<Col>,
        data_cols: Vec<Col>,
    },
}

impl Default for Trie {
    fn default() -> Self {
        Trie::Node(HashMap::default())
    }
}

impl Trie {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            Trie::Node(m) => m.len(),
            Trie::Data(..) => panic!("not a data"),
        }
    }

    pub fn get(&self, id: Id) -> Option<&Self> {
        match self {
            Trie::Node(m) => m.get(&id),
            Trie::Data(..) => panic!("not a node"),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (Id, &Self)> {
        match self {
            Trie::Node(m) => m.iter().map(|(k, v)| (*k, v)),
            Trie::Data(..) => panic!("not a node"),
        }
    }

    fn get_map_mut(&mut self) -> &mut HashMap<Id, Self> {
        if let Trie::Node(ref mut map) = *self {
            map
        } else {
            panic!("not a node")
        }
    }

    pub fn get_data(&self) -> Chunks<Value> {
        if let Trie::Data(arity, data) = self {
            if *arity > 0 {
                data.chunks(*arity)
            } else {
                data.chunks(1)
            }
        } else {
            panic!("not a data")
        }
    }

    pub fn insert(&mut self, ids: &[Id], data: &[Value]) {
        let mut trie = self;
        for id in &ids[..ids.len() - 1] {
            trie = trie.get_map_mut().entry(*id).or_default();
        }

        match trie.get_map_mut().entry(ids[ids.len() - 1]) {
            Entry::Vacant(e) => {
                e.insert(Trie::Data(data.len(), data.into()));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                Trie::Node(_) => unreachable!(),
                Trie::Data(arity, vec) => {
                    assert_eq!(*arity, data.len());
                    vec.extend(data.iter().cloned());
                }
            },
        }
    }
}
