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
    Vec(Vec<Id>, SmallVec<[Value; 2]>),
    Data(usize, SmallVec<[Value; 2]>),
    Nil,
}

#[derive(Debug, Clone, Copy)]
pub enum TrieRef<'a> {
    Node(&'a HashMap<Id, Trie>),
    Vec(&'a [Id], &'a [Value]),
    Data(usize, &'a [Value]),
    Nil,
}

impl<'a> TrieRef<'a> {

    pub fn for_each(&self, mut f: impl FnMut(Id, TrieRef)) {
        match self {
            TrieRef::Node(m) => m.iter().for_each(|(k, v)| f(*k, v.as_ref())),
            TrieRef::Vec(ids, data) => {
                if ids.len() == 1 {
                    f(ids[0], TrieRef::Data(data.len(), data));
                } else {

                    f(ids[0], TrieRef::Vec(&ids[1..], data));
                }
            }
            TrieRef::Data(..) => panic!("not a node"),
            TrieRef::Nil => {}
        }
    }

    pub fn len(&self) -> usize {
        match self {
            TrieRef::Nil => 0,
            TrieRef::Node(m) => m.len(),
            TrieRef::Vec(_, _) => 1,
            TrieRef::Data(..) => panic!("not a data"),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, id: Id) -> Option<TrieRef<'a>> {
        match self {
            TrieRef::Nil => None,
            TrieRef::Node(m) => m.get(&id).map(|t| t.as_ref()),
            TrieRef::Vec(ids, data) => {
                if ids[0] == id {
                    if ids[1..].is_empty() {
                        Some(TrieRef::Data(data.len(), data))
                    } else {
                        Some(TrieRef::Vec(&ids[1..], data))
                    }
                } else {
                    None
                }
            }
            TrieRef::Data(..) => panic!("not a node"),
        }
    }

    pub fn get_data(&self) -> Chunks<Value> {
        if let TrieRef::Data(arity, data) = self {
            if *arity > 0 {
                data.chunks(*arity)
            } else {
                data.chunks(1)
            }
        } else {
            panic!("not a data {:#?}", self)
        }
    }
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
        Trie::Nil
    }
}

impl Trie {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            Trie::Nil => 0,
            Trie::Node(m) => m.len(),
            Trie::Vec(_, _) => 1,
            Trie::Data(..) => panic!("not a data"),
        }
    }

    pub fn as_ref(&self) -> TrieRef {
        match self {
            Trie::Nil => TrieRef::Nil,
            Trie::Node(m) => TrieRef::Node(m),
            Trie::Vec(ids, vals) => TrieRef::Vec(ids, vals),
            Trie::Data(id, vals) => TrieRef::Data(*id, vals),
        }
    }

    pub fn get(&self, id: Id) -> Option<TrieRef> {
        match self {
            Trie::Nil => None,
            Trie::Node(m) => m.get(&id).map(|t| t.as_ref()),
            Trie::Vec(ids, data) => {
                if ids[0] == id {
                    Some(TrieRef::Vec(&ids[1..], data))
                } else {
                    None
                }
            }
            Trie::Data(..) => panic!("not a node"),
        }
    }

    pub fn for_each(&self, mut f: impl FnMut(Id, TrieRef)) {
        match self {
            Trie::Nil => {}
            Trie::Node(m) => m.iter().for_each(|(k, v)| f(*k, v.as_ref())),
            Trie::Vec(ids, data) => f(ids[0], TrieRef::Vec(&ids[1..], data)),
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

        match self {
            Trie::Nil => {
                *self = Trie::Vec(ids.to_vec(), data.to_vec().into());
            }
            Trie::Vec(ids_0, data_0) => {
                let mut m = HashMap::default();
                if ids_0[1..].is_empty() {
                    m.insert(ids_0[0], Trie::Data(data_0.len(), data_0.clone()));
                } else {
                    m.insert(ids_0[0], Trie::Vec(ids_0[1..].to_vec(), data_0.clone()));
                }
                *self = Trie::Node(m);
                self.insert(ids, data);
            }
            Trie::Node(m) => {
                let id = ids[0];
                let entry = m.entry(id);
                match entry {
                    Entry::Occupied(mut e) => {
                        if ids[1..].is_empty() {
                            e.insert(Trie::Data(data.len(), data.to_vec().into()));
                        } else {
                            e.get_mut().insert(&ids[1..], data);
                        }
                    }
                    Entry::Vacant(e) => {
                        if ids[1..].is_empty() {
                            e.insert(Trie::Data(data.len(), data.to_vec().into()));
                        } else {
                            e.insert(Trie::Vec(ids[1..].to_vec(), data.to_vec().into()));
                        }
                    }
                } 
            }
            Trie::Data(arity, vec) => {
                assert_eq!(*arity, data.len());
                vec.extend(data.iter().cloned());
            }
        }

    //     let mut trie = self;
    //     for id in &ids[..ids.len() - 1] {
    //         trie = trie.get_map_mut().entry(*id).or_default();
    //     }

    //     match trie.get_map_mut().entry(ids[ids.len() - 1]) {
    //         Entry::Vacant(e) => {
    //             e.insert(Trie::Data(data.len(), data.into()));
    //         }
    //         Entry::Occupied(mut e) => match e.get_mut() {
    //             Trie::Node(_) | Trie::Vec(..) => unreachable!(),
    //             Trie::Data(arity, vec) => {
    //                 assert_eq!(*arity, data.len());
    //                 vec.extend(data.iter().cloned());
    //             }
    //         },
    //     }
    }
}
