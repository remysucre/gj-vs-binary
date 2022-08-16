use rustc_hash::FxHashMap as HashMap;
use indexmap::IndexMap;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::iter::{once, Once};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum Value {
    Str(String),
    Num(i32),
}

pub type Relation = IndexMap<String, Vec<Value>>;
pub type DB = HashMap<String, Relation>;

#[derive(Debug, Clone)]
pub enum Trie {
    Node(HashMap<Value, Self>),
    Sing(Value, Box::<Self>),
    Data(Vec<(Value, Self)>),
    Nil,
}

impl Default for Trie {
    fn default() -> Self {
        Trie::Node(HashMap::default())
    }
}

pub fn intersect_priority(t_1: &Trie, t_2: &Trie) -> Ordering {
    match (t_1, t_2) {
        (Trie::Data(_), _) => Ordering::Less,
        (_, Trie::Data(_)) => Ordering::Greater,
        _ => t_1.len().cmp(&t_2.len()),
    }
}

impl Trie {
    pub fn len(&self) -> usize {
        match self {
            Trie::Sing(_, _) => 1,
            Trie::Node(map) => map.len(),
            Trie::Data(rows) => rows.len(),
            // Trie::Nil => 0,
            Trie::Nil => panic!("Cannot get length of nil"),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> Iter<'_> {
        match self {
            Trie::Node(map) => Iter::Map(map.iter()),
            Trie::Sing(key, value) => Iter::Once(once((key, value))),
            Trie::Data(rows) => Iter::Vec(rows.iter()),
            // Trie::Nil => Iter::Empty,
            Trie::Nil => panic!("Cannot iterate over a nil trie"),
        }
    }

    pub fn get(&self, key: &Value) -> Option<&Trie> {
        match self {
            Trie::Node(map) => map.get(key),
            Trie::Sing(k, v) => {
                if k == key {
                    Some(v)
                } else {
                    None
                }
            }
            Trie::Data(_) => panic!("Cannot get from Data"),
            Trie::Nil => panic!("Cannot get from Nil"),
        }
    }

    pub fn get_mut(&mut self, key: &Value) -> Option<&mut Trie> {
        match self {
            Trie::Node(map) => map.get_mut(key),
            Trie::Sing(k, v) => {
                if k == key {
                    Some(v)
                } else {
                    None
                }
            }
            Trie::Data(_) => panic!("Cannot get from Data"),
            Trie::Nil => panic!("Cannot get from Nil"),
        }
    }

    fn entry(&mut self, key: Value) -> std::collections::hash_map::Entry<'_, Value, Trie>{
        match self {
            Trie::Node(map) => map.entry(key),
            _ => panic!("Cannot insert into a non-node trie"),
        }
    }

    pub fn insert(&mut self, keys: &[Value], mut data: Vec<Value>) {
        let mut trie = self;
        for k in keys {
            trie = trie.entry(k.clone()).or_default();
        }

        if !data.is_empty() {
            let single = data.drain(..).fold(Trie::Nil, |t, v| Trie::Sing(v, Box::new(t)));
            *trie = single;
        } else {
            *trie = Trie::Nil;
        }
    }
}

pub enum Iter<'a> {
    Once(Once<(&'a Value, &'a Trie)>),
    Map(std::collections::hash_map::Iter<'a, Value, Trie>),
    Vec(std::slice::Iter<'a, (Value, Trie)>),
    Empty,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Value, &'a Trie);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Iter::Empty => None,
            Iter::Once(iter) => iter.next(),
            Iter::Map(iter) => iter.next(),
            Iter::Vec(iter) => iter.next().map(|(k, v)| (k, v)),
        }
    }
}
