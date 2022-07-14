use std::collections::HashMap;
use std::fmt::Debug;

type Id = i32;

#[derive(Debug, Clone)]
pub enum Trie<T> {
    Node(HashMap<Id, Self>),
    Data(Vec<Vec<T>>),
}

impl<T> Default for Trie<T> {
    fn default() -> Self {
        Trie::Node(HashMap::new())
    }
}

impl<T> Trie<T> {
    pub fn len(&self) -> usize {
        self.get_map().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get_map(&self) -> &HashMap<Id, Self> {
        if let Trie::Node(ref map) = *self {
            map
        } else {
            panic!("get_map() called on a Trie::Data");
        }
    }

    pub fn get_map_mut(&mut self) -> &mut HashMap<Id, Self> {
        if let Trie::Node(ref mut map) = *self {
            map
        } else {
            panic!("get_map_mut() called on a Trie::Data");
        }
    }

    pub fn get_data(&self) -> &[Vec<T>] {
        if let Trie::Data(ref data) = *self {
            data
        } else {
            panic!("get_data() called on a Trie::Node");
        }
    }

    pub fn get_data_mut(&mut self) -> &mut Vec<Vec<T>> {
        if let Trie::Data(ref mut data) = *self {
            data
        } else {
            panic!("get_data_mut() called on a Trie::Node");
        }
    }

    pub fn insert(&mut self, ids: &[Id], data: Vec<T>) {
        let mut trie = self;
        for id in &ids[..ids.len() - 1] {
            trie = trie.get_map_mut().entry(*id).or_default();
        }

        let d = trie
            .get_map_mut()
            .entry(*ids.last().unwrap())
            .or_insert_with(|| Trie::Data(vec![]));
        if !data.is_empty() {
            d.get_data_mut().push(data);
        }
    }
}