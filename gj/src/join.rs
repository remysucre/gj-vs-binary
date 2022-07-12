use crate::*;
use std::fmt::Debug;
use std::collections::HashMap;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
enum Value {
    Int(usize),
    String(String),
}

#[derive(Debug, Clone, Default)]
struct Trie(HashMap<Value, Self>);

impl Trie {
    fn insert(&mut self, shuffle: &[usize], tuple: &[Value]) {
        // debug_assert_eq!(shuffle.len(), tuple.len());
        debug_assert!(shuffle.len() <= tuple.len());
        let mut trie = self;
        for i in shuffle {
            trie = trie.0.entry(tuple[*i].clone()).or_default();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trie() {
        let mut t = Trie::default();
        t.insert(&vec![0, 1, 2, 3], &vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::String("hello".to_string())]);
        t.insert(&vec![0, 1, 2, 3], &vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::String("world".to_string())]);
        t.insert(&vec![0, 1, 2, 3], &vec![Value::Int(1), Value::Int(3), Value::Int(3), Value::String("from".to_string())]);
        t.insert(&vec![0, 1, 2, 3], &vec![Value::Int(1), Value::Int(3), Value::Int(4), Value::String("seattle".to_string())]);
        println!("{:?}", t);
    }
}