use std::fmt::Debug;
use std::collections::HashMap;

#[derive(Debug, Clone)]
enum Trie<T> {
    Node(HashMap<usize, Self>),
    Data(Option<Vec<T>>),
}

impl<T> Default for Trie<T> {
    fn default() -> Self {
        Trie::Node(HashMap::new())
    }
}

impl<T> Trie<T> {
    fn insert(&mut self, ids: &[usize], data: Option<T> ) {
        let mut trie = self;
        for (i, id) in ids.iter().enumerate() {
            if let Trie::Node(node) = trie {
                trie = node.entry(*id).or_insert_with(|| {
                    if i == ids.len() - 1 {
                        Trie::Data(data.as_ref().map(|_| vec![]))
                    } else {
                        Trie::default()
                    }
                });
            } else {
                panic!("Trie::insert: tuple longer than trie depth");
            }
        }
        if let Trie::Data(Some(d)) = trie {
            if let Some(v) = data {
                d.push(v);
            } else {
                panic!("Missing data");
            }
        } else {
            assert!(data.is_none());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trie() {
        let mut t = Trie::default();
        t.insert(&vec![1, 2, 3], Some(vec!["hello"]));
        t.insert(&vec![1, 2, 3], Some(vec!["world"]));
        t.insert(&vec![1, 3, 3], Some(vec!["from"]));
        t.insert(&vec![1, 3, 4], Some(vec!["seattle"]));
        t.insert(&vec![1, 3, 5], Some(vec!["and", "washinton"]));
        println!("{:?}", t);
    }
}