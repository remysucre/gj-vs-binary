use std::fmt::Debug;
use std::collections::HashMap;

#[derive(Debug, Clone)]
enum Trie<T> {
    Node(HashMap<usize, Self>),
    Data(Vec<Vec<T>>),
}

impl<T> Default for Trie<T> {
    fn default() -> Self {
        Trie::Node(HashMap::new())
    }
}

impl<T> Trie<T> {
    fn len(&self) -> usize {
        self.get_map().len()
    }

    fn get_map(&self) -> &HashMap<usize, Self> {
        if let Trie::Node(ref map) = *self {
            map
        } else {
            panic!("get_map() called on a Trie::Data");
        }
    }

    fn get_map_mut(&mut self) -> &mut HashMap<usize, Self> {
        if let Trie::Node(ref mut map) = *self {
            map
        } else {
            panic!("get_map_mut() called on a Trie::Data");
        }
    }

    fn get_data(&self) -> &[Vec<T>] {
        if let Trie::Data(ref data) = *self {
            data
        } else {
            panic!("get_data() called on a Trie::Node");
        }
    }

    fn get_data_mut(&mut self) -> &mut Vec<Vec<T>> {
        if let Trie::Data(ref mut data) = *self {
            data
        } else {
            panic!("get_data_mut() called on a Trie::Node");
        }
    }

    fn insert(&mut self, ids: &[usize], data: Vec<T> ) {
        let mut trie = self;
        for id in &ids[ ..ids.len()-1] {
            trie = trie.get_map_mut().entry(*id).or_default();
        }

        let d = trie
            .get_map_mut()
            .entry(*ids.last().unwrap())
            .or_insert_with(|| Trie::Data(vec![]));
        d.get_data_mut().push(data);
    }
}

fn join<T, F>(
    relations: &[&Trie<T>], 
    plan: &[Vec<usize>], 
    f: &mut F, 
) where
    T: Clone + Debug,
    F: FnMut(&[&[T]]),
{
    if plan.is_empty() {
        let mut payload = Vec::new();

        for relation in relations {
            for data in relation.get_data() {
                payload.push(&data[..]);
            }
        }
        f(&payload);
    }
    
    let js = &plan[0];

    let j_min = js
        .iter()
        .copied()
        .min_by_key(|&j| relations[j].len())
        .unwrap();
    
    let mut intersection: Vec<_> = relations[j_min].get_map().keys().copied().collect();

    for &j in js {
        if j != j_min {
            let rj = relations[j].get_map();
            intersection.retain(|i| rj.contains_key(i));
        }
    }

    let empty = Trie::default();

    for id in intersection {
        let mut rels = Vec::new();
        for (i, r) in relations.iter().enumerate() {
            if js.contains(&i) {
                rels.push(r.get_map().get(&id).unwrap_or(&empty));
            } else {
                rels.push(r);
            }
        }
        join(&rels, &plan[1..], f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trie() {
        let mut t = Trie::default();
        t.insert(&vec![1, 2, 3], vec!["hello"]);
        t.insert(&vec![1, 2, 3], vec!["world"]);
        t.insert(&vec![1, 3, 3], vec!["from"]);
        t.insert(&vec![1, 3, 4], vec!["seattle"]);
        t.insert(&vec![1, 3, 5], vec!["and", "washinton"]);
        println!("{:?}", t);
    }
}