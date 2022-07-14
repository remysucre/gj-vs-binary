use std::fmt::Debug;
use std::collections::HashMap;

use itertools::Itertools;

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

    pub fn insert(&mut self, ids: &[Id], data: Vec<T> ) {
        let mut trie = self;
        for id in &ids[ ..ids.len()-1] {
            trie = trie.get_map_mut().entry(*id).or_default();
        }

        let d = trie
            .get_map_mut()
            .entry(*ids.last().unwrap())
            .or_insert_with(|| Trie::Data(vec![]));
        if !data.is_empty() {
            d.get_data_mut().push(data);
        }
        // d.get_data_mut().push(data);
    }
}

pub fn join<T, F>(
    relations: &[&Trie<T>], 
    plan: &[Vec<usize>], 
    f: &mut F, 
) where
    T: Clone + Debug,
    F: FnMut(&[&[T]]),
{
    if plan.is_empty() {
        relations
            .iter()
            .map(|r| r.get_data())
            .filter(|r| !r.is_empty())
            .multi_cartesian_product()
            .for_each(|row| {
                let payload: Vec<_> = row.iter().map(|r| &r[..]).collect();
                f(&payload[..]);
            });
        return;
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

        for n in 1..10 {

            let mut relations = vec![];

            for _ in 0..3 {
                let mut r: Trie<()> = Trie::default();
                
                r.insert(&[0, 0], vec![()]);
                for i in 1..n {
                    r.insert(&[0, i], vec![()]);
                    r.insert(&[i, 0], vec![()]);
                }

                relations.push(r);
            }
    
            let tries: Vec<&Trie<_>> = relations.iter().collect();

            let mut result = 0;

            join(
                &tries,
                &[vec![0, 1], vec![1, 2], vec![0, 2]], 
                &mut |_: &[&[()]]| { result += 1; }
            );
    
            assert_eq!(result, 3*n-2);
        }

    }
}