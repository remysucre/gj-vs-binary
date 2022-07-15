use crate::trie::*;

use std::fmt::Debug;

fn select<'a, T, F>(relations: &[&'a Trie<T>], f: &mut F, tuple: &mut Vec<&'a [T]>)
where
    T: Clone + Debug,
    F: FnMut(&[&[T]]),
{
    if relations.is_empty() {
        f(tuple)
    } else {
        let rels = &relations[1..];
        for v in relations[0].get_data().unwrap() {
            tuple.push(v);
            select(rels, f, tuple);
            tuple.pop();
        }
    }
}

pub fn join<'a, T, F>(
    relations: &[&'a Trie<T>],
    plan: &[Vec<usize>],
    payload: &[usize],
    f: &mut F,
    empty: &'a Trie<T>,
) where
    T: Clone + Debug,
    F: FnMut(&[&[T]]),
{
    if !plan.is_empty() {
        let js = &plan[0];

        let j_min = js
            .iter()
            .copied()
            .min_by_key(|&j| relations[j].get_map().unwrap().len())
            .unwrap();

        let mut intersection: Vec<_> = relations[j_min]
            .get_map()
            .unwrap()
            .keys()
            .copied()
            .collect();

        for &j in js {
            if j != j_min {
                let rj = relations[j].get_map();
                intersection.retain(|i| rj.unwrap().contains_key(i));
            }
        }

        for id in intersection {
            let mut rels = Vec::new();
            for (i, r) in relations.iter().enumerate() {
                if js.contains(&i) {
                    rels.push(r.get_map().unwrap().get(&id).unwrap_or(empty));
                } else {
                    rels.push(r);
                }
            }
            join(&rels, &plan[1..], payload, f, empty);
        }
    } else {
        let rels: Vec<_> = payload.iter().map(|&i| relations[i]).collect();
        let mut tuple = Vec::new();
        select(&rels, f, &mut tuple);
    }
}
