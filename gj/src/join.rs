use crate::trie::*;

use std::fmt::Debug;

pub fn join<'a, T, F>(
    relations: &[&'a Trie<T>],
    plan: &[Vec<usize>],
    f: &mut F,
    tuple: &mut Vec<&'a [T]>,
    empty: &'a Trie<T>,
) where
    T: Clone + Debug,
    F: FnMut(&[&[T]]),
{
    if plan.is_empty() {
        if relations.is_empty() {
            f(tuple)
        } else {
            let rels = &relations[1..];
            if relations[0].get_data().is_empty() {
                join(rels, plan, f, tuple, empty);
            } else {
                for v in relations[0].get_data() {
                    tuple.push(v);
                    join(rels, plan, f, tuple, empty);
                    tuple.pop();
                }
            }
        }
    } else {
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

        for id in intersection {
            let mut rels = Vec::new();
            for (i, r) in relations.iter().enumerate() {
                if js.contains(&i) {
                    rels.push(r.get_map().get(&id).unwrap_or(empty));
                } else {
                    rels.push(r);
                }
            }
            join(&rels, &plan[1..], f, tuple, empty);
        }
    }
}
