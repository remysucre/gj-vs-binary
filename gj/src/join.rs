use bumpalo::Bump;

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
        for v in relations[0].get_data().unwrap() {
            tuple.push(v);
            select(&relations[1..], f, tuple);
            tuple.pop();
        }
    }
}

pub fn fj<T, F>(relations: &[&Table<T>], plan: &[Vec<usize>], payload: &[usize], f: &mut F)
where
    T: Clone + Debug,
    F: FnMut(&[&[T]]),
{
    if !plan.is_empty() {
        let js = &plan[0];

        if let Some(j_min) = js.iter().find(|&&j| matches!(relations[j], Table::Arr(_))) {
            if let Table::Arr((id_cols, data_cols)) = relations[*j_min] {
                for (i, id) in id_cols[0].iter().enumerate() {
                    if let Some(tries) = js
                        .iter()
                        .filter(|j| j != &j_min)
                        .map(|&j| {
                            relations[j]
                                .get_map()
                                .unwrap()
                                .get(id)
                                .map(|trie| (j, trie))
                        })
                        .collect::<Option<Vec<_>>>()
                    {
                        // TODO singleton compression
                        let a = Bump::new();
                        let mut trie_min = Trie::new_in(&a);
                        let ids: Vec<_> = id_cols[1..].iter().map(|c| c[i]).collect();
                        let data: Vec<_> = data_cols.iter().map(|c| c[i].clone()).collect();
                        trie_min.insert(&a, &ids, data);

                        let mut rels: Vec<_> = relations
                            .iter()
                            .map(|t| match t {
                                Table::Arr(_) => &trie_min,
                                Table::Trie(trie) => trie,
                            })
                            .collect();

                        for (j, trie) in tries.iter() {
                            rels[*j] = trie;
                        }
                        join(&rels, &plan[1..], payload, f);
                    }
                }
            } else {
                unreachable!()
            }
        } else {
            let rels: Vec<_> = relations
                .iter()
                .map(|t| match t {
                    Table::Arr(_) => unreachable!(),
                    Table::Trie(trie) => trie,
                })
                .collect();
            join(&rels, plan, payload, f);
        }
    } else {
        unreachable!()
    }
}

pub fn join<T, F>(relations: &[&Trie<T>], plan: &[Vec<usize>], payload: &[usize], f: &mut F)
where
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

        for (id, trie_min) in relations[j_min].get_map().unwrap().iter() {
            if let Some(tries) = js
                .iter()
                .filter(|&j| j != &j_min)
                .map(|&j| {
                    relations[j]
                        .get_map()
                        .unwrap()
                        .get(id)
                        .map(|trie| (j, trie))
                })
                .collect::<Option<Vec<_>>>()
            {
                let mut rels = relations.to_vec();
                rels[j_min] = trie_min;
                for (j, trie) in tries {
                    rels[j] = trie;
                }
                join(&rels, &plan[1..], payload, f);
            }
        }
    } else {
        // TODO refactor payload to point to relation directly
        let relations = relations
            .iter()
            .copied()
            .filter(|trie| !trie.get_data().unwrap().is_empty())
            .collect::<Vec<_>>();
        let rels: Vec<_> = payload.iter().map(|&i| relations[i]).collect();
        let mut tuple = Vec::new();
        select(&rels, f, &mut tuple);
    }
}
