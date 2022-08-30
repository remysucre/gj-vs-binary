use crate::{sql::Attribute, trie::*, Relation};

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

pub fn join<T, F>(relations: &[&Table<T>], plan: &[Vec<usize>], payload: &[usize], f: &mut F)
where
    T: Clone + Debug,
    F: FnMut(&[&[T]]),
{
    let js = &plan[0];

    for j_min in js {
        if let Tb::Arr((id_cols, data_cols)) = &relations[*j_min].data {
            for i in 0..id_cols[0].len() {
                let mut trie_min = Trie::default();
                let ids: Vec<_> = id_cols.iter().map(|c| c[i].as_num()).collect();
                let data: Vec<_> = data_cols.iter().map(|c| c[i].clone()).collect();
                // TODO singleton compression
                trie_min.insert(&ids, data);
                let rels: Vec<_> = relations
                    .iter()
                    .map(|t| match &t.data {
                        Tb::Arr(_) => &trie_min,
                        Tb::Trie(trie) => trie,
                    })
                    .collect();
                join_inner(&rels, plan, payload, f);
            }
            return;
        }
    }
}

fn join_inner<T, F>(relations: &[&Trie<T>], plan: &[Vec<usize>], payload: &[usize], f: &mut F)
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
                join_inner(&rels, &plan[1..], payload, f);
            }
        }
    } else {
        let rels: Vec<_> = payload.iter().map(|&i| relations[i]).collect();
        let mut tuple = Vec::new();
        select(&rels, f, &mut tuple);
    }
}

pub fn bushy_join_inner(
    relations: &[&Trie<Value>],
    compiled_plan: &[Vec<usize>],
    tuple: &[Value],
    plan: &[Vec<&Attribute>],
    out_vars: &[Vec<Attribute>],
    out: &mut Relation,
) {
    if compiled_plan.is_empty() {
        let payload = Vec::new();
        materialize(relations, tuple, plan, &payload, out_vars, out);
    } else {
        let js = &compiled_plan[0];

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
                let mut t = tuple.to_vec();
                t.push(Value::Num(*id));
                bushy_join_inner(&rels, &compiled_plan[1..], &t[..], plan, out_vars, out);
            }
        }
    }
}

fn materialize(
    relations: &[&Trie<Value>],
    tuple: &[Value],
    plan: &[Vec<&Attribute>],
    payload: &[&[Value]],
    out_vars: &[Vec<Attribute>],
    out: &mut Relation,
) {
    if relations.is_empty() {
        unimplemented!()
    } else {
        let mut p = payload.to_vec();
        for vs in relations[0].get_data().unwrap() {
            p.push(vs);
            materialize(relations, tuple, plan, &p, out_vars, out);
            p.pop();
        }
    }
}
