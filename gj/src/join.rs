use crate::{trie::*, Relation};

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
                                .get(&id.as_num())
                                .map(|trie| (j, trie))
                        })
                        .collect::<Option<Vec<_>>>()
                    {
                        // TODO singleton compression
                        let mut trie_min = Trie::default();
                        let ids: Vec<_> = id_cols[1..].iter().map(|c| c[i].as_num()).collect();
                        let data: Vec<_> = data_cols.iter().map(|c| c[i].clone()).collect();
                        trie_min.insert(&ids, data);

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
                        join_inner(&rels, &plan[1..], payload, f);
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
            join_inner(&rels, plan, payload, f);
        }
    } else {
        unreachable!()
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

pub struct Tab<'a, T> {
    vars: &'a [String],
    table: &'a Table<'a, T>,
    rel: &'a mut Relation,
    ids: &'a mut Vec<i32>,
}

pub fn semijoin(relations: &mut [&mut Tab<Value>], plan: &[Vec<usize>]) {
    if !plan.is_empty() {
        let js = &plan[0];

        if let Some(j_min) = js.iter().find(|&&j| matches!(relations[j].table, Table::Arr(_))) {
            if let Table::Arr((id_cols, data_cols)) = relations[*j_min].table {
                for (i, id) in id_cols[0].iter().enumerate() {
                    if let Some(tries) = js
                        .iter()
                        .filter(|j| j != &j_min)
                        .map(|&j| {
                            relations[j].table
                                .get_map()
                                .unwrap()
                                .get(&id.as_num())
                                .map(|trie| (j, trie))
                        })
                        .collect::<Option<Vec<_>>>()
                    {
                        // TODO singleton compression
                        let mut trie_min = Trie::default();
                        let ids: Vec<_> = id_cols[1..].iter().map(|c| c[i].as_num()).collect();
                        let data: Vec<_> = data_cols.iter().map(|c| c[i].clone()).collect();
                        trie_min.insert(&ids, data);

                        let mut rels: Vec<_> = relations
                            .iter_mut()
                            .map(|t| match t.table {
                                Table::Arr(_) => 
                                Rel {
                                    vars: t.vars,
                                    trie: &trie_min,
                                    rel: t.rel,
                                    ids: t.ids,
                                },
                                Table::Trie(trie) => 
                                Rel {
                                    vars: t.vars,
                                    trie,
                                    rel: t.rel,
                                    ids: t.ids,
                                },
                            })
                            .collect();

                        rels[*j_min].ids.push(id.as_num());
                        for (j, trie) in tries.iter() {
                            rels[*j].trie = trie;
                            rels[*j].ids.push(id.as_num());
                        }
                        let mut rel_refs: Vec<_> = rels.iter_mut().collect();
                        semijoin_inner(&mut rel_refs, &plan[1..]);
                        rels[*j_min].ids.pop();
                        for (j, _) in tries.iter() {
                            rels[*j].ids.pop();
                        }
                    }
                }
            } else {
                unreachable!()
            }
        } else {
            let mut rels: Vec<_> = relations
                .iter_mut()
                .map(|t| match t.table {
                    Table::Arr(_) => unreachable!(),
                    Table::Trie(trie) => 
                    Rel {
                        vars: t.vars,
                        trie,
                        rel: t.rel,
                        ids: t.ids,
                    },
                })
                .collect();
            let mut rel_refs: Vec<_> = rels.iter_mut().collect();
            semijoin_inner(&mut rel_refs[..], plan);
        }
    } else {
        unreachable!()
    }
}

pub struct Rel<'a, T> {
    vars: &'a [String],
    trie: &'a Trie<T>,
    rel: &'a mut Relation,
    ids: &'a mut Vec<i32>,
}

fn semijoin_inner(relations: &mut [&mut Rel<Value>], plan: &[Vec<usize>])
{
    if !plan.is_empty() {
        let js = &plan[0];

        let j_min = js
            .iter()
            .copied()
            .min_by_key(|&j| relations[j].trie.get_map().unwrap().len())
            .unwrap();

        for (id, trie_min) in relations[j_min].trie.get_map().unwrap().iter() {
            if let Some(tries) = js
                .iter()
                .filter(|&j| j != &j_min)
                .map(|&j| {
                    relations[j].trie
                        .get_map()
                        .unwrap()
                        .get(id)
                        .map(|trie| (j, trie))
                })
                .collect::<Option<Vec<_>>>()
            {
                let mut rels = relations.iter_mut().map(|r| {
                    Rel {
                        vars: r.vars,
                        trie: r.trie,
                        rel: r.rel,
                        ids: r.ids,
                    }
                }).collect::<Vec<_>>();
                
                rels[j_min].trie = trie_min;
                rels[j_min].ids.push(*id);
                for (j, trie) in &tries {
                    rels[*j].trie = trie;
                    rels[*j].ids.push(*id);
                }
                let mut rs: Vec<_> = rels.iter_mut().collect();
                semijoin_inner(&mut rs[..], &plan[1..]);
                rels[j_min].ids.pop();
                for (j, _) in &tries {
                    rels[*j].ids.pop();
                }
            }
        }
    } else {
        reduce(relations);
    }
}

fn reduce<'a>(relations: &mut [&'a mut Rel<Value>])
where
{
    for relation in relations {
        for (i, id) in relation.ids.iter().enumerate() {
            let col = relation
                .rel
                .entry(relation.vars[i].clone())
                .or_default();
            col.push(Value::Num(*id));
        }
        if let Trie::Data(data) = relation.trie {
            for tuple in data {
                for (i, v) in tuple.iter().enumerate() {
                    let col = relation
                        .rel
                        .entry(relation.vars[i+relation.ids.len()].clone())
                        .or_default();
                    col.push(v.clone());
                }
            }
        } else {
            unreachable!()
        }
    }
}