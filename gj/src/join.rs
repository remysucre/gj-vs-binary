use rustc_hash::{FxHashMap, FxHashSet};

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

pub struct Tab<'a, T> {
    pub schema: &'a Schema<'a>,
    pub table: &'a Table<'a, T>,
    pub rel: &'a mut Relation,
    pub ids: &'a mut Vec<i32>,
}

pub fn sj(relations: &mut [&mut FlatRelation], plan: &[Vec<usize>], shuffles: &[&[usize]]) -> bool {
    if plan.is_empty() {
        true
    } else {
        let js = &plan[0];

        let mut maps: Vec<_> = js
            .iter()
            .map(|&j| {
                let mut map = FxHashMap::default();
                for (ids, data) in relations[j].iter() {
                    map.entry(ids[0])
                        .or_insert(vec![])
                        .push((ids[1..].to_vec(), data.to_vec()));
                }
                map
            })
            .collect();

        let map_min = maps.iter().min_by_key(|m| m.keys().len()).unwrap();

        // TODO just iterate over map_min.keys()
        let intersection: FxHashSet<_> = map_min
            .keys()
            .copied()
            .filter(|id| maps.iter().all(|m| m.contains_key(id)))
            .collect();

            
        let mut success = false;
        
        let mut inter = FxHashSet::default();
        
        for id in intersection {
            let mut rels: Vec<_> = relations.iter_mut().map(|r| &mut (**r)).collect();
            for (j, map) in js.iter().zip(maps.iter_mut()) {
                rels[*j] = map.get_mut(&id).unwrap();
            }
            
            let mut new_shuffles = shuffles.to_vec();
            
            for j in js {
                new_shuffles[*j] = &shuffles[*j][1..];
            }
            
            let keep = sj(&mut rels, &plan[1..], &new_shuffles[..]);
            if keep {
                // println!("KEEPING");
                success = true;
                inter.insert(id);
            }
        }

        // println!("INTER SIZE {:?}", inter.len());
            
        for map in &mut maps {
            map.retain(|id, _| inter.contains(id));
        }

        for (j, map) in js.iter().zip(maps.iter()) {
            let rel = &mut relations[*j];
            **rel = vec![];
            for (id, r) in map.iter() {
                for (ids, data) in r {
                    let mut new_ids = ids.to_vec();
                    new_ids.insert(shuffles[*j][0], *id);
                    rel.push((new_ids, data.to_vec()));
                }
            }
        }

        success
    }
}

pub fn semijoin(relations: &mut [Tab<Value>], plan: &[Vec<usize>]) {
    let js = &plan[0];

    for j_min in js {
        if let Tb::Arr((id_cols, data_cols)) = &relations[*j_min].table.data {
            for i in 0..id_cols[0].len() {
                let mut trie_min = Trie::default();
                let ids: Vec<_> = id_cols.iter().map(|c| c[i].as_num()).collect();
                let data: Vec<_> = data_cols.iter().map(|c| c[i].clone()).collect();
                // TODO singleton compression
                trie_min.insert(&ids, data);
                let mut rels: Vec<_> = relations
                    .iter_mut()
                    .map(|t| match &t.table.data {
                        Tb::Arr(_) => Rel {
                            vars: &t.schema.1,
                            trie: &trie_min,
                            rel: t.rel,
                            ids: t.ids,
                        },
                        Tb::Trie(trie) => Rel {
                            vars: &t.schema.1,
                            trie,
                            rel: t.rel,
                            ids: t.ids,
                        },
                    })
                    .collect();
                semijoin_inner(&mut rels, plan);
            }
            return;
        }
    }
}

pub struct Rel<'a, T> {
    vars: &'a [&'a str],
    trie: &'a Trie<T>,
    rel: &'a mut Relation,
    ids: &'a mut Vec<i32>,
}

fn semijoin_inner(relations: &mut [Rel<Value>], plan: &[Vec<usize>]) {
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
                    relations[j]
                        .trie
                        .get_map()
                        .unwrap()
                        .get(id)
                        .map(|trie| (j, trie))
                })
                .collect::<Option<Vec<_>>>()
            {
                let mut rels: Vec<_> = relations
                    .iter_mut()
                    .map(|r| Rel {
                        vars: r.vars,
                        trie: r.trie,
                        rel: r.rel,
                        ids: r.ids,
                    })
                    .collect();

                rels[j_min].trie = trie_min;
                rels[j_min].ids.push(*id);
                for (j, trie) in &tries {
                    rels[*j].trie = trie;
                    rels[*j].ids.push(*id);
                }

                semijoin_inner(&mut rels, &plan[1..]);
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

fn reduce(relations: &mut [Rel<Value>]) {
    for relation in relations {
        if let Trie::Data(data) = relation.trie {
            if data.is_empty() {
                for (i, id) in relation.ids.iter().enumerate() {
                    // TODO only materialize needed columns
                    let col = relation
                        .rel
                        .entry(relation.vars[i].to_string())
                        .or_default();
                    col.push(Value::Num(*id));
                }
            } else {
                for tuple in data {
                    for (i, id) in relation.ids.iter().enumerate() {
                        // TODO only materialize needed columns
                        let col = relation
                            .rel
                            .entry(relation.vars[i].to_string())
                            .or_default();
                        col.push(Value::Num(*id));
                    }

                    for (i, v) in tuple.iter().enumerate() {
                        let col = relation
                            .rel
                            .entry(relation.vars[i + relation.ids.len()].to_string())
                            .or_default();
                        col.push(v.clone());
                    }
                }
            }
        } else {
            unreachable!()
        }
    }
}
