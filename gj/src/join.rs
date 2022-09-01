use crate::{sql::Attribute, trie::*};

use std::{collections::HashMap, fmt::Debug};

// fn select<'a, T, F>(relations: &[&'a Trie<T>], f: &mut F, tuple: &mut Vec<&'a [T]>)
// where
//     T: Clone + Debug,
//     F: FnMut(&[&[T]]),
// {
//     if relations.is_empty() {
//         f(tuple)
//     } else {
//         for v in relations[0].get_data().unwrap() {
//             tuple.push(v);
//             select(&relations[1..], f, tuple);
//             tuple.pop();
//         }
//     }
// }

// pub fn join<T, F>(relations: &[&Table<T>], plan: &[Vec<usize>], payload: &[usize], f: &mut F)
// where
//     T: Clone + Debug,
//     F: FnMut(&[&[T]]),
// {
//     let js = &plan[0];

//     for j_min in js {
//         if let Tb::Arr((id_cols, data_cols)) = &relations[*j_min].data {
//             for i in 0..id_cols[0].len() {
//                 let mut trie_min = Trie::default();
//                 let ids: Vec<_> = id_cols.iter().map(|c| c[i].as_num()).collect();
//                 let data: Vec<_> = data_cols.iter().map(|c| c[i].clone()).collect();
//                 // TODO singleton compression
//                 trie_min.insert(&ids, data);
//                 let rels: Vec<_> = relations
//                     .iter()
//                     .map(|t| match &t.data {
//                         Tb::Arr(_) => &trie_min,
//                         Tb::Trie(trie) => trie,
//                     })
//                     .collect();
//                 join_inner(&rels, plan, payload, f);
//             }
//             return;
//         }
//     }
// }

// fn join_inner<T, F>(relations: &[&Trie<T>], plan: &[Vec<usize>], payload: &[usize], f: &mut F)
// where
//     T: Clone + Debug,
//     F: FnMut(&[&[T]]),
// {
//     if !plan.is_empty() {
//         let js = &plan[0];

//         let j_min = js
//             .iter()
//             .copied()
//             .min_by_key(|&j| relations[j].get_map().unwrap().len())
//             .unwrap();

//         for (id, trie_min) in relations[j_min].get_map().unwrap().iter() {
//             if let Some(tries) = js
//                 .iter()
//                 .filter(|&j| j != &j_min)
//                 .map(|&j| {
//                     relations[j]
//                         .get_map()
//                         .unwrap()
//                         .get(id)
//                         .map(|trie| (j, trie))
//                 })
//                 .collect::<Option<Vec<_>>>()
//             {
//                 let mut rels = relations.to_vec();
//                 rels[j_min] = trie_min;
//                 for (j, trie) in tries {
//                     rels[j] = trie;
//                 }
//                 join_inner(&rels, &plan[1..], payload, f);
//             }
//         }
//     } else {
//         let rels: Vec<_> = payload.iter().map(|&i| relations[i]).collect();
//         let mut tuple = Vec::new();
//         select(&rels, f, &mut tuple);
//     }
// }

pub fn bushy_join<'a>(
    tables: &[Tb<'a, '_, Value<'a>>],
    compiled_plan: &[Vec<usize>],
    tuple: &[i32],
    plan: &[Vec<&Attribute>],
    out_vars: &[Vec<Vec<Attribute>>],
    out: &mut HashMap<Attribute, usize>,
    view_len: usize,
    new_columns: &mut Vec<Vec<Value<'a>>>,
) {
    let j_min = tables.iter().position(|t| matches!(t, Tb::Arr(_))).unwrap();

    if let Tb::Arr((id_cols, data_cols)) = &tables[j_min] {
        for i in 0..id_cols[0].len() {
            let mut trie_min = Trie::default();
            let ids: Vec<_> = id_cols.iter().map(|c| c[i].as_num()).collect();
            let data: Vec<_> = data_cols.iter().map(|c| c[i].clone()).collect();
            // TODO singleton compression
            trie_min.insert(&ids, data);
            let rels: Vec<_> = tables
                .iter()
                .map(|t| match &t {
                    Tb::Arr(_) => &trie_min,
                    Tb::Trie(trie) => trie,
                })
                .collect();
            bushy_join_inner(
                &rels,
                compiled_plan,
                tuple,
                plan,
                out_vars,
                out,
                view_len,
                new_columns,
            );
        }
        return;
    }
}

pub fn bushy_join_inner<'a>(
    relations: &[&Trie<Value<'a>>],
    compiled_plan: &[Vec<usize>],
    tuple: &[i32],
    plan: &[Vec<&Attribute>],
    out_vars: &[Vec<Vec<Attribute>>],
    out: &mut HashMap<Attribute, usize>,
    view_len: usize,
    new_columns: &mut Vec<Vec<Value<'a>>>,
) {
    if compiled_plan.is_empty() {
        let payload = Vec::new();
        materialize(
            relations,
            tuple,
            plan,
            &payload,
            out_vars,
            out,
            view_len,
            new_columns,
        );
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
                t.push(*id);
                bushy_join_inner(
                    &rels,
                    &compiled_plan[1..],
                    &t[..],
                    plan,
                    out_vars,
                    out,
                    view_len,
                    new_columns,
                );
            }
        }
    }
}

fn materialize<'a>(
    relations: &[&Trie<Value<'a>>],
    tuple: &[i32],
    plan: &[Vec<&Attribute>],
    payload: &[&[Value<'a>]],
    out_vars: &[Vec<Vec<Attribute>>],
    out: &mut HashMap<Attribute, usize>,
    view_len: usize,
    new_columns: &mut Vec<Vec<Value<'a>>>,
) {
    if relations.is_empty() {
        for (id, attrs) in tuple.iter().zip(plan.iter()) {
            // let idx = out
            //     .get(attrs[0]).copied()
            //     .unwrap_or_else(|| {
            //         let l = view_len + new_columns.len();
            //         out.insert(attrs[0].clone(), l);
            //         l
            //     }) - view_len;

            let idx = *out
                .entry(attrs[0].clone())
                .or_insert(view_len + new_columns.len())
                - view_len;

            if idx == new_columns.len() {
                new_columns.push(Vec::new());
            }
            new_columns[idx].push(Value::Num(*id));
            for a in attrs {
                out.insert((*a).clone(), idx + view_len);
            }
        }
        for (vals, attrs) in payload.iter().zip(out_vars.iter()) {
            for (a, v) in attrs.iter().zip(vals.iter()) {
                let idx = *out
                    .entry(a[0].clone())
                    .or_insert_with(|| view_len + new_columns.len())
                    - view_len;
                if idx == new_columns.len() {
                    new_columns.push(Vec::new());
                }
                new_columns[idx].push((*v).clone());
                for a in &a[1..] {
                    out.insert(a.clone(), idx + view_len);
                }
            }
        }
    } else {
        let mut p = payload.to_vec();

        if relations[0].get_data().unwrap().is_empty() {
            materialize(
                &relations[1..],
                tuple,
                plan,
                &p,
                out_vars,
                out,
                view_len,
                new_columns,
            );
        } else {
            for vs in relations[0].get_data().unwrap() {
                p.push(vs);
                materialize(
                    &relations[1..],
                    tuple,
                    plan,
                    &p,
                    out_vars,
                    out,
                    view_len,
                    new_columns,
                );
                p.pop();
            }
        }
    }
}

// pub fn bushy_join<'a>(
//     tables: &[Tb<'a, Value<'a>>],
//     compiled_plan: &[Vec<usize>],
//     tuple: &mut Vec<Value<'a>>,
//     out: &mut Vec<Vec<Value<'a>>>,
// ) {
//     let js = &compiled_plan[0];

//     for j_min in js {
//         if let Tb::Arr((id_cols, data_cols)) = &tables[*j_min] {
//             for i in 0..id_cols[0].len() {
//                 let mut trie_min = Trie::default();
//                 let ids: Vec<_> = id_cols.iter().map(|c| c[i].as_num()).collect();
//                 let data: Vec<_> = data_cols.iter().map(|c| c[i].clone()).collect();
//                 trie_min.insert(&ids, data);
//                 let rels: Vec<_> = tables
//                     .iter()
//                     .map(|t| match &t {
//                         Tb::Arr(_) => &trie_min,
//                         Tb::Trie(trie) => trie,
//                     })
//                     .collect();
//                 bushy_join_inner(&rels, compiled_plan, tuple, out);
//             }
//             return;
//         }
//     }
// }

// pub fn bushy_join_inner<'a>(
//     relations: &[&Trie<Value<'a>>],
//     compiled_plan: &[Vec<usize>],
//     tuple: &mut Vec<Value<'a>>,
//     out: &mut Vec<Vec<Value<'a>>>,
// ) {
//     if compiled_plan.is_empty() {
//         materialize(relations, tuple, out);
//     } else {
//         let js = &compiled_plan[0];

//         let j_min = js
//             .iter()
//             .copied()
//             .min_by_key(|&j| relations[j].get_map().unwrap().len())
//             .unwrap();

//         for (id, trie_min) in relations[j_min].get_map().unwrap().iter() {
//             if let Some(tries) = js
//                 .iter()
//                 .filter(|&j| j != &j_min)
//                 .map(|&j| {
//                     relations[j]
//                         .get_map()
//                         .unwrap()
//                         .get(id)
//                         .map(|trie| (j, trie))
//                 })
//                 .collect::<Option<Vec<_>>>()
//             {
//                 let mut rels = relations.to_vec();
//                 rels[j_min] = trie_min;
//                 for (j, trie) in tries {
//                     rels[j] = trie;
//                 }
//                 tuple.push(Value::Num(*id));
//                 bushy_join_inner(&rels, &compiled_plan[1..], tuple, out);
//                 tuple.pop();
//             }
//         }
//     }
// }

// fn materialize<'a>(
//     relations: &[&Trie<Value<'a>>],
//     tuple: &mut Vec<Value<'a>>,
//     out: &mut Vec<Vec<Value<'a>>>,
// ) {
//     if relations.is_empty() {
//         out.push(tuple.to_vec());

//     } else if relations[0].get_data().unwrap().is_empty() {
//         materialize(&relations[1..], tuple, out);
//     } else {
//         for vs in relations[0].get_data().unwrap() {
//             for v in vs { tuple.push(v.clone()); }
//             materialize(&relations[1..], tuple, out);
//             for _v in vs { tuple.pop(); }
//         }
//     }
// }
