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

// pub fn bushy_join<'a>(
//     tables: &'a [Tb<'a, Value>],
//     compiled_plan: &[Vec<usize>],
//     tuple: &[i32],
//     plan: &[Vec<&Attribute>],
//     o: OutInfo<'a>,
// ) {
//     let j_min = tables.iter().position(|t| matches!(t, Tb::Arr(_))).unwrap();

//     if let Tb::Arr((id_cols, data_cols)) = &tables[j_min] {
//         for i in 0..id_cols[0].len() {
//             let mut trie_min = Trie::default();
//             let ids: Vec<_> = id_cols.iter().map(|c| c[i].as_num()).collect();
//             let data: Vec<_> = data_cols.iter().map(|c| &c[i]).collect();
//             // TODO singleton compression
//             trie_min.insert(&ids, data);
//             let rels: Vec<_> = tables
//                 .iter()
//                 .map(|t| match &t {
//                     Tb::Arr(_) => &trie_min,
//                     Tb::Trie(trie) => trie,
//                 })
//                 .collect();
//             bushy_join_inner(
//                 &rels,
//                 compiled_plan,
//                 tuple,
//                 plan,
//                 OutInfo {
//                     out_vars: o.out_vars,
//                     out: o.out,
//                     view_len: o.view_len,
//                     new_columns: o.new_columns,
//                 },
//             );
//         }
//         return;
//     }
// }

// pub struct OutInfo<'a> {
//     pub out_vars: &'a [Vec<Vec<Attribute>>],
//     pub out: &'a mut HashMap<Attribute, usize>,
//     pub view_len: usize,
//     pub new_columns: &'a mut Vec<Vec<ValRef<'a>>>,
// }

// pub fn bushy_join_inner<'a>(
//     relations: &[&'a Trie<Value>],
//     compiled_plan: &[Vec<usize>],
//     tuple: &[i32],
//     plan: &[Vec<&Attribute>],
//     o: OutInfo<'a>,
// ) {
//     if compiled_plan.is_empty() {
//         let payload = Vec::new();
//         materialize(relations, tuple, plan, &payload, o);
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
//                 let mut t = tuple.to_vec();
//                 t.push(*id);
//                 bushy_join_inner(
//                     &rels,
//                     &compiled_plan[1..],
//                     &t[..],
//                     plan,
//                     OutInfo {
//                         out_vars: o.out_vars,
//                         out: o.out,
//                         view_len: o.view_len,
//                         new_columns: o.new_columns,
//                     },
//                 );
//             }
//         }
//     }
// }

// fn materialize<'a>(
//     relations: &[&'a Trie<Value>],
//     tuple: &[i32],
//     plan: &[Vec<&Attribute>],
//     payload: &[&[&'a Value]],
//     o: OutInfo<'a>,
// ) {
//     if relations.is_empty() {
//         for (id, attrs) in tuple.iter().zip(plan.iter()) {
//             let idx = *o
//                 .out
//                 .entry(attrs[0].clone())
//                 .or_insert_with(|| o.view_len + o.new_columns.len())
//                 - o.view_len;
//             if idx == o.new_columns.len() {
//                 o.new_columns.push(Vec::new());
//             }
//             o.new_columns[idx].push(ValRef::Id(*id));
//             for a in attrs {
//                 o.out.insert((*a).clone(), idx + o.view_len);
//             }
//         }
//         for (vals, attrs) in payload.iter().zip(o.out_vars.iter()) {
//             for (a, v) in attrs.iter().zip(vals.iter()) {
//                 let idx = *o
//                     .out
//                     .entry(a[0].clone())
//                     .or_insert_with(|| o.view_len + o.new_columns.len())
//                     - o.view_len;
//                 if idx == o.new_columns.len() {
//                     o.new_columns.push(Vec::new());
//                 }
//                 o.new_columns[idx].push(ValRef::Val(v));
//                 for a in &a[1..] {
//                     o.out.insert(a.clone(), idx + o.view_len);
//                 }
//             }
//         }
//     } else {
//         let mut p = payload.to_vec();

//         if relations[0].get_data().unwrap().is_empty() {
//             materialize(
//                 &relations[1..],
//                 tuple,
//                 plan,
//                 &p,
//                 OutInfo {
//                     out_vars: o.out_vars,
//                     out: o.out,
//                     view_len: o.view_len,
//                     new_columns: o.new_columns,
//                 },
//             );
//         } else {
//             for vs in relations[0].get_data().unwrap() {
//                 p.push(vs);
//                 materialize(
//                     &relations[1..],
//                     tuple,
//                     plan,
//                     &p,
//                     OutInfo {
//                         out_vars: o.out_vars,
//                         out: o.out,
//                         view_len: o.view_len,
//                         new_columns: o.new_columns,
//                     },
//                 );
//                 p.pop();
//             }
//         }
//     }
// }

pub fn bushy_join<'a>(
    tables: &[Tb<'a, Value>],
    compiled_plan: &[Vec<usize>],
    tuple: &mut Vec<ValRef<'a>>,
    out: &mut Vec<Vec<ValRef<'a>>>,
) {
    let js = &compiled_plan[0];

    for j_min in js {
        if let Tb::Arr((id_cols, data_cols)) = &tables[*j_min] {
            for i in 0..id_cols[0].len() {
                let mut trie_min = Trie::default();
                let ids: Vec<_> = id_cols.iter().map(|c| c[i].as_num()).collect();
                let data: Vec<_> = data_cols.iter().map(|c| &c[i]).collect();
                trie_min.insert(&ids, data);
                let rels: Vec<_> = tables
                    .iter()
                    .map(|t| match &t {
                        Tb::Arr(_) => &trie_min,
                        Tb::Trie(trie) => trie,
                    })
                    .collect();
                bushy_join_inner(&rels, compiled_plan, tuple, out);
            }
            return;
        }
    }
}

pub fn bushy_join_inner<'a>(
    relations: &[&Trie<'a, Value>],
    compiled_plan: &[Vec<usize>],
    tuple: &mut Vec<ValRef<'a>>,
    out: &mut Vec<Vec<ValRef<'a>>>,
) {
    if compiled_plan.is_empty() {
        materialize(relations, tuple, out);
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
                tuple.push(ValRef::Id(*id));
                bushy_join_inner(&rels, &compiled_plan[1..], tuple, out);
                tuple.pop();
            }
        }
    }
}

fn materialize<'a>(
    relations: &[&Trie<'a, Value>],
    tuple: &mut Vec<ValRef<'a>>,
    out: &mut Vec<Vec<ValRef<'a>>>,
) {
    if relations.is_empty() {
        out.push(tuple.to_vec());

    } else if relations[0].get_data().unwrap().is_empty() {
        materialize(&relations[1..], tuple, out);
    } else {
        for vs in relations[0].get_data().unwrap() {
            for v in vs { tuple.push(ValRef::Val(v)); }
            materialize(&relations[1..], tuple, out);
            for _v in vs { tuple.pop(); }
        }
    }
}