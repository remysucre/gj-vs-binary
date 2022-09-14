use std::{collections::HashSet, mem::take};

use crate::{
    sql::{Attribute, FAKE},
    trie::*,
    Args,
};
use smallvec::SmallVec;

pub fn optimize(_args: &Args, plan: Vec<Instruction2>) -> Vec<Instruction2> {
    // combining lookups is unconditional
    let mut new_plan = vec![];
    let mut current_lookups: Vec<Lookup2> = vec![];

    for instr in plan.clone() {
        if let Instruction2::Lookup(lookups) = instr {
            assert_eq!(lookups.len(), 1);

            let mut not_mergeable = vec![];
            for lookup in lookups {
                if current_lookups
                    .iter()
                    .any(|l| l.relation == lookup.relation)
                {
                    not_mergeable.push(lookup);
                } else {
                    current_lookups.push(lookup);
                }
            }

            if !not_mergeable.is_empty() {
                panic!("not mergeable");
                // assert!(!current_lookups.is_empty());
                // new_plan.push(Instruction2::Lookup(take(&mut current_lookups)));
                // new_plan.push(Instruction2::Lookup(not_mergeable));
            }
        } else {
            if !current_lookups.is_empty() {
                new_plan.push(Instruction2::Lookup(take(&mut current_lookups)));
            }
            new_plan.push(instr);
        }
    }

    if !current_lookups.is_empty() {
        new_plan.push(Instruction2::Lookup(current_lookups));
    }

    for instr in &new_plan {
        if let Instruction2::Lookup(lookups) = instr {
            assert!(!lookups.is_empty());
            let relations: HashSet<usize> = lookups.iter().map(|l| l.relation).collect();
            assert_eq!(lookups.len(), relations.len());
        }
    }

    assert!(new_plan.len() <= plan.len());
    new_plan
}

// pub fn bushy_join<'a>(
//     tables: &[Tb<'a, '_, Value<'a>>],
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

#[derive(Debug, PartialEq, Eq)]
pub enum Instruction {
    Scan,
    Intersect { relations: Vec<usize> },
    Lookup(Vec<Lookup>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Instruction2 {
    Intersect(Vec<Intersection2>),
    Lookup(Vec<Lookup2>),
}

#[derive(Clone, PartialEq, Eq)]
pub struct Intersection2 {
    pub relation: usize,
    pub attr: Attribute,
}

impl std::fmt::Debug for Intersection2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.relation == FAKE {
            write!(f, "{:?}", self.attr)
        } else {
            write!(f, "{:?} @ {:?})", self.attr, self.relation)
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Lookup2 {
    pub key: usize,
    pub relation: usize,
    pub left: Attribute,
    pub right: Attribute,
}

impl std::fmt::Debug for Lookup2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.relation == FAKE {
            write!(f, "{:?} = {:?})", self.left, self.right)
        } else {
            write!(
                f,
                "{:?} @ {:?} = {:?} @ {:?})",
                self.key, self.left, self.relation, self.right
            )
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Lookup {
    pub key: usize, // index into the tuple
    pub relation: usize,
}

// Assumes that the first table is a scan
pub fn free_join(
    args: &Args,
    tables: &[Table],
    compiled_plan: &[Instruction2],
    out: &mut Vec<Vec<Value>>,
) {
    let mut compiled_plan = compiled_plan.to_vec();
    println!("n instructions: {}", compiled_plan.len());
    if let Table::Arr { id_cols, data_cols } = &tables[0] {
        let rels: SmallVec<[_; 8]> = tables[1..]
            .iter()
            .map(|t| match &t {
                Table::Arr { .. } => unreachable!("Only left table can be flat"),
                Table::Trie(trie) => trie.as_ref(),
            })
            .collect();

        let mut id_iters: Vec<_> = id_cols.iter().map(|c| c.iter()).collect();
        let mut data_iters: Vec<_> = data_cols.iter().map(|c| c.iter()).collect();

        // assert!(matches!(&compiled_plan[0], Instruction::Scan));

        let mut ctx = JoinContext {
            args,
            n_lookups: 0,
            singleton: vec![],
            tuple: vec![],
            out,
        };

        // unroll the outer scan
        while let Some(v) = id_iters[0].next() {
            ctx.tuple.push(v.as_num());

            for id_iter in &mut id_iters[1..] {
                ctx.tuple.push(id_iter.next().unwrap().as_num());
            }

            for data_iter in &mut data_iters {
                ctx.singleton.push(data_iter.next().unwrap().clone());
            }

            ctx.join(&rels, &mut compiled_plan);
            ctx.tuple.clear();
            ctx.singleton.clear();
        }

        log::debug!("{} lookups", ctx.n_lookups);

        // for i in 0..id_cols[0].len() {
        //     let singleton: SmallVec<[_; 4]> = id_cols
        //         .iter()
        //         .map(|c| &c[i])
        //         .chain(
        //             data_cols
        //                 .iter()
        //                 .map(|c| &c[i])
        //         ).collect();
        //     singleton_join_inner(&singleton,&rels, compiled_plan, tuple, out);
        // }
    } else {
        unreachable!("The first table must be flat");
    }
}

struct JoinContext<'a> {
    n_lookups: usize,
    singleton: Vec<Value>,
    tuple: Vec<i32>,
    out: &'a mut Vec<Vec<Value>>,
    args: &'a Args,
}

impl JoinContext<'_> {
    fn join<'b>(&mut self, relations: &[TrieRef<'b>], compiled_plan: &mut [Instruction2]) {
        let (instr, rest) = if let Some(tup) = compiled_plan.split_first_mut() {
            tup
        } else {
            return self.materialize(relations);
        };

        match instr {
            Instruction2::Intersect(js) => {
                let j_min = js
                    .iter()
                    .min_by_key(|&j| relations[j.relation].len())
                    .unwrap()
                    .relation;

                relations[j_min].for_each(|id, trie_min| {
                    if let Some(tries) = js
                        .iter()
                        .filter(|&j| j.relation != j_min)
                        .map(|j| relations[j.relation].get(id).map(|trie| (j, trie)))
                        .collect::<Option<SmallVec<[_; 8]>>>()
                    {
                        let mut rels: SmallVec<[_; 8]> = SmallVec::from_slice(relations);
                        rels[j_min] = trie_min;
                        for (j, trie) in tries {
                            rels[j.relation] = trie;
                        }
                        self.tuple.push(id);
                        self.join(&rels, rest);
                        self.tuple.pop();
                    }
                })
            }
            Instruction2::Lookup(lookups) => {
                assert!(!lookups.is_empty());
                if self.args.optimize > 0 {
                    lookups.sort_unstable_by_key(|l| relations[l.relation].len());
                }

                let mut rels: SmallVec<[TrieRef<'b>; 8]> = SmallVec::from_slice(relations);
                for lookup in lookups {
                    let value = self.tuple[lookup.key];
                    self.n_lookups += 1;
                    if let Some(r) = rels[lookup.relation].get(value) {
                        rels[lookup.relation] = r;
                    } else {
                        return;
                    }
                }

                if rest.is_empty() {
                    self.materialize(&rels);
                } else {
                    self.join(&rels, rest);
                }
            }
        }
    }

    fn materialize(&mut self, relations: &[TrieRef]) {
        if relations.is_empty() {
            let t: Vec<_> = self
                .tuple
                .iter()
                .map(|i| Value::Num(*i))
                .chain(self.singleton.iter().cloned())
                .collect();
            self.out.push(t);
        } else if relations[0].get_data().len() == 0 {
            self.materialize(&relations[1..]);
        } else {
            for vs in relations[0].get_data() {
                for v in vs {
                    self.singleton.push(v.clone());
                }
                self.materialize(&relations[1..]);
                for _v in vs {
                    self.singleton.pop();
                }
            }
        }
    }
}

// fn materialize_single<'a>(
//     relations: &[&Trie<Value<'a>>],
//     tuple: &mut Vec<Value<'a>>,
//     out: &mut Vec<Vec<Value<'a>>>,
// ) {
//     if relations.is_empty() {
//         out.push(tuple.to_vec());
//     } else if relations[0].get_data().unwrap().is_empty() {
//         materialize_single(singleton, &relations[1..], tuple, out);
//     } else {
//         for vs in relations[0].get_data().unwrap() {
//             for v in vs {
//                 tuple.push(v.clone());
//             }
//             materialize_single(singleton, &relations[1..], tuple, out);
//             for _v in vs {
//                 tuple.pop();
//             }
//         }
//     }
// }

// fn bushy_join_inner<'a>(
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
