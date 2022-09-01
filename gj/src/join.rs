use crate::trie::*;

pub fn bushy_join<'a>(
    tables: &[Tb<'a, '_, Value<'a>>],
    compiled_plan: &[Vec<usize>],
    tuple: &mut Vec<Value<'a>>,
    out: &mut Vec<Vec<Value<'a>>>,
) {
    let js = &compiled_plan[0];

    for j_min in js {
        if let Tb::Arr((id_cols, data_cols)) = &tables[*j_min] {
            for i in 0..id_cols[0].len() {
                let mut trie_min = Trie::default();
                let ids: Vec<_> = id_cols.iter().map(|c| c[i].as_num()).collect();
                let data: Vec<_> = data_cols.iter().map(|c| c[i].clone()).collect();
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
    relations: &[&Trie<Value<'a>>],
    compiled_plan: &[Vec<usize>],
    tuple: &mut Vec<Value<'a>>,
    out: &mut Vec<Vec<Value<'a>>>,
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
                tuple.push(Value::Num(*id));
                bushy_join_inner(&rels, &compiled_plan[1..], tuple, out);
                tuple.pop();
            }
        }
    }
}

fn materialize<'a>(
    relations: &[&Trie<Value<'a>>],
    tuple: &mut Vec<Value<'a>>,
    out: &mut Vec<Vec<Value<'a>>>,
) {
    if relations.is_empty() {
        out.push(tuple.to_vec());
    } else if relations[0].get_data().unwrap().is_empty() {
        materialize(&relations[1..], tuple, out);
    } else {
        for vs in relations[0].get_data().unwrap() {
            for v in vs {
                tuple.push(v.clone());
            }
            materialize(&relations[1..], tuple, out);
            for _v in vs {
                tuple.pop();
            }
        }
    }
}
