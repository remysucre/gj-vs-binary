use crate::trie::*;

pub fn join<F>(relations: &[&Trie], plan: &[Vec<usize>], f: &mut F, tuple: &mut Vec<Value>)
where F: FnMut(&[Value]),
{
    if !plan.is_empty() {
        let js = &plan[0];

        let j_min = js
            .iter()
            .copied()
            .min_by(|&j1, &j2| intersect_priority(relations[j1], relations[j2]))
            .unwrap();

        let trie_min = relations[j_min];

        for (id, trie_min) in trie_min.iter() {
            if let Some(tries) = js
                .iter()
                .filter(|&j| j != &j_min)
                .map(|&j| {
                    relations[j]
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
                tuple.push(id.clone());
                join(&rels, &plan[1..], f, tuple);
                tuple.pop();
            }
        }
    } 
    else {
        select(relations, f, tuple);
    }
}

fn select<F>(relations: &[&Trie], f: &mut F, tuple: &mut Vec<Value>)
where
    F: FnMut(&[Value]),
{
    if relations.is_empty() {
        f(tuple)
    } else {
        match relations[0] {
            Trie::Nil => select(&relations[1..], f, tuple),
            Trie::Sing(v, t) => {
                tuple.push(v.clone());
                let mut rel = vec![&**t];
                rel.extend_from_slice(&relations[1..]);
                select(&rel, f, tuple);
                tuple.pop();
            }
            Trie::Data(rows) => {
                for (v, t) in rows {
                    tuple.push(v.clone());
                    let mut rel = vec![t];
                    rel.extend_from_slice(&relations[1..]);
                    select(&rel, f, tuple);
                    tuple.pop();
                }
            }
            Trie::Node(_) => panic!("Cannot select from a trie node")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal() {
        let mut r = Trie::default();
        let mut s = Trie::default();
        let mut t = Trie::default();

        let n = 11;

        for i in 1..n {
            let i = Value::Num(i);
            r.insert(&[Value::Num(0), i.clone()], vec![]);
            s.insert(&[Value::Num(0), i.clone()], vec![]);
            t.insert(&[Value::Num(0), i.clone()], vec![]);

            r.insert(&[i.clone(), Value::Num(0)], vec![]);
            s.insert(&[i.clone(), Value::Num(0)], vec![]);
            t.insert(&[i.clone(), Value::Num(0)], vec![]);
        }

        r.insert(&[Value::Num(0), Value::Num(0)], vec![]);
        s.insert(&[Value::Num(0), Value::Num(0)], vec![]);
        t.insert(&[Value::Num(0), Value::Num(0)], vec![]);

        let mut result = vec![];

        let mut tuple = vec![];

        join(&[&r, &s, &t], &[vec![0, 1], vec![1, 2], vec![0, 2]], &mut |t| { result.push(t.to_vec()) }, &mut tuple);

        println!("{:?}", result.len());
    }
}
