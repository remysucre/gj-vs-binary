use std::time::Instant;

use gj::{join::*, trie::*, util::*};

fn main() {
    let plan = vec![
        vec![
            "t.id".to_string(),
            "miidx.movie_id".to_string(),
            "mi.movie_id".to_string(),
            "mc.movie_id".to_string(),
        ],
        vec!["t.kind_id".to_string(), "kt.id".to_string()],
        vec!["it2.id".to_string(), "mi.info_type_id".to_string()],
        vec!["it.id".to_string(), "miidx.info_type_id".to_string()],
        vec!["mc.company_type_id".to_string(), "ct.id".to_string()],
        vec!["mc.company_id".to_string(), "cn.id".to_string()],
    ];

    let payload = vec![
        "mi.info".to_string(),
        "miidx.info".to_string(),
        "t.title".to_string(),
    ];

    let compiled_plan = compile_plan(&plan);

    let db = load_db(&plan, &payload);

    let relations: Vec<&Trie<String>> = db.iter().collect();

    let mut result: Vec<_> = vec!["Ctyri slunce", "6.7", "USA:21 January 2012"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    let start = Instant::now();

    let payload = vec![2, 1, 0];

    join(&relations[..], &compiled_plan, &payload, &mut |t| {
        aggregate_min(&mut result, t)
    });

    println!("{:?}", result);
    println!("{:?}", start.elapsed());
}
