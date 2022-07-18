use std::time::Instant;

use gj::{join::*, trie::*, util::*};

fn main() {

    let (plan, payload) = sql_to_gj("profile.json").unwrap();

    let (compiled_plan, compiled_payload) = compile_plan(&plan, &payload);

    let db = load_db(&plan, &payload);

    let relations: Vec<&Trie<String>> = db.iter().collect();

    let mut result = vec![];

    let start = Instant::now();

    join(&relations[..], &compiled_plan, &compiled_payload, &mut |t| {
        aggregate_min(&mut result, t)
    });

    println!("{:?}", result);
    println!("{:?}", start.elapsed());
}
