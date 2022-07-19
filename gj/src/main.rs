use std::time::Instant;

use gj::{*, join::*, util::*};

fn main() {

    let (scan, plan, payload) = sql_to_gj("profile.json").unwrap();

    let (compiled_plan, compiled_payload) = compile_plan(&plan, &payload);

    let mut db = DB::new();

    load_db_mut(&mut db, &scan);

    let start = Instant::now();
    let relations = build_tries(&db, &plan, &payload);
    println!("trie construction takes {:?}", start.elapsed());

    let mut result = vec![];

    let start = Instant::now();

    join(&relations.iter().collect::<Vec<_>>(), &compiled_plan, &compiled_payload, &mut |t| {
        aggregate_min(&mut result, t)
    });

    println!("join takes {:?}", start.elapsed());
    println!("{:?}", result);
}
