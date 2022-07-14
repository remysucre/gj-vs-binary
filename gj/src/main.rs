use std::time::Instant;

use qry::*;

use gj::schema::*;
use gj::util::*;
use gj::join::*;

fn main() {
     // Variable ordering
    // t.id = miidx.movie_id = mi.movie_id = mc.movie_id
    // t.kind_id = kt.id
    // it2.id = mi.info_type_id
    // it.id = miidx.info_type_id
    // mc.company_type_id = ct.id 
    // mc.company_id = cn.id
    let plan = vec![
        vec!["t.id".to_string(), "miidx.movie_id".to_string(), "mi.movie_id".to_string(), "mc.movie_id".to_string()],
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

    // let mut count = 0;

    join(
        &relations[..], 
        &compiled_plan, 
        &mut |t| { aggregate_min(&mut result, &t) }
        // &mut |t| { count += 1}
    );

    println!("{:?}", result);
    // println!("{}", count);
    println!("{:?}", start.elapsed());

}