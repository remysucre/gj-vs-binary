use std::{collections::HashMap, time::Instant};

use gj::{
    join::*,
    sql::{
        get_join_tree, get_payload, get_scans, to_gj_plan, to_left_deep_plan, to_materialize,
        update_materialize_map,
    },
    util::*, trie::{RawValue, Value},
};

fn main() {
    for (q, i) in queries() {
        println!("running query {}: {} ", q, i);

        let scan_tree = get_join_tree(&format!("../logs/scan-profiles/{}.json", q)).unwrap();
        let plan_tree = get_join_tree(&format!("../logs/plan-profiles/{}.json", i)).unwrap();

        let scan = get_scans(&scan_tree);
        let payload = get_payload(&plan_tree);
        let plan = to_gj_plan(&plan_tree);

        let raw_db = load_db(q, &scan, &plan);

        let db: HashMap<_, _> = raw_db
            .iter()
            .map(|(name, table)| {
                let t: HashMap<_, _> = table
                    .iter()
                    .map(|(attr, col)| {
                        let c: Vec<_> = col
                            .iter()
                            .map(|val| {
                                match val {
                                    RawValue::Num(id) => Value::Num(*id),
                                    RawValue::Str(s) => Value::Str(s.as_str()),
                                }
                            }).collect();
                        (attr.clone(), c)
                    })
                    .collect();
                (name.clone(), t)
            })
            .collect();
        
        let mut materialized_columns = Vec::new();
        let mut views = HashMap::new();
        let mut in_view = HashMap::new();

        let start = Instant::now();

        for node in to_materialize(&plan_tree) {
            let plan = to_left_deep_plan(node);
            let (compiled_plan, _) = compile_gj_plan(&plan, &[], &in_view);

            let (tables, out_vars) =
                build_tables(&db, &materialized_columns, &views, &in_view, &plan);

            let mut new_columns = Vec::new();
            let mut out = HashMap::new();
            let mut tuple = Vec::new();
            let view_len = materialized_columns.len();

            let now = Instant::now();

            bushy_join(
                &tables,
                &compiled_plan,
                &tuple,
                &plan,
                &out_vars,
                &mut out,
                view_len,
                &mut new_columns,
            );

            println!("bushy join takes {}", now.elapsed().as_secs_f32());

            views.insert(node, out);

            materialized_columns.extend(new_columns);
            update_materialize_map(node, &mut in_view);
        }

        let elapsed = start.elapsed().as_secs_f32();
        println!("join takes: {:?}", elapsed);

        print!("output: ");

        for attr in payload {
            let col = in_view
                .get(attr.table_name.as_str())
                .map(|tree_op| {
                    &materialized_columns[*views.get(tree_op).unwrap().get(attr).unwrap()]
                })
                .unwrap();
            print!(
                "{:?}",
                col.iter()
                    .min_by(|x, y| { x.partial_cmp(y).unwrap() })
                    .unwrap()
            );
        }

        let now = start.elapsed().as_secs_f32();

        println!();
        println!("total takes: {:?}", now);
    }
}

// mapping between the original query ID to duckdb's ID
fn queries() -> Vec<(&'static str, &'static str)> {
    // let queries = vec![
    //     ("31c", "IMDBQ108"),
    //     ("31b", "IMDBQ107"),
    //     ("25c", "IMDBQ090"),
    //     ("25a", "IMDBQ088"),
    //     ("8c", "IMDBQ029"),
    //     ("17e", "IMDBQ064"),
    //     ("16b", "IMDBQ057"),
    //     ("8d", "IMDBQ030"),
    //     ("6f", "IMDBQ023"),
    // ];

    // /*
    let bushy = false;
    let linear = true;

    let mut queries = vec![];

    if linear {
        queries.extend_from_slice(&[
            ("1a", "IMDBQ001"),
            ("1b", "IMDBQ002"),
            ("1c", "IMDBQ003"),
            ("1d", "IMDBQ004"),
            ("2a", "IMDBQ005"),
            ("2b", "IMDBQ006"),
            // ("2c", "IMDBQ007"), // EMPTY
            ("2d", "IMDBQ008"),
            ("3a", "IMDBQ009"),
            ("3b", "IMDBQ010"),
            ("3c", "IMDBQ011"),
            ("4a", "IMDBQ012"),
            ("4b", "IMDBQ013"),
            ("4c", "IMDBQ014"),
            // EMPTY input ("5a", "IMDBQ015"),
            // EMPTY input ("5b", "IMDBQ016"),
            ("5c", "IMDBQ017"),
        ])
    }

    if bushy {
        queries.extend_from_slice(&[
            ("6a", "IMDBQ018"),
            ("6b", "IMDBQ019"),
            ("6c", "IMDBQ020"),
            ("6d", "IMDBQ021"),
            ("6e", "IMDBQ022"),
            ("6f", "IMDBQ023"),
            ("7a", "IMDBQ024"),
            ("7b", "IMDBQ025"),
            ("7c", "IMDBQ026"),
            ("8a", "IMDBQ027"),
            ("8b", "IMDBQ028"),
            ("8c", "IMDBQ029"), // SLOW
            ("8d", "IMDBQ030"), // SLOW
            ("9a", "IMDBQ031"),
            ("9b", "IMDBQ032"),
            ("9c", "IMDBQ033"),
            ("9d", "IMDBQ034"), // SLOW
            ("10a", "IMDBQ035"),
            // ("10b", "IMDBQ036"), // EMPTY
            ("10c", "IMDBQ037"),
        ])
    }

    if linear {
        queries.extend_from_slice(&[
            ("11a", "IMDBQ038"),
            ("11b", "IMDBQ039"),
            ("11c", "IMDBQ040"),
            ("11d", "IMDBQ041"),
            ("12a", "IMDBQ042"),
            ("12b", "IMDBQ043"), // TRIE SLOW
            ("12c", "IMDBQ044"),
            ("13a", "IMDBQ045"),
            ("13b", "IMDBQ046"), // TRIE SLOW
            ("13c", "IMDBQ047"), // TRIE SLOW
            ("13d", "IMDBQ048"),
            ("14a", "IMDBQ049"),
            ("14b", "IMDBQ050"),
            ("14c", "IMDBQ051"),
            ("15a", "IMDBQ052"),
            ("15b", "IMDBQ053"),
            ("15c", "IMDBQ054"),
            ("15d", "IMDBQ055"),
        ])
    }

    if bushy {
        queries.extend_from_slice(&[
            ("16a", "IMDBQ056"),
            ("16b", "IMDBQ057"),
            ("16c", "IMDBQ058"),
            ("16d", "IMDBQ059"),
            ("17a", "IMDBQ060"),
            ("17b", "IMDBQ061"),
            ("17c", "IMDBQ062"),
            ("17d", "IMDBQ063"),
            ("17e", "IMDBQ064"),
            ("17f", "IMDBQ065"),
            ("18a", "IMDBQ066"),
            ("18b", "IMDBQ067"),
            ("18c", "IMDBQ068"),
            ("19a", "IMDBQ069"),
            ("19b", "IMDBQ070"),
            ("19c", "IMDBQ071"),
            ("19d", "IMDBQ072"),
            ("20a", "IMDBQ073"),
            ("20b", "IMDBQ074"),
            ("20c", "IMDBQ075"),
        ])
    }

    if linear {
        queries.extend_from_slice(&[
            ("21a", "IMDBQ076"),
            ("21b", "IMDBQ077"),
            ("21c", "IMDBQ078"),
        ])
    }

    if bushy {
        queries.extend_from_slice(&[
            ("22a", "IMDBQ079"),
            ("22b", "IMDBQ080"),
            ("22c", "IMDBQ081"),
            ("22d", "IMDBQ082"),
            ("23a", "IMDBQ083"), // SLOW
            ("23b", "IMDBQ084"),
            ("23c", "IMDBQ085"), // SLOW
            ("24a", "IMDBQ086"),
            ("24b", "IMDBQ087"),
            ("25a", "IMDBQ088"),
            ("25b", "IMDBQ089"),
            ("25c", "IMDBQ090"),
            ("26a", "IMDBQ091"),
            ("26b", "IMDBQ092"),
            ("26c", "IMDBQ093"),
            ("27a", "IMDBQ094"),
            ("27b", "IMDBQ095"),
            ("27c", "IMDBQ096"),
            ("28a", "IMDBQ097"),
            ("28b", "IMDBQ098"),
            ("28c", "IMDBQ099"),
            ("29a", "IMDBQ100"),
            ("29b", "IMDBQ101"),
            ("29c", "IMDBQ102"),
            ("30a", "IMDBQ103"),
            ("30b", "IMDBQ104"),
            ("30c", "IMDBQ105"),
            ("31a", "IMDBQ106"),
            ("31b", "IMDBQ107"),
            ("31c", "IMDBQ108"),
        ])
    }

    if linear {
        queries.extend_from_slice(&[
            // ("32a", "IMDBQ109"), // TRIE SLOW // EMPTY
            ("32b", "IMDBQ110"), // TRIE SLOW
        ])
    }

    if bushy {
        queries.extend_from_slice(&[
            ("33a", "IMDBQ111"), // SLOW
            ("33b", "IMDBQ112"), // SLOW
            ("33c", "IMDBQ113"), // SLOW
        ])
    }

    // */
    queries
}
