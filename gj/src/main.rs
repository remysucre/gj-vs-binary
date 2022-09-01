use std::{collections::HashMap, time::Instant};

use gj::{join::*, sql::*, util::*};
use indexmap::IndexMap;

fn main() {
    for (q, i) in queries() {
        println!("running query {}: {} ", q, i);

        let scan_tree = get_join_tree(&format!("../logs/scan-profiles/{}.json", q)).unwrap();
        let plan_tree = get_join_tree(&format!("../logs/plan-profiles/{}.json", i)).unwrap();

        let scan = get_scans(&scan_tree);
        let payload = get_payload(&plan_tree);
        let plan = to_gj_plan(&plan_tree);

        let raw_db = load_db(q, &scan, &plan);
        let db = from_raw(&raw_db);

        let mut in_view = HashMap::new();
        let mut provides = IndexMap::new();
        let mut build_plans = IndexMap::new();
        let mut compiled_plans = IndexMap::new();

        let tm = to_materialize(&plan_tree);

        let root = tm[tm.len() - 1];

        for node in to_materialize(&plan_tree) {
            let plan = to_left_deep_plan(node);
            let compiled_plan = compile_plan(&plan, &in_view);
            let (out_schema, build_plan) = compute_full_plan(&db, &plan, &provides, &in_view);

            build_plans.insert(node, build_plan);
            provides.insert(node, out_schema);
            compiled_plans.insert(node, compiled_plan);

            map_tables_to_node(node, &mut in_view);
        }

        let mut views = HashMap::new();

        let start = Instant::now();

        // TODO hash treeop by address
        for (node, compiled_plan) in &compiled_plans {
            let build_plan = &build_plans[node];

            let build_start = Instant::now();
            let tables = build_tables(&db, &views, build_plan);
            println!("Building takes {}", build_start.elapsed().as_secs_f32());

            let mut intermediate = Vec::new();
            let mut tuple = vec![];

            println!("Running join");
            let join_start = Instant::now();
            bushy_join(&tables, compiled_plan, &mut tuple, &mut intermediate);
            println!("Join took {:?}", join_start.elapsed().as_secs_f32());

            views.insert(node, intermediate);
        }

        println!("Bushy join takes {:?}", start.elapsed().as_secs_f32());

        let final_attrs = provides.get(&root).unwrap();
        let final_view = views.get(&root).unwrap();

        print!("output ");

        let payload_ids: Vec<_> = payload
            .iter()
            .map(|p| {
                final_attrs
                    .iter()
                    .position(|attrs| attrs.contains(p))
                    .unwrap()
            })
            .collect();

        let mut result = Vec::new();

        for row in final_view {
            if result.is_empty() {
                result = payload_ids.iter().map(|i| &row[*i]).collect();
            } else {
                for (j, i) in payload_ids.iter().enumerate() {
                    if result[j] > &row[*i] {
                        result[j] = &row[*i];
                    }
                }
            }
        }

        println!("{:?}", result);
        println!("Total takes {}", start.elapsed().as_secs_f32());
    }
}

// mapping between the original query ID to duckdb's ID
fn queries() -> Vec<(&'static str, &'static str)> {
    // let queries = vec![("33c", "IMDBQ113")];

    let queries = vec![
        ("16a", "IMDBQ056"),
        ("16b", "IMDBQ057"),
        ("16c", "IMDBQ058"),
        ("16d", "IMDBQ059"),
        ("17e", "IMDBQ064"),
        ("6f", "IMDBQ023"),
        ("8c", "IMDBQ029"),
        ("8d", "IMDBQ030"),
    ];

    /*
    let bushy = true;
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
