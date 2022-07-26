use std::time::Instant;

use gj::{join::*, util::*, *};

fn main() {
    // a db maps each table name to a table;
    // a table maps each column name to a column (vector of values)
    let mut db = DB::new();

    for (q, number) in queries() {
        // due to an issue in duckdb's profiler, for each query we need to parse two different profiles
        // one providing the scans and the other the join order
        let plan_profile = format!("../logs/plan-profiles/{}.json", number);
        let (_, plan, payload) = sql_to_gj(&plan_profile).unwrap();
        let scan_profile = format!("../logs/scan-profiles/{}.json", q);
        let (scan, _, _) = sql_to_gj(&scan_profile).unwrap();
        // println!("{:#?}", (&scan, &plan, &payload));

        // the compiled plan and payload refer to each table by index
        let (compiled_plan, compiled_payload) = compile_plan(&plan, &payload);
        // println!("{:#?}", (&compiled_plan, &compiled_payload));

        // load the parquet file into memory
        load_db_mut(&mut db, q, &scan);

        let start = Instant::now();
        let relations = build_tries(&db, &plan, &payload);
        println!("trie construction takes {:?}", start.elapsed());
        // assert!(relations.iter().all(|t| !t.get_map().unwrap().[is_empty()));

        let mut result = vec![];

        let start = Instant::now();

        join(
            &relations.iter().collect::<Vec<_>>(),
            &compiled_plan,
            &compiled_payload,
            &mut |t| aggregate_min(&mut result, t),
        );

        println!("join takes {:?}", start.elapsed());
        println!("{:?}", result);
        // TODO try removing this, we already clean elsewhere
        clean_db(&mut db);
    }
}

// mapping between the original query ID to duckdb's ID
fn queries() -> Vec<(&'static str, &'static str)> {
    vec![
        ("1a", "IMDBQ001"),
        ("1b", "IMDBQ002"),
        ("1c", "IMDBQ003"),
        ("1d", "IMDBQ004"),
        ("2a", "IMDBQ005"),
        ("2b", "IMDBQ006"),
        ("2c", "IMDBQ007"),
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
        // ("8c", "IMDBQ029"), // SLOW
        // ("8d", "IMDBQ030"), // SLOW
        ("9a", "IMDBQ031"),
        ("9b", "IMDBQ032"),
        ("9c", "IMDBQ033"),
        // ("9d", "IMDBQ034"), // SLOW
        ("10a", "IMDBQ035"),
        ("10b", "IMDBQ036"), // EMPTY
        ("10c", "IMDBQ037"),
        ("11a", "IMDBQ038"),
        ("11b", "IMDBQ039"),
        ("11c", "IMDBQ040"),
        ("11d", "IMDBQ041"),
        ("12a", "IMDBQ042"),
        ("12b", "IMDBQ043"),
        ("12c", "IMDBQ044"),
        ("13a", "IMDBQ045"),
        ("13b", "IMDBQ046"),
        ("13c", "IMDBQ047"),
        ("13d", "IMDBQ048"),
        ("14a", "IMDBQ049"),
        ("14b", "IMDBQ050"),
        ("14c", "IMDBQ051"),
        ("15a", "IMDBQ052"),
        ("15b", "IMDBQ053"),
        ("15c", "IMDBQ054"),
        ("15d", "IMDBQ055"),
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
        ("21a", "IMDBQ076"),
        ("21b", "IMDBQ077"),
        ("21c", "IMDBQ078"),
        ("22a", "IMDBQ079"),
        ("22b", "IMDBQ080"),
        ("22c", "IMDBQ081"),
        ("22d", "IMDBQ082"),
        // ("23a", "IMDBQ083"), // SLOW
        ("23b", "IMDBQ084"),
        // ("23c", "IMDBQ085"), // SLOW
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
        ("32a", "IMDBQ109"), // EMPTY
        ("32b", "IMDBQ110"),
        // ("33a", "IMDBQ111"), // SLOW
        // ("33b", "IMDBQ112"), // SLOW
        // ("33c", "IMDBQ113"), // SLOW
    ]
}
