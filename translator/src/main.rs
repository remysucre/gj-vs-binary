use std::env;
use std::fs;

use translator::*;

fn check_each_join_is_hash_join(root: &TreeOp) -> bool {
    let map_func = |node: &TreeOp| -> bool {
        match node.name.as_str() {
            str if str.ends_with("JOIN") && !str.starts_with("HASH") => false,
            _ => true,
        }
    };
    let reduce_func = |a: bool, b: bool| -> bool { a && b };
    let combine_func = |a: bool, b: bool| -> bool { a && b };
    let default_func = || true;
    let traverse_funcs = TraverseFuncs {
        map_func: &map_func,
        combine_func: &combine_func,
        reduce_func: &reduce_func,
        default_func: &default_func,
    };
    return traverse(root, &traverse_funcs);
}

// 0. Each Join is hash join
// 1. Bushy Join is bushy or not the width of join trees BFS{}
// 2. Aggregates is only on the top

// 3. => GJ (Variable Ordering)
//      Sequence of Sets {each attribute is decoreted with table name}
//      We may need to modify duckdb to generate the attributes with tabel name
//      Fork of Duckdb -> it is better to be submodules

fn main() {
    let args: Vec<String> = env::args().collect();

    let filename = &args[1];

    // File to String
    let contents =
        fs::read_to_string(filename).expect(format!("Cannot read file {}", filename).as_str());

    // Parse the string of data into serde_json::Value.
    let mut root: TreeOp = serde_json::from_str(contents.as_str()).expect("Failed to Parse Json!");

    // Result
    let result_collector = &root.children;
    println!("Collector is {}", result_collector.len());

    parse_tree_extra_info(&mut root);
    check_each_join_is_hash_join(&root);
    let gj_plan = to_gj_plan(&mut root);
    println!("{:?}", gj_plan);
}

// Waive
