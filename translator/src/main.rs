use std::env;
use std::fs;
use std::cmp;
use translator::*;

// 0. Each Join is hash join
fn check_each_join_is_hash_join(root: &TreeOp) -> bool {
    let map_func = |node: &TreeOp| -> bool {
        // We only care about the join without hash impl that it should be false;
        !matches!(node.name.as_str(), str if str.contains("JOIN") && !str.contains("HASH"))
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
    traverse(root, &traverse_funcs)
}

// 1. Bushy Join is bushy or not the width of join trees BFS{}
fn get_width_for_join_only_tree(root: &TreeOp) -> usize {
    let map_func = |node: &TreeOp| -> usize {
        match &node.attr {
            Some(NodeAttr::Join(_)) => node.children.len(),
            _ => 0
        }
    };
    let reduce_func = |a: usize, b: usize| -> usize {a + b};
    let combine_func = |map:usize, reduce: usize| -> usize {cmp::max(map, reduce)};
    let default_func = || 0;
    let traverse_funcs = TraverseFuncs {
        map_func: &map_func,
        combine_func: &combine_func,
        reduce_func: &reduce_func,
        default_func: &default_func,
    };
    traverse(root, &traverse_funcs)
}
// 2. Aggregates is only on the top

fn check_aggregate_is_on_top_only(root: &TreeOp) -> bool {

    struct TraverseState {
        is_aggr : bool,
        is_join : bool,
        results : bool
    }


    let map_func = |node: &TreeOp| -> TraverseState {
        match node.name.as_str() {
            str if str.contains("AGGREGATE") =>
                TraverseState { is_aggr: true, is_join: false, results: true },
            str if str.contains("JOIN") =>
                TraverseState { is_aggr: false, is_join: true, results: true },
            _ =>
                TraverseState { is_aggr: false, is_join: false, results: true },
        }
    };

    let reduce_func =
        |a: TraverseState, b: TraverseState|
            TraverseState {
                is_aggr: a.is_aggr || b.is_aggr, // ANY recursive Children has aggregate
                is_join: false, // Not Necessary since only want to know current nodes
                results: a.results && b.results // ALL CASES DO NOT CONTAIN AGGR INSIDE JOIN
            };

    let combine_func =
        |map: TraverseState, reduce: TraverseState| {
            TraverseState {
                is_aggr: map.is_aggr || reduce.is_aggr,
                is_join: false,
                // if current node is join and chileren has aggr, then results should be false
                results: !(map.is_join && reduce.is_aggr)
            }
        };

    let default_func = || TraverseState {
        is_aggr: false,
        is_join: false,
        results: true
    };

    let traverse_funcs = TraverseFuncs {
        map_func: &map_func,
        combine_func: &combine_func,
        reduce_func: &reduce_func,
        default_func: &default_func,
    };
    let res: TraverseState = traverse(root, &traverse_funcs);
    res.results
}

// 4. Look if any two or more variables share the same table name?
//    We can do the check on GJ Plan

// 5. Sort-Merge Join | Sort Trie | Segmented Array
//

fn main() {
    let args: Vec<String> = env::args().collect();

    // The input would be directory
    // and traverse all json
    // print name and check
    // check three or four cases and output to the screen

    let filename = &args[1];

    // File to String
    let contents =
        fs::read_to_string(filename).unwrap_or_else(|_| panic!("Cannot read file {}", filename));

    // Parse the string of data into serde_json::Value.
    let mut root: TreeOp = serde_json::from_str(contents.as_str()).expect("Failed to Parse Json!");

    // let result_collector = &root.children;
    // println!("Collector is {}", result_collector.len());

    parse_tree_extra_info(&mut root);
    let allhash = check_each_join_is_hash_join(&root);
    let width = get_width_for_join_only_tree(&root);
    let topaggr = check_aggregate_is_on_top_only(&root);
    println!("{:?} {:?} {:?}", allhash, width, topaggr);

    // [["t.id", "miidx.movie_id", "mi.movie_id", "mc.movie_id"], ["t.kind_id", "kt.id"], ["mi.info_type_id", "it2.id"], ["miidx.info_type_id", "it.id"], ["mc.company_type_id", "ct.id"], ["mc.company_id", "cn.id"]]
    let gj_plan = to_gj_plan(&mut root);
    println!("{:?}", gj_plan);
}

// Waive
