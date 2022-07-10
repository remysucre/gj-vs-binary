extern crate core;

use std::borrow::{Borrow, BorrowMut};
// use serde_json::Value;
use serde::{Serialize, Deserialize};
// use indextree::Arena;
use std::env;
use std::fs;

#[derive(Serialize, Deserialize, Debug)]
enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Serialize, Deserialize, Debug)]
struct Attribute {
    attr_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Equalizer {
    left_attr: Attribute,
    right_attr: Attribute,
}


// Force to rename "extra-info" into "extra_info"
#[derive(Serialize, Deserialize, Debug)]
struct JoinAttr {
    join_type : JoinType,
    equalizers : Vec<Equalizer>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ScanAttr {
    table_name : String,
    attributes : Vec<Attribute>,
}

#[derive(Serialize, Deserialize, Debug)]
enum NodeAttr {
    Join(JoinAttr),
    Scan(ScanAttr),
}

#[derive(Serialize, Deserialize, Debug)]
struct TreeOp {
    name: String,
    cardinality: i32,
    extra_info: String,
    children: Vec<Box<TreeOp>>,
    attr: Option<NodeAttr>,
}

struct TraverseFuncs<'a, MR, RR, CR> {
    map_func: &'a dyn Fn(&TreeOp) -> MR,
    combine_func: &'a dyn Fn(MR, RR) -> CR,
    reduce_func: &'a dyn Fn(RR, CR) -> RR,
    default_func: &'a dyn Fn() -> RR
}

fn traverse<R, S, T>(node: &TreeOp, traverse_funcs: &TraverseFuncs<R, S, T>) -> T
{
    let map_result: R = (traverse_funcs.map_func)(node);
    let reduce_result: S = node.children.iter()
        .map(|child_node| traverse(child_node, traverse_funcs))
        .fold((traverse_funcs.default_func)(), |a:S, b:T | -> S {(traverse_funcs.reduce_func)(a, b)});
    let combine_result: T = (traverse_funcs.combine_func)(map_result, reduce_result);
    return combine_result;
}

fn preorder_traverse_mut<T>(node: &mut TreeOp, func: &T) -> ()
    where T: Fn(&mut TreeOp) -> () {
    func(node);
    for child_node in node.children.iter_mut() {
        preorder_traverse_mut(child_node, func);
    }
}


fn postorder_traverse_mut<T>(node: &mut TreeOp, func: &T) -> ()
    where T: Fn(&mut TreeOp) -> () {
    for child_node in node.children.iter_mut() {
        postorder_traverse_mut(child_node, func);
    }
    func(node);
}




fn parse_tree_extra_info(root: &mut TreeOp) {
    let parse_func = |node: &mut TreeOp| -> () {
        match node.name.as_str() {
            "HASH_JOIN" => {
                let tmp_extra_info : String = node.extra_info.replace("[INFOSEPARATOR]", "");
                let tmp_strs: Vec<&str> = tmp_extra_info.split("\n").collect();
                let info_strs: Vec<&str> = tmp_strs.into_iter().filter(|&s| s.len() != 0).collect();
                let join_type_str: &str = info_strs.first().expect("Failed to Get Type");
                let join_type : JoinType = match join_type_str {
                    "INNER" => JoinType::Inner,
                    _ => panic!("Fail to parse Join Type {}", tmp_extra_info)
                };
                node.attr = Some(NodeAttr::Join(JoinAttr { join_type: join_type, equalizers: vec![] }));
            },
            "SEQ_SCAN" => {
                let tmp_extra_info : String = node.extra_info.replace("[INFOSEPARATOR]", "");
                let tmp_strs: Vec<&str> = tmp_extra_info.split("\\n").collect();
                let info_strs: Vec<&str> = tmp_strs.into_iter().filter(|&s| s.len() != 0).collect();
                let table_name : String = info_strs.first().expect("Failed to Get Table").to_string();
                node.attr = Some(NodeAttr::Scan(ScanAttr { table_name: table_name, attributes: vec![] }));
            },
            _ => (),
        }
    };
    preorder_traverse_mut(root, &parse_func);
}

fn check_each_join_is_hash_join(root: &TreeOp) -> bool {
    let map_func = |node: &TreeOp| -> bool {
        match node.name.as_str() {
            str if str.ends_with("JOIN") && !str.starts_with("HASH") => false,
            _ => true
        }
    };
    let reduce_func = | a: bool, b: bool|-> bool {a && b};
    let combine_func = | a: bool, b: bool|-> bool {a && b};
    let default_func = || true;
    let traverse_funcs = TraverseFuncs{
        map_func: &map_func,
        combine_func: &combine_func,
        reduce_func: &reduce_func,
        default_func: &default_func};
    let results = traverse(root, &traverse_funcs);
    return results;
}

// 0. Each Join is hash join
// 1. Bushy Join is bushy or not the width of join trees BFS{}
// 2. Aggregates is only on the top

// 3. => GJ (Variable Ordering)
//      Sequence of Sets {each attribute is decoreted with table name}
//      We may need to modify duckdb to generate the attributes with tabel name
//      Fork of Duckdb -> it is better to be submodules

fn main() {

    let args: Vec<String>  = env::args().collect();

    let filename = &args[1];

    // File to String
    let contents = fs::read_to_string(filename).expect(format!("Cannot read file {}", filename).as_str());

    // Parse the string of data into serde_json::Value.
    let mut root: TreeOp = serde_json::from_str(contents.as_str()).expect("Failed to Parse Json!");

    // Result
    let result_collector: &[Box<TreeOp>]  = root.children.borrow();
    println!("Collector is {}", result_collector.len());

    parse_tree_extra_info(root.borrow_mut());
    check_each_join_is_hash_join(root.borrow());
    // Create a new arena
    // let arena = &mut Arena::new();

    // Add some new nodes to the arena
    // let a = arena.new_node(1);
    // let b = arena.new_node(2);

    // Append a to b
    // assert!(a.checked_append(b, arena).is_ok());
    // assert_eq!(b.ancestors(arena).into_iter().count(), 2);
}

// Waive