use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Attribute {
    attr_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Equalizer {
    left_attr: Attribute,
    right_attr: Attribute,
}

// Force to rename "extra-info" into "extra_info"
#[derive(Serialize, Deserialize, Debug)]
pub struct JoinAttr {
    join_type : JoinType,
    equalizers : Vec<Equalizer>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ScanAttr {
    table_name : String,
    attributes : Vec<Attribute>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum NodeAttr {
    Join(JoinAttr),
    Scan(ScanAttr),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TreeOp {
    pub name: String,
    pub cardinality: i32,
    pub extra_info: String,
    pub children: Vec<Box<TreeOp>>,
    pub attr: Option<NodeAttr>,
}

pub struct TraverseFuncs<'a, MR, RR, CR> {
    pub map_func: &'a dyn Fn(&TreeOp) -> MR,
    pub combine_func: &'a dyn Fn(MR, RR) -> CR,
    pub reduce_func: &'a dyn Fn(RR, CR) -> RR,
    pub default_func: &'a dyn Fn() -> RR
}

pub fn traverse<R, S, T>(node: &TreeOp, traverse_funcs: &TraverseFuncs<R, S, T>) -> T
{
    let map_result: R = (traverse_funcs.map_func)(node);
    let reduce_result: S = node.children.iter()
        .map(|child_node| traverse(child_node, traverse_funcs))
        .fold((traverse_funcs.default_func)(), |a:S, b:T | -> S {(traverse_funcs.reduce_func)(a, b)});
    let combine_result: T = (traverse_funcs.combine_func)(map_result, reduce_result);
    return combine_result;
}

pub fn preorder_traverse_mut<T>(node: &mut TreeOp, func: &T) -> ()
    where T: Fn(&mut TreeOp) -> () {
    func(node);
    for child_node in node.children.iter_mut() {
        preorder_traverse_mut(child_node, func);
    }
}

pub fn postorder_traverse_mut<T>(node: &mut TreeOp, func: &T) -> ()
    where T: Fn(&mut TreeOp) -> () {
    for child_node in node.children.iter_mut() {
        postorder_traverse_mut(child_node, func);
    }
    func(node);
}

pub fn parse_tree_extra_info(root: &mut TreeOp) {
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