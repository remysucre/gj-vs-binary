use std::{error::Error, fs, path};

use derivative::Derivative;
use serde::{Deserialize, Serialize};

use crate::join::{Instruction2, Intersection2, Lookup2};
use crate::*;

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    pub table_name: String,
    pub attr_name: String,
}

impl std::fmt::Debug for Attribute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.table_name, self.attr_name)
    }
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct Equalizer {
    pub left_attr: Attribute,
    pub right_attr: Attribute,
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct JoinAttr {
    pub join_type: JoinType,
    pub equalizers: Vec<Equalizer>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct ScanAttr {
    pub table_name: String,
    pub attributes: Vec<Attribute>,
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct ProjectAttr {
    columns: Vec<Attribute>,
}

#[derive(Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
pub enum NodeAttr {
    Join(JoinAttr),
    Scan(ScanAttr),
    Project(ProjectAttr),
    Filter,
}

#[derive(Derivative)]
#[derivative(Debug, Hash, PartialEq, Eq)]
#[derive(Serialize, Deserialize)]
pub struct TreeOp {
    pub name: String,
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub timing: f64,
    pub cardinality: u64,
    pub extra_info: String,
    pub children: Vec<Box<TreeOp>>,
    pub attr: Option<NodeAttr>,
}

pub fn get_filter_cost(root: &TreeOp) -> f64 {
    let mut cost = 0.0;

    postorder_traverse(root, &mut |node| {
        if let Some(NodeAttr::Scan(_)) | Some(NodeAttr::Filter) = &node.attr {
            if node.extra_info.contains("Filters") {
                cost += node.timing;
            }
        }
    });

    cost
}

// fn preorder_traverse_mut<T>(node: &mut TreeOp, func: &mut T)
// where
//     T: FnMut(&mut TreeOp),
// {
//     func(node);
//     for child_node in node.children.iter_mut() {
//         preorder_traverse_mut(child_node, func);
//     }
// }

fn postorder_traverse<'a>(node: &'a TreeOp, func: &mut dyn FnMut(&'a TreeOp)) {
    for child_node in node.children.iter() {
        postorder_traverse(child_node, func);
    }
    func(node);
}

fn inorder_traverse<'a, T>(node: &'a TreeOp, func: &mut T)
where
    T: FnMut(&'a TreeOp),
{
    if !node.children.is_empty() {
        inorder_traverse(&node.children[0], func);
    }
    func(node);
    if !node.children.is_empty() {
        for child_node in &node.children[1..] {
            inorder_traverse(child_node, func);
        }
    }
}

fn inorder_traverse_mut<T>(node: &mut TreeOp, func: &mut T)
where
    T: FnMut(&mut TreeOp),
{
    if !node.children.is_empty() {
        inorder_traverse_mut(&mut node.children[0], func);
    }
    func(node);
    if !node.children.is_empty() {
        for child_node in &mut node.children[1..] {
            inorder_traverse_mut(child_node, func);
        }
    }
}

pub fn get_join_tree(file_name: &str) -> Result<TreeOp, Box<dyn Error>> {
    let sql = fs::read_to_string(path::Path::new(file_name))?;
    let mut root: TreeOp = serde_json::from_str(sql.as_str())?;
    parse_tree_extra_info(&mut root);
    Ok(root)
}

// fill in the attributes for scans, projections and joins
pub fn parse_tree_extra_info(root: &mut TreeOp) {
    let mut parse_func = |node: &mut TreeOp| match node.name.as_str() {
        "HASH_JOIN" => {
            let extra_info: Vec<_> = node
                .extra_info
                .split('\n')
                .filter(|s| !s.is_empty())
                .collect();

            let join_type = match extra_info[0] {
                "INNER" => JoinType::Inner,
                "MARK" => return,
                _ => panic!("Fail to parse Join Type {}", extra_info[0]),
            };

            let mut equalizers = Vec::new();

            for pred in &extra_info[1..] {
                let equalizer = pred.split('=').map(|s| s.trim()).collect::<Vec<_>>();
                let left_attr = equalizer[0]
                    .split('.')
                    .map(|s| s.trim())
                    .collect::<Vec<_>>();
                let right_attr = equalizer[1]
                    .split('.')
                    .map(|s| s.trim())
                    .collect::<Vec<_>>();
                // HACK in the profile generated by unmodified duckdb
                // the table name is not included in the attribute name.
                // Here we use the attribute name as deadbeef,
                // and get the table name from the profile generated
                // by patched duckdb
                equalizers.push(Equalizer {
                    left_attr: if left_attr.len() == 1 {
                        Attribute {
                            table_name: left_attr[0].to_string(),
                            attr_name: left_attr[0].to_string(),
                        }
                    } else {
                        Attribute {
                            table_name: left_attr[0].to_string(),
                            attr_name: left_attr[1].to_string(),
                        }
                    },
                    right_attr: if right_attr.len() == 1 {
                        Attribute {
                            table_name: right_attr[0].to_string(),
                            attr_name: right_attr[0].to_string(),
                        }
                    } else {
                        Attribute {
                            table_name: right_attr[0].to_string(),
                            attr_name: right_attr[1].to_string(),
                        }
                    },
                });
            }

            node.attr = Some(NodeAttr::Join(JoinAttr {
                join_type,
                equalizers,
            }));
        }
        "SEQ_SCAN" => {
            let extra_info: Vec<_> = node.extra_info.split("[INFOSEPARATOR]").collect();
            let table_name = extra_info[0].trim();
            let info_strs: Vec<_> = extra_info[1]
                .split('\n')
                .filter(|s| !s.is_empty())
                .collect();

            node.attr = Some(NodeAttr::Scan(ScanAttr {
                table_name: table_name.to_string(),
                attributes: info_strs
                    .iter()
                    .map(|s| Attribute {
                        table_name: table_name.to_string(),
                        attr_name: s.to_string(),
                    })
                    .collect(),
            }));
        }
        "PROJECTION" => {
            let columns: Vec<_> = node
                .extra_info
                .split('\n')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    let names: Vec<_> = s.split('.').map(|s| s.trim()).collect();
                    // HACK similar to the above, we use "" as deadbeef
                    // and get the table name from profile by the patched duckdb.
                    if names.len() == 1 {
                        Attribute {
                            table_name: "".to_string(),
                            attr_name: names[0].to_string(),
                        }
                    } else {
                        Attribute {
                            table_name: names[0].to_string(),
                            attr_name: names[1].to_string(),
                        }
                    }
                })
                .collect();
            node.attr = Some(NodeAttr::Project(ProjectAttr { columns }));
        }
        "CHUNK_SCAN" | "RESULT_COLLECTOR" | "SIMPLE_AGGREGATE" | "Query" => {}
        "FILTER" => {
            node.attr = Some(NodeAttr::Filter);
        }
        _ => panic!("Unknown node type {}", node.name),
    };
    inorder_traverse_mut(root, &mut parse_func);
}

pub fn get_scans<'a>(root: &'a TreeOp) -> Vec<&'a ScanAttr> {
    let mut scan = vec![];

    let mut collect_vars = |node: &'a TreeOp| {
        if let Some(NodeAttr::Scan(attr)) = &node.attr {
            scan.push(attr);
        }
    };

    preorder_traverse(root, &mut collect_vars);
    scan
}

pub fn get_payload<'a>(root: &'a TreeOp) -> Vec<&'a Attribute> {
    let mut payload = vec![];

    let mut collect_vars = |node: &'a TreeOp| {
        if let Some(NodeAttr::Project(cols)) = &node.attr {
            payload.extend(cols.columns.iter().filter(|a| !a.table_name.is_empty()));
        }
    };

    preorder_traverse(root, &mut collect_vars);
    payload
}

pub fn to_gj_plan(root: &TreeOp) -> Vec<Vec<&Attribute>> {
    to_plan(root, postorder_traverse)
}

// TODO: undo occurence merging
pub fn to_left_deep_plan(root: &TreeOp) -> Vec<Vec<&Attribute>> {
    to_plan(root, traverse_left)
}

fn to_plan<'a, F>(root: &'a TreeOp, mut traverse: F) -> Vec<Vec<&'a Attribute>>
where
    F: FnMut(&'a TreeOp, &mut dyn FnMut(&'a TreeOp)),
{
    let mut plan: Vec<Vec<&'a Attribute>> = vec![];

    let mut collect_plan = |node: &'a TreeOp| {
        if let Some(NodeAttr::Join(attr)) = &node.attr {
            for equalizer in &attr.equalizers {
                let lattr = &equalizer.left_attr;
                let rattr = &equalizer.right_attr;

                let lpos_opt = plan.iter().position(|x| x.contains(&lattr));
                let rpos_opt = plan.iter().position(|x| x.contains(&rattr));

                match (lpos_opt, rpos_opt) {
                    (Some(_lpos), Some(_rpos)) => {} // TODO add this back assert_eq!(lpos, rpos),
                    (Some(lpos), None) => plan[lpos].push(rattr),
                    (None, Some(rpos)) => plan[rpos].push(lattr),
                    (None, None) => plan.push(vec![lattr, rattr]),
                }
            }
        }
    };

    traverse(root, &mut collect_plan);

    plan
}

pub fn to_binary_plan<'a>(root: &'a TreeOp) -> Vec<Vec<&'a Attribute>> {
    let mut plan: Vec<Vec<&'a Attribute>> = vec![];

    let mut collect_plan = |node: &'a TreeOp| {
        if let Some(NodeAttr::Join(attr)) = &node.attr {
            let mut attrs = IndexSet::default();
            for equalizer in &attr.equalizers {
                let lattr = &equalizer.left_attr;
                let rattr = &equalizer.right_attr;

                attrs.insert(lattr);
                attrs.insert(rattr);
            }
            plan.push(attrs.into_iter().collect());
        }
    };

    traverse_left(root, &mut collect_plan);

    plan
}

pub fn get_scan_join_attrs<'a>(root: &'a TreeOp) -> IndexSet<Attribute> {
    let mut left_table = None;
    let mut scan_join_attrs = IndexSet::default();

    traverse_left(root, &mut |node: &'a TreeOp| {
        if let Some(NodeAttr::Join(join)) = &node.attr {
            for eql in &join.equalizers {
                let l = &eql.left_attr;

                if left_table.is_none() {
                    left_table = Some(&l.table_name);
                }
                if &l.table_name == left_table.unwrap() {
                    scan_join_attrs.insert(Attribute {
                        table_name: l.table_name.clone(),
                        attr_name: l.attr_name.clone(),
                    });
                }
            }
        }
    });

    scan_join_attrs
}

pub const FAKE: usize = 9999;

pub fn to_binary_plan2<'a>(
    root: &'a TreeOp,
    in_view: &HashMap<&str, &'a TreeOp>,
    provides: &IndexMap<&'a TreeOp, Vec<Vec<Attribute>>>,
) -> Vec<Instruction2> {
    let mut program = vec![];

    let attrs = get_scan_join_attrs(root);

    let mut groups: Vec<_> = attrs.iter().map(|a| vec![a.clone()]).collect();

    traverse_left(root, &mut |node: &'a TreeOp| {
        if let Some(NodeAttr::Join(join)) = &node.attr {
            for equalizer in &join.equalizers {
                // the attribute can be from a view, so we find its representative
                let lattr = if let Some(node) = in_view.get(equalizer.left_attr.table_name.as_str())
                {
                    let attrs = &provides[node];
                    let i = attrs
                        .iter()
                        .position(|a| a.contains(&equalizer.left_attr))
                        .unwrap();
                    &attrs[i][0]
                } else {
                    &equalizer.left_attr
                };
                let rattr =
                    if let Some(node) = in_view.get(equalizer.right_attr.table_name.as_str()) {
                        let attrs = &provides[node];
                        let i = attrs
                            .iter()
                            .position(|a| a.contains(&equalizer.right_attr))
                            .unwrap();
                        &attrs[i][0]
                    } else {
                        &equalizer.right_attr
                    };

                let l_pos_opt = groups.iter().position(|g| g.contains(lattr));
                let r_pos_opt = groups.iter().position(|g| g.contains(rattr));

                match (l_pos_opt, r_pos_opt) {
                    (Some(lpos), Some(rpos)) => {
                        assert_eq!(lpos, rpos, "left {:?}, right {:?}", &lattr, &rattr)
                    }
                    (Some(lpos), None) => {
                        let instr = Instruction2::Lookup(vec![Lookup2 {
                            key: FAKE,
                            relation: FAKE,
                            left: groups[lpos][0].clone(),
                            right: rattr.clone(),
                        }]);
                        groups[lpos].push(rattr.clone());
                        program.push(instr);
                    }
                    (None, Some(rpos)) => {
                        let instr = Instruction2::Lookup(vec![Lookup2 {
                            key: FAKE,
                            relation: FAKE,
                            left: lattr.clone(),
                            right: groups[rpos][0].clone(),
                        }]);
                        groups[rpos].push(lattr.clone());
                        program.push(instr);
                    }
                    (None, None) => {
                        let instr = Instruction2::Intersect(vec![
                            Intersection2 {
                                relation: FAKE,
                                attr: lattr.clone(),
                            },
                            Intersection2 {
                                relation: FAKE,
                                attr: rattr.clone(),
                            },
                        ]);
                        groups.push(vec![lattr.clone(), rattr.clone()]);
                        program.push(instr);
                    }
                }
            }
        }
    });

    program
}

// fn traverse_lr<'a, T>(node: &'a TreeOp, func: &mut T)
// where
//     T: FnMut(&'a TreeOp),
// {
//     if !node.children.is_empty() {
//         traverse_lr(&node.children[0], func);
//     }

//     func(node);

//     if !node.children.is_empty() {
//         for child_node in &node.children[1..] {
//             preorder_traverse(child_node, func);
//         }
//     }
// }

fn preorder_traverse<'a, T>(node: &'a TreeOp, func: &mut T)
where
    T: FnMut(&'a TreeOp),
{
    func(node);
    for child_node in &node.children {
        preorder_traverse(child_node, func);
    }
}

pub fn to_semijoin_plan<'a>(root: &'a TreeOp) -> Vec<Vec<&'a Attribute>> {
    let mut plan: Vec<Vec<&'a Attribute>> = vec![];
    let mut build_plan = |node: &'a TreeOp| {
        if let Some(NodeAttr::Join(attr)) = &node.attr {
            for equalizer in &attr.equalizers {
                let lattr = &equalizer.left_attr;
                let rattr = &equalizer.right_attr;

                let lpos_opt = plan.iter().position(|x| x.contains(&lattr));
                let rpos_opt = plan.iter().position(|x| x.contains(&rattr));

                match (lpos_opt, rpos_opt) {
                    (Some(_lpos), Some(_rpos)) => {} // TODO add this back assert_eq!(lpos, rpos),
                    (Some(lpos), None) => plan[lpos].push(rattr),
                    (None, Some(rpos)) => plan[rpos].push(lattr),
                    (None, None) => plan.push(vec![lattr, rattr]),
                }
            }
        }
    };
    traverse_left(root, &mut build_plan);
    plan
}

pub fn traverse_left<'a>(node: &'a TreeOp, func: &mut dyn FnMut(&'a TreeOp)) {
    if !node.children.is_empty() {
        traverse_left(&node.children[0], func);
    }
    func(node);
}

fn traverse_rlm<'a, T>(node: &'a TreeOp, func: &mut T, is_right_child: bool)
where
    T: FnMut(&'a TreeOp, bool),
{
    match node.children.len() {
        1 => traverse_rlm(&node.children[0], func, is_right_child),
        x if x > 1 => {
            for child_node in &node.children[1..] {
                traverse_rlm(child_node, func, true);
            }
            traverse_rlm(&node.children[0], func, false);
        }
        _ => {}
    }

    func(node, is_right_child);
}

pub fn to_materialize<'a>(root: &'a TreeOp) -> Vec<&'a TreeOp> {
    let mut nodes = vec![];
    let mut collect_reduce = |node: &'a TreeOp, is_right_child: bool| {
        if let Some(NodeAttr::Join(_attr)) = &node.attr {
            if is_right_child {
                nodes.push(node);
            }
        }
    };
    traverse_rlm(root, &mut collect_reduce, true);
    nodes
}

pub fn map_tables_to_node<'a>(root: &'a TreeOp, map: &mut HashMap<&'a str, &'a TreeOp>) {
    let mut collect_reduce = |node: &'a TreeOp| {
        if let Some(NodeAttr::Join(attr)) = &node.attr {
            for equalizer in &attr.equalizers {
                let lattr = &equalizer.left_attr;
                let rattr = &equalizer.right_attr;
                map.insert(lattr.table_name.as_str(), root);
                map.insert(rattr.table_name.as_str(), root);
            }
        }
    };
    inorder_traverse(root, &mut collect_reduce);
}
