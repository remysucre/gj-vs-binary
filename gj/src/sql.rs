use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    pub table_name: String,
    pub attr_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Equalizer {
    left_attr: Attribute,
    right_attr: Attribute,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinAttr {
    join_type: JoinType,
    equalizers: Vec<Equalizer>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ScanAttr {
    pub table_name: String,
    pub attributes: Vec<Attribute>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProjectAttr {
    columns: Vec<Attribute>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum NodeAttr {
    Join(JoinAttr),
    Scan(ScanAttr),
    Project(ProjectAttr),
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
    pub default_func: &'a dyn Fn() -> RR,
}

pub fn traverse<R, S, T>(node: &TreeOp, traverse_funcs: &TraverseFuncs<R, S, T>) -> T {
    let map_result: R = (traverse_funcs.map_func)(node);
    let reduce_result: S = node
        .children
        .iter()
        .map(|child_node| traverse(child_node, traverse_funcs))
        .fold((traverse_funcs.default_func)(), |a: S, b: T| -> S {
            (traverse_funcs.reduce_func)(a, b)
        });
    (traverse_funcs.combine_func)(map_result, reduce_result)
}

pub fn preorder_traverse_mut<T>(node: &mut TreeOp, func: &mut T)
where
    T: FnMut(&mut TreeOp),
{
    func(node);
    for child_node in node.children.iter_mut() {
        preorder_traverse_mut(child_node, func);
    }
}

pub fn postorder_traverse_mut<T>(node: &mut TreeOp, func: &mut T)
where
    T: FnMut(&mut TreeOp),
{
    for child_node in node.children.iter_mut() {
        postorder_traverse_mut(child_node, func);
    }
    func(node);
}

pub fn parse_tree_extra_info(root: &mut TreeOp) {
    let mut parse_func = |node: &mut TreeOp| match node.name.as_str() {
        "HASH_JOIN" => {
            let extra_info: Vec<_> = node
                .extra_info
                .split('\n')
                .filter(|s| !s.is_empty())
                .collect();

            let join_type = match &extra_info[0] {
                &"INNER" => JoinType::Inner,
                _ => panic!("Fail to parse Join Type {}", extra_info[0]),
            };

            let mut equalizers = Vec::new();

            for pred in &extra_info[1..] {
                let equalizer = pred.split('=').map(|s| s.trim()).collect::<Vec<_>>();
                let left_attr = equalizer[0].split('.').map(|s| s.trim()).collect::<Vec<_>>();
                let right_attr = equalizer[1].split('.').map(|s| s.trim()).collect::<Vec<_>>();
                equalizers.push(Equalizer {
                    left_attr: Attribute {
                        table_name: left_attr[0].to_string(),
                        attr_name: left_attr[1].to_string(),
                    },
                    right_attr: Attribute {
                        table_name: right_attr[0].to_string(),
                        attr_name: right_attr[1].to_string(),
                    },
                });
            }

            node.attr = Some(NodeAttr::Join(JoinAttr {
                join_type,
                equalizers,
            }));
        }
        "SEQ_SCAN" => {
            let extra_info = node.extra_info.replace("[INFOSEPARATOR]", "");
            let info_strs: Vec<_> = extra_info.split('\n').filter(|s| !s.is_empty()).collect();

            let table_name: String = info_strs.first().expect("Failed to Get Table").to_string();
            node.attr = Some(NodeAttr::Scan(ScanAttr {
                table_name,
                attributes: info_strs[1..].iter().map(|s| {
                    let attr_strs: Vec<_> = s.split('.').map(|s| s.trim()).collect();
                    Attribute {
                        table_name: attr_strs[0].to_string(),
                        attr_name: attr_strs[1].to_string(),
                    }
                }).collect(),
            }));
        }
        "PROJECTION" => {
            let columns: Vec<_> = node
                .extra_info
                .split('\n')
                .filter(|s| !s.is_empty())
                .map(|s| {
                    let names: Vec<_> = s.split('.').map(|s| s.trim()).collect();
                    Attribute {
                        table_name: names[0].to_string(),
                        attr_name: names[1].to_string(),
                    }
                })
                .collect();
            node.attr = Some(NodeAttr::Project(ProjectAttr { columns }));
        }
        _ => (),
    };
    preorder_traverse_mut(root, &mut parse_func);
}

pub fn to_gj_plan(root: &mut TreeOp) -> (Vec<ScanAttr>, Vec<Vec<Attribute>>, Vec<Attribute>) {
    let mut scan: Vec<ScanAttr> = vec![];
    let mut plan: Vec<Vec<Attribute>> = vec![];
    let mut payload: Vec<Attribute> = vec![];
 
    let mut get_plan = |node: &mut TreeOp| {
        match &node.attr {
            Some(NodeAttr::Join(attr)) => {
                for equalizer in &attr.equalizers {
                    let lattr = &equalizer.left_attr;
                    let rattr = &equalizer.right_attr;
        
                    // Find in plan the index of vector which contains attr_name;
                    let lpos_opt = plan.iter().position(|x| x.contains(&lattr));
                    let rpos_opt = plan.iter().position(|x| x.contains(&rattr));
        
                    // We have four cases and enumerate
                    match (lpos_opt, rpos_opt) {
                        (Some(lpos), Some(rpos)) => assert_eq!(lpos, rpos),
                        (Some(lpos), None) => plan[lpos].push(rattr.to_owned()),
                        (None, Some(rpos)) => plan[rpos].push(lattr.to_owned()),
                        (None, None) => plan.push(vec![lattr.to_owned(), rattr.to_owned()]),
                    }
                }
            }
            Some(NodeAttr::Project(cols)) => {
                payload.extend_from_slice(&cols.columns);
            }
            Some(NodeAttr::Scan(attr)) => {
                scan.push(attr.clone());
            }
            _ => (),
        }
    };

    postorder_traverse_mut(root, &mut get_plan);

    (scan, plan, payload)
}
