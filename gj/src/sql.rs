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
    pub cardinality: u32,
    pub extra_info: String,
    pub children: Vec<Box<TreeOp>>,
    pub attr: Option<NodeAttr>,
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
                // TODO remove this hack
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
                    // TODO remove this hack
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
                    let lpos_opt = plan.iter().position(|x| x.contains(lattr));
                    let rpos_opt = plan.iter().position(|x| x.contains(rattr));

                    // We have four cases and enumerate
                    match (lpos_opt, rpos_opt) {
                        (Some(_lpos), Some(_rpos)) => {} // TODO add this back assert_eq!(lpos, rpos),
                        (Some(lpos), None) => plan[lpos].push(rattr.to_owned()),
                        (None, Some(rpos)) => plan[rpos].push(lattr.to_owned()),
                        (None, None) => plan.push(vec![lattr.to_owned(), rattr.to_owned()]),
                    }
                }
            }
            Some(NodeAttr::Project(cols)) => {
                for mut a in cols.columns.iter().cloned() {
                    if !a.table_name.is_empty() {
                        if a.attr_name == "\"name\"" {
                            a.attr_name = "name".to_string();
                        }
                        payload.push(a);
                    }
                }
                // payload.extend_from_slice(&cols.columns);
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
