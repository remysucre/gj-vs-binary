// use core::slice::SlicePattern;
use std::collections::{HashMap, HashSet};
use std::path;

use indexmap::{IndexMap, IndexSet};
use once_cell::sync::Lazy;
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    file::reader::{FileReader, SerializedFileReader},
    record::Field,
    schema::types::Type,
};

use std::fs::File;
use std::sync::{Arc, Mutex};

use crate::join::{Instruction, Instruction2, Lookup};
use crate::trie::Tb;
use crate::{
    sql::*,
    trie::{Trie, Value},
    *,
};

pub fn from_raw(rdb: &RawDB) -> DB {
    rdb.iter()
        .map(|(name, table)| {
            let t: HashMap<_, _> = table
                .iter()
                .map(|(attr, col)| {
                    let c: Vec<_> = col
                        .iter()
                        .map(|val| match val {
                            RawValue::Num(id) => Value::Num(*id),
                            RawValue::Str(s) => Value::Str(s.as_str()),
                        })
                        .collect();
                    (attr.clone(), c)
                })
                .collect();
            (name.clone(), t)
        })
        .collect()
}

pub fn debug_build_plans<'a>(
    build_plans: &IndexMap<&'a TreeOp, BuildPlan<'a>>,
    provides: &IndexMap<&'a TreeOp, Vec<Vec<Attribute>>>,
) {
    for p in build_plans {
        for (t, id_cols, data_cols) in p.1 {
            println!("PLAN");
            for col in id_cols {
                match col {
                    ColID::Name(s) => println!(" {} ", s),
                    ColID::Id(i) => {
                        if let TableID::Node(n) = t {
                            println!(" {:?} ", provides[n][*i]);
                        }
                    }
                }
            }
            for col in data_cols {
                match col {
                    ColID::Name(s) => println!(" {} ", s),
                    ColID::Id(i) => {
                        if let TableID::Node(n) = t {
                            println!(" {:?} ", provides[n][*i]);
                        }
                    }
                }
            }
        }
    }
}

pub fn compile_plan<'a>(
    _plan: &[Vec<&'a Attribute>],
    node: &TreeOp,
    // which tree node provides which table
    views: &HashMap<&str, &TreeOp>,
) -> Vec<Instruction> {
    let mut compiled_plan = Vec::new();
    // let mut table_ids = IndexSet::<&TreeOp>::new();
    let mut table_ids = HashMap::new();
    let mut view_ids = HashMap::new();
    let mut groups: Vec<HashSet<Attribute>> = vec![];

    let mut left_table = None;
    traverse_left(node, &mut |node: &TreeOp| {
        if let Some(NodeAttr::Join(join)) = &node.attr {
            assert_eq!(join.join_type, JoinType::Inner);
            assert_eq!(node.children.len(), 2);

            for eq in &join.equalizers {
                if left_table.is_none() {
                    left_table = Some(&eq.left_attr.table_name);
                    groups.push(HashSet::from([eq.left_attr.clone()]));
                } else if left_table.unwrap() == &eq.left_attr.table_name
                    && !groups.iter().any(|g| g.contains(&eq.left_attr))
                {
                    groups.push(HashSet::from([eq.left_attr.clone()]));
                }
            }
        }
    });

    traverse_left(node, &mut |node: &TreeOp| match &node.attr {
        Some(NodeAttr::Scan(_)) => {
            compiled_plan.push(Instruction::Scan);
        }
        Some(NodeAttr::Join(join)) => {
            assert_eq!(join.join_type, JoinType::Inner);
            assert_eq!(node.children.len(), 2);

            for eq in &join.equalizers {
                let l = groups.iter().position(|g| g.contains(&eq.left_attr));
                let r = groups.iter().position(|g| g.contains(&eq.right_attr));
                match (l, r) {
                    (Some(_), Some(_)) => (), // already accounted for
                    (None, Some(i)) => {
                        let len = table_ids.len() + view_ids.len();
                        let table_i = views
                            .get(eq.left_attr.table_name.as_str())
                            .map(|tree_op| view_ids.entry(tree_op).or_insert(len))
                            .unwrap_or_else(|| {
                                table_ids
                                    .entry(eq.left_attr.table_name.as_str())
                                    .or_insert(len)
                            });
                        compiled_plan.push(Instruction::Lookup(vec![Lookup {
                            key: i,
                            relation: *table_i,
                        }]));
                        groups[i].insert(eq.left_attr.clone());
                    }
                    (Some(i), None) => {
                        let len = table_ids.len() + view_ids.len();
                        let table_i = views
                            .get(eq.right_attr.table_name.as_str())
                            .map(|tree_op| view_ids.entry(tree_op).or_insert(len))
                            .unwrap_or_else(|| {
                                table_ids
                                    .entry(eq.right_attr.table_name.as_str())
                                    .or_insert(len)
                            });
                        compiled_plan.push(Instruction::Lookup(vec![Lookup {
                            key: i,
                            relation: *table_i,
                        }]));
                        groups[i].insert(eq.right_attr.clone());
                    }
                    (None, None) => {
                        // let li = table_ids.insert_full(&*node.children[0]).0;
                        // let ri = table_ids.insert_full(&*node.children[1]).0;

                        let len = table_ids.len() + view_ids.len();
                        let li = *views
                            .get(eq.left_attr.table_name.as_str())
                            .map(|tree_op| view_ids.entry(tree_op).or_insert(len))
                            .unwrap_or_else(|| {
                                table_ids
                                    .entry(eq.left_attr.table_name.as_str())
                                    .or_insert(len)
                            });

                        let len = table_ids.len() + view_ids.len();
                        let ri = *views
                            .get(eq.right_attr.table_name.as_str())
                            .map(|tree_op| view_ids.entry(tree_op).or_insert(len))
                            .unwrap_or_else(|| {
                                table_ids
                                    .entry(eq.right_attr.table_name.as_str())
                                    .or_insert(len)
                            });

                        compiled_plan.push(Instruction::Intersect {
                            relations: vec![li, ri],
                        });
                        let mut new_group = HashSet::new();
                        new_group.insert(eq.left_attr.clone());
                        new_group.insert(eq.right_attr.clone());
                        groups.push(new_group)
                    }
                }
            }
        }
        _ => (),
    });

    // dbg!(groups);

    compiled_plan.dedup_by(|a, b| a == b);
    compiled_plan
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum TableID<'a> {
    Name(String),
    Node(&'a TreeOp),
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum ColID {
    Id(usize),
    Name(String),
}

pub type ViewSchema = Vec<Vec<Attribute>>;

pub type BuildPlan<'a> = Vec<(TableID<'a>, Vec<ColID>, Vec<ColID>)>;

// pub fn compute_build_plan<'a>(
//     db: &DB,
//     root: &TreeOp,
//     provides: &IndexMap<&'a TreeOp, Vec<Vec<Attribute>>>,
//     in_view: &HashMap<&str, &'a TreeOp>,
// ) -> (ViewSchema, BuildPlan<'a>) {
//     let mut build_plan: IndexMap<TableID, IndexMap<ColID, Vec<Attribute>>> = IndexMap::new();

//     // traverse plan bottom up to collect table and column ordering
//     for attrs in plan {
//         for a in attrs {
//             let col_id;
//             let table_id;

//             if let Some(node) = in_view.get(a.table_name.as_str()) {
//                 table_id = TableID::Node(&**node);
//                 col_id = ColID::Id(
//                     provides[node]
//                         .iter()
//                         .position(|attrs| attrs.contains(a))
//                         .unwrap(),
//                 );
//             } else {
//                 table_id = TableID::Name(a.table_name.clone());
//                 col_id = ColID::Name(a.attr_name.clone());
//             };

//             build_plan
//                 .entry(table_id)
//                 .or_default()
//                 .insert(col_id, vec![]);
//         }
//     }

//     // collect data columns to the back of building order
//     // collect attributes attached to each column
//     // a view column can have more than 1 attributes if it was a join column
//     for (table_id, column_ids) in build_plan.iter_mut() {
//         match table_id {
//             TableID::Name(table_name) => {
//                 let table = &db[table_name];
//                 for attr in table.keys() {
//                     let cid = ColID::Name(attr.attr_name.clone());
//                     if !column_ids.contains_key(&cid) {
//                         column_ids.insert(cid, vec![attr.clone()]);
//                     }
//                 }
//             }
//             TableID::Node(node) => {
//                 let attr_sets = &provides[node];
//                 for (i, attrs) in attr_sets.iter().enumerate() {
//                     let cid = ColID::Id(i);
//                     if !column_ids.contains_key(&cid) {
//                         column_ids.insert(cid, attrs.to_vec());
//                     }
//                 }
//             }
//         }
//     }

//     // collect build plans into a table ordering, each with a column ordering
//     let mut build_plan_out = Vec::new();

//     for (t_id, col_id_map) in &build_plan {
//         let mut id_cols = Vec::new();
//         let mut data_cols = Vec::new();

//         for (col_id, attrs) in col_id_map {
//             if attrs.is_empty() {
//                 id_cols.push(col_id.clone());
//             } else {
//                 data_cols.push(col_id.clone());
//             }
//         }

//         build_plan_out.push((t_id.clone(), id_cols, data_cols));
//     }

//     // the output schema for this materialized view
//     let mut out_schema: Vec<Vec<Attribute>> = Vec::new();

//     let mut left_table = None;

//     // first pass: push all left table variables to the front
//     for attrs in plan {
//         if left_table.is_none() {
//             left_table = Some(&attrs[0].table_name);
//             out_schema.push(attrs.iter().copied().cloned().collect());
//         } else if attrs.iter().any(|a| &a.table_name == left_table.unwrap()) {
//             out_schema.push(attrs.iter().copied().cloned().collect());
//         }
//     }

//     // then push other variables in order

//     for attrs in plan {
//         if !attrs.iter().any(|a| &a.table_name == left_table.unwrap()) {
//             out_schema.push(attrs.iter().copied().cloned().collect());
//         }
//     }

//     // for attrs in plan {
//     //     out_schema.push(attrs.iter().copied().cloned().collect());
//     // }

//     // collect data columns from the build plan
//     for attrs in build_plan.values().flat_map(|m| m.values()) {
//         // join columns have empty attrs
//         if !attrs.is_empty() {
//             out_schema.push(attrs.to_vec());
//         }
//     }

//     (out_schema, build_plan_out)
// }

pub fn compute_full_plan<'a>(
    db: &DB,
    // plan: &[Vec<&Attribute>],
    root: &'a TreeOp,
    plan: &mut [Instruction2],
    provides: &IndexMap<&'a TreeOp, Vec<Vec<Attribute>>>,
    in_view: &HashMap<&str, &'a TreeOp>,
) -> (ViewSchema, BuildPlan<'a>) {
    let mut build_plan: IndexMap<TableID, IndexMap<ColID, Vec<Attribute>>> = IndexMap::new();

    let mut tables = IndexSet::<TableID>::new();
    let mut get_table_idx = |a: &Attribute| -> usize {
        // dbg!(a);
        let col_id;
        let table_id;

        if let Some(node) = in_view.get(a.table_name.as_str()) {
            table_id = TableID::Node(&**node);
            col_id = ColID::Id(
                provides[node]
                    .iter()
                    .position(|attrs| attrs.contains(a))
                    .unwrap_or_else(|| panic!("cannot find attribute: {:?}", a)),
            );
        } else {
            table_id = TableID::Name(a.table_name.clone());
            col_id = ColID::Name(a.attr_name.clone());
        };

        build_plan
            .entry(table_id.clone())
            .or_default()
            .insert(col_id, vec![]);

        let idx = tables.insert_full(table_id).0;
        idx - 1 // account for the scan table being present
    };

    let mut attr_positions = HashMap::<Attribute, usize>::new();
    for a in get_scan_join_attrs(root) {
        get_table_idx(&a);
        attr_positions.insert(a, attr_positions.len());
    }
    let mut n_vars = attr_positions.len();

    // traverse plan bottom up to collect table and column ordering
    for instruction in plan {
        match instruction {
            Instruction2::Intersect(relations) => {
                for r in relations {
                    attr_positions.insert(r.attr.clone(), n_vars);
                    r.relation = get_table_idx(&r.attr);
                }
                n_vars += 1;
            }
            Instruction2::Lookup(lookups) => {
                for lookup in lookups {
                    let i = attr_positions[&lookup.left];
                    attr_positions.insert(lookup.right.clone(), i);
                    lookup.key = i;
                    lookup.relation = get_table_idx(&lookup.right);
                }
            }
        }
    }

    // collect data columns to the back of building order
    // collect attributes attached to each column
    // a view column can have more than 1 attributes if it was a join column
    for (table_id, column_ids) in build_plan.iter_mut() {
        match table_id {
            TableID::Name(table_name) => {
                let table = &db[table_name];
                for attr in table.keys() {
                    let cid = ColID::Name(attr.attr_name.clone());
                    if !column_ids.contains_key(&cid) {
                        column_ids.insert(cid, vec![attr.clone()]);
                    }
                }
            }
            TableID::Node(node) => {
                let attr_sets = &provides[node];
                for (i, attrs) in attr_sets.iter().enumerate() {
                    let cid = ColID::Id(i);
                    if !column_ids.contains_key(&cid) {
                        column_ids.insert(cid, attrs.to_vec());
                    }
                }
            }
        }
    }

    // collect build plans into a table ordering, each with a column ordering
    let mut build_plan_out = Vec::new();

    for (t_id, col_id_map) in &build_plan {
        let mut id_cols = Vec::new();
        let mut data_cols = Vec::new();

        for (col_id, attrs) in col_id_map {
            if attrs.is_empty() {
                id_cols.push(col_id.clone());
            } else {
                data_cols.push(col_id.clone());
            }
        }

        build_plan_out.push((t_id.clone(), id_cols, data_cols));
    }

    // the output schema for this materialized view
    let mut out_schema: Vec<Vec<Attribute>> = Vec::new();

    for (attr, pos) in &attr_positions {
        if *pos >= out_schema.len() {
            out_schema.resize_with(pos + 1, Default::default);
        }
        out_schema[*pos].push(attr.clone());
    }

    // dbg!(&attr_positions);
    // dbg!(&out_schema);

    // let scan_attrs = get_scan_join_attrs(root);
    // let scan_data_attrs = db
    //     .get(&scan_attrs[0].table_name)
    //     .unwrap()
    //     .keys()
    //     .filter(|a| !scan_attrs.contains(&**a))
    //     .cloned();

    // // splice in data columns from scan table
    // let tail = out_schema.split_off(scan_attrs.len());
    // out_schema.extend(scan_data_attrs.into_iter().map(|a| vec![a]));
    // out_schema.extend(tail);

    // collect data columns from the build plan
    for attrs in build_plan.values().flat_map(|m| m.values()) {
        // join columns have empty attrs
        if !attrs.is_empty() {
            out_schema.push(attrs.to_vec());
        }
    }

    (out_schema, build_plan_out)
}

static RELATIONS: Lazy<Mutex<HashMap<String, RawRelation>>> = Lazy::new(Default::default);

pub fn load_db(args: &Args, q: &str, scan: &[&ScanAttr], plan: &[Vec<&Attribute>]) -> RawDB {
    let tables = scan
        .iter()
        .map(|s| s.table_name.as_str())
        .collect::<HashSet<_>>();

    let mut plan_table_name = HashMap::new();

    for node in plan {
        for a in node {
            let t_name = tables
                .get(a.table_name.as_str())
                .map(|s| &**s)
                .unwrap_or_else(|| find_shared(&a.table_name));
            let ns = plan_table_name.entry(t_name).or_insert(vec![]);

            if !ns.contains(&a.table_name.as_str()) {
                ns.push(a.table_name.as_str());
            }
        }
    }

    let mut db = RawDB::new();

    let mut loaded = HashSet::new();

    for attr in scan {
        for table_name in &plan_table_name[&attr.table_name.as_str()] {
            if loaded.contains(table_name) {
                continue;
            }

            println!("Loading {} to DB", table_name);

            let mut col_types = attr
                .attributes
                .iter()
                .map(|a| Arc::new(type_of(&a.attr_name)))
                .collect();

            let table_schema = Type::group_type_builder("duckdb_schema")
                .with_fields(&mut col_types)
                .build()
                .unwrap();

            let pq = if tables.contains(table_name) || !args.cache {
                from_parquet(q, table_name, table_schema)
            } else {
                // not in tables, so it's a base table that we can maybe cache
                let mut rels = RELATIONS.lock().unwrap();
                let mut key = table_name.to_string();
                key += &format!("{:?}", table_schema);
                rels.entry(key)
                    .and_modify(|_| println!("Using cached table {}", table_name))
                    // no schema, we want to load the full relation
                    .or_insert_with(|| from_parquet(q, table_name, table_schema))
                    .clone()
            };

            db.insert(table_name.to_string(), pq);

            loaded.insert(table_name);
        }
    }

    db
}

pub fn from_parquet(query: &str, t_name: &str, schema: Type) -> RawRelation {
    let mut table = HashMap::<Attribute, RawCol>::default();
    let path_s = format!(
        "../queries/preprocessed/join-order-benchmark/data/{}/{}.parquet",
        query, t_name
    );
    let path = path::Path::new(&path_s);
    let file = File::open(path)
        .or_else(|_| {
            let shared_name = find_shared(t_name);
            let path_s = format!("../data/imdb/{}.parquet", shared_name);
            let path = path::Path::new(&path_s);
            File::open(path)
        })
        .unwrap();

    let reader = SerializedFileReader::new(file).unwrap();

    let rows = reader.get_row_iter(Some(schema)).unwrap();

    for row in rows {
        if row.get_column_iter().any(|(_, f)| matches!(f, Field::Null)) {
            continue;
        }
        for (col_name, field) in row.get_column_iter() {
            match field {
                Field::Int(i) => {
                    let col = table
                        .entry(Attribute {
                            table_name: t_name.to_string(),
                            attr_name: col_name.to_string(),
                        })
                        .or_default();
                    col.push(RawValue::Num(*i));
                }
                Field::Str(s) => {
                    let col = table
                        .entry(Attribute {
                            table_name: t_name.to_string(),
                            attr_name: col_name.to_string(),
                        })
                        .or_default();
                    col.push(RawValue::Str(s.to_string()));
                }
                Field::Null => {
                    unreachable!("Null found when loading DB");
                }
                _ => {
                    panic!("Unsupported field type {:?}", field);
                }
            }
        }
    }
    Arc::new(table)
}

fn find_shared(table_name: &str) -> &str {
    match table_name.trim_end_matches(char::is_numeric) {
        "a" => "aka_name",
        "an" => "aka_name",
        "at" => "aka_title",
        "ci" => "cast_info",
        "chn" => "char_name",
        "cct" => "comp_cast_type",
        "cn" => "company_name",
        "ct" => "company_type",
        "cc" => "complete_cast",
        "it" => "info_type",
        "k" => "keyword",
        "kt" => "kind_type",
        "lt" => "link_type",
        "mc" => "movie_companies",
        "mi" => "movie_info",
        "miidx" => "movie_info_idx",
        "mi_idx" => "movie_info_idx",
        "mk" => "movie_keyword",
        "ml" => "movie_link",
        "n" => "name",
        "pi" => "person_info",
        "rt" => "role_type",
        "t" => "title",
        _ => panic!("unsupported table {}", table_name),
    }
}

// TODO lots of repetition, refactor this
pub fn build_tables<'a, 'b>(
    db: &'b DB<'a>,
    views: &HashMap<&TreeOp, Vec<Vec<Value<'a>>>>,
    plan: &BuildPlan,
) -> Vec<Tb<'a, 'b, Value<'a>>> {
    let mut tables = vec![];

    let (t_id, id_col_ids, data_col_ids) = &plan[0];

    if let TableID::Name(t) = t_id {
        let to_attr = |cid: &ColID| {
            if let ColID::Name(a) = cid {
                Attribute {
                    table_name: t.to_string(),
                    attr_name: a.to_string(),
                }
            } else {
                unreachable!("DB table must have named column IDs")
            }
        };

        let id_attrs: Vec<_> = id_col_ids.iter().map(to_attr).collect();
        let data_attrs: Vec<_> = data_col_ids.iter().map(to_attr).collect();
        let flat_table = build_flat_table(db, t, &id_attrs, &data_attrs);
        tables.push(Tb::Arr(flat_table));
    } else {
        unreachable!("Left table cannot be a view");
    }

    for (t_id, id_col_ids, data_col_ids) in &plan[1..] {
        match t_id {
            TableID::Name(t) => {
                let to_attr = |cid: &ColID| {
                    if let ColID::Name(a) = cid {
                        Attribute {
                            table_name: t.to_string(),
                            attr_name: a.to_string(),
                        }
                    } else {
                        unreachable!("DB table must have named column IDs")
                    }
                };

                let id_attrs: Vec<_> = id_col_ids.iter().map(to_attr).collect();
                let data_attrs: Vec<_> = data_col_ids.iter().map(to_attr).collect();
                let trie = build_trie_from_db(db, t, &id_attrs, &data_attrs);
                tables.push(Tb::Trie(trie));
            }
            TableID::Node(node) => {
                let to_id = |cid: &ColID| {
                    if let ColID::Id(i) = cid {
                        *i
                    } else {
                        unreachable!("View table must have usize column IDs")
                    }
                };

                let id_ids: Vec<_> = id_col_ids.iter().map(to_id).collect();
                let data_ids: Vec<_> = data_col_ids.iter().map(to_id).collect();
                let trie = build_trie_from_view(views, node, &id_ids, &data_ids);
                tables.push(Tb::Trie(trie));
            }
        }
    }

    tables
}

fn build_flat_table<'a, 'b>(
    db: &'b DB<'a>,
    table_name: &str,
    id_attrs: &[Attribute],
    data_attrs: &[Attribute],
) -> (Vec<&'b [Value<'a>]>, Vec<&'b [Value<'a>]>) {
    let rel = &db[table_name];
    let id_cols: Vec<_> = id_attrs
        .iter()
        .map(|a| rel.get(a).unwrap().as_slice())
        .collect();
    let data_cols: Vec<_> = data_attrs
        .iter()
        .map(|a| rel.get(a).unwrap().as_slice())
        .collect();
    (id_cols, data_cols)
}

fn build_trie_from_view<'a>(
    views: &HashMap<&TreeOp, Vec<Vec<Value<'a>>>>,
    node: &TreeOp,
    id_ids: &[usize],
    data_ids: &[usize],
) -> Trie<Value<'a>> {
    let mut trie = Trie::default();

    for row in views.get(node).unwrap() {
        let ids: Vec<_> = id_ids.iter().map(|i| row[*i].as_num()).collect();
        let data: Vec<_> = data_ids.iter().map(|i| row[*i].clone()).collect();
        trie.insert(&ids, data);
    }

    trie
}

fn build_trie_from_db<'a>(
    db: &DB<'a>,
    table_name: &str,
    id_attrs: &[Attribute],
    data_attrs: &[Attribute],
) -> Trie<Value<'a>> {
    let rel = &db[table_name];
    let id_cols: Vec<_> = id_attrs.iter().map(|a| rel.get(a).unwrap()).collect();
    let data_cols: Vec<_> = data_attrs.iter().map(|a| rel.get(a).unwrap()).collect();

    let mut trie = Trie::default();

    for i in 0..id_cols[0].len() {
        let ids: Vec<_> = id_cols.iter().map(|col| col[i].as_num()).collect();
        let data: Vec<_> = data_cols.iter().map(|col| col[i].clone()).collect();

        trie.insert(&ids, data);
    }

    trie
}

fn type_of(col: &str) -> Type {
    let (physical_type, converted_type) = if col.ends_with("id") || col.ends_with("year") {
        (PhysicalType::INT32, ConvertedType::INT_32)
    } else {
        (PhysicalType::BYTE_ARRAY, ConvertedType::UTF8)
    };
    Type::primitive_type_builder(col, physical_type)
        .with_converted_type(converted_type)
        .with_repetition(Repetition::OPTIONAL)
        .build()
        .unwrap()
}
