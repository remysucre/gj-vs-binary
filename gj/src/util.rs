use std::collections::{HashMap, HashSet};
use std::path;
use std::time::Instant;

use indexmap::IndexMap;
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    file::reader::{FileReader, SerializedFileReader},
    record::Field,
    schema::types::Type,
};

use std::fs::File;
use std::sync::Arc;

use crate::trie::Tb;
use crate::{
    sql::*,
    trie::{Trie, Value},
    *,
};

pub fn compile_gj_plan<'a>(
    plan: &[Vec<&'a Attribute>],
    payload: &[&'a Attribute],
    views: &HashMap<&str, &TreeOp>,
) -> (Vec<Vec<usize>>, Vec<usize>) {
    let mut compiled_plan = Vec::new();
    let mut compiled_payload = Vec::new();
    let mut table_ids = HashMap::new();
    let mut view_ids = HashMap::new();

    for node in plan {
        let mut node_ids = vec![];
        for a in node {
            let l = table_ids.len() + view_ids.len();

            let id = views
                .get(a.table_name.as_str())
                .map(|tree_op| view_ids.entry(tree_op).or_insert(l))
                .unwrap_or_else(|| table_ids.entry(a.table_name.as_str()).or_insert(l));

            if !node_ids.contains(id) {
                node_ids.push(*id);
            }

            if payload.iter().any(|b| a.table_name == b.table_name)
                && !compiled_payload.contains(id)
            {
                compiled_payload.push(*id);
            }
        }
        compiled_plan.push(node_ids);
    }
    (compiled_plan, compiled_payload)
}

// take the min of each attribute
// the result is in the form [[t1.c1, t1.c2, ...], [t2.c1, t2.c2, ...], ...]
pub fn aggregate_min(result: &mut Vec<Vec<Value>>, payload: &[&[Value]]) {
    if result.is_empty() {
        result.extend(payload.iter().map(|ss| ss.to_vec()));
    } else if payload.len() == result.len() {
        for (i, s) in payload.iter().enumerate() {
            for (j, v) in s.iter().enumerate() {
                if result[i][j] > *v {
                    result[i][j] = v.clone();
                }
            }
        }
    }
}

pub fn load_db(q: &str, scan: &[&ScanAttr], plan: &[Vec<&Attribute>]) -> DB {
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

    let mut db = DB::new();

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

            db.insert(
                table_name.to_string(),
                from_parquet(q, table_name, table_schema),
            );

            loaded.insert(table_name);
        }
    }

    db
}

pub fn from_parquet(query: &str, t_name: &str, schema: Type) -> Relation {
    let mut table = Relation::default();
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
                    col.push(Value::Num(*i));
                }
                Field::Str(s) => {
                    let col = table
                        .entry(Attribute {
                            table_name: t_name.to_string(),
                            attr_name: col_name.to_string(),
                        })
                        .or_default();
                    col.push(Value::Str(s.to_string()));
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
    table
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Tid<'a> {
    Name(&'a str),
    Node(&'a TreeOp),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Cid<'a> {
    Idx(usize),
    Attr(&'a Attribute),
}

pub fn build_ts<'a>(
    db: &'a DB,
    materialized_columns: &'a [Vec<Value>],
    views: &'a HashMap<&'a TreeOp, HashMap<Attribute, usize>>,
    in_view: &'a HashMap<&'a str, &'a TreeOp>,
    plan: &'a [Vec<&'a Attribute>],
) -> (Vec<Tb<'a, Value>>, Vec<Vec<Vec<Attribute>>>) {
    let mut tables = Vec::new();
    let mut id_cols = IndexMap::new();
    let mut data_cols = IndexMap::new();
    let mut out_vars = IndexMap::new();

    for node in plan {
        for a in node {
            let t = a.table_name.as_str();

            if let Some(tree_op) = in_view.get(t) {
                let tid = Tid::Node(tree_op);
                let idx = *views[tree_op].get(a).unwrap();
                let col = &materialized_columns[idx];

                id_cols
                    .entry(tid)
                    .or_insert(IndexMap::new())
                    .insert(Cid::Idx(idx), col);
            } else {
                let tid = Tid::Name(t);
                let col = &db[t][a];
                id_cols
                    .entry(tid)
                    .or_insert(IndexMap::new())
                    .insert(Cid::Attr(a), col);
            }
        }
    }

    for (t, cols) in &id_cols {
        match t {
            Tid::Node(tree_op) => {
                for (attr, data_col_idx) in views.get(tree_op).unwrap() {
                    let data_col = &materialized_columns[*data_col_idx];
                    if !cols.contains_key(&Cid::Idx(*data_col_idx)) {
                        data_cols
                            .entry(t.clone())
                            .or_insert(IndexMap::new())
                            .insert(Cid::Idx(*data_col_idx), data_col);
                        out_vars
                            .entry(t.clone())
                            .or_insert(IndexMap::new())
                            .entry(Cid::Idx(*data_col_idx))
                            .or_insert(vec![])
                            .push(attr.clone());
                    }
                }
            }
            Tid::Name(t_name) => {
                for (attr, data_col) in db.get(*t_name).unwrap() {
                    if !cols.contains_key(&Cid::Attr(attr)) {
                        data_cols
                            .entry(t.clone())
                            .or_insert(IndexMap::new())
                            .insert(Cid::Attr(attr), data_col);
                        out_vars
                            .entry(t.clone())
                            .or_insert(IndexMap::new())
                            .entry(Cid::Attr(attr))
                            .or_insert(vec![])
                            .push(attr.clone());
                    }
                }
            }
        }
    }

    let start = Instant::now();

    let (trie_name, cols) = id_cols.first().unwrap();
    if let Tid::Name(s) = trie_name {
        println!("building flat table on {}", s);
    } else {
        println!("building flat table on intermediate");
    }

    let d_cols: Vec<_> = data_cols
        .get(trie_name)
        .map(|cs| cs.values().collect::<Vec<_>>())
        .unwrap_or_default();

    let mut ids = Vec::with_capacity(cols.len());
    let mut data = Vec::with_capacity(d_cols.len());

    for col in cols.values() {
        ids.push(&col[..])
    }

    for d_col in d_cols {
        data.push(&d_col[..])
    }

    tables.push(Tb::Arr((ids, data)));

    println!("building table takes {}s", start.elapsed().as_secs_f32());

    for (table_name, cols) in id_cols.iter().skip(1) {
        let start = Instant::now();
        if let Tid::Name(s) = table_name {
            println!("building table on {}", s);
        } else {
            println!("building table on intermediate");
        }

        let mut trie = Trie::default();

        let d_cols: Vec<_> = data_cols
            .get(table_name)
            .map(|cs| cs.values().collect::<Vec<_>>())
            .unwrap_or_default();

        let id_len = cols.len();
        let d_len = d_cols.len();

        for i in 0..cols[0].len() {
            let mut ids = Vec::with_capacity(id_len);
            let mut data = Vec::with_capacity(d_len);

            for col in cols.values() {
                ids.push(col[i].as_num());
            }

            for col in &d_cols {
                data.push(&col[i]);
            }

            trie.insert(&ids, data);
        }

        tables.push(Tb::Trie(trie));

        println!("building table takes {}", start.elapsed().as_secs_f32());
    }
    let vars = out_vars
        .into_values()
        .map(|cols| cols.into_values().collect())
        .collect();

    (tables, vars)
}

pub fn build_tables<'a>(
    db: &'a DB,
    materialized_columns: &'a [Vec<Value>],
    views: &'a HashMap<&'a TreeOp, HashMap<Attribute, usize>>,
    in_view: &'a HashMap<&'a str, &'a TreeOp>,
    plan: &'a [Vec<&'a Attribute>],
) -> (Vec<Tb<'a, Value>>, Vec<Vec<Vec<Attribute>>>) {
    let mut tables = Vec::new();
    let mut id_cols = IndexMap::new();
    let mut data_cols = IndexMap::new();
    let mut out_vars = IndexMap::new();

    for node in plan {
        for a in node {
            let t = a.table_name.as_str();

            if let Some(tree_op) = in_view.get(t) {
                let tid = Tid::Node(tree_op);
                let idx = *views[tree_op].get(a).unwrap();
                let col = &materialized_columns[idx];

                id_cols
                    .entry(tid)
                    .or_insert(IndexMap::new())
                    .insert(Cid::Idx(idx), col);
            } else {
                let tid = Tid::Name(t);
                let col = &db[t][a];
                id_cols
                    .entry(tid)
                    .or_insert(IndexMap::new())
                    .insert(Cid::Attr(a), col);
            }
        }
    }

    for (t, cols) in &id_cols {
        match t {
            Tid::Node(tree_op) => {
                for (attr, data_col_idx) in views.get(tree_op).unwrap() {
                    let data_col = &materialized_columns[*data_col_idx];
                    if !cols.contains_key(&Cid::Idx(*data_col_idx)) {
                        data_cols
                            .entry(t.clone())
                            .or_insert(IndexMap::new())
                            .insert(Cid::Idx(*data_col_idx), data_col);
                        out_vars
                            .entry(t.clone())
                            .or_insert(IndexMap::new())
                            .entry(Cid::Idx(*data_col_idx))
                            .or_insert(vec![])
                            .push(attr.clone());
                    }
                }
            }
            Tid::Name(t_name) => {
                for (attr, data_col) in db.get(*t_name).unwrap() {
                    if !cols.contains_key(&Cid::Attr(attr)) {
                        data_cols
                            .entry(t.clone())
                            .or_insert(IndexMap::new())
                            .insert(Cid::Attr(attr), data_col);
                        out_vars
                            .entry(t.clone())
                            .or_insert(IndexMap::new())
                            .entry(Cid::Attr(attr))
                            .or_insert(vec![])
                            .push(attr.clone());
                    }
                }
            }
        }
    }

    let start = Instant::now();

    let (trie_name, cols) = id_cols.first().unwrap();
    if let Tid::Name(s) = trie_name {
        println!("building flat table on {}", s);
    } else {
        println!("building flat table on intermediate");
    }

    let d_cols: Vec<_> = data_cols
        .get(trie_name)
        .map(|cs| cs.values().collect::<Vec<_>>())
        .unwrap_or_default();

    let mut ids = Vec::with_capacity(cols.len());
    let mut data = Vec::with_capacity(d_cols.len());

    for col in cols.values() {
        ids.push(&col[..])
    }

    for d_col in d_cols {
        data.push(&d_col[..])
    }

    tables.push(Tb::Arr((ids, data)));

    println!("building table takes {}s", start.elapsed().as_secs_f32());

    for (table_name, cols) in id_cols.iter().skip(1) {
        let start = Instant::now();
        if let Tid::Name(s) = table_name {
            println!("building table on {}", s);
        } else {
            println!("building table on intermediate");
        }

        let mut trie = Trie::default();

        let d_cols: Vec<_> = data_cols
            .get(table_name)
            .map(|cs| cs.values().collect::<Vec<_>>())
            .unwrap_or_default();

        let id_len = cols.len();
        let d_len = d_cols.len();

        for i in 0..cols[0].len() {
            let mut ids = Vec::with_capacity(id_len);
            let mut data = Vec::with_capacity(d_len);

            for col in cols.values() {
                ids.push(col[i].as_num());
            }

            for col in &d_cols {
                data.push(&col[i]);
            }

            trie.insert(&ids, data);
        }

        tables.push(Tb::Trie(trie));

        println!("building table takes {}", start.elapsed().as_secs_f32());
    }
    let vars = out_vars
        .into_values()
        .map(|cols| cols.into_values().collect())
        .collect();

    (tables, vars)
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
