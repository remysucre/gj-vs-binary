use std::collections::HashMap;
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

use crate::{
    // join::, sj},
    trie::{Table, Tb},
};
use crate::{
    sql::*,
    trie::{Trie, Value},
    *,
};

pub fn compile_bushy_plan(
    plan: &[Vec<&Attribute>],
    views: &HashMap<&str, &TreeOp>,
) -> Vec<Vec<usize>> {
    let mut compiled_plan = Vec::new();
    let mut table_ids = HashMap::new();
    let mut view_ids = HashMap::new();

    for node in plan {
        let mut node_ids = vec![];
        for a in node {
            let l = table_ids.len() + view_ids.len();

            if let Some(tree_op) = views.get(a.table_name.as_str()) {
                let id = view_ids.entry(tree_op).or_insert(l);
                if !node_ids.contains(id) {
                    node_ids.push(*id);
                }
            } else {
                let id = table_ids.entry(a.table_name.as_str()).or_insert(l);
                if !node_ids.contains(id) {
                    node_ids.push(*id);
                }
            }
        }
        compiled_plan.push(node_ids);
    }
    compiled_plan
}

pub fn compile_gj_plan(
    plan: &[Vec<&Attribute>],
    payload: &[&Attribute],
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
                .map(|tree_op| {
                    view_ids.entry(tree_op).or_insert(l)
                })
                .unwrap_or_else(|| {
                    table_ids.entry(a.table_name.as_str()).or_insert(l)
                });
            
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

pub fn compile_plan(
    plan: &[Vec<&Attribute>],
    payload: &[&Attribute],
) -> (Vec<Vec<usize>>, Vec<usize>) {
    let mut compiled_plan = Vec::new();
    let mut compiled_payload = Vec::new();
    let mut table_ids = HashMap::new();

    for node in plan {
        let mut node_ids = Vec::new();
        for a in node {
            let l = table_ids.len();
            let id = table_ids.entry(a.table_name.clone()).or_insert(l);
            node_ids.push(*id);

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

fn is_shared(table_name: &str) -> bool {
    matches!(
        table_name,
        "aka_name"
            | "aka_title"
            | "cast_info"
            | "char_name"
            | "comp_cast_type"
            | "company_name"
            | "company_type"
            | "complete_cast"
            | "info_type"
            | "keyword"
            | "kind_type"
            | "link_type"
            | "movie_companies"
            | "movie_info"
            | "movie_info_idx"
            | "movie_keyword"
            | "movie_link"
            | "name"
            | "person_info"
            | "role_type"
            | "title"
    )
}

pub fn clean_db(db: &mut DB) {
    db.retain(|t, _| is_shared(t));
}

pub fn load_db(q: &str, scan: &[&ScanAttr]) -> DB {
    println!("Query: {}", q);
    let mut db = DB::new();
    for attr in scan {
        let table_name = &attr.table_name;
        let cols = &attr.attributes;
        println!("Loading {}", table_name);
        let table = db.entry(table_name.to_string()).or_default();
        let mut col_types = vec![];

        for col in cols {
            if table.get(&col.attr_name).is_none() {
                col_types.push(Arc::new(type_of(&col.attr_name)));
            }
        }
        let table_schema = Type::group_type_builder("duckdb_schema")
            .with_fields(&mut col_types)
            .build()
            .unwrap();

        let file_name = if is_shared(table_name) {
            format!("../data/imdb/{}.parquet", table_name)
        } else {
            format!(
                "../queries/preprocessed/join-order-benchmark/data/{}/{}.parquet",
                q, table_name
            )
        };
        from_parquet(table, &file_name, table_schema);
    }
    db
}

pub fn from_parquet(table: &mut Relation, file_path: &str, schema: Type) {
    let path = path::Path::new(file_path);
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();

    let rows = reader.get_row_iter(Some(schema)).unwrap();

    // NOTE this is awkward. Ideally we want to load column by column,
    // but the ColumnReader API is lacking.
    // changing to use Data Fusion's RecordBatch API may be cleaner.
    for row in rows {
        // check if row has nulls TODO handle this carefully
        if row.get_column_iter().any(|(_, f)| matches!(f, Field::Null)) {
            continue;
        }
        for (col_name, field) in row.get_column_iter() {
            match field {
                Field::Int(i) => {
                    let col = table.entry(col_name.to_string()).or_default();
                    col.push(Value::Num(*i));
                }
                Field::Str(s) => {
                    let col = table.entry(col_name.to_string()).or_default();
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

pub fn build_ts<'a>(
    db: &'a DB, 
    views: &'a Views<'a>, 
    in_view: &'a HashMap<&'a str, &'a TreeOp>, 
    plan: &'a [Vec<&'a Attribute>]
) -> Vec<Table<'a, Value>> {

    let mut tables = Vec::new();
    let mut id_cols = IndexMap::new();
    let mut data_cols = IndexMap::new();

    for node in plan {
        for a in node {
            let trie_name = a.table_name.as_str();
            let mut table_name = a.table_name.as_str();

            if let Some(tree_op) = in_view.get(table_name) {
                id_cols
                    .entry(trie_name)
                    .or_insert(IndexMap::new())
                    .insert(&**a, &views.get(tree_op).unwrap()[&a]);
            } else {
                if !db.contains_key(table_name) {
                    table_name = find_shared(table_name);
                }

                id_cols
                    .entry(trie_name)
                    .or_insert(IndexMap::new())
                    .insert(&**a, &db.get(table_name).unwrap()[&a.attr_name]);
            }
        }
    }

    for (&trie_name, cols) in &id_cols {
        let mut table_name = trie_name;

        if let Some(tree_op) = in_view.get(table_name) {
            for (attr, data_col) in &views[tree_op] {
                if !cols.contains_key(attr) {
                    data_cols
                        .entry(trie_name)
                        .or_insert(IndexMap::new())
                        .insert(attr.clone(), data_col);
                }
            }
        } else {
            if !db.contains_key(table_name) {
                table_name = find_shared(table_name);
            }
    
            for (attr, data_col) in &db[table_name] {
                let a = Attribute {
                    table_name: table_name.to_string(),
                    attr_name: attr.to_string(),
                };
                if !cols.contains_key(&a) {
                    data_cols
                        .entry(trie_name)
                        .or_insert(IndexMap::new())
                        .insert(a, data_col);
                }
            }
        }

    }

    let start = Instant::now();

    let (trie_name, cols) = id_cols.first().unwrap();
    println!("building flat table on {}", trie_name);

    let mut ids = vec![];
    let mut data = vec![];
    let mut vs = vec![];

    // for (&col_name, col) in cols {
    //     vs.push(col_name.as_str());
    //     ids.push(&col[..])
    // }

    if let Some(cols) = data_cols.get(trie_name) {
        for (col_name, col) in cols {
            vs.push((*col_name).clone());
            data.push(&col[..])
        }
    }

    tables.push(Table {
        schema: vs,
        data: Tb::Arr((ids, data)),
    });

    println!(
        "building {} takes {}s",
        trie_name,
        start.elapsed().as_secs_f32()
    );

    for (table_name, cols) in id_cols.iter().skip(1) {
        let start = Instant::now();
        println!("building trie on {}", table_name);

        let mut trie = Trie::default();
        let mut vs = vec![];

        // for (&col_name, _col) in cols {
        //     vs.push(col_name.as_str());
        // }

        // if let Some(cols) = data_cols.get(table_name) {
        //     for (&col_name, _col) in cols {
        //         vs.push(col_name.as_str());
        //     }
        // }

        for i in 0..cols[0].len() {
            let mut ids = Vec::new();
            let mut data = Vec::new();

            for (_col_name, col) in cols {
                ids.push(col[i].as_num());
            }

            if let Some(cols) = data_cols.get(table_name) {
                for (_col_name, col) in cols {
                    data.push(col[i].clone());
                }
            }

            trie.insert(&ids, data);
        }

        tables.push(Table {
            schema: vs,
            data: Tb::Trie(trie),
        });

        println!(
            "building {} takes {}s",
            table_name,
            start.elapsed().as_secs_f32()
        );
    }
    tables
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
