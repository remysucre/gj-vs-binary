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
            plan_table_name.insert(t_name, a.table_name.as_str());
        }
    }

    let mut db = DB::new();

    for attr in scan {
        let table_name = plan_table_name[&attr.table_name.as_str()];
        println!("Loading {}", table_name);

        let cols = &attr.attributes;
        let table = db.entry(table_name.to_string()).or_default();

        let mut col_types = vec![];

        for col in cols {
            if table.get(&col).is_none() {
                col_types.push(Arc::new(type_of(&col.attr_name)));
            }
        }

        let table_schema = Type::group_type_builder("duckdb_schema")
            .with_fields(&mut col_types)
            .build()
            .unwrap();

        from_parquet(table, q, table_name, table_schema);
    }

    db
}

pub fn from_parquet(table: &mut Relation, query: &str, t_name: &str, schema: Type) {
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

pub fn build_tables<'a>(
    db: &'a DB,
    materialized_columns: &'a [Vec<Value>],
    views: &HashMap<&TreeOp, HashMap<Attribute, usize>>,
    in_view: &'a HashMap<&'a str, &'a TreeOp>,
    plan: &'a [Vec<&'a Attribute>],
) -> (Vec<Tb<'a, Value>>, Vec<Vec<Attribute>>) {
    let mut tables = Vec::new();
    let mut id_cols = IndexMap::new();
    let mut data_cols = IndexMap::new();
    let mut out_vars = Vec::new();

    for node in plan {
        for a in node {
            let t = a.table_name.as_str();

            let col = in_view
                .get(t)
                .map(|tree_op| &materialized_columns[*views.get(tree_op).unwrap().get(a).unwrap()])
                .unwrap_or_else(|| db.get(t).unwrap().get(a).unwrap());

            id_cols
                .entry(t)
                .or_insert(IndexMap::new())
                .insert(&**a, col);
        }
    }

    for (&t, cols) in &id_cols {
        if let Some(tree_op) = in_view.get(t) {
            for (attr, data_col_idx) in views.get(tree_op).unwrap() {
                let data_col = &materialized_columns[*data_col_idx];
                if !cols.contains_key(attr) {
                    data_cols
                        .entry(t)
                        .or_insert(IndexMap::new())
                        .insert(attr.clone(), data_col);
                }
            }
        } else {
            for (attr, data_col) in db.get(t).unwrap() {
                if !cols.contains_key(attr) {
                    data_cols
                        .entry(t)
                        .or_insert(IndexMap::new())
                        .insert(attr.clone(), data_col);
                }
            }
        }
    }

    for cols in data_cols.values() {
        let vars = cols.keys().cloned().collect();
        out_vars.push(vars);
    }

    let start = Instant::now();

    let (trie_name, cols) = id_cols.first().unwrap();
    println!("building flat table on {}", trie_name);

    let mut ids = vec![];
    let mut data = vec![];

    for col in cols.values() {
        ids.push(&col[..])
    }

    if let Some(cols) = data_cols.get(trie_name) {
        for col in cols.values() {
            data.push(&col[..])
        }
    }

    tables.push(Tb::Arr((ids, data)));

    println!(
        "building {} takes {}s",
        trie_name,
        start.elapsed().as_secs_f32()
    );

    for (table_name, cols) in id_cols.iter().skip(1) {
        let start = Instant::now();
        println!("building trie on {}", table_name);

        let mut trie = Trie::default();

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

        tables.push(Tb::Trie(trie));

        println!(
            "building {} takes {}s",
            table_name,
            start.elapsed().as_secs_f32()
        );
    }
    (tables, out_vars)
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
