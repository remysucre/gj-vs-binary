use std::path;
use std::{collections::HashMap, error::Error};
use std::time::Instant;

use indexmap::IndexMap;
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    file::reader::{FileReader, SerializedFileReader},
    record::Field,
    schema::types::Type,
};

use std::sync::Arc;
use std::{fs, fs::File};

use crate::trie::Table;
use crate::{
    sql::*,
    trie::{Trie, Value},
    *,
};

pub type Plan = Vec<Vec<Attribute>>;
pub type Payload = Vec<Attribute>;

pub fn sql_to_gj(file_name: &str) -> Result<(Vec<ScanAttr>, Plan, Payload), Box<dyn Error>> {
    let sql = fs::read_to_string(path::Path::new(file_name))?;
    let mut root: TreeOp = serde_json::from_str(sql.as_str())?;
    parse_tree_extra_info(&mut root);
    Ok(to_gj_plan(&mut root))
}

// compile a plan (a list of multiway joins) into a list of trie indices,
// where the trie with index i is stored at position i by load_db.
pub fn compile_plan(
    plan: &[Vec<Attribute>],
    payload: &[Attribute],
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
            if let Some(pos) = payload.iter().position(|b| a.table_name == b.table_name) {
                if !compiled_payload.contains(&pos) {
                    compiled_payload.push(pos);
                }
            }
        }
        compiled_plan.push(node_ids);
    }
    (compiled_plan, compiled_payload)
}

// take the min of each attribute
// the result is in the form [[t1.c1, t1.c2, ...], [t2.c1, t2.c2, ...], ...]
pub fn aggregate_min(result: &mut Vec<Vec<Value>>, payload: &[&[Value]]) {
    // println!("aggregate_min");
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

pub fn load_db_mut(db: &mut DB, q: &str, scan: &[ScanAttr]) {
    println!("Query: {}", q);
    for attr in scan {
        let table_name = &attr.table_name;
        let cols = &attr.attributes;
        println!("Loading {}", table_name);
        // if !is_shared(table_name) {
        // TODO figure out how to handle nulls when loading lazily
        db.remove(table_name);
        // }
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
}

pub fn from_parquet(table: &mut Relation, file_path: &str, schema: Type) {
    let path = path::Path::new(file_path);
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();

    // println!("{:?}", schema);

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
                    let col =
                        table
                            .entry(col_name.to_string())
                            .or_insert(if col_name.ends_with("id") {
                                Col::IdCol(vec![])
                            } else {
                                Col::DataCol(vec![])
                            });
                    // TODO very ugly code
                    if let Col::IdCol(ref mut v) = col {
                        v.push(*i);
                    } else if let Col::DataCol(ref mut v) = col {
                        v.push(Value::Num(*i));
                    } else {
                        panic!("unexpected column type");
                    }
                }
                Field::Str(s) => {
                    let col = table
                        .entry(col_name.to_string())
                        .or_insert(Col::DataCol(vec![]));
                    if let Col::DataCol(ref mut v) = col {
                        v.push(Value::Str(s.to_string()));
                    } else {
                        panic!("expected str col");
                    }
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

pub fn build_tables<'a>(db: &'a DB, plan: &'a [Vec<Attribute>], payload: &'a [Attribute]) -> Vec<Table<'a, Value>> {
    let mut tables = Vec::new();
    let mut columns = IndexMap::new();

    for node in plan {
        for a in node {
            let trie_name = a.table_name.as_str();
            let mut table_name = a.table_name.as_str();
            if !db.contains_key(table_name) {
                table_name = find_shared(table_name);
            }
            let col_name = &a.attr_name;
            let col = db[table_name].get(col_name).unwrap();
            columns
                .entry(trie_name.to_string())
                .or_insert(vec![])
                .push(col);
        }
    }

    for a in payload {
        let trie_name = a.table_name.as_str();
        let mut table_name = a.table_name.as_str();
        if !db.contains_key(table_name) {
            table_name = find_shared(table_name);
        }
        let col_name = &a.attr_name;
        // println!("table {} column {}", table_name, col_name);
        columns
            .entry(trie_name.to_string())
            .or_insert(vec![])
            .push(&db.get(table_name).unwrap()[col_name]);
    }

    for (i, (table_name, cols)) in columns.iter().enumerate() {
        let start = Instant::now();
        if i == 0 {
            println!("building flat table on {}", table_name);
            let mut ids = vec![];
            let mut data = vec![];
            for col in cols {
                match col {
                    Col::IdCol(ref v) => {
                        ids.push(&v[..]);
                    }
                    Col::DataCol(ref v) => {
                        data.push(&v[..]);
                    }
                }
            }
            tables.push(Table::Arr((ids, data)));
        } else {
            let mut trie = Trie::default();
            for i in 0..cols[0].len() {
                let mut ids = Vec::new();
                let mut data = Vec::new();
                for col in cols {
                    match col {
                        Col::IdCol(ref v) => {
                            ids.push(v[i]);
                        }
                        Col::DataCol(ref v) => {
                            data.push(v[i].clone());
                        }
                    }
                }
                trie.insert(&ids, data);
            }
            tables.push(Table::Trie(trie));
        }
        println!("building {} takes {}s", table_name, start.elapsed().as_secs_f32());
    }

    tables
}

pub fn build_tries<'a>(db: &'a DB, plan: &'a [Vec<Attribute>], payload: &'a [Attribute]) -> Vec<Trie<Value>> {
    let mut tries = Vec::new();
    let mut columns = IndexMap::new();

    for node in plan {
        for a in node {
            let trie_name = a.table_name.as_str();
            let mut table_name = a.table_name.as_str();
            if !db.contains_key(table_name) {
                table_name = find_shared(table_name);
            }
            let col_name = &a.attr_name;
            let col = db[table_name].get(col_name).unwrap();
            columns
                .entry(trie_name.to_string())
                .or_insert(vec![])
                .push(col);
        }
    }

    for a in payload {
        let trie_name = a.table_name.as_str();
        let mut table_name = a.table_name.as_str();
        if !db.contains_key(table_name) {
            table_name = find_shared(table_name);
        }
        let col_name = &a.attr_name;
        // println!("table {} column {}", table_name, col_name);
        columns
            .entry(trie_name.to_string())
            .or_insert(vec![])
            .push(&db.get(table_name).unwrap()[col_name]);
    }

    for (table_name, cols) in columns {
        let start = Instant::now();
        let mut trie = Trie::default();
        for i in 0..cols[0].len() {
            let mut ids = Vec::new();
            let mut data = Vec::new();
            for col in &cols {
                match col {
                    Col::IdCol(ref v) => {
                        ids.push(v[i]);
                    }
                    Col::DataCol(ref v) => {
                        data.push(v[i].clone());
                    }
                }
            }
            trie.insert(&ids, data);
        }
        tries.push(trie);
        println!("building {} takes {}s", table_name, start.elapsed().as_secs_f32());
    }

    tries
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
