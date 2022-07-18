use std::path;
use std::{collections::HashMap, error::Error};

use indexmap::IndexMap;
use parquet::{
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    file::reader::{FileReader, SerializedFileReader},
    record::Field,
    schema::types::Type,
};

use std::sync::Arc;
use std::{fs, fs::File};

use crate::{sql::*, trie::Trie};

pub fn sql_to_gj(file_name: &str) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    let sql = fs::read_to_string(path::Path::new(file_name))?;
    let mut root: TreeOp = serde_json::from_str(sql.as_str())?;
    Ok(to_gj_plan(&mut root))
}

// compile a plan (a list of multiway joins) into a list of trie indices,
// where the trie with index i is stored at position i by load_db.
pub fn compile_plan(plan: &[Vec<String>]) -> Vec<Vec<usize>> {
    let mut compiled_plan = Vec::new();
    let mut table_ids = HashMap::new();
    for node in plan {
        let mut node_ids = Vec::new();
        for a in node {
            let (table, _) = a.split_at(a.find('.').unwrap());
            let l = table_ids.len();
            let id = table_ids.entry(table.to_string()).or_insert(l);
            node_ids.push(*id);
        }
        compiled_plan.push(node_ids);
    }
    compiled_plan
}

pub fn aggregate_min(result: &mut [String], payload: &[&[String]]) {
    let pl: Vec<_> = payload.iter().map(|ss| ss.concat()).collect();

    if pl.len() == result.len() {
        for (i, s) in pl.iter().enumerate() {
            if result[i].is_empty() || s < &result[i] {
                result[i] = s.to_string();
            }
        }
    }
}

pub fn load_db(plan: &[Vec<String>], payload: &[String]) -> Vec<Trie<String>> {
    let mut schema = IndexMap::new();
    for node in plan {
        for a in node {
            let names: Vec<_> = a.split('.').collect();
            let table = names[0];
            let column = names[1];
            schema
                .entry(table.to_string())
                .or_insert(vec![])
                .push(type_of(table, column));
        }
    }

    for a in payload {
        let names: Vec<_> = a.split('.').collect();
        let table = names[0];
        let column = names[1];
        schema
            .entry(table.to_string())
            .or_insert(vec![])
            .push(type_of(table, column));
    }

    let mut tries = vec![];
    for (table, types) in schema {
        let mut ts: Vec<_> = types.iter().map(|t| Arc::new(t.clone())).collect();
        let table_schema = Type::group_type_builder("duckdb_schema")
            .with_fields(&mut ts)
            .build()
            .unwrap();
        let file_name = format!("../temp/{}.parquet", table);
        tries.push(load_parquet(&file_name, table_schema).unwrap());
    }

    tries
}

pub fn load_parquet(file_path: &str, schema: Type) -> Result<Trie<String>, Box<dyn Error>> {
    let path = path::Path::new(file_path);
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;

    let mut trie = Trie::default();
    let rows = reader.get_row_iter(Some(schema))?;

    for row in rows {
        let mut ids = vec![];
        let mut data = vec![];

        for (_, field) in row.get_column_iter() {
            match field {
                Field::Int(i) => {
                    ids.push(*i);
                }
                Field::Str(v) => {
                    data.push(v.clone());
                }
                _ => {
                    panic!("Unsupported field type");
                }
            }
        }

        trie.insert(&ids[..], data);
    }

    Ok(trie)
}

fn type_of(table: &str, col: &str) -> Type {
    let mut column_name = format!("{}.{}", table, col);
    column_name = match table {
        "mc" | "mi" | "miidx" | "t" => column_name,
        _ => col.to_string(),
    };
    // let column_name = col;
    if col.ends_with("id") {
        Type::primitive_type_builder(&column_name, PhysicalType::INT32)
            .with_repetition(Repetition::OPTIONAL) // TODO: support optional
            .with_converted_type(ConvertedType::INT_32)
            .build()
            .unwrap()
    } else {
        Type::primitive_type_builder(&column_name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8)
            .with_repetition(Repetition::OPTIONAL) // TODO: support nullable
            .build()
            .unwrap()
    }
}
