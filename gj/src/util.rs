use std::{error::Error, collections::HashMap};
use std::path;

use serde::de::DeserializeOwned;
use parquet::{
    file::reader::{FileReader, SerializedFileReader}, 
    basic::{ConvertedType, Repetition, Type as PhysicalType},
    schema::types::Type,
    record::Field,
};
use indexmap::IndexMap;

use std::fs::File;
use std::sync::Arc;

use crate::join::Trie;

pub fn load_csv<T: DeserializeOwned>(file_name: &str) -> Result<Vec<T>, Box<dyn Error>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .escape(Some(b'\\'))
        .from_path(path::Path::new(&file_name))?;
    let mut table = Vec::new();
    for result in rdr.deserialize() {
        let tuple: T = result?;
        table.push(tuple);
    }
    Ok(table)
}

pub fn compile_plan(plan: &[Vec<String>]) -> Vec<Vec<usize>>{
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

pub fn aggregate_min(result: &mut Vec<String>,  payload: &[&[String]]) {
    let pl: Vec<_> = payload.iter().map(|ss| ss.concat().to_string()).collect();

    if pl.len() == result.len() {
        for (i, s) in pl.iter().enumerate() {
            if result[i].len() == 0 || s <& result[i] {
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
            schema.entry(table.to_string()).or_insert(vec![]).push(type_of(table, column));
        }
    }

    for a in payload {
        let names: Vec<_> = a.split('.').collect();
        let table = names[0];
        let column = names[1];
        schema.entry(table.to_string()).or_insert(vec![]).push(type_of(table, column));
    }

    let mut tries = vec![];
    for (table, types) in schema {
        let mut ts: Vec<_> = types.iter().map(|t| Arc::new(t.clone())).collect();
        let table_schema = Type::group_type_builder(&"duckdb_schema")
            .with_fields(&mut ts)
            .build().unwrap();
        let file_name = format!("../data/{}.parquet", table);
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
                },
                Field::Str(v) => {
                    data.push(v.clone());
                },
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
    // let table_name = match table {
    //     "cn" => "cn",
    //     "ct" => "ct",
    //     "it" => "it",
    //     "it2" => "it2",
    //     "kt" => "kt",
    //     "mc" => "movie_companies",
    //     "mi" => "movie_info",
    //     "miidx" => "movie_info_idx",
    //     "t" => "title",
    //     _ => panic!("Unsupported table"),
    // };
    let mut column_name = format!("{}.{}", table, col);
    column_name = match table {
        "mc" | "mi" | "miidx" | "t" => {
            column_name
        },
        _ => col.to_string()
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