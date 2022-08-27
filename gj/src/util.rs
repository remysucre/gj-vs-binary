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
    join::{semijoin, sj},
    trie::{Table, Tb, insert},
};
use crate::{
    join::Tab,
    sql::*,
    trie::{FlatRelation, Trie, Value},
    *,
};

fn sjr(db: &mut DB, node: &TreeOp) {
    let plan = to_semijoin_plan(node);
    let tables = build_tables(db, &plan);

    let mut t_with_new_rels: Vec<_> = tables
        .into_iter()
        .map(|t| (t, Relation::default(), Vec::<i32>::new()))
        .collect();

    let mut tabs: Vec<_> = t_with_new_rels
        .iter_mut()
        .map(|(table, rel, ids)| Tab {
            schema: &table.schema,
            table,
            rel,
            ids,
        })
        .collect();

    let compiled_plan = compile_plan(&plan, &[]).0;
    println!("COMPILED {:#?}", compiled_plan);

    semijoin(&mut tabs, &compiled_plan);
    println!("DONE SJ");

    let rel_names: Vec<_> = t_with_new_rels
        .into_iter()
        .map(|(t, rel, _)| {
            let mut t_name = t.schema.0.to_string();
            if !db.contains_key(&t_name) {
                t_name = find_shared(&t_name).to_string();
            }
            (rel, t_name)
        })
        .collect();

    for (rel, t_name) in rel_names {
        // println!("TNAME {}", t_name);
        // println!("{:#?}", rel.keys());
        // let t = db.get_mut(&t_name).unwrap();
        // *t = rel;
        db.insert(t_name, rel);
        // std::mem::swap(t, &mut rel);
        // new_rels.leak();
    }
}

pub fn aggr(db: &DB, payload: &[&Attribute]) {
    print!("output: ");
    for a in payload {
        let mut t_name = a.table_name.as_str();
        if !db.contains_key(t_name) {
            t_name = find_shared(t_name);
        }

        let col = db.get(t_name).unwrap().get(&a.attr_name).unwrap();
        print!(
            "{:?}",
            col.iter()
                .min_by(|x, y| {
                    match (x, y) {
                        (Value::Num(x), Value::Num(y)) => x.cmp(y),
                        (Value::Str(x), Value::Str(y)) => x.cmp(y),
                        _ => panic!("unsupported type"),
                    }
                })
                .unwrap()
        );
    }
    println!();
}

pub fn semijoin_reduce(db: &mut DB, root: &TreeOp) {
    println!("START SEMIJOIN");

    for node in to_materialize(root) {
        sjr(db, node);
    }

    println!("FIRST SEMIJOIN DONE");

    for node in to_materialize(root).iter().rev().skip(1) {
        sjr(db, node);
    }
    println!("END SEMIJOIN");
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

pub fn sj_reduce<'a>(db: &'a DB, plan: &'a [Vec<&Attribute>], payload: &[&Attribute]) {
    let mut relations = build_rels(db, plan);
    let shuffles: Vec<_> = relations.iter().map(|t| {
        std::iter::repeat(0).take(t.len()).collect::<Vec<usize>>()
    }).collect();
    let shuffle_slices = shuffles.iter().map(|s| s.as_slice()).collect::<Vec<_>>();
    let (compiled_plan, compiled_payload) = compile_plan(plan, payload);

    println!("Compiled plan: {:?}", compiled_plan);
    
    for r in &relations {
        println!("BEFORE");
        println!("{:?}", r.len());
    }
    
    let mut rel_refs: Vec<_> = relations.iter_mut().collect();
    sj(&mut rel_refs, &compiled_plan, &shuffle_slices);

    for r in &relations {
        println!("AFTER");
        println!("{:?}", r.len());
    }

    let mut result: Vec<Vec<&'a Value>> = relations.iter().map(|_| vec![]).collect::<Vec<_>>();

    for i in compiled_payload {
        for (_ids, vals) in &relations[i] {
            if result[i].is_empty() {
                result[i] = vals.to_vec();
            } else {
                for (idx, val) in vals.iter().enumerate() {
                    if result[i][idx] > val {
                        result[i][idx] = val;
                    }
                }
            }
        }
    }
    println!("{:?}", result);
}

pub fn build_rels<'a>(db: &'a DB, plan: &'a [Vec<&'a Attribute>]) -> Vec<FlatRelation<'a>> {
    let mut tables = Vec::new();
    let mut id_cols = IndexMap::new();
    let mut data_cols = IndexMap::new();

    for node in plan {
        for a in node {
            let trie_name = a.table_name.as_str();
            let mut table_name = a.table_name.as_str();

            if !db.contains_key(table_name) {
                table_name = find_shared(table_name);
            }
            id_cols
                .entry(trie_name)
                .or_insert(IndexMap::new())
                .insert(&a.attr_name, &db.get(table_name).unwrap()[&a.attr_name]);
        }
    }

    for (&trie_name, cols) in &id_cols {
        let mut table_name = trie_name;

        if !db.contains_key(table_name) {
            table_name = find_shared(table_name);
        }

        for (attr, data_col) in &db[table_name] {
            if !cols.contains_key(attr) {
                data_cols
                    .entry(trie_name)
                    .or_insert(IndexMap::new())
                    .insert(attr, data_col);
            }
        }
    }

    for (table_name, cols) in &id_cols {
        let start = Instant::now();
        println!("building FlatRelation on {}", table_name);

        let mut rel = FlatRelation::new();

        for i in 0..cols[0].len() {
            let mut ids = Vec::new();
            let mut data = Vec::new();

            for (_col_name, col) in cols {
                ids.push(col[i].as_num());
            }

            if let Some(cols) = data_cols.get(table_name) {
                for (_col_name, col) in cols {
                    data.push(&col[i]);
                }
            }

            insert(&mut rel, &ids, &data);
        }

        tables.push(rel);

        println!(
            "building {} takes {}s",
            table_name,
            start.elapsed().as_secs_f32()
        );
    }
    tables
}

pub fn build_tables<'a>(db: &'a DB, plan: &'a [Vec<&'a Attribute>]) -> Vec<Table<'a, Value>> {
    let mut tables = Vec::new();
    let mut id_cols = IndexMap::new();
    let mut data_cols = IndexMap::new();

    for node in plan {
        for a in node {
            let trie_name = a.table_name.as_str();
            let mut table_name = a.table_name.as_str();

            if !db.contains_key(table_name) {
                table_name = find_shared(table_name);
            }
            // println!("TABLE {}", table_name);

            // println!("Attribute {}", &a.attr_name);
            id_cols
                .entry(trie_name)
                .or_insert(IndexMap::new())
                .insert(&a.attr_name, &db.get(table_name).unwrap()[&a.attr_name]);
        }
    }

    for (&trie_name, cols) in &id_cols {
        let mut table_name = trie_name;

        if !db.contains_key(table_name) {
            table_name = find_shared(table_name);
        }

        for (attr, data_col) in &db[table_name] {
            if !cols.contains_key(attr) {
                data_cols
                    .entry(trie_name)
                    .or_insert(IndexMap::new())
                    .insert(attr, data_col);
            }
        }
    }

    let start = Instant::now();

    let (trie_name, cols) = id_cols.first().unwrap();
    println!("building flat table on {}", trie_name);

    let mut ids = vec![];
    let mut data = vec![];
    let mut vs = vec![];

    for (&col_name, col) in cols {
        vs.push(col_name.as_str());
        ids.push(&col[..])
    }

    if let Some(cols) = data_cols.get(trie_name) {
        for (col_name, col) in cols {
            vs.push(col_name);
            data.push(&col[..])
        }
    }

    tables.push(Table {
        schema: (trie_name, vs),
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

        for (&col_name, _col) in cols {
            vs.push(col_name.as_str());
        }

        if let Some(cols) = data_cols.get(table_name) {
            for (&col_name, _col) in cols {
                vs.push(col_name.as_str());
            }
        }

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
            schema: (table_name, vs),
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
