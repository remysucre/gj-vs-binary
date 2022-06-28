use std::error::Error;
use std::io;
use std::path;
use std::process;

use serde::de::DeserializeOwned;

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