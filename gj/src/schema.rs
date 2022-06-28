use std::fmt::Display;

use serde::Deserialize;

use gj_derive::IntoValues;

// Debug + Clone + Hash + Eq + Default + Display {}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum Value {
    Int(u64),
    IntOpt(Option<u64>),
    String(String),
}

pub trait IntoValues {
    fn into_values(self) -> Vec<Value>;
}

impl Default for Value {
    fn default() -> Self {
        Value::IntOpt(None)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i),
            Value::IntOpt(i) => write!(f, "{}", i.unwrap_or(0)),
            Value::String(s) => write!(f, "{}", s),
        }
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Int(v)
    }
}

impl From<Option<u64>> for Value {
    fn from(v: Option<u64>) -> Self {
        Value::IntOpt(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

// CREATE TABLE aka_name (
//     id integer NOT NULL PRIMARY KEY,
//     person_id integer NOT NULL,
//     name text NOT NULL,
//     imdb_index character varying(12),
//     name_pcode_cf character varying(5),
//     name_pcode_nf character varying(5),
//     surname_pcode character varying(5),
//     md5sum character varying(32)
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct AkaName {
    id: u64,
    person_id: u64,
    name: String,
    imdb_index: String,
    name_pcode_cf: String,
    name_pcode_nf: String,
    surname_pcode: String,
    mdsum: String,
}

// CREATE TABLE aka_title (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     title text NOT NULL,
//     imdb_index character varying(12),
//     kind_id integer NOT NULL,
//     production_year integer,
//     phonetic_code character varying(5),
//     episode_of_id integer,
//     season_nr integer,
//     episode_nr integer,
//     note text,
//     md5sum character varying(32)
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct AkaTitle {
    id: u64,
    movie_id: u64,
    title: String,
    imdb_index: String,
    kind_id: u64,
    production_year: Option<u64>,
    phonetic_code: String,
    episode_of_id: Option<u64>,
    season_nr: Option<u64>,
    episode_nr: Option<u64>,
    note: String,
    mdsum: String,
}

// CREATE TABLE cast_info (
//     id integer NOT NULL PRIMARY KEY,
//     person_id integer NOT NULL,
//     movie_id integer NOT NULL,
//     person_role_id integer,
//     note text,
//     nr_order integer,
//     role_id integer NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct CastInfo {
    id: u64,
    person_id: u64,
    movie_id: u64,
    person_role_id: Option<u64>,
    note: String,
    nr_order: Option<u64>,
    role_id: u64,
}

// CREATE TABLE char_name (
//     id integer NOT NULL PRIMARY KEY,
//     name text NOT NULL,
//     imdb_index character varying(12),
//     imdb_id integer,
//     name_pcode_nf character varying(5),
//     surname_pcode character varying(5),
//     md5sum character varying(32)
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct CharName {
    id: u64,
    name: String,
    imdb_index: String,
    imdb_id: Option<u64>,
    name_pcode_nf: String,
    surname_pcode: String,
    mdsum: String,
}

// CREATE TABLE comp_cast_type (
//     id integer NOT NULL PRIMARY KEY,
//     kind character varying(32) NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct CompCastType {
    id: u64,
    kind: String,
}

// CREATE TABLE company_name (
//     id integer NOT NULL PRIMARY KEY,
//     name text NOT NULL,
//     country_code character varying(255),
//     imdb_id integer,
//     name_pcode_nf character varying(5),
//     name_pcode_sf character varying(5),
//     md5sum character varying(32)
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct CompanyName {
    id: u64,
    name: String,
    country_code: String,
    imdb_id: Option<u64>,
    name_pcode_nf: String,
    name_pcode_sf: String,
    mdsum: String,
}

// CREATE TABLE company_type (
//     id integer NOT NULL PRIMARY KEY,
//     kind character varying(32) NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct CompanyType {
    id: u64,
    kind: String,
}

// CREATE TABLE complete_cast (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer,
//     subject_id integer NOT NULL,
//     status_id integer NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct CompleteCast {
    id: u64,
    movie_id: Option<u64>,
    subject_id: u64,
    status_id: u64,
}

// CREATE TABLE info_type (
//     id integer NOT NULL PRIMARY KEY,
//     info character varying(32) NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct InfoType {
    id: u64,
    info: String,
}

// CREATE TABLE keyword (
//     id integer NOT NULL PRIMARY KEY,
//     keyword text NOT NULL,
//     phonetic_code character varying(5)
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct Keyword {
    id: u64,
    keyword: String,
    phonetic_code: String,
}

// CREATE TABLE kind_type (
//     id integer NOT NULL PRIMARY KEY,
//     kind character varying(15) NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct KindType {
    id: u64,
    kind: String,
}

// CREATE TABLE link_type (
//     id integer NOT NULL PRIMARY KEY,
//     link character varying(32) NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct LinkType {
    id: u64,
    link: String,
}

// CREATE TABLE movie_companies (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     company_id integer NOT NULL,
//     company_type_id integer NOT NULL,
//     note text
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct MovieCompanies {
    id: u64,
    movie_id: u64,
    company_id: u64,
    company_type_id: u64,
    note: String,
}

// CREATE TABLE movie_info (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     info_type_id integer NOT NULL,
//     info text NOT NULL,
//     note text
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct MovieInfo {
    id: u64,
    movie_id: u64,
    info_type_id: u64,
    info: String,
    note: String,
}

// CREATE TABLE movie_info_idx (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     info_type_id integer NOT NULL,
//     info text NOT NULL,
//     note text
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct MovieInfoIdx {
    id: u64,
    movie_id: u64,
    info_type_id: u64,
    info: String,
    note: String,
}

// CREATE TABLE movie_keyword (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     keyword_id integer NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct MovieKeyword {
    id: u64,
    movie_id: u64,
    keyword_id: u64,
}

// CREATE TABLE movie_link (
//     id integer NOT NULL PRIMARY KEY,
//     movie_id integer NOT NULL,
//     linked_movie_id integer NOT NULL,
//     link_type_id integer NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct MovieLink {
    id: u64,
    movie_id: u64,
    linked_movie_id: u64,
    link_type_id: u64,
}

// CREATE TABLE name (
//     id integer NOT NULL PRIMARY KEY,
//     name text NOT NULL,
//     imdb_index character varying(12),
//     imdb_id integer,
//     gender character varying(1),
//     name_pcode_cf character varying(5),
//     name_pcode_nf character varying(5),
//     surname_pcode character varying(5),
//     md5sum character varying(32)
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct Name {
    id: u64,
    name: String,
    imdb_index: String,
    imdb_id: Option<u64>,
    gender: String,
    name_pcode_cf: String,
    name_pcode_nf: String,
    surname_pcode: String,
    md5sum: String,
}

// CREATE TABLE person_info (
//     id integer NOT NULL PRIMARY KEY,
//     person_id integer NOT NULL,
//     info_type_id integer NOT NULL,
//     info text NOT NULL,
//     note text
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct PersonInfo {
    id: u64,
    person_id: u64,
    info_type_id: u64,
    info: String,
    note: String,
}

// CREATE TABLE role_type (
//     id integer NOT NULL PRIMARY KEY,
//     role character varying(32) NOT NULL
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct RoleType {
    id: u64,
    role: String,
}

// CREATE TABLE title (
//     id integer NOT NULL PRIMARY KEY,
//     title text NOT NULL,
//     imdb_index character varying(12),
//     kind_id integer NOT NULL,
//     production_year integer,
//     imdb_id integer,
//     phonetic_code character varying(5),
//     episode_of_id integer,
//     season_nr integer,
//     episode_nr integer,
//     series_years character varying(49),
//     md5sum character varying(32)
// );
#[derive(Debug, Deserialize, IntoValues)]
pub struct Title {
    id: u64,
    title: String,
    imdb_index: String,
    kind_id: u64,
    production_year: Option<u64>,
    imdb_id: Option<u64>,
    phonetic_code: String,
    episode_of_id: Option<u64>,
    season_nr: Option<u64>,
    episode_nr: Option<u64>,
    series_years: String,
    md5sum: String,
}