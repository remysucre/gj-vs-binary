use qry::*;

use gj::schema::*;
use gj::util::*;

fn main() {

    let company_name: Vec<CompanyName> = load_csv("../temp/company_name.csv").unwrap();
    let company_type: Vec<CompanyType> = load_csv("../temp/company_type.csv").unwrap();
    let info_type: Vec<InfoType> = load_csv("../temp/info_type.csv").unwrap();
    let kind_type: Vec<KindType> = load_csv("../temp/kind_type.csv").unwrap();
    let movie_companies: Vec<MovieCompanies> = load_csv("../temp/movie_companies.csv").unwrap();
    let movie_info: Vec<MovieInfo> = load_csv("../temp/movie_info.csv").unwrap();
    let movie_info_idx: Vec<MovieInfoIdx> = load_csv("../temp/movie_info_idx.csv").unwrap();
    let title: Vec<Title> = load_csv("../temp/title.csv").unwrap();

    // let mut db = Database::default();
    // db.add_relation_with_data("company_name", 7, data)
}