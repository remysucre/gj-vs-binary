use std::time::Instant;

use qry::*;

use gj::schema::*;
use gj::util::*;

fn main() {

    let cn: Vec<CompanyName> = load_csv("../temp/cn.csv").unwrap();
    let ct: Vec<CompanyType> = load_csv("../temp/ct.csv").unwrap();
    let it: Vec<InfoType> = load_csv("../temp/it.csv").unwrap();
    let it2: Vec<InfoType> = load_csv("../temp/it2.csv").unwrap();
    let kt: Vec<KindType> = load_csv("../temp/kt.csv").unwrap();
    let mc: Vec<MovieCompanies> = load_csv("../temp/mc.csv").unwrap();
    let mi: Vec<MovieInfo> = load_csv("../temp/mi.csv").unwrap();
    let miidx: Vec<MovieInfoIdx> = load_csv("../temp/miidx.csv").unwrap();
    let t: Vec<Title> = load_csv("../temp/t.csv").unwrap();

    let mut db = Database::default();
    
    let mut cn_data = vec![];
    for t in cn {
        let vals = t.into_values();
        cn_data.append(& mut vec![vals[0].clone()]);
    }
    db.add_relation_with_data("cn", 1, cn_data);

    let mut ct_data = vec![];
    for t in ct {
        let vals = t.into_values();
        ct_data.append(& mut vec![vals[0].clone()]);
    }
    db.add_relation_with_data("ct", 1, ct_data);

    let mut it_data = vec![];
    for t in it {
        let vals = t.into_values();
        it_data.append(& mut vec![vals[0].clone()]);
    }
    db.add_relation_with_data("it", 1, it_data);

    let mut it2_data = vec![];
    for t in it2 {
        let vals = t.into_values();
        it2_data.append(& mut vec![vals[0].clone()]);
    }
    db.add_relation_with_data("it2", 1, it2_data);

    let mut kt_data = vec![];
    for t in kt {
        let vals = t.into_values();
        kt_data.append(& mut vec![vals[0].clone()]);
    }
    db.add_relation_with_data("kt", 1, kt_data);

    let mut mc_data = vec![];
    for t in mc {
        let vals = t.into_values();
        mc_data.append(& mut vec![vals[1].clone(), vals[2].clone(), vals[3].clone()]);
    }
    db.add_relation_with_data("mc", 3, mc_data);

    let mut mi_data = vec![];
    for t in mi {
        let vals = t.into_values();
        mi_data.append(& mut vec![vals[1].clone(), vals[2].clone(), vals[3].clone()]);
    }
    db.add_relation_with_data("mi", 3, mi_data);

    let mut miidx_data = vec![];
    for t in miidx {
        let vals = t.into_values();
        miidx_data.append(& mut vec![vals[1].clone(), vals[2].clone(), vals[3].clone()]);
    }
    db.add_relation_with_data("miidx", 3, miidx_data);

    let mut t_data = vec![];
    for t in t {
        let vals = t.into_values();
        t_data.append(& mut vec![vals[0].clone(), vals[1].clone(), vals[3].clone()]);
    }
    db.add_relation_with_data("t", 3, t_data);

    // Variable ordering
    // t.id = miidx.movie_id = mi.movie_id = mc.movie_id
    // t.kind_id = kt.id
    // it2.id = mi.info_type_id
    // it.id = miidx.info_type_id
    // mc.company_type_id = ct.id 
    // mc.company_id = cn.id

    let q = Query::new(vec![
        Atom::new("t", vec![Term("t.id"), Term("t.title"),  /* Term("t.imdb_index"),*/ Term("t.kind_id"), /*Term("t.production_year"), Term("t.imdb_id"), Term("t.phonetic_code"), Term("t.episode_of_id"), Term("t.season_nr"), Term("t.episode_nr"), Term("t.series_years"), Term("t.md5sum")*/ ]),
        Atom::new("miidx", vec![/*Term("miidx.id"),*/ Term("t.id"), Term("it.id"), Term("miidx.info"), /*Term("miidx.note")*/]),
        Atom::new("mi", vec![/*Term("mi.id"),*/ Term("t.id"), Term("it2.id"), Term("mi.info"), /*Term("mi.note")*/]),
        Atom::new("mc", vec![/*Term("mc.id"),*/ Term("t.id"), Term("cn.id"), Term("ct.id"), /*Term("mc.note")*/]),
        Atom::new("kt", vec![Term("t.kind_id"), /*Term("kt.kind")*/]),
        Atom::new("it2", vec![Term("it2.id"), /*Term("it2.info")*/]),
        Atom::new("it", vec![Term("it.id"), /*Term("it.info")*/]),
        Atom::new("ct", vec![Term("ct.id"), /*Term("ct.kind")*/]),
        Atom::new("cn", vec![Term("cn.id"), /*Term("cn.name"), Term("cn.country_code"), Term("cn.imdb_id"), Term("cn.name_pcode_nf"), Term("cn.name_pcode_sf"), Term("cn.md5sum")*/]),
    ]);
    let mut ctx = EvalContext::default();

    let varmap = vec![
        "t.kind_id", 
        "it2.id", 
        "it.id", 
        "ct.id", 
        "cn.id", //
        "t.id", 
        "t.title", // SELECT
        "miidx.info", // SELECT
        "mi.info", // SELECT
    ];

    let mut result = vec![Value::String("Database".to_string()), Value::String("Rules".to_string()), Value::String("The World".to_string())];
    let now = Instant::now();
    let f = |t: &[Value]| {
        if result[0] > t[7] {
            result[0] = t[7].clone();
        }
        if result[1] > t[8] {
            result[1] = t[8].clone();
        }
        if result[2] > t[6] {
            result[2] = t[6].clone();
        }
        Ok(())
    };
    q.join(&varmap, &db, &mut ctx, f).unwrap();
    println!("{}", now.elapsed().as_secs_f32());
    let mut result = vec![Value::String("Database".to_string()), Value::String("Rules".to_string()), Value::String("The World".to_string())];
    let now = Instant::now();
    let f = |t: &[Value]| {
        if result[0] > t[7] {
            result[0] = t[7].clone();
        }
        if result[1] > t[8] {
            result[1] = t[8].clone();
        }
        if result[2] > t[6] {
            result[2] = t[6].clone();
        }
        Ok(())
    };
    q.join(&varmap, &db, &mut ctx, f).unwrap();
    println!("{}", now.elapsed().as_secs_f32());

    println!("{:?}", result);
}