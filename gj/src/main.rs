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
        cn_data.append(& mut t.into_values());
    }
    db.add_relation_with_data("cn", 7, cn_data);

    let mut ct_data = vec![];
    for t in ct {
        ct_data.append(& mut t.into_values());
    }
    db.add_relation_with_data("ct", 2, ct_data);

    let mut it_data = vec![];
    for t in it {
        it_data.append(& mut t.into_values());
    }
    db.add_relation_with_data("it", 2, it_data);

    let mut it2_data = vec![];
    for t in it2 {
        it2_data.append(& mut t.into_values());
    }
    db.add_relation_with_data("it2", 2, it2_data);

    let mut kt_data = vec![];
    for t in kt {
        kt_data.append(& mut t.into_values());
    }
    db.add_relation_with_data("kt", 2, kt_data);

    let mut mc_data = vec![];
    for t in mc {
        mc_data.append(& mut t.into_values());
    }
    db.add_relation_with_data("mc", 3, mc_data);

    let mut mi_data = vec![];
    for t in mi {
        mi_data.append(& mut t.into_values());
    }
    db.add_relation_with_data("mi", 5, mi_data);

    let mut miidx_data = vec![];
    for t in miidx {
        miidx_data.append(& mut t.into_values());
    }
    db.add_relation_with_data("miidx", 3, miidx_data);

    let mut t_data = vec![];
    for t in t {
        t_data.append(& mut t.into_values());
    }
    db.add_relation_with_data("t", 3, t_data);

    // Variable ordering
    // t.id = mid.movie_id = mi.movie_id = mc.movie_id
    // t.kind_id = kt.id
    // mi.info_type_id = it.id = mid.info_type_id
    // mc.company_type_id = ct.id 
    // mc.company_id = cn.id
}