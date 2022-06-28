use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use std::env;
use std::fs;

use std::collections::HashMap;

fn main() {
    let args: Vec<String> = env::args().collect();
    let f = &args[1];
    let mode = &args[2];
    let name = &args[3];

    let sql = fs::read_to_string(f).expect("Unable to read file");

    let dialect = GenericDialect {};

    let mut ast = Parser::parse_sql(&dialect, &sql).unwrap();
    assert_eq!(ast.len(), 1, "File must contain exactly 1 statement");

    let mut stmt = ast.pop().unwrap();

    if let Statement::Query(q) = &mut stmt {
        if let SetExpr::Select(q) = &mut q.body {
            let mut filters = vec![];
            let mut joins = vec![];

            if let Some(sel) = &q.selection {
                get_joins(sel, &mut joins, &mut filters);
            }

            if mode == "filters" {
                let mut from_aliases: HashMap<String, TableWithJoins> = HashMap::new();
                map_from_aliases(&q.from, &mut from_aliases);

                let mut filter_aliases: HashMap<String, Vec<String>> = HashMap::new();
                map_filter_aliases(&filters, &mut filter_aliases);

                // constructs the filter queries for each table matching by alias
                for (filter_alias, parsed_filters) in &filter_aliases {
                    for (from_alias, parsed_from) in &from_aliases {
                        if filter_alias == from_alias {
                            if let TableFactor::Table {name: n, alias: _, args: _, with_hints: _} = &parsed_from.relation {
                                let name_string = n.to_string();
                                println!("COPY (SELECT * FROM {} WHERE {}) TO '../data/{}/{}.csv' (HEADER, DELIMITER ',', ESCAPE '\\');", parsed_from, parsed_filters.join(" AND "), name, name_string);
                            }
                        }
                    }
                }
            }
            
            if mode == "joins" {
                let j = joins.pop().expect("Query has no joins");
                q.selection = Some(joins.drain(..).fold(mk_join(j), |l, r| Expr::BinaryOp {
                    left: Box::new(l),
                    op: BinaryOperator::And,
                    right: Box::new(mk_join(r)),
                }));
                println!("{};", stmt);
            }
        } else {
            panic!("Only SELECT-PROJECT-JOIN queries are supported");
        }
    } else {
        panic!("Only SELECT queries are supported");
    }
}

// constructs a join on the basis of the parameters
fn mk_join(lr: (Vec<Ident>, Vec<Ident>)) -> Expr {
    let (l, r) = lr;
    Expr::BinaryOp {
        left: Box::new(Expr::CompoundIdentifier(l)),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::CompoundIdentifier(r)),
    }
}

// maps table aliases (ie. cn) to their full from statements (ie. company_name AS cn)
fn map_from_aliases(froms: &Vec<TableWithJoins>, aliases: &mut HashMap<String, TableWithJoins>) {
    for from in froms {
        if let TableFactor::Table {name: _, alias, args: _, with_hints: _} = &from.relation {
            if let Some(a) = &alias {
                let alias_string = a.to_string();
                aliases.entry(alias_string).or_insert(from.clone());
            }
        }
    }
}

// maps table aliases (ie. cn) to their filter statements (ie. cn.country_code = '[us]')
fn map_filter_aliases(filters: &Vec<Expr>, aliases: &mut HashMap<String, Vec<String>>) {
    for fil in filters {
        let alias_string = get_filter_alias(fil);
        aliases.entry(alias_string).or_insert(vec![]);
        let alias_string = get_filter_alias(fil);
        if let Some(fs) = aliases.get_mut(&alias_string) {
            fs.push((*fil).to_string());
        }
    }
}

// gets the filter alias from the expression
fn get_filter_alias(filter: &Expr) -> String {
    if let Expr::Nested(nest) = filter {
        return get_filter_alias(nest);
    }
    if let Expr::BinaryOp {
        left: l,
        op: o,
        right: r,
    } = filter
    {
        match (&**l, o, &**r) {
            (Expr::CompoundIdentifier(l), _, _) => {
                let alias_string = (&l[0]).to_string();
                return alias_string;
            }
            (_, _, Expr::CompoundIdentifier(r)) => {
                let alias_string = (&r[0]).to_string();
                return alias_string;
            }
            (e_1, _, e_2) => {
                let left_branch = get_filter_alias(e_1);
                if left_branch.is_empty() {
                    return get_filter_alias(e_2);
                }
                return left_branch;
            }
        }
    }
    return String::new();
}

// gets all of the joins and filters from the expression
fn get_joins(e: &Expr, joins: &mut Vec<(Vec<Ident>, Vec<Ident>)>, filters: &mut Vec<Expr>) {
    if let Expr::BinaryOp {
        left: l,
        op: o,
        right: r,
    } = e
    {
        match (&**l, o, &**r) {
            (Expr::CompoundIdentifier(l), BinaryOperator::Eq, Expr::CompoundIdentifier(r)) => {
                joins.push((l.clone(), r.clone()))
            }
            (e_l, BinaryOperator::And, e_r) => {
                get_joins(e_l, joins, filters);
                get_joins(e_r, joins, filters)
            }
            _ => filters.push(e.clone()),
        }
    } else {
        filters.push(e.clone());
    }
}
