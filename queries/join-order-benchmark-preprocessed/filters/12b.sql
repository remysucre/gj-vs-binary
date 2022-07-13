COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/12b/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_type AS ct WHERE ct.kind IS NOT NULL AND (ct.kind = 'production companies' OR ct.kind = 'distributors')) TO '../data/12b/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'bottom 10 rank') TO '../data/12b/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'budget') TO '../data/12b/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2000 AND (t.title LIKE 'Birdemic%' OR t.title LIKE '%Movie%')) TO '../data/12b/t.parquet' (FORMAT 'parquet');
