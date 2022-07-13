COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/13b/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'release dates') TO '../data/13b/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.title <> '' AND (t.title LIKE '%Champion%' OR t.title LIKE '%Loser%')) TO '../data/13b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM kind_type AS kt WHERE kt.kind = 'movie') TO '../data/13b/kt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'rating') TO '../data/13b/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/13b/cn.parquet' (FORMAT 'parquet');
