COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'rating') TO '../data/12c/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info_idx AS mi_idx WHERE mi_idx.info > '7.0') TO '../data/12c/mi_idx.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/12c/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'genres') TO '../data/12c/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 2000 AND 2010) TO '../data/12c/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]') TO '../data/12c/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Drama', 'Horror', 'Western', 'Family')) TO '../data/12c/mi.parquet' (FORMAT 'parquet');
