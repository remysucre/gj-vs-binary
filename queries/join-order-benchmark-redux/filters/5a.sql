COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/5a/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German')) TO '../data/5a/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note LIKE '%(theatrical)%' AND mc.note LIKE '%(France)%') TO '../data/5a/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2005) TO '../data/5a/t.parquet' (FORMAT 'parquet');
