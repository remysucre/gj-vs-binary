COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/5b/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2010) TO '../data/5b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('USA', 'America')) TO '../data/5b/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note LIKE '%(VHS)%' AND mc.note LIKE '%(USA)%' AND mc.note LIKE '%(1994)%') TO '../data/5b/mc.parquet' (FORMAT 'parquet');
