COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/1b/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'bottom 10 rank') TO '../data/1b/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%') TO '../data/1b/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 2005 AND 2010) TO '../data/1b/t.parquet' (FORMAT 'parquet');
