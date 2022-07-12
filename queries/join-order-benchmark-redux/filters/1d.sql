COPY (SELECT * FROM title AS t WHERE t.production_year > 2000) TO '../data/1d/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/1d/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%') TO '../data/1d/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'bottom 10 rank') TO '../data/1d/it.parquet' (FORMAT 'parquet');
