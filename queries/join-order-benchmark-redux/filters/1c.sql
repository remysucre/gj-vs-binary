COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/1c/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%' AND (mc.note LIKE '%(co-production)%')) TO '../data/1c/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'top 250 rank') TO '../data/1c/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2010) TO '../data/1c/t.parquet' (FORMAT 'parquet');
