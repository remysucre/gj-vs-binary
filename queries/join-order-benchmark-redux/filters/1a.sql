COPY (SELECT * FROM company_type AS ct WHERE ct.kind = 'production companies') TO '../data/1a/ct.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'top 250 rank') TO '../data/1a/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%' AND (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%')) TO '../data/1a/mc.parquet' (FORMAT 'parquet');
