COPY (SELECT * FROM movie_info_idx AS mi_idx WHERE mi_idx.info > '2.0') TO '../data/4c/mi_idx.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 1990) TO '../data/4c/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'rating') TO '../data/4c/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword LIKE '%sequel%') TO '../data/4c/k.parquet' (FORMAT 'parquet');
