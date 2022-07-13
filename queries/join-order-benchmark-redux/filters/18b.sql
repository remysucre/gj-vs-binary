COPY (SELECT * FROM name AS n WHERE n.gender IS NOT NULL AND n.gender = 'f') TO '../data/18b/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 2008 AND 2014) TO '../data/18b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')) TO '../data/18b/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Horror', 'Thriller') AND mi.note IS NULL) TO '../data/18b/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'genres') TO '../data/18b/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'rating') TO '../data/18b/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info_idx AS mi_idx WHERE mi_idx.info > '8.0') TO '../data/18b/mi_idx.parquet' (FORMAT 'parquet');
