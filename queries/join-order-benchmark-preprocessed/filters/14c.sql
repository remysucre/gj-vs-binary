COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'countries') TO '../data/14c/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2005) TO '../data/14c/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info_idx AS mi_idx WHERE mi_idx.info < '8.5') TO '../data/14c/mi_idx.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'rating') TO '../data/14c/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM kind_type AS kt WHERE kt.kind IN ('movie', 'episode')) TO '../data/14c/kt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IS NOT NULL AND k.keyword IN ('murder', 'murder-in-title', 'blood', 'violence')) TO '../data/14c/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Danish', 'Norwegian', 'German', 'USA', 'American')) TO '../data/14c/mi.parquet' (FORMAT 'parquet');
