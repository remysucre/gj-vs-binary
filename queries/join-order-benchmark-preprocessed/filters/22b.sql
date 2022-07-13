COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'rating') TO '../data/22b/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2009) TO '../data/22b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'countries') TO '../data/22b/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info_idx AS mi_idx WHERE mi_idx.info < '7.0') TO '../data/22b/mi_idx.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('murder', 'murder-in-title', 'blood', 'violence')) TO '../data/22b/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code <> '[us]') TO '../data/22b/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM kind_type AS kt WHERE kt.kind IN ('movie', 'episode')) TO '../data/22b/kt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Germany', 'German', 'USA', 'American')) TO '../data/22b/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_companies AS mc WHERE mc.note NOT LIKE '%(USA)%' AND mc.note LIKE '%(200%)%') TO '../data/22b/mc.parquet' (FORMAT 'parquet');
