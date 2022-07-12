COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'genres') TO '../data/31c/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')) TO '../data/31c/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'votes') TO '../data/31c/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital')) TO '../data/31c/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.name LIKE 'Lionsgate%') TO '../data/31c/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War')) TO '../data/31c/mi.parquet' (FORMAT 'parquet');
