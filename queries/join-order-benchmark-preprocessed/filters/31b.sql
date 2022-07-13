COPY (SELECT * FROM movie_companies AS mc WHERE mc.note LIKE '%(Blu-ray)%') TO '../data/31b/mc.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'votes') TO '../data/31b/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital')) TO '../data/31b/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2000 AND (t.title LIKE '%Freddy%' OR t.title LIKE '%Jason%' OR t.title LIKE 'Saw%')) TO '../data/31b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.gender = 'm') TO '../data/31b/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')) TO '../data/31b/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'genres') TO '../data/31b/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.name LIKE 'Lionsgate%') TO '../data/31b/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Horror', 'Thriller')) TO '../data/31b/mi.parquet' (FORMAT 'parquet');
