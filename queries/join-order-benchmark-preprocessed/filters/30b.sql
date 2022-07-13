COPY (SELECT * FROM title AS t WHERE t.production_year > 2000 AND (t.title LIKE '%Freddy%' OR t.title LIKE '%Jason%' OR t.title LIKE 'Saw%')) TO '../data/30b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct2 WHERE cct2.kind = 'complete+verified') TO '../data/30b/cct2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'votes') TO '../data/30b/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')) TO '../data/30b/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital')) TO '../data/30b/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.gender = 'm') TO '../data/30b/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Horror', 'Thriller')) TO '../data/30b/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct1 WHERE cct1.kind IN ('cast', 'crew')) TO '../data/30b/cct1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'genres') TO '../data/30b/it1.parquet' (FORMAT 'parquet');
