COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'genres') TO '../data/30c/it1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('murder', 'violence', 'blood', 'gore', 'death', 'female-nudity', 'hospital')) TO '../data/30c/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(writer)', '(head writer)', '(written by)', '(story)', '(story editor)')) TO '../data/30c/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'votes') TO '../data/30c/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct2 WHERE cct2.kind = 'complete+verified') TO '../data/30c/cct2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.gender = 'm') TO '../data/30c/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM comp_cast_type AS cct1 WHERE cct1.kind = 'cast') TO '../data/30c/cct1.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Horror', 'Action', 'Sci-Fi', 'Thriller', 'Crime', 'War')) TO '../data/30c/mi.parquet' (FORMAT 'parquet');
