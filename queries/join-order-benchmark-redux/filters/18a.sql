COPY (SELECT * FROM info_type AS it2 WHERE it2.info = 'votes') TO '../data/18a/it2.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(producer)', '(executive producer)')) TO '../data/18a/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.gender = 'm' AND n.name LIKE '%Tim%') TO '../data/18a/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it1 WHERE it1.info = 'budget') TO '../data/18a/it1.parquet' (FORMAT 'parquet');
