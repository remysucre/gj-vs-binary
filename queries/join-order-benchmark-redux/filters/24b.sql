COPY (SELECT * FROM keyword AS k WHERE k.keyword IN ('hero', 'martial-arts', 'hand-to-hand-combat', 'computer-animated-movie')) TO '../data/24b/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM cast_info AS ci WHERE ci.note IN ('(voice)', '(voice: Japanese version)', '(voice) (uncredited)', '(voice: English version)')) TO '../data/24b/ci.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM role_type AS rt WHERE rt.role = 'actress') TO '../data/24b/rt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM company_name AS cn WHERE cn.country_code = '[us]' AND cn.name = 'DreamWorks Animation') TO '../data/24b/cn.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IS NOT NULL AND (mi.info LIKE 'Japan:%201%' OR mi.info LIKE 'USA:%201%')) TO '../data/24b/mi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.gender = 'f' AND n.name LIKE '%An%') TO '../data/24b/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 2010 AND t.title LIKE 'Kung Fu Panda%') TO '../data/24b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'release dates') TO '../data/24b/it.parquet' (FORMAT 'parquet');
