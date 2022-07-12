COPY (SELECT * FROM name AS n WHERE n.name_pcode_cf BETWEEN 'A' AND 'F' AND (n.gender = 'm' OR (n.gender = 'f' AND n.name LIKE 'B%'))) TO '../data/7a/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM person_info AS pi WHERE pi.note = 'Volker Boehm') TO '../data/7a/pi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'mini biography') TO '../data/7a/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM link_type AS lt WHERE lt.link = 'features') TO '../data/7a/lt.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 1980 AND 1995) TO '../data/7a/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM aka_name AS an WHERE an.name LIKE '%a%') TO '../data/7a/an.parquet' (FORMAT 'parquet');
