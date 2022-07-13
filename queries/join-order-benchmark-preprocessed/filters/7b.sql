COPY (SELECT * FROM aka_name AS an WHERE an.name LIKE '%a%') TO '../data/7b/an.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year BETWEEN 1980 AND 1984) TO '../data/7b/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM name AS n WHERE n.name_pcode_cf LIKE 'D%' AND n.gender = 'm') TO '../data/7b/n.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM info_type AS it WHERE it.info = 'mini biography') TO '../data/7b/it.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM person_info AS pi WHERE pi.note = 'Volker Boehm') TO '../data/7b/pi.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM link_type AS lt WHERE lt.link = 'features') TO '../data/7b/lt.parquet' (FORMAT 'parquet');
