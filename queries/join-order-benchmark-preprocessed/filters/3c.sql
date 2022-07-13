COPY (SELECT * FROM keyword AS k WHERE k.keyword LIKE '%sequel%') TO '../data/3c/k.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM title AS t WHERE t.production_year > 1990) TO '../data/3c/t.parquet' (FORMAT 'parquet');
COPY (SELECT * FROM movie_info AS mi WHERE mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'USA', 'American')) TO '../data/3c/mi.parquet' (FORMAT 'parquet');
