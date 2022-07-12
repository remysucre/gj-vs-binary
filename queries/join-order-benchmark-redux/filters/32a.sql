COPY (SELECT * FROM keyword AS k WHERE k.keyword = '10,000-mile-club') TO '../data/32a/k.parquet' (FORMAT 'parquet');
