set -v

if [ ! -d "./data/imdb_parquet" ]
then
    mkdir ./data/imdb_parquet
fi

./duckdb/build/release/duckdb -c ".read './scripts/transform.sql'"
