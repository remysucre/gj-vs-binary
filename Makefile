.PHONY: duckdb preprocessor

# duckdb build
# this is separated into two parts to accomodate the info tweak difference in remy's duckdb fork

duckdb_pull:
	git submodule update --init

duckdb_make:
	cd duckdb && make

duckdb: duckdb_make

# imdb data

imdb.tgz:
	cd data/imdb && wget -nc http://homepages.cwi.nl/~boncz/job/imdb.tgz

imdb_csv: imdb.tgz
	cd data/imdb && tar -xf imdb.tgz --keep-old-files

imdb_plain.db: imdb_csv
	cd data/imdb && bash import.sh

clean_imdb_csv:
	cd data/imdb && rm -f *.csv

imdb: imdb_plain.db clean_imdb_csv

clean_imdb:
	cd data/imdb && rm -f *.db
	cd data/imdb && rm -f *.tgz
	cd data/imdb && rm -f schematext.sql

# preprocessor script

preprocessor:
	cd preprocessor && cargo build --release

clean_preprocessor:
	cd preprocessor && rm -f -d -r target

# job queries

job_preprocessed: preprocessor duckdb imdb
	cd preprocessor && bash run.sh join-order-benchmark imdb

test_job_preprocessed: job_preprocessed
	cd preprocessor && bash test.sh join-order-benchmark imdb

clean_job_preprocessed:
	cd queries/preprocessed/join-order-benchmark && rm -f -d -r filters
	cd queries/preprocessed/join-order-benchmark && rm -f -d -r joins
	cd queries/preprocessed/join-order-benchmark && rm -f -d -r data
