.PHONY: all clean clean_all

all: $(DATA)

DUCKDB=duckdb/build/release/duckdb
PREPROCESSOR=preprocessor/target/release/preprocessor
IMDB=data/imdb/imdb_plain.db
DATA=queries/preprocessed/data

$(DUCKDB): duckdb/src
	$(MAKE) -C duckdb -j

duckdb/src: .gitmodules
	git submodule update --init

$(PREPROCESSOR):
	cd preprocessor && cargo build --release

$(IMDB): duckdb
	$(MAKE) -C data/imdb

$(DATA): preprocessor/run.sh $(IMDB) $(PREPROCESSOR)
	cd preprocessor && bash $< join-order-benchmark imdb

test: preprocessed/test.sh $(DATA)
	cd preprocessor && bash $< join-order-benchmark imdb

clean_all: clean
	rm -rf duckdb && mkdir duckdb
	$(MAKE) -C data/imdb clean_all
	cd queries/preprocessed/join-order-benchmark \
	&& rm -f -d -r filters \
	&& rm -f -d -r joins \
	&& rm -f -d -r data

clean: clean_imdb clean_job_preprocessed
	$(MAKE) -C duckdb clean
	$(MAKE) -C data/imdb clean
	cd preprocessor && cargo clean