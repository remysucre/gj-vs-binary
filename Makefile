.PHONY: all clean clean_all

DUCKDB=duckdb/build/release/duckdb
PREPROCESSOR=preprocessor/target/release/preprocessor
IMDB=data/imdb/imdb_plain.db
DATA=queries/preprocessed/join-order-benchmark/data

all: $(DATA)

$(DUCKDB): duckdb/src
	$(MAKE) -C duckdb -j

duckdb/src: .gitmodules
	git submodule update --init

$(PREPROCESSOR):
	cd preprocessor && cargo build --release

$(IMDB): duckdb
	$(MAKE) -C data/imdb

$(DATA): preprocessor/run.sh $(DUCKDB) $(IMDB) $(PREPROCESSOR)
	cd preprocessor && bash run.sh join-order-benchmark imdb && touch ../$@

test: preprocessed/test.sh $(DATA)
	cd preprocessor && bash test.sh join-order-benchmark imdb

clean_all: clean
	rm -rf duckdb && mkdir duckdb
	$(MAKE) -C data/imdb clean_all
	cd queries/preprocessed/join-order-benchmark \
	&& rm -f -d -r filters \
	&& rm -f -d -r joins \
	&& rm -f -d -r data

clean:
	$(MAKE) -C duckdb clean
	$(MAKE) -C data/imdb clean
	cd preprocessor && cargo clean
