cp '../../data/imdb/imdb_plain.db' '../../data/imdb/imdb.db'
cd filters
../../../duckdb/build/release/duckdb << EOF
.open '../../../data/imdb/imdb.db'
.read $1
EOF
cd ..