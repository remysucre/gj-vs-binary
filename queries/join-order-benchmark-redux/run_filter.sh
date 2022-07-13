cp '../../data/imdb_plain.db' '../../data/imdb.db'
cd filters
../../../duckdb/build/release/duckdb <<EOF
.open '../../../data/imdb.db'
.read $1
EOF
cd ..