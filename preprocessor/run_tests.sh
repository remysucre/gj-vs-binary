cp '../../data/imdb_plain.db' '../../data/imdb.db'
for File in `ls $1 -I fkindexes.sql -I schema.sql -I README.md`
do
	bash ./generate_tests.sh $File
done

../../duckdb/build/release/duckdb <<EOF
.open '../../data/imdb.db'
.read temp.sql
EOF

rm temp.sql