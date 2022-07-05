# Use: bash run_tests.sh <path-to-job-queries>

# Appends each query's test to temp.sql
for File in `ls $1 -I fkindexes.sql -I schema.sql -I README.md`
do
	bash ./generate_test.sh $File >> temp.sql
done

# Runs all tests in temp.sql
cp '../../data/imdb_plain.db' '../../data/imdb.db'
../../duckdb/build/release/duckdb <<EOF
.open '../../data/imdb.db'
.read temp.sql
EOF

rm temp.sql
