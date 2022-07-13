# Use: bash test.sh <path-to-original-queries> <path-to-preprocessed-queries>
# Example: bash test.sh ../queries/join-order-benchmark ../queries/join-order-benchmark-preprocessed

original_queries=$1
preprocessed_queries=$2

# Appends each query's test to temp.sql
for query in `ls $original_queries -I fkindexes.sql -I schema.sql -I README.md`
do
	# Run the original benchmark query
	echo "CREATE TABLE original_query AS "
	cat "$original_queries/$query"

	# Load the filtered tables from the preprocessed parquet
	for preprocessed_table in `ls $preprocessed_queries/data/${query%.*}/`
	do
		echo "DROP TABLE IF EXISTS ${preprocessed_table%.*};"
		echo "CREATE TABLE ${preprocessed_table%.*} AS SELECT * FROM '$preprocessed_queries/data/${query%.*}/$preprocessed_table';"
	done

	# Run the preprocessed join query using the filtered tables
	echo "CREATE TABLE preprocessed_query AS "
	cat "$preprocessed_queries/joins/$query"

	# Get the difference between the original and preprocessed query
	echo ".print 'Testing $query'"
	echo "SELECT * FROM original_query EXCEPT SELECT * FROM preprocessed_query;" 
	echo "DROP TABLE IF EXISTS original_query;"
	echo "DROP TABLE IF EXISTS preprocessed_query;"

done >> temp.sql

# Runs all tests in temp.sql
cp '../../data/imdb_plain.db' '../../data/imdb.db'
../duckdb/build/release/duckdb << EOF
.open '../../data/imdb.db'
.read temp.sql
EOF

rm temp.sql
