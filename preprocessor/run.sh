# Use: bash run.sh <path-to-original-queries> <path-to-preprocessed-queries>
# Example: bash run.sh ../queries/join-order-benchmark ../queries/join-order-benchmark-redux

original_queries=$1
preprocessed_queries=$2

# Run every query through the preprocessor to get its separated filters and joins
for query in `ls $original_queries -I fkindexes.sql -I schema.sql -I README.md`
do
	./target/release/preprocessor $original_queries/$query filters "${query%.*}" > $preprocessed_queries/filters/$query
	./target/release/preprocessor $original_queries/$query joins "${query%.*}" > $preprocessed_queries/joins/$query
done
