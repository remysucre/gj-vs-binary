# Use: bash run.sh <queries> <database>
# Example: bash run.sh join-order-benchmark imdb

original_queries="../queries/original/$1"
preprocessed_queries="../queries/preprocessed/$1"

mkdir $preprocessed_queries/filters
mkdir $preprocessed_queries/joins
mkdir $preprocessed_queries/data

# Run every query through the preprocessor to get its separated filters and joins
for query in `ls $original_queries -I fkindexes.sql -I schema.sql -I README.md`
do
	./target/release/preprocessor $original_queries/$query filters "${query%.*}" > $preprocessed_queries/filters/$query
	./target/release/preprocessor $original_queries/$query joins "${query%.*}" > $preprocessed_queries/joins/$query
done

cd $preprocessed_queries
bash generate_filter_tables.sh $2
