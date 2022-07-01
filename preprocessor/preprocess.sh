# Use: bash preprocess.sh <path-to-job-queries> <path-to-job-redux-folder>

# Run every query through the preprocessor to get its separated filters and joins
for File in `ls $1 -I fkindexes.sql -I schema.sql -I README.md`
do
	./target/release/preprocessor $1/$File filters "${File%.*}" > $2/filters/$File
	./target/release/preprocessor $1/$File joins "${File%.*}" > $2/joins/$File
done
