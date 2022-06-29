for File in `ls $1 -I fkindexes.sql -I schema.sql -I README.md`
do
	./target/release/preprocessor $1/$File filters > $2/filters/$File
	./target/release/preprocessor $1/$File joins > $2/joins/$File
done

# Usage: bash normalize.sh <old dir> <new dir>
# Winston's example using his file-schema: bash normalize.sh ../../queries/join-order-benchmark/ ../../queries/join-order-benchmark-redux/