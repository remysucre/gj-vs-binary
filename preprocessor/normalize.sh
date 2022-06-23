for File in `ls $1`
do
	./target/release/preprocessor $1/$File > $2/$File
done

# Usage: bash normalize.sh <old dir> <new dir>
# Winston's example using his file-schema: bash normalize.sh ../../queries/join-order-benchmark/ ../../queries/join-order-benchmark-preprocessed/