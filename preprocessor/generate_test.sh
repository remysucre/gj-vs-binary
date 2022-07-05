# Use: bash generate_test.sh <query-name>

F=$1

# Run the original benchmark query
echo "CREATE TABLE original_query AS "
cat "../../queries/join-order-benchmark/$F"

# Load the filtered tables from the preprocessed parquet
for File in `ls ../../queries/join-order-benchmark-redux/data/${F%.*}/`
do
    echo "DROP TABLE IF EXISTS ${File%.*};"
    echo "CREATE TABLE ${File%.*} AS SELECT * FROM '../../queries/join-order-benchmark-redux/data/${F%.*}/$File';"
done

# Run the preprocessed join query using the filtered tables
echo "CREATE TABLE preprocessed_query AS "
cat "../../queries/join-order-benchmark-redux/joins/$F"

# Get the difference between the original and preprocessed query
echo ".print 'Testing $F'"
echo "SELECT * FROM original_query EXCEPT SELECT * FROM preprocessed_query;" 
echo "DROP TABLE IF EXISTS original_query;"
echo "DROP TABLE IF EXISTS preprocessed_query;"
