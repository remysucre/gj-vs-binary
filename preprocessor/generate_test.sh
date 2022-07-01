# Use: bash generate_test.sh <query-name>

F=$1

# Run the original benchmark query
echo "CREATE TABLE original_query AS "
cat "../../queries/join-order-benchmark/$F"

# Load the filtered tables from the preprocessed csv
for File in `ls ../../queries/join-order-benchmark-redux/data/${F%.*}/`
do
    echo "DROP TABLE IF EXISTS ${File%.*};"
    if [[ "$File" == "mi_idx"* ]]; # mi_idx are often too large for duckdb's csv parsing
    then
        echo ".mode csv"
        echo "CREATE TABLE ${File%.*} (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));"
        echo ".import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/${F%.*}/$File' ${File%.*}"
    else
        echo "CREATE TABLE ${File%.*} AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/${F%.*}/$File', header=True, delim=',', escape='\');"
    fi
done

# Run the preprocessed join query using the filtered tables
echo "CREATE TABLE preprocessed_query AS "
cat "../../queries/join-order-benchmark-redux/joins/$F"

# Get the difference between the original and preprocessed query
echo ".print 'Testing $F'"
echo "SELECT * FROM original_query EXCEPT SELECT * FROM preprocessed_query;" 
echo "DROP TABLE IF EXISTS original_query;"
echo "DROP TABLE IF EXISTS preprocessed_query;"
