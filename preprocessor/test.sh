F=$1
rm temp/before.sql
echo "CREATE TABLE before AS " >> temp/before.sql
cat "../../queries/join-order-benchmark/$F" >> temp/before.sql

rm temp/filters.sql
for File in `ls ../../queries/join-order-benchmark-redux/data/${F%.*}/`
do
    echo "DROP TABLE ${File%.*};" >> temp/filters.sql
    echo "CREATE TABLE ${File%.*} AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/${F%.*}/$File', header=True, delim=',', escape='\');" >> temp/filters.sql
done

rm temp/join.sql
echo "CREATE TABLE after AS " >> temp/join.sql
cat "../../queries/join-order-benchmark-redux/joins/$F" >> temp/join.sql

../../duckdb/build/release/duckdb <<EOF
.open '../../data/imdb.db'
.read 'temp/before.sql'
.read 'temp/filters.sql'
.read 'temp/join.sql'
SELECT * FROM before EXCEPT SELECT * FROM after;
EOF
