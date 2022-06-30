F=$1
rm temp/before.sql
echo "CREATE TABLE before AS " >> temp/before.sql
cat "../../queries/join-order-benchmark/$F" >> temp/before.sql

rm temp/filters.sql
for File in `ls ../../queries/join-order-benchmark-redux/data/${F%.*}/`
do
    echo "DROP TABLE IF EXISTS ${File%.*};" >> temp/filters.sql
    if [ "$File" = "mi_idx.csv" ];
    then
        echo ".mode csv" >> temp/filters.sql
        echo "CREATE TABLE mi_idx (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));" >> temp/filters.sql
        echo ".import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/${F%.*}/mi_idx.csv' mi_idx" >> temp/filters.sql
    else
        echo "CREATE TABLE ${File%.*} AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/${F%.*}/$File', header=True, delim=',', escape='\');" >> temp/filters.sql
    fi
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
DROP TABLE IF EXISTS after;
DROP TABLE IF EXISTS before;
EOF
