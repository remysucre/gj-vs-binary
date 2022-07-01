F=$1
echo "CREATE TABLE before AS " >> temp.sql
cat "../../queries/join-order-benchmark/$F" >> temp.sql

for File in `ls ../../queries/join-order-benchmark-redux/data/${F%.*}/`
do
    echo "DROP TABLE IF EXISTS ${File%.*};" >> temp.sql
    if [[ "$File" == "mi_idx"* ]];
    then
        echo ".mode csv" >> temp.sql
        echo "CREATE TABLE ${File%.*} (id integer NOT NULL PRIMARY KEY, movie_id integer NOT NULL, info_type_id integer NOT NULL, info character varying NOT NULL, note character varying(1));" >> temp.sql
        echo ".import --csv --skip 1 '../../queries/join-order-benchmark-redux/data/${F%.*}/$File' ${File%.*}" >> temp.sql
    else
        echo "CREATE TABLE ${File%.*} AS SELECT * FROM read_csv_auto('../../queries/join-order-benchmark-redux/data/${F%.*}/$File', header=True, delim=',', escape='\');" >> temp.sql
    fi
done

echo "CREATE TABLE after AS " >> temp.sql
cat "../../queries/join-order-benchmark-redux/joins/$F" >> temp.sql

echo ".print 'Testing $F'" >> temp.sql
echo "SELECT * FROM before EXCEPT SELECT * FROM after;" >> temp.sql 
echo "DROP TABLE IF EXISTS before;" >> temp.sql 
echo "DROP TABLE IF EXISTS after;" >> temp.sql 
