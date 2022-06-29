cp '../../data/imdb_plain.db' '../../data/imdb.db'
for File in `ls $1 -I fkindexes.sql -I schema.sql -I README.md`
do
    echo "Testing $File..."
	sh ./test.sh $File
done