for File in `ls $1 -I fkindexes.sql -I schema.sql -I README.md`
do
    cp '../../data/imdb_plain.db' '../../data/imdb.db'
    echo "Testing $File..."
	sh ./test.sh $File
done