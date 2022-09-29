for File in $(ls *sql)
do
	echo "PRAGMA profiling_output='./${File%.*}.json';"
	cat $File
done
