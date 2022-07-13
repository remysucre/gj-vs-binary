for File in `ls filters`
do
    cd data
    mkdir ${File%.*}
    cd ..
	sh ./run_filter.sh "$File"
done