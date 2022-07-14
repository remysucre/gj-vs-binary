download_imdb_files:
	cd data/imdb && wget -nc http://homepages.cwi.nl/~boncz/job/imdb.tgz
	cd data/imdb && tar -xf imdb.tgz --keep-old-files

import_imdb_database:
	cd data/imdb && bash import.sh

clean_imdb_files:
	cd data/imdb && rm -f imdb.tgz
	cd data/imdb && rm -f *.csv
	cd data/imdb && rm -f schematext.sql
