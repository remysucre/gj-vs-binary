imdb_plain.db: import.sh import.sql schematext.sql imdb.tgz
	bash $<

imdb.tgz:
	wget -nc http://homepages.cwi.nl/~boncz/job/imdb.tgz && tar -xf imdb.tgz --keep-old-files

clean:
	rm -f *.db

clean_all:
	rm -f *.db
	rm -f *.tgz
	rm -f *.csv
	rm -f schematext.sql