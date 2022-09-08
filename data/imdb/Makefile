imdb_plain.db: import.sh import.sql title.csv
	bash $<

title.csv: imdb.tgz
	cp -p $< temp.tgz && tar -xf temp.tgz && rm temp.tgz

imdb.tgz:
	wget -nc http://homepages.cwi.nl/~boncz/job/imdb.tgz

clean:
	rm -f *.db

clean_all:
	rm -f *.db
	rm -f *.tgz
	rm -f *.csv
	rm -f schematext.sql
