SQL_DIR := sql
OUT_DIR := sql_output

SQL_FILES := $(wildcard $(SQL_DIR)/*.sql)
CSV_FILES := $(patsubst $(SQL_DIR)/%.sql,$(OUT_DIR)/%.csv,$(SQL_FILES))

.PHONY: all
all: $(CSV_FILES)

sql_output/musicbrainz_area.csv: sql/musicbrainz_area.sql
	mbslave psql --superuser -f $(HOME)/work/music_artists_data_paper/sql/musicbrainz_area.sql
	sudo mv /tmp/musicbrainz_area.csv sql_output/musicbrainz_area.csv

sql_output/musicbrainz_artist_end_date.csv: sql/musicbrainz_artist_end_date.sql
	mbslave psql --superuser -f $(HOME)/work/music_artists_data_paper/sql/musicbrainz_artist_end_date.sql
	sudo mv /tmp/musicbrainz_artist_end_date.csv sql_output/musicbrainz_artist_end_date.csv

sql_output/musicbrainz_releases.csv: sql/musicbrainz_releases.sql
	mbslave psql --superuser -f $(HOME)/work/music_artists_data_paper/sql/musicbrainz_releases.sql
	sudo mv /tmp/musicbrainz_releases.csv sql_output/musicbrainz_releases.csv

sql_output/musicbrainz_urls.csv: sql/musicbrainz_urls.sql
	mbslave psql --superuser -f $(HOME)/work/music_artists_data_paper/sql/musicbrainz_urls.sql
	sudo mv /tmp/musicbrainz_urls.csv sql_output/musicbrainz_urls.csv
