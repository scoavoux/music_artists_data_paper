BASE_DIR := $(HOME)/work/music_artists_data_paper/musicbrainz_sql
OUT_DIR := $(BASE_DIR)/output

SQL_FILES := $(wildcard $(BASE_DIR)/*.sql)
CSV_FILES := $(patsubst $(BASE_DIR)/%.sql,$(OUT_DIR)/%.csv,$(SQL_FILES))

.PHONY: all
all: $(CSV_FILES)

$(OUT_DIR)/%.csv: $(BASE_DIR)/%.sql
	mbslave psql -f $< > $@
	Rscript upload_to_s3_musicbrainz.R $@
