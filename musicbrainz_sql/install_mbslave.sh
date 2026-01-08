# install dependencies
sudo apt install python3 python3-dev pipx postgresql
# install mbslaves and make sure executable is in path
pipx install git+https://github.com/acoustid/mbslave.git
pipx ensurepath
# restart terminal so that $PATH updates and mbslave can be found.
# RESTART TERMINAL
# declare the path to mbslave.conf
# failing to do that generates cryptic errors.
export MBSLAVE_CONFIG="/home/onyxia/work/music_artists_data_paper/musicbrainz_sql/mbslave.conf"
# Start postgresql
sudo service postgresql start
# setup passwords
# not sure if the musicbrainz bit is necessary
sudo -u postgres psql -d postgres
# \password postgres
# postgres
# postgres
# \password musicbrainz
# musicbrainz
# musicbrainz
# \q
# Generates the postgresql dataset
mbslave init --create-user --create-database
mkdir /home/onyxia/work/music_artists_data_paper/musicbrainz_sql/output
# Updates the dataset (later)
mbslave sync

