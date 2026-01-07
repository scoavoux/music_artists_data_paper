sudo apt install python3 python3-dev pipx postgresql
pipx install git+https://github.com/acoustid/mbslave.git
pipx ensurepath
# restart terminal
sudo service postgresql start
sudo -u postgres psql -d postgres
# \password postgres
# postgres
# postgres
# \password musicbrainz
# musicbrainz
# musicbrainz
# \q
# modify the config file mbslave.conf to decomment password, et mettre le password admin
# add ignore=medium for solving a bug https://github.com/acoustid/mbslave/issues/21
export MBSLAVE_CONFIG="/home/onyxia/work/mauvaisgenre/sql/mbslave.conf"
mbslave init --create-user --create-database
mbslave sync

