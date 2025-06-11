#!/bin/bash

# For Debian/Ubuntu-based images
apt-get update
apt-get install -y postgresql
service postgresql start

# Initialize the database if not exists
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = 'postgres_db'" | grep -q 1 || sudo -u postgres psql -c "CREATE DATABASE postgres_db;"
# Initialize the user if not exists
sudo -u postgres psql -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$(whoami)') THEN CREATE ROLE \"$(whoami)\" WITH SUPERUSER LOGIN; END IF; END \$\$;"

