#!/bin/bash

# This makes sure the script is run on zsh
chsh -s /bin/zsh
# This script installs PostgreSQL on a macOS system using Homebrew.
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# password will be requested during the installation

brew install postgresql@17

brew services start postgresql@17

# Add to shell config for future sessions (avoid duplicate lines)
if ! grep -q 'export PATH="/usr/local/opt/postgresql@17/bin:$PATH"' ~/.bashrc; then
  echo 'export PATH="/usr/local/opt/postgresql@17/bin:$PATH"' >> ~/.bashrc
fi
if ! grep -q 'export PATH="/usr/local/opt/postgresql@17/bin:$PATH"' ~/.zshrc; then
  echo 'export PATH="/usr/local/opt/postgresql@17/bin:$PATH"' >> ~/.zshrc
fi


export PATH="/usr/local/opt/postgresql@17/bin:$PATH"
# Initialize the database if not exists
psql -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'postgres_db'" | grep -q 1 || psql -d postgres -c "CREATE DATABASE postgres_db;"
# Create a user with the same name as the current user
# Initialize the user if not exists
psql -d postgres -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$(whoami)') THEN CREATE ROLE \"$(whoami)\" WITH SUPERUSER LOGIN; END IF; END \$\$;"
