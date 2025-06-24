#!/bin/bash

# Build the Docker image using the directory name as the image name
echo "Building the Docker images..."
docker build -t $(basename "$(pwd)") -f Dockerfile .
docker build -t $(basename "$(pwd)") -f Dockerfile.spark .
docker build -t $(basename "$(pwd)") -f Dockerfile.airflow .

