#!/bin/bash

# Build the Docker image using the directory name as the image name
echo "Building the Docker images FROM SCRATCH..."
docker build --no-cache -t $(basename "$(pwd)") -f Dockerfile .
docker build --no-cache -t $(basename "$(pwd)") -f Dockerfile.spark .
docker build --no-cache -t $(basename "$(pwd)") -f Dockerfile.airflow .

