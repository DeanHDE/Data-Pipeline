#!/bin/bash

# Build the Docker image using the directory name as the image name
echo "Building the Docker image..."
docker build -t $(basename "$(pwd)") -f Dockerfile .

