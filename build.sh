#!/bin/bash

# Build the Docker image using the directory name as the image name
echo "Building the Docker image..."
docker build -t $(basename "$(pwd)") -f Dockerfile .

# Run the container with bind mount
#echo "Running the container..."
docker run -it --mount type=bind,source="$(pwd)",target=/workspace $(basename "$(pwd)") /bin/bash