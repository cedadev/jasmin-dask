# jasmin-dask

This repository provides the Docker image and Helm chart for deploying Dask on JASMIN.

## Building the Docker image

```bash
IMAGE="cedadev/dask"
TAG="$(git describe --long --tags --always --dirty)"
docker build -t ${IMAGE}:${TAG} docker

# Optionally tag as latest
docker tag ${IMAGE}:${TAG} ${IMAGE}:latest

# Optionally push to Docker Hub
docker push ${IMAGE}:${TAG}
docker push ${IMAGE}:latest
```
