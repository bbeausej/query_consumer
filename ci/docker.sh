#!/bin/bash -e

set -e

export DOCKER_NAMESPACE="test"

export BASE_IMAGE="query_consumer"

if [[ ${CI_JOB_MANUAL} == "true" ]]; then
  export LATEST_GIT_TAG=$(git describe --abbrev=0)
  export DOCKER_BUILD_NB=$(cat .docker_build_number || echo 0)
  export IMAGE_TAG=${IMAGE_TAG:-$LATEST_GIT_TAG-${CI_COMMIT_REF_NAME//\//-}.$DOCKER_BUILD_NB}
else
  export IMAGE_TAG=${IMAGE_TAG:-${CI_COMMIT_REF_NAME//\//-}}
fi

# Create the Docker builder
echo "Creating the Docker builder"
docker buildx create --name services-builder --driver docker-container --use --bootstrap

# Login to ECR
aws ecr get-login-password --region "us-east-1" | docker login --password-stdin --username AWS ${DOCKER_ECR}

# We build Arm and x64 images and push them to ECR and Turbulent's registry
export DOCKER_BUILD_CMD="docker buildx build --platform linux/amd64,linux/arm64 --progress plain --push"

# Build the main image
echo "Building the base image"
time ${DOCKER_BUILD_CMD} -f docker/Dockerfile \
  -t ${HARBOR_REGISTRY}/${DOCKER_NAMESPACE}/${BASE_IMAGE}:${IMAGE_TAG} \
  -t ${DOCKER_ECR}/${BASE_IMAGE}:${IMAGE_TAG} .


if [[ ${CI_JOB_MANUAL} == "true" ]]; then
  echo $((DOCKER_BUILD_NB + 1)) > .docker_build_number
fi
