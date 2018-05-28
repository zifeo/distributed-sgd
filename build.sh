#!/usr/bin/env bash

sbt clean assembly
docker build -t zifeo/dsgd:node -f kube/jvm/Dockerfile .
docker push zifeo/dsgd:node

#docker build -t zifeo/dsgd:grafana -f kube/grafana/Dockerfile kube/grafana
#docker push zifeo/dsgd:grafana
