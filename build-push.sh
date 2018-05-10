#!/usr/bin/env bash

sbt clean assembly
docker build -t zifeo/dsgd:node -f dock/jvm/Dockerfile .
docker push zifeo/dsgd:node

docker build -t zifeo/dsgd:grafana -f dock/grafana/Dockerfile dock/grafana
docker push zifeo/dsgd:grafana

docker build -t zifeo/dsgd:influxdb -f dock/influxdb/Dockerfile dock/influxdb
docker push zifeo/dsgd:influxdb
