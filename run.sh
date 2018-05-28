#!/usr/bin/env bash

CONFIG="kube/config-sync.yaml"

function help() {
    echo "Usage: ./run.sh"
    echo "  -h --help"
    echo "  -async"
    echo "  -sync"
}

while [ "$1" != "" ]; do

    KEY=$(echo $1 | awk -F= '{print $1}')
    VALUE=$(echo $1 | awk -F= '{print $2}')

    case $KEY in
        -h | --help)
            help
            exit
            ;;
        -async)
            CONFIG="kube/config-async.yaml"
            ;;
        -sync)
            CONFIG="kube/config-sync.yaml"
            ;;
        *)
            echo "Error: unknown arg \"$KEY\""
            help
            exit 1
            ;;
    esac
    shift
done

function sigint() {
    kubectl delete -f kube/dsgd.yaml
    kubectl delete -f $CONFIG
    exit 0
}

trap sigint INT

kubectl create -f $CONFIG
kubectl create -f kube/dsgd.yaml
MASTER_POD=$(kubectl get po -l app=dsgd-master -o go-template --template '{{range .items}}{{.metadata.name}}{{end}}')
sleep 2

while true
do
    kubectl logs $MASTER_POD -f 2>/dev/null
    sleep 1
done
