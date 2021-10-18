#!/bin/sh

docker build -t orlandobrea/test-kubernetes-client .
docker push orlandobrea/test-kubernetes-client
