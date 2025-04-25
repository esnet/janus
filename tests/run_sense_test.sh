#!/bin/bash

# export KUBECONFIG=/root/.kube/config
# ls -l $KUBECONFIG

script_dir=$(dirname $0)
echo $script_dir
ls -l $script_dir/db-test-sense.json
ls -l $script_dir/janus-sense-test.conf

cd $script_dir

python3 test_sense.py
