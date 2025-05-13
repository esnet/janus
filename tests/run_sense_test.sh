#!/bin/bash

script_dir=$(dirname $0)
cp $script_dir/db-test-sense-special.json db-test-sense.json
# python3 $script_dir/test_sense.py
# pytest $script_dir/test_sense.py
pip install --no-cache-dir --break-system-packages --ignore-requires-python --use-pep517 coverage
# coverage run -m pytest --junitxml=test-results/junit.xml $script_dir/test_sense.py 
coverage run -m pytest --junitxml=test-results/junit.xml $script/test_sense_host_networking.py $script_dir/test_sense.py 
coverage report
coverage html -d test-results

