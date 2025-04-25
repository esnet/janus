#!/bin/bash

script_dir=$(dirname $0)
cp $script_dir/db-test-sense-special.json db-test-sense.json
# python3 $script_dir/test_sense.py
pytest $script_dir/test_sense.py

