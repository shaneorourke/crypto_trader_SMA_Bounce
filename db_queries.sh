#!/bin/bash
PATH=$(dirname "$0")

cd $PATH &&
source trader/bin/activate &&
python db_queries.py