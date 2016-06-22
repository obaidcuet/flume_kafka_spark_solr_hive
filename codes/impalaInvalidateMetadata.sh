#!/bin/bash

. ~/.bash_profile
impala-shell -i hadoopedgenode:impalaport -d prod_cel_fct_raw_data -q "invalidate metadata"

