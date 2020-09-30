#!/usr/bin/env zsh

hdfs dfs -ls /user/hive/warehouse/law.db | grep 'law_visit_log_all'
