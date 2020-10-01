#!/usr/bin/env zsh
###############################################################################
# 在 master 上启动 Flume-NG
###############################################################################

flume-ng agent -c $FLUME_HOME/conf/ -f $FLUME_HOME/conf/spool-memory-kafka.conf \
    -n "client" -Dflume.root.logger=INFO,console
