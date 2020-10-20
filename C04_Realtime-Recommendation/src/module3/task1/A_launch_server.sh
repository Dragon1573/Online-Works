#!/usr/bin/env zsh
###############################################################################
# 在 master 上启动 Zookeeper 与 Kafka
###############################################################################

# 启动 Zookeeper
service keeper.service start
# 启动 Kafka
service kafka.service start
