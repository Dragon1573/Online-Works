#!/usr/bin/env zsh
###############################################################################
# 在 slave1 上创建 Kafka 主题
###############################################################################

# 移除已经存在的主题
kafka-topics.sh --zookeeper slave1:2181,slave2:2181,slave3:2181 --topic 'shop' --delete --if-exists
# 创建 Kafka 主题
kafka-topics.sh --create --zookeeper slave1:2181,slave2:2181,slave3:2181 \
    --partitions 1 --replication-factor 1 --topic 'shop'
