#!/usr/bin/env zsh
###############################################################################
# 在 slave1 上创建 Kafka 消费者
###############################################################################

# 创建 Kafka 消费者
kafka-console-consumer.sh --topic 'shop' --bootstrap-server slave1:9092,slave2:9092,slave3:9092 --from-beginning
