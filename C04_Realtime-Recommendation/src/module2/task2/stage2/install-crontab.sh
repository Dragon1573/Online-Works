#!/usr/bin/env bash
###############################################################################
# 激活 Linux 定时任务
###############################################################################

# 清空目录
if [ -d "/tmp/flink-datasets" ]; then
    rm -rvf "/tmp/flink-datasets/*"
fi
if [ ! -d "/tmp/flink-datasets" ]; then
  mkdir -v "/tmp/flink-datasets"
fi

# 将记录生成脚本添加到 Linux 定时任务中，每一分钟导出一个文件
if [ -f "/root/Downloads/flink.cron" ]; then
    crontab "/root/Downloads/flink.cron"
    echo "定时任务已安装！"
fi
