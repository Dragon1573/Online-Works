#!/usr/bin/env zsh
###############################################################################
# 激活 Linux 定时任务
###############################################################################

# 清空目录
if [ -d "/tmp/flink_datasets/" ]; then
    rm -rf "/tmp/flink_datasets"
fi
mkdir "/tmp/flink_datasets/"

# 将记录生成脚本添加到 Linux 定时任务中，每一分钟导出一个文件
if [ -f "/root/Downloads/flink.cron" ]; then
    crontab "/root/Downloads/flink.cron"
fi
