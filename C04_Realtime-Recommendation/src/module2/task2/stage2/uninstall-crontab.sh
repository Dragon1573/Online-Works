#!/usr/bin/env bash
###############################################################################
# 卸载 Linux Crontab 任务
###############################################################################

if [ -n "$(crontab -l)" ]; then
  crontab -r
  echo "定时任务已卸载！"
else
  echo "没有定时任务！"
fi
