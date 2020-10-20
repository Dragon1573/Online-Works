#!/usr/bin/env zsh
###############################################################################
# 数据导入前的预处理操作
###############################################################################

# 将数据集上传到集群主节点（在 IDEA Powershell 中执行）
scp resources/case_data_new.csv root@master:~/Downloads/

# 剔除 CSV 数据的标题行（在 master 上执行）
sed -i '1d' Downloads/case_data_new.csv

# 按顺序启动 Apache Hive 服务和 HiveServer2 远程登陆服务
service hadoop.service start
service metastore.service start
service hiveserver2.service start
