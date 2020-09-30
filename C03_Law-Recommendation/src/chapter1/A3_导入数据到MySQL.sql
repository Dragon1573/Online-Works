-- 移除已经存在的数据库
DROP DATABASE IF EXISTS law;
-- 新建数据库
CREATE DATABASE law;
-- 切换到数据库
USE law;
-- 关闭日志
SET sql_log_bin = OFF;
-- 关闭事务自动提交
SET autocommit = 0;
-- 指定字段编码方式
SET NAMES utf8;
-- 载入SQL脚本文件
SOURCE /opt/lawtime_one.sql;
-- 重命名表
RENAME TABLE lawtime_one TO law_visit_log_all;
