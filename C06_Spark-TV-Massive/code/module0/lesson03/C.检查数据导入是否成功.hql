-- 检查数据导入是否成功
-------------------------------------------------------------------------------

-- 展示数据库中的所有表
USE user_profile;
SHOW TABLES;

-- 检查数据集是否正常
DESCRIBE mediamatch_usermsg;
SELECT count(*)
FROM mediamatch_usermsg;

DESCRIBE mediamatch_userevent;
SELECT count(*)
FROM mediamatch_userevent;

DESCRIBE mmconsume_billevents;
SELECT count(*)
FROM mmconsume_billevents;

DESCRIBE order_index_v3;
SELECT count(*)
FROM order_index_v3;

DESCRIBE media_index_3m;
SELECT count(*)
FROM media_index_3m;
