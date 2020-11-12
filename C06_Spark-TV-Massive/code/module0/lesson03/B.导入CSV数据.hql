-- 从本地文件系统中载入 CSV 数据集至 Hive 数据库
-------------------------------------------------------------------------------

-- 切换至默认数据库
USE default;

-- 创建数据表
CREATE DATABASE IF NOT EXISTS user_profile;
-- 切换至项目数据库
USE user_profile;

-- 载入【用户基本信息表】
DROP TABLE IF EXISTS mediamatch_usermsg;
CREATE TABLE mediamatch_usermsg
(
    terminal_no STRING,
    phone_no    STRING,
    sm_name     STRING,
    run_name    STRING,
    sm_code     STRING,
    owner_name  STRING,
    owner_code  STRING,
    run_time    STRING,
    addressoj   STRING,
    open_time   STRING,
    force       STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\073";
LOAD DATA LOCAL INPATH '/root/Downloads/C06/datasets/mediamatch_usermsg.csv' OVERWRITE INTO TABLE mediamatch_usermsg;

-- 载入【用户状态信息变更表】
DROP TABLE IF EXISTS mediamatch_userevent;
CREATE TABLE mediamatch_userevent
(
    phone_no   STRING,
    run_name   STRING,
    run_time   STRING,
    owner_name STRING,
    owner_code STRING,
    open_time  STRING,
    sm_name    STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\073";
LOAD DATA LOCAL INPATH '/root/Downloads/C06/datasets/mediamatch_userevent.csv' OVERWRITE INTO TABLE mediamatch_userevent;

-- 载入【账单信息表】
DROP TABLE IF EXISTS mmconsume_billevents;
CREATE TABLE mmconsume_billevents
(
    terminal_no STRING,
    phone_no    STRING,
    fee_code    STRING,
    year_month  STRING,
    owner_name  STRING,
    owner_code  STRING,
    sm_name     STRING,
    should_pay  DOUBLE,
    favour_fee  DOUBLE
) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\073";
LOAD DATA LOCAL INPATH '/root/Downloads/C06/datasets/mmconsume_billevents.csv' OVERWRITE INTO TABLE mmconsume_billevents;

-- 载入【订单信息表】
DROP TABLE IF EXISTS order_index_v3;
CREATE TABLE order_index_v3
(
    phone_no      STRING,
    owner_name    STRING,
    optdate       STRING,
    prodname      STRING,
    sm_name       STRING,
    offerid       INT,
    offername     STRING,
    business_name STRING,
    owner_code    STRING,
    prodprcid     INT,
    prodprcname   STRING,
    effdate       STRING,
    expdate       STRING,
    orderdate     STRING,
    cost          STRING,
    mode_time     STRING,
    prodstatus    STRING,
    run_name      STRING,
    orderno       STRING,
    offertype     STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\073";
LOAD DATA LOCAL INPATH '/root/Downloads/C06/datasets/order_index.csv' OVERWRITE INTO TABLE order_index_v3;

-- 载入【用户收视行为信息表】
DROP TABLE IF EXISTS media_index_3m;
CREATE TABLE media_index_3m
(
    terminal_no   STRING,
    phone_no      STRING,
    duration      BIGINT,
    station_name  STRING,
    origin_time   STRING,
    end_time      STRING,
    owner_code    STRING,
    owner_name    STRING,
    vod_cat_tags  STRING,
    resolution    STRING,
    audio_lang    STRING,
    region        STRING,
    res_name      STRING,
    res_type      INT,
    vod_title     STRING,
    category_name STRING,
    program_title STRING,
    sm_name       STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\073";
LOAD DATA LOCAL INPATH '/root/Downloads/C06/datasets/media_index.csv' OVERWRITE INTO TABLE media_index_3m;
