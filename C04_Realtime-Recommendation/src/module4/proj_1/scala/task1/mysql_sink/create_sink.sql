DROP DATABASE IF EXISTS shop;

CREATE DATABASE shop DEFAULT CHARACTER SET utf8;

CREATE TABLE IF NOT EXISTS sale_volume_daily
(
    datetime    VARCHAR(20) NOT NULL COMMENT '日期',
    sale_volume DOUBLE      NOT NULL COMMENT '当日销售额'
)
    COMMENT '每日销售额';

CREATE TABLE IF NOT EXISTS store_sale_daily
(
    datetime VARCHAR(20) NOT NULL COMMENT '日期',
    store_id VARCHAR(20) NOT NULL COMMENT '门店编号',
    sale     DOUBLE      NOT NULL COMMENT '销售额'
)
    COMMENT '门店每日销售额';

CREATE TABLE IF NOT EXISTS visit_count_daily
(
    datetime    VARCHAR(20) NOT NULL COMMENT '日期',
    visit_count DOUBLE      NOT NULL COMMENT '当日用户访问量'
)
    COMMENT '每日用户访问流量';


