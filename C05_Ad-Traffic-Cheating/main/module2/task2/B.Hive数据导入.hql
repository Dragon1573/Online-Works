-- 切换到数据库
USE advertisement;

-- 如果数据表存在，则移除数据表
DROP TABLE IF EXISTS case_data_sample;

-- 创建Hive表
CREATE TABLE case_data_sample
(
    rank         INT,
    dt           INT,
    cookie       STRING,
    ip           STRING,
    idfa         STRING,
    imei         STRING,
    android      STRING,
    openudid     STRING,
    mac          STRING,
    timestamps   INT,
    camp         INT,
    creativeid   INT,
    mobile_os    INT,
    mobile_type  STRING,
    app_key_md5  STRING,
    app_name_md5 STRING,
    placementid  STRING,
    useragent    STRING,
    mediaid      STRING,
    os_type      STRING,
    born_time    INT,
    label        INT
) ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH
    SERDEPROPERTIES
        ("separatorChar" = ",","quotechar" = "\"")
    STORED AS TEXTFILE;

-- 导入数据到case_data_sample_tmp表
LOAD DATA LOCAL INPATH '/root/Downloads/case_data_new.csv' INTO TABLE case_data_sample;
