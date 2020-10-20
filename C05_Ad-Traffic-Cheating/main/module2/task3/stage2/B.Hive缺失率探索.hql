-- 统计缺失率
USE advertisement;
SELECT sum(CASE WHEN dt = '' THEN 1 ELSE 0 END) / 1704154 * 100           AS dt_null_count,
       sum(CASE WHEN cookie = '' THEN 1 ELSE 0 END) / 1704154 * 100       AS cookie_null_count,
       sum(CASE WHEN ip = '' THEN 1 ELSE 0 END) / 1704154 * 100           AS ip_null_count,
       sum(CASE WHEN idfa = '' THEN 1 ELSE 0 END) / 1704154 * 100         AS idfa_null_count,
       sum(CASE WHEN imei = '' THEN 1 ELSE 0 END) / 1704154 * 100         AS imei_null_count,
       sum(CASE WHEN android = '' THEN 1 ELSE 0 END) / 1704154 * 100      AS android_null_count,
       sum(CASE WHEN openudid = '' THEN 1 ELSE 0 END) / 1704154 * 100     AS openudid_null_count,
       sum(CASE WHEN mac = '' THEN 1 ELSE 0 END) / 1704154 * 100          AS mac_null_count,
       sum(CASE WHEN timestamps = '' THEN 1 ELSE 0 END) / 1704154 * 100   AS timestamps_null_count,
       sum(CASE WHEN camp = '' THEN 1 ELSE 0 END) / 1704154 * 100         AS camp_null_count,
       sum(CASE WHEN creativeid = 0 THEN 1 ELSE 0 END) / 1704154 * 100    AS creativeid_null_count,
       sum(CASE WHEN mobile_os IS NULL THEN 1 ELSE 0 END) / 1704154 * 100 AS mobile_os_null_count,
       sum(CASE WHEN mobile_type = '' THEN 1 ELSE 0 END) / 1704154 * 100  AS mobile_type_null_count,
       sum(CASE WHEN app_key_md5 = '' THEN 1 ELSE 0 END) / 1704154 * 100  AS app_key_md5_null_count,
       sum(CASE WHEN app_name_md5 = '' THEN 1 ELSE 0 END) / 1704154 * 100 AS app_name_md5_null_count,
       sum(CASE WHEN placementid = '' THEN 1 ELSE 0 END) / 1704154 * 100  AS placementid_null_count,
       sum(CASE WHEN useragent = '' THEN 1 ELSE 0 END) / 1704154 * 100    AS useragent_null_count,
       sum(CASE WHEN mediaid = '' THEN 1 ELSE 0 END) / 1704154 * 100      AS mediaid_null_count,
       sum(CASE WHEN os_type = '' THEN 1 ELSE 0 END) / 1704154 * 100      AS os_type_null_count,
       sum(CASE WHEN born_time = '' THEN 1 ELSE 0 END) / 1704154 * 100    AS born_time_null_count
FROM case_data_sample;
