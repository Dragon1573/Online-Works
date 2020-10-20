-- 数据集总记录数
USE advertisement;
SELECT COUNT(*)
FROM case_data_sample;

-- 数据集日流量
USE advertisement;
SELECT dt, COUNT(*) AS daycount
FROM case_data_sample
GROUP BY dt
ORDER BY dt;
