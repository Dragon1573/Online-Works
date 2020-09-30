# 通过Sqoop进行数据迁移
sqoop import --connect jdbc:mysql://master:3306/law --driver com.mysql.jdbc.Driver \
  --username root --password 123456 --table law_visit_log_all \
  --hive-import --hive-database law --hive-table law_visit_log_all --delete-target-dir \
  --split-by timestamps --m 20 --direct
