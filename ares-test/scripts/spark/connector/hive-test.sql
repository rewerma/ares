-- Spark 集群已整合 Hive。USING hive 会创建视图，直接查询 Hive 表。
CREATE TABLE test1
USING hive
OPTIONS (
    'table_name'='default.t_user',
    'type' = 'source'
);

CREATE TABLE test2
USING hive
OPTIONS (
    'table_name'='default.t_user4',
    'type' = 'sink,source'
);

truncate table test2;
insert into test2 select id, name, c_time from test1;
select * from test2;
