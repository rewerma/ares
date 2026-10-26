-- hive3 与 hive 相同，都是创建视图直接查询 Spark 集群中的 Hive 表。
CREATE TABLE test1
USING hive3
OPTIONS (
    'table_name'='default.t_user',
    'type' = 'source'
);

CREATE TABLE test2
USING hive3
OPTIONS (
    'table_name'='default.t_user4',
    'type' = 'sink,source'
);

truncate table test2;
insert into test2 select id, name, c_time from test1;
select * from test2;
