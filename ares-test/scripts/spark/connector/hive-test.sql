CREATE TABLE test1
WITH (
    'connector' = 'hive',
    'metastore_uri' = 'thrift://localhost:9083',
    'table_name'='default.t_user',
    'type' = 'source'
);

CREATE TABLE test2
WITH (
    'connector' = 'hive',
    'metastore_uri' = 'thrift://localhost:9083',
    'table_name'='default.t_user4',
--     'read_partitions' = '["c_time=20210102", "v_group=abc"]',
    'type' = 'sink,source'
);


truncate table test2;
insert into test2 select  id, name, c_time from test1;
select * from tEst2;