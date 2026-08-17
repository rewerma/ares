CREATE TABLE test1
WITH (
    'connector' = 'hive3',
    'metastore_uri' = 'thrift://localhost:9083',
    'table_name'='default.t_user',
    'type' = 'source'
);

CREATE TABLE test2
WITH (
    'connector' = 'hive3',
    'metastore_uri' = 'thrift://localhost:9083',
    'table_name'='default.t_user4',
    'type' = 'sink,source'
);

truncate table test2;
insert into test2 select id, name, c_time from test1;
select * from test2;
