CREATE TABLE test1 (
    id NUMBER(10,0),
    name VARCHAR,
    c_time TIMESTAMP
)
WITH (
    'connector' = 'FileLocal',
    'path' = '/Users/rewerma/Develop/git_aliyun/ares/scripts',
    'file_format_type'='text',
    'delimiter' = ',',
    'type' = 'source'
);

CREATE TABLE test2
(
    id NUMBER(10,0),
    name VARCHAR,
    c_time TIMESTAMP
)
WITH (
    'connector' = 'FileLocal',
    'path' = '/Users/rewerma/Develop/git_aliyun/ares/scripts2',
    'file_format_type'='text',
    'delimiter' = ',',
--     'file_name_expression' = '${transactionId}_${now}',
--     'is_enable_transaction' = 'false',
    'field_delimiter' = ',',
    'type' = 'sink,source'
);

insert into test2 select * from test1;
-- update test2 a, test1 b set a.name = b.name where a.id = b.id;
truncate table test2;
-- select * from test2;
