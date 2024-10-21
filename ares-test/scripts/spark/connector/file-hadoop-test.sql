CREATE TABLE test1 (
    id NUMBER(10,0),
    name VARCHAR,
    c_time TIMESTAMP
)
WITH (
    'connector' = 'FileHadoop',
    'fs.defaultFS' = 'hdfs://localhost:9000',
    'path' = '/mytest/sample',
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
    'connector' = 'FileHadoop',
    'fs.defaultFS' = 'hdfs://localhost:9000',
    'path' = '/mytest/sample2',
    'file_format_type'='text',
    'delimiter' = ',',
--     'file_name_expression' = '${transactionId}_${now}',
--     'is_enable_transaction' = 'false',
    'field_delimiter' = ',',
    'type' = 'sink,source'
);

truncate table test2;
select * from test2;
insert into test2 select * from test1;
select * from test2;