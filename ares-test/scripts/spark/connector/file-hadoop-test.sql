CREATE TABLE test1
USING FileHadoop
OPTIONS (
    'fs.defaultFS' = 'hdfs://localhost:9000',
    'path' = '/mytest/sample',
    'file_format_type'='text',
    'delimiter' = ',',
    'schema' = '{"columns":[{"name":"id","type":"decimal(10,0)"},{"name":"name","type":"string"},{"name":"c_time","type":"timestamp"}]}',
    'type' = 'source'
);

CREATE TABLE test2
USING FileHadoop
OPTIONS (
    'fs.defaultFS' = 'hdfs://localhost:9000',
    'path' = '/mytest/sample2',
    'file_format_type'='text',
    'delimiter' = ',',
--     'file_name_expression' = '${transactionId}_${now}',
--     'is_enable_transaction' = 'false',
    'field_delimiter' = ',',
    'schema' = '{"columns":[{"name":"id","type":"decimal(10,0)"},{"name":"name","type":"string"},{"name":"c_time","type":"timestamp"}]}',
    'type' = 'sink,source'
);

truncate table test2;
select * from test2;
insert into test2 select * from test1;
select * from test2;
